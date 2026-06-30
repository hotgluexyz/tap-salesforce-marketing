"""Direct async SOAP client for SFMC tracking events (clicks, opens, bounces, unsubs)."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET

import aiohttp

NS = "http://exacttarget.com/wsdl/partnerAPI"

RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
DEFAULT_EVENTS_CONCURRENCY = 20
DEFAULT_EVENTS_CHUNK_HOURS = 6
DEFAULT_EVENTS_MIN_CHUNK_MINUTES = 30
DEFAULT_SOAP_RETRIES = 5

LOGGER = logging.getLogger(__name__)

EVENT_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    "click": {
        "object_type": "ClickEvent",
        "date_property": "EventDate",
        "properties": [
            "BatchID",
            "CreatedDate",
            "EventDate",
            "EventType",
            "ID",
            "SendID",
            "SubscriberKey",
            "TriggeredSendDefinitionObjectID",
            "URL",
            "URLID",
        ],
    },
    "open": {
        "object_type": "OpenEvent",
        "date_property": "EventDate",
        "properties": [
            "BatchID",
            "EventDate",
            "EventType",
            "ID",
            "SendID",
            "SubscriberKey",
            "TriggeredSendDefinitionObjectID",
        ],
    },
    "bounce": {
        "object_type": "BounceEvent",
        "date_property": "EventDate",
        "properties": [
            "BatchID",
            "BounceCategory",
            "BounceType",
            "EventDate",
            "EventType",
            "ID",
            "SendID",
            "SMTPCode",
            "SubscriberKey",
            "TriggeredSendDefinitionObjectID",
        ],
    },
    "unsub": {
        "object_type": "UnsubEvent",
        "date_property": "EventDate",
        "properties": [
            "BatchID",
            "EventDate",
            "EventType",
            "ID",
            "IsMasterUnsubscribed",
            "SendID",
            "SubscriberKey",
            "TriggeredSendDefinitionObjectID",
        ],
    },
    "sent": {
        "object_type": "SentEvent",
        "date_property": "EventDate",
        "properties": [
            "BatchID",
            "EventDate",
            "EventType",
            "ID",
            "SendID",
            "SubscriberKey",
            "TriggeredSendDefinitionObjectID",
        ],
    },
}

DEFAULT_EVENT_TYPES = ("click", "open", "bounce", "unsub")

EVENT_TYPE_DEFAULTS = {
    "click": "Click",
    "open": "Open",
    "bounce": "Bounce",
    "unsub": "Unsub",
    "sent": "Sent",
}


def parse_event_types(config: Dict[str, Any]) -> List[str]:
    raw = config.get("event_types", DEFAULT_EVENT_TYPES)
    if isinstance(raw, str):
        keys = [k.strip() for k in raw.split(",") if k.strip()]
    else:
        keys = list(raw)

    unknown = [k for k in keys if k not in EVENT_DEFINITIONS]
    if unknown:
        raise ValueError(
            "Unknown event_types: {}. Valid values: {}"
            .format(", ".join(unknown), ", ".join(EVENT_DEFINITIONS))
        )
    return keys


def iter_time_chunks(
    start: datetime, end: datetime, chunk_hours: int
) -> List[Tuple[datetime, datetime]]:
    chunks: List[Tuple[datetime, datetime]] = []
    cursor = start
    delta = timedelta(hours=chunk_hours)
    while cursor < end:
        chunk_end = min(cursor + delta, end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
    return chunks


def _parse_datetime(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class SFMCClient:
    def __init__(self, config: Dict[str, Any], concurrency: int = 40) -> None:
        self.config = config
        self.concurrency = concurrency
        self._token: Optional[str] = None
        self._token_expires: float = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(concurrency)

        sub_domain = config["sub_domain"]
        self.auth_url = f"https://{sub_domain}.auth.marketingcloudapis.com/v2/token"
        self.soap_url = f"https://{sub_domain}.soap.marketingcloudapis.com/Service.asmx"

    async def __aenter__(self) -> "SFMCClient":
        timeout_seconds = float(self.config.get("request_timeout") or 120)
        timeout = aiohttp.ClientTimeout(total=timeout_seconds, connect=30)
        self._session = aiohttp.ClientSession(timeout=timeout)
        await self._ensure_token()
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._session:
            await self._session.close()

    async def _ensure_token(self) -> str:
        if self._token and time.monotonic() < self._token_expires - 60:
            return self._token

        assert self._session is not None
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }
        async with self._session.post(self.auth_url, json=payload) as resp:
            body = await resp.json()
            if resp.status != 200:
                raise RuntimeError(f"Auth failed ({resp.status}): {body}")
            self._token = body["access_token"]
            self._token_expires = time.monotonic() + int(body.get("expires_in", 1200))
            return self._token

    async def soap_call(
        self, action: str, body_inner: str, retries: int = DEFAULT_SOAP_RETRIES
    ) -> ET.Element:
        assert self._session is not None
        token = await self._ensure_token()

        envelope = f"""<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:a="http://schemas.xmlsoap.org/ws/2004/08/addressing"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <s:Header>
    <a:Action s:mustUnderstand="1">{action}</a:Action>
    <a:To s:mustUnderstand="1">{self.soap_url}</a:To>
    <fueloauth xmlns="http://exacttarget.com">{token}</fueloauth>
  </s:Header>
  <s:Body>
    {body_inner}
  </s:Body>
</s:Envelope>"""

        last_error: Optional[Exception] = None
        for attempt in range(retries):
            async with self._semaphore:
                try:
                    async with self._session.post(
                        self.soap_url,
                        data=envelope,
                        headers={
                            "Content-Type": "text/xml; charset=utf-8",
                            "SOAPAction": action,
                        },
                    ) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            error = RuntimeError(
                                f"SOAP {action} HTTP {resp.status}: {text[:500]}"
                            )
                            if (
                                resp.status in RETRYABLE_HTTP_STATUS
                                and attempt < retries - 1
                            ):
                                last_error = error
                                LOGGER.warning(
                                    "SOAP %s HTTP %s (attempt %s/%s), retrying",
                                    action,
                                    resp.status,
                                    attempt + 1,
                                    retries,
                                )
                                await asyncio.sleep(min(2 ** attempt, 30))
                                continue
                            raise error
                        root = ET.fromstring(text)
                        status_el = root.find(f".//{{{NS}}}OverallStatus")
                        status = status_el.text if status_el is not None else ""
                        if status and status != "OK" and "MoreDataAvailable" not in status:
                            if "Error" in status or status == "Error":
                                error = RuntimeError(
                                    f"SOAP {action} failed: {status} — {text[:800]}"
                                )
                                if attempt < retries - 1:
                                    last_error = error
                                    LOGGER.warning(
                                        "SOAP %s status %s (attempt %s/%s), retrying",
                                        action,
                                        status,
                                        attempt + 1,
                                        retries,
                                    )
                                    await asyncio.sleep(min(2 ** attempt, 30))
                                    continue
                                raise error
                        return root
                except (aiohttp.ClientError, asyncio.TimeoutError, ET.ParseError) as exc:
                    last_error = exc
                    if attempt < retries - 1:
                        LOGGER.warning(
                            "SOAP %s transport error (attempt %s/%s): %s",
                            action,
                            attempt + 1,
                            retries,
                            exc,
                        )
                        await asyncio.sleep(min(2 ** attempt, 30))
        raise RuntimeError(f"SOAP {action} failed after {retries} attempts: {last_error}")

    def _build_date_filter(
        self, date_property: str, start: datetime, end: datetime
    ) -> str:
        start_str = start.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = end.strftime("%Y-%m-%dT%H:%M:%S")
        return f"""
        <Filter xsi:type="ComplexFilterPart">
          <LeftOperand xsi:type="SimpleFilterPart">
            <Property>{date_property}</Property>
            <SimpleOperator>greaterThanOrEqual</SimpleOperator>
            <Value>{start_str}</Value>
          </LeftOperand>
          <LogicalOperator>AND</LogicalOperator>
          <RightOperand xsi:type="SimpleFilterPart">
            <Property>{date_property}</Property>
            <SimpleOperator>lessThan</SimpleOperator>
            <Value>{end_str}</Value>
          </RightOperand>
        </Filter>"""

    def _parse_results(
        self, root: ET.Element
    ) -> Tuple[List[Dict[str, str]], bool, Optional[str]]:
        results: List[Dict[str, str]] = []
        for result_el in root.findall(f".//{{{NS}}}Results"):
            row: Dict[str, str] = {}
            for child in result_el:
                tag = child.tag.replace(f"{{{NS}}}", "")
                if tag in ("PartnerKey", "ObjectID", "CustomerKey"):
                    continue
                row[tag] = child.text or ""
            if row:
                results.append(row)

        more_el = root.find(f".//{{{NS}}}MoreDataAvailable")
        more = more_el is not None and more_el.text == "true"

        req_el = root.find(f".//{{{NS}}}RequestID")
        request_id = req_el.text if req_el is not None else None
        return results, more, request_id

    async def retrieve_events(
        self,
        event_key: str,
        chunk_start: datetime,
        chunk_end: datetime,
    ) -> AsyncIterator[Dict[str, str]]:
        definition = EVENT_DEFINITIONS[event_key]
        object_type = definition["object_type"]
        date_property = definition["date_property"]
        properties = definition["properties"]

        props_xml = "\n".join(f"<Properties>{p}</Properties>" for p in properties)
        filter_xml = self._build_date_filter(date_property, chunk_start, chunk_end)

        body = f"""
        <RetrieveRequestMsg xmlns="http://exacttarget.com/wsdl/partnerAPI">
          <RetrieveRequest>
            <ObjectType>{object_type}</ObjectType>
            {props_xml}
            {filter_xml}
          </RetrieveRequest>
        </RetrieveRequestMsg>"""

        root = await self.soap_call("Retrieve", body)
        rows, more, request_id = self._parse_results(root)

        for row in rows:
            yield row

        while more and request_id:
            continue_body = f"""
            <RetrieveRequestMsg xmlns="http://exacttarget.com/wsdl/partnerAPI">
              <RetrieveRequest>
                <ContinueRequest>{request_id}</ContinueRequest>
              </RetrieveRequest>
            </RetrieveRequestMsg>"""
            root = await self.soap_call("Retrieve", continue_body)
            rows, more, request_id = self._parse_results(root)
            for row in rows:
                yield row


async def _fetch_chunk(
    client: SFMCClient,
    event_key: str,
    chunk_start: datetime,
    chunk_end: datetime,
    on_record: Callable[[str, Dict[str, str]], None],
    min_chunk: timedelta,
) -> None:
    try:
        async for row in client.retrieve_events(event_key, chunk_start, chunk_end):
            on_record(event_key, row)
    except Exception as exc:
        window = chunk_end - chunk_start
        if window <= min_chunk:
            raise RuntimeError(
                "Failed to fetch {} from {} to {} after splitting to {}-minute windows: {}"
                .format(
                    event_key,
                    chunk_start.isoformat(),
                    chunk_end.isoformat(),
                    int(min_chunk.total_seconds() // 60),
                    exc,
                )
            ) from exc

        midpoint = chunk_start + (window / 2)
        LOGGER.warning(
            "Retrying %s with smaller windows after error (%s → %s): %s",
            event_key,
            chunk_start.isoformat(),
            chunk_end.isoformat(),
            exc,
        )
        await _fetch_chunk(
            client, event_key, chunk_start, midpoint, on_record, min_chunk
        )
        await _fetch_chunk(
            client, event_key, midpoint, chunk_end, on_record, min_chunk
        )


async def sync_events_async(
    config: Dict[str, Any],
    event_ranges: Sequence[Tuple[str, datetime, datetime]],
    on_record: Callable[[str, Dict[str, str]], None],
) -> None:
    concurrency = int(config.get("events_concurrency", DEFAULT_EVENTS_CONCURRENCY))
    chunk_hours = int(config.get("events_chunk_hours", DEFAULT_EVENTS_CHUNK_HOURS))
    min_chunk_minutes = int(
        config.get("events_min_chunk_minutes", DEFAULT_EVENTS_MIN_CHUNK_MINUTES)
    )
    min_chunk = timedelta(minutes=min_chunk_minutes)

    tasks: List[Tuple[str, datetime, datetime]] = []
    for event_key, start, end in event_ranges:
        tasks.extend(
            (event_key, chunk_start, chunk_end)
            for chunk_start, chunk_end in iter_time_chunks(start, end, chunk_hours)
        )

    if not tasks:
        return

    LOGGER.info(
        "Starting %s event chunks (chunk_hours=%s, concurrency=%s, min_chunk_minutes=%s)",
        len(tasks),
        chunk_hours,
        concurrency,
        min_chunk_minutes,
    )

    async with SFMCClient(config, concurrency=concurrency) as client:
        await asyncio.gather(
            *[
                _fetch_chunk(
                    client, event_key, chunk_start, chunk_end, on_record, min_chunk
                )
                for event_key, chunk_start, chunk_end in tasks
            ]
        )


def sync_events(
    config: Dict[str, Any],
    event_ranges: Sequence[Tuple[str, str, datetime]],
    on_record: Callable[[str, Dict[str, str]], None],
) -> None:
    parsed_ranges = [
        (event_key, _parse_datetime(start), end)
        for event_key, start, end in event_ranges
    ]
    asyncio.run(sync_events_async(config, parsed_ranges, on_record))
