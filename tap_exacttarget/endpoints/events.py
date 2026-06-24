import copy
from datetime import datetime, timezone

import singer

from tap_exacttarget.dao import DataAccessObject, exacttarget_error_handling
from tap_exacttarget.sfmc_events_client import (
    EVENT_TYPE_DEFAULTS,
    parse_event_types,
    sync_events,
)
from tap_exacttarget.state import incorporate, save_state, get_last_record_value_for_table


LOGGER = singer.get_logger()


class EventDataAccessObject(DataAccessObject):

    TABLE = 'event'
    KEY_PROPERTIES = ['SendID', 'EventType', 'SubscriberKey', 'EventDate']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['EventDate']

    def filter_keys_and_parse(self, obj):
        return self.parse_object(obj)

    def _normalize_record(self, event_key, row):
        record = dict(row)

        if not record.get('EventType'):
            record['EventType'] = EVENT_TYPE_DEFAULTS.get(event_key, event_key)

        for int_field in ('SendID', 'BatchID'):
            value = record.get(int_field)
            if value is not None and value != '':
                try:
                    record[int_field] = int(value)
                except (TypeError, ValueError):
                    pass

        return record

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        event_types = parse_event_types(self.config)

        if not self.config.get('sub_domain'):
            raise RuntimeError(
                'sub_domain is required for the event stream '
                '(direct SOAP retrieval uses OAuth2 tenant endpoints).'
            )

        catalog_copy = copy.deepcopy(self.catalog)
        end = datetime.now(timezone.utc)
        event_ranges = []

        for event_key in event_types:
            start = get_last_record_value_for_table(self.state, event_key, self.config)
            if start is None:
                raise RuntimeError('start_date not defined!')
            event_ranges.append((event_key, start, end))
            LOGGER.info(
                "Queued %s from %s to %s",
                event_key,
                start,
                end.isoformat(),
            )

        LOGGER.info(
            "Fetching event types %s (chunk_hours=%s, concurrency=%s, min_chunk_minutes=%s)",
            ", ".join(event_types),
            self.config.get('events_chunk_hours', 6),
            self.config.get('events_concurrency', 20),
            self.config.get('events_min_chunk_minutes', 30),
        )

        def on_record(event_key, row):
            record = self._normalize_record(event_key, row)

            self.state = incorporate(
                self.state,
                event_key,
                'EventDate',
                record.get('EventDate'),
            )

            if record.get('SubscriberKey') is None:
                LOGGER.info(
                    "SubscriberKey is NULL so ignoring %s record with SendID: %s and EventDate: %s",
                    event_key,
                    record.get('SendID'),
                    record.get('EventDate'),
                )
                return

            self.write_records_with_transform(record, catalog_copy, table)

        sync_events(self.config, event_ranges, on_record)

        end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
        for event_key in event_types:
            self.state = incorporate(self.state, event_key, 'EventDate', end_str)

        save_state(self.state)

        LOGGER.info(
            "Completed event sync for types: %s",
            ", ".join(event_types),
        )
