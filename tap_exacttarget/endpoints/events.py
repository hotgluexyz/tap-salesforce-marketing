import copy
import time

import FuelSDK
import singer
from concurrent.futures import ThreadPoolExecutor, as_completed

from tap_exacttarget.client import get_auth_stub, request
from tap_exacttarget.dao import DataAccessObject, exacttarget_error_handling
from tap_exacttarget.pagination import before_now, get_date_page, increment_date
from tap_exacttarget.state import get_last_record_value_for_table, incorporate, save_state

LOGGER = singer.get_logger()


def _iter_date_windows(start, unit):
    window_start = start
    window_end = increment_date(window_start, unit)
    while before_now(window_start):
        yield window_start, window_end
        window_start = window_end
        window_end = increment_date(window_start, unit)


@exacttarget_error_handling
def _fetch_window_events(auth_stub, event_name, selector, window_start, unit, batch_size):
    window_end = increment_date(window_start, unit)
    search_filter = get_date_page('EventDate', window_start, unit)
    started = time.perf_counter()

    events = list(request(
        event_name,
        selector,
        auth_stub,
        search_filter,
        batch_size=batch_size,
    ))

    LOGGER.info(
        "Fetched %s window %s to %s in %.2fs (%s records)",
        event_name,
        window_start,
        window_end,
        time.perf_counter() - started,
        len(events),
    )

    return window_start, events


class EventDataAccessObject(DataAccessObject):

    TABLE = 'event'
    KEY_PROPERTIES = ['SendID', 'EventType', 'SubscriberKey', 'EventDate']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['EventDate']

    def _auth_stub_pool(self, size):
        pool = getattr(self, '_auth_stubs', None)
        if pool is None:
            pool = []
            self._auth_stubs = pool

        while len(pool) < size:
            LOGGER.info(
                "Creating auth stub %s of %s for concurrent event sync",
                len(pool) + 1,
                size,
            )
            pool.append(get_auth_stub(self.config))

        return pool[:size]

    def _process_window_events(self, event_name, table, catalog_copy, window_start, raw_events):
        for event in raw_events:
            event = self.filter_keys_and_parse(event)

            self.state = incorporate(
                self.state,
                event_name,
                'EventDate',
                event.get('EventDate'),
            )

            if event.get('SubscriberKey') is None:
                LOGGER.info(
                    "SubscriberKey is NULL so ignoring {} record with SendID: {} and EventDate: {}"
                    .format(event_name, event.get('SendID'), event.get('EventDate'))
                )
                continue

            self.write_records_with_transform(event, catalog_copy, table)

        self.state = incorporate(self.state, event_name, 'EventDate', window_start)
        save_state(self.state)

    def _process_window_batch(self, event_name, selector, table, catalog_copy, batch, unit, executor):
        auth_stubs = self._auth_stub_pool(len(batch))
        batch_started = time.perf_counter()

        LOGGER.info(
            "Fetching %s %s windows concurrently from %s to %s",
            len(batch),
            event_name,
            batch[0][0],
            batch[-1][0],
        )

        futures = [
            executor.submit(
                _fetch_window_events,
                auth_stubs[index],
                event_name,
                selector,
                window_start,
                unit,
                self.batch_size,
            )
            for index, (window_start, _window_end) in enumerate(batch)
        ]

        results_by_start = {}
        next_index = 0

        for future in as_completed(futures):
            window_start, raw_events = future.result()
            results_by_start[window_start] = raw_events

            while next_index < len(batch):
                expected_start = batch[next_index][0]
                if expected_start not in results_by_start:
                    break

                self._process_window_events(
                    event_name,
                    table,
                    catalog_copy,
                    expected_start,
                    results_by_start.pop(expected_start),
                )
                next_index += 1

        LOGGER.info(
            "Completed batch of %s %s windows in %.2fs",
            len(batch),
            event_name,
            time.perf_counter() - batch_started,
        )

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        endpoints = {
            'click': FuelSDK.ET_ClickEvent,
            'open': FuelSDK.ET_OpenEvent,
            'bounce': FuelSDK.ET_BounceEvent,
            'unsub': FuelSDK.ET_UnsubEvent,
        }
        concurrency = int(self.config.get('pagination__event_concurrency', 5))
        catalog_copy = copy.deepcopy(self.catalog)

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            for event_name, selector in endpoints.items():
                start = get_last_record_value_for_table(self.state, event_name, self.config)

                if start is None:
                    raise RuntimeError('start_date not defined!')

                pagination_unit = self.config.get(
                    'pagination__{}_interval_unit'.format(event_name), 'minutes')
                pagination_quantity = self.config.get(
                    'pagination__{}_interval_quantity'.format(event_name), 10)

                unit = {pagination_unit: int(pagination_quantity)}
                batch = []

                for window in _iter_date_windows(start, unit):
                    batch.append(window)

                    if len(batch) >= concurrency:
                        self._process_window_batch(
                            event_name,
                            selector,
                            table,
                            catalog_copy,
                            batch,
                            unit,
                            executor,
                        )
                        batch = []

                if batch:
                    self._process_window_batch(
                        event_name,
                        selector,
                        table,
                        catalog_copy,
                        batch,
                        unit,
                        executor,
                    )
