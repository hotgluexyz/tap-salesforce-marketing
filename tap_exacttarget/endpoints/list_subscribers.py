import FuelSDK
import copy
import singer

from tap_exacttarget.client import request
from tap_exacttarget.dao import (DataAccessObject, exacttarget_error_handling)
from tap_exacttarget.endpoints.subscribers import SubscriberDataAccessObject
from tap_exacttarget.pagination import get_date_page, before_now, \
    increment_date
from tap_exacttarget.state import incorporate, save_state, \
    get_last_record_value_for_table
from tap_exacttarget.util import partition_all, sudsobj_to_dict


LOGGER = singer.get_logger()


def _get_subscriber_key(list_subscriber):
    # return the 'SubscriberKey' of the subscriber
    return list_subscriber.SubscriberKey


def _get_list_subscriber_filter(_list, start):
    return {
        'Property': 'ModifiedDate',
        'SimpleOperator': 'greaterThan',
        'Value': start
    }


class ListSubscriberDataAccessObject(DataAccessObject):

    TABLE = 'list_subscriber'
    KEY_PROPERTIES = ['SubscriberKey', 'ListID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['ModifiedDate']

    def __init__(self, config, state, auth_stub, catalog):
        super().__init__(
            config, state, auth_stub, catalog)

        self.replicate_subscriber = False
        self.subscriber_catalog = None

    # error handling is not required as this function is called from
    # 'sync_data' hence it will be back-off from that function
    def _get_all_subscribers_list(self):
        """
        Find the 'All Subscribers' list via the SOAP API, and return it.
        """
        result = request('List', FuelSDK.ET_List, self.auth_stub, {
            'Property': 'ListName',
            'SimpleOperator': 'equals',
            'Value': 'All Subscribers',
        }, batch_size=self.batch_size)

        lists = list(result)

        if len(lists) != 1:
            msg = ('Found {} all subscriber lists, expected one!'
                   .format(len(lists)))
            raise RuntimeError(msg)

        return sudsobj_to_dict(lists[0])

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        subscriber_dao = SubscriberDataAccessObject(
            self.config,
            self.state,
            self.auth_stub,
            self.subscriber_catalog)

        # pass config to return start date if not bookmark is found
        start = get_last_record_value_for_table(self.state, table, self.config)

        all_subscribers_list = self._get_all_subscribers_list()

        stream = request('ListSubscriber',
                            FuelSDK.ET_List_Subscriber,
                            self.auth_stub,
                            _get_list_subscriber_filter(
                                 all_subscribers_list,
                                 start),
                             batch_size=self.batch_size)

        batch_size = 100

        if self.replicate_subscriber:
            subscriber_dao.write_schema()

        catalog_copy = copy.deepcopy(self.catalog)

        synced_subscribers_keys = set()

        for list_subscribers_batch in partition_all(stream, batch_size):
            for list_subscriber in list_subscribers_batch:
                list_subscriber = self.filter_keys_and_parse(
                    list_subscriber)

                if list_subscriber.get('ModifiedDate'):
                    self.state = incorporate(
                        self.state,
                        table,
                        'ModifiedDate',
                        list_subscriber.get('ModifiedDate'))

                self.write_records_with_transform(list_subscriber, catalog_copy, table)

            if self.replicate_subscriber:
                # make the list of subscriber keys
                subscriber_keys = list(map(
                    _get_subscriber_key, list_subscribers_batch))

                # filter out all the subscriber keys that are already in the set
                subscriber_keys = [key for key in subscriber_keys if key not in synced_subscribers_keys]

                # add the subscriber keys to the set
                synced_subscribers_keys.update(subscriber_keys)

                # pass the list of 'subscriber_keys' to fetch subscriber details
                subscriber_dao.pull_subscribers_batch(subscriber_keys)

            save_state(self.state)

