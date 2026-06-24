import unittest
import tap_exacttarget
from tap_exacttarget.pagination import get_date_page, increment_date


class TestPagination(unittest.TestCase):

    def test_increment_date(self):
        self.assertEqual(
            increment_date("2015-09-28T10:05:53Z"),
            "2015-09-29T10:05:53Z")
        self.assertEqual(
            increment_date("2015-09-28T10:05:53Z", {'hours': 1}),
            "2015-09-28T11:05:53Z")

    def test_get_date_page_uses_half_open_interval(self):
        page_filter = get_date_page('EventDate', '2026-01-01T00:00:00Z', {'minutes': 30})

        self.assertEqual(page_filter['LogicalOperator'], 'AND')
        self.assertEqual(
            page_filter['LeftOperand'],
            {
                'Property': 'EventDate',
                'SimpleOperator': 'greaterThanOrEqual',
                'Value': '2026-01-01T00:00:00Z',
            },
        )
        self.assertEqual(
            page_filter['RightOperand'],
            {
                'Property': 'EventDate',
                'SimpleOperator': 'lessThan',
                'Value': '2026-01-01T00:30:00Z',
            },
        )
