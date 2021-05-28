# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.test import BaseTestCase


class TestMaintainanceController(BaseTestCase):
    """MaintainanceController integration test stubs"""

    def test_get_feed_names(self):
        """Test case for get_feed_names

        Displays list of ingestion rules / feed names
        """
        response = self.client.open(
            '/v2/rules/list',
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
