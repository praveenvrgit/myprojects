# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.models.update_rule import UpdateRule  # noqa: E501
from swagger_server.test import BaseTestCase


class TestMaintainanceController(BaseTestCase):
    """MaintainanceController integration test stubs"""

    def test_delete_feed_names(self):
        """Test case for delete_feed_names

        Deletes a ingestion rule
        """
        response = self.client.open(
            '/v2/rules/{feedname}'.format(feedname='feedname_example'),
            method='DELETE')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_get_feed_names_details(self):
        """Test case for get_feed_names_details

        Displays information on ingestion rule
        """
        response = self.client.open(
            '/v2/rules/{feedname}'.format(feedname='feedname_example'),
            method='GET')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_update_feed_names(self):
        """Test case for update_feed_names

        Update ingestion rules / feed names
        """
        body = UpdateRule()
        response = self.client.open(
            '/v2/rules/update',
            method='PUT',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
