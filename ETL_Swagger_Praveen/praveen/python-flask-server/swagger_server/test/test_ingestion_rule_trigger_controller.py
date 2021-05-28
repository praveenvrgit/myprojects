# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.test import BaseTestCase


class TestIngestionRuleTriggerController(BaseTestCase):
    """IngestionRuleTriggerController integration test stubs"""

    def test_trigger_ingestion(self):
        """Test case for trigger_ingestion

        Trigger a data ingestion rule
        """
        response = self.client.open(
            '/v2/trigger/{feedname}'.format(feedname='feedname_example'),
            method='POST',
            content_type='application/x-www-form-urlencoded')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
