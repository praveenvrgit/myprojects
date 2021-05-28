# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.test import BaseTestCase


class TestTCSUtilsController(BaseTestCase):
    """TCSUtilsController integration test stubs"""

    def test_predictive_analysis(self):
        """Test case for predictive_analysis

        Predictive Analysis Usecase for Telcom & Manufacturing industry
        """
        response = self.client.open(
            '/v2/utils/predictiveAnalysis'.format(feedname='feedname_example'),
            method='POST',
            content_type='application/x-www-form-urlencoded')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
