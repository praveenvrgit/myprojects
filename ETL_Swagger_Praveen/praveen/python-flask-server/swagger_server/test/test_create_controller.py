# coding: utf-8

from __future__ import absolute_import

from flask import json
from six import BytesIO

from swagger_server.models.avro_file import AvroFile  # noqa: E501
from swagger_server.models.delimited_file import DelimitedFile  # noqa: E501
from swagger_server.models.fixed_file import FixedFile  # noqa: E501
from swagger_server.models.kafka import Kafka  # noqa: E501
from swagger_server.models.my_sql import MySQL  # noqa: E501
from swagger_server.test import BaseTestCase


class TestCreateController(BaseTestCase):
    """CreateController integration test stubs"""

    def test_create_new_avro_file_rules(self):
        """Test case for create_new_avro_file_rules

        Create New rules for AVRO File data ingestion
        """
        body = AvroFile()
        response = self.client.open(
            '/v2/create/avro',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_new_delimited_file_rules(self):
        """Test case for create_new_delimited_file_rules

        Create New rules for Delimited File data ingestion
        """
        body = DelimitedFile()
        response = self.client.open(
            '/v2/create/delimitedFlatFile',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_new_fixed_file_rules(self):
        """Test case for create_new_fixed_file_rules

        Create New rules for Fixed Width File data ingestion
        """
        body = FixedFile()
        response = self.client.open(
            '/v2/create/fixedWidthFlatFile',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_new_ingestion_rules(self):
        """Test case for create_new_ingestion_rules

        Create New rules for KAFKA data ingestion
        """
        body = Kafka()
        response = self.client.open(
            '/v2/create/kafka',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_new_json_file_rules(self):
        """Test case for create_new_json_file_rules

        Create New rules for JSON File data ingestion
        """
        body = AvroFile()
        response = self.client.open(
            '/v2/create/json',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_new_my_sql_rules(self):
        """Test case for create_new_my_sql_rules

        Create New rules for MYSQL data ingestion
        """
        body = MySQL()
        response = self.client.open(
            '/v2/create/mySQL',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_new_orc_file_rules(self):
        """Test case for create_new_orc_file_rules

        Create New rules for ORC File data ingestion
        """
        body = AvroFile()
        response = self.client.open(
            '/v2/create/orc',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))

    def test_create_new_parquet_file_rules(self):
        """Test case for create_new_parquet_file_rules

        Create New rules for PARQUET File data ingestion
        """
        body = AvroFile()
        response = self.client.open(
            '/v2/create/parquet',
            method='POST',
            data=json.dumps(body),
            content_type='application/json')
        self.assert200(response,
                       'Response body is : ' + response.data.decode('utf-8'))


if __name__ == '__main__':
    import unittest
    unittest.main()
