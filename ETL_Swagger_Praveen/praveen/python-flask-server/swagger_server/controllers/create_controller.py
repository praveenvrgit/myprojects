import connexion
import six

from swagger_server.models.avro_file import AvroFile  # noqa: E501
from swagger_server.models.delimited_file import DelimitedFile  # noqa: E501
from swagger_server.models.fixed_file import FixedFile  # noqa: E501
from swagger_server.models.kafka import Kafka  # noqa: E501
from swagger_server.models.my_sql import MySQL  # noqa: E501
from swagger_server import util


def create_new_avro_file_rules(body):  # noqa: E501
    """Create New rules for AVRO File data ingestion

     # noqa: E501

    :param body: New AVRO File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = AvroFile.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def create_new_delimited_file_rules(body):  # noqa: E501
    """Create New rules for Delimited File data ingestion

     # noqa: E501

    :param body: New Delimited File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = DelimitedFile.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def create_new_fixed_file_rules(body):  # noqa: E501
    """Create New rules for Fixed Width File data ingestion

     # noqa: E501

    :param body: New Fixed File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = FixedFile.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def create_new_ingestion_rules(body):  # noqa: E501
    """Create New rules for KAFKA data ingestion

     # noqa: E501

    :param body: New KAFKA data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = Kafka.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def create_new_json_file_rules(body):  # noqa: E501
    """Create New rules for JSON File data ingestion

     # noqa: E501

    :param body: New JSON File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = AvroFile.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def create_new_my_sql_rules(body):  # noqa: E501
    """Create New rules for MYSQL data ingestion

     # noqa: E501

    :param body: New MYSQL data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = MySQL.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def create_new_orc_file_rules(body):  # noqa: E501
    """Create New rules for ORC File data ingestion

     # noqa: E501

    :param body: New ORC File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = AvroFile.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'


def create_new_parquet_file_rules(body):  # noqa: E501
    """Create New rules for PARQUET File data ingestion

     # noqa: E501

    :param body: New PARQUET File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = AvroFile.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
