import connexion
import six

from swagger_server.models.update_rule import UpdateRule  # noqa: E501
from swagger_server import util


def delete_feed_names(feedname):  # noqa: E501
    """Deletes a ingestion rule

     # noqa: E501

    :param feedname: Name of ingestion rule
    :type feedname: str

    :rtype: None
    """
    return 'do some magic!'


def get_feed_names_details(feedname):  # noqa: E501
    """Displays information on ingestion rule

     # noqa: E501

    :param feedname: Name of ingest rule
    :type feedname: str

    :rtype: None
    """
    import json
    import pandas as pd
    import mysql.connector
    from mysql.connector import Error
    from mysql.connector import errorcode
    
    connection = mysql.connector.connect(host='10.3.2.13',
                             database='etl',
                             user='etlmysql',
                             password='tata@123')
    df = pd.read_sql("SELECT * FROM etl.feedcontrol where feedname = 'TEst3'  ", connection)
    df1=df.head()
    return df1.to_json()


def update_feed_names(body):  # noqa: E501
    """Update ingestion rules / feed names

    Update list of ingestion rules / feed names # noqa: E501

    :param body: New KAFKA data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = UpdateRule.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
