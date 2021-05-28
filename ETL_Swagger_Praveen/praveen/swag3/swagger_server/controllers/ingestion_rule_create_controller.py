import connexion
import six

from swagger_server.models.avro_file import AvroFile  # noqa: E501
from swagger_server.models.delimited_file import DelimitedFile  # noqa: E501
from swagger_server.models.fixed_file import FixedFile  # noqa: E501
from swagger_server.models.kafka import Kafka  # noqa: E501
from swagger_server.models.my_sql import MySQL  # noqa: E501
from swagger_server import util
import os

def mysqlInsert(mapperInsertSet, feedcontrolInsetSet, filemetadataInsertSet):
    import mysql.connector
    from mysql.connector import Error
    from mysql.connector import errorcode
    import pandas as pd

    try:
        connection = mysql.connector.connect(host='10.3.2.13',
                                 database='etl',
                                 user='etlmysql',
                                 password='tata@123')
        
        mycursor = connection.cursor()
        retVal=''
        
        #Mapper table inserts
        if len(mapperInsertSet) > 0 :
            sql_insert_query = """ INSERT INTO etl.mapper (feedname, key_col, value_col) 
                               VALUES (%s,%s,%s) """        
            result  = mycursor.executemany(sql_insert_query, mapperInsertSet)
            connection.commit()
            retVal=str(mycursor.rowcount) + " Records inserted successfully into etl.mapper table"
        
        #Mapper table inserts
        if len(feedcontrolInsetSet) > 0 :
            sql_insert_query = """ INSERT INTO etl.feedcontrol (feedname,filename,dataformat,landingpath,rejectthreshold,processtype,headertrailerflag,sourcetablename) 
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s) """        
            result  = mycursor.executemany(sql_insert_query, feedcontrolInsetSet)
            connection.commit()
            retVal=retVal + '\n' + str(mycursor.rowcount) + " Records inserted successfully into etl.feedcontrol table"

        #Mapper table inserts
        if len(filemetadataInsertSet) > 0 :
            sql_insert_query = """ INSERT INTO etl.filemetadata (feedname,filename,filetype,filedelimiter) 
                               VALUES (%s,%s,%s,%s) """        
            result  = mycursor.executemany(sql_insert_query, filemetadataInsertSet)
            connection.commit()
            retVal=retVal + '\n' + str(mycursor.rowcount) + " Records inserted successfully into etl.filemetadata table"
            
        return retVal
    except mysql.connector.Error as error :
        retVal ="Failed inserting records into metatdata tables" + str(error)
        return retVal
    finally:
        #closing database connection.
        if(connection.is_connected()):
            mycursor.close()
            connection.close()
            #print("connection is closed")
                
def create_new_avro_file_rules(body):  # noqa: E501
    """Create New rules for AVRO File data ingestion

     # noqa: E501

    :param body: New AVRO File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        #body = AvroFile.from_dict(connexion.request.get_json())  # noqa: E501
        data = connexion.request.get_json()
        print(data)
        mapperSet = [ ]
        filemetadataInsertSet=[]
        feedcontrolSet = [(data['feedName'], os.path.basename(data['inputFileLocation']), 'AVRO', os.path.dirname(data['inputFileLocation']), 0, '', 'N', '' )]
        retValue=mysqlInsert(mapperSet, feedcontrolSet, filemetadataInsertSet)
    else:
        print("Not a json")
    return retValue


def create_new_delimited_file_rules(body):  # noqa: E501
    """Create New rules for Delimited File data ingestion

     # noqa: E501

    :param body: New Delimited File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        #body = DelimitedFile.from_dict(connexion.request.get_json())  # noqa: E501
        data = connexion.request.get_json()
        print(data)
        mapperSet = [ ]
        filemetadataInsertSet = [(data['feedName'], os.path.basename(data['inputFileLocation']), 'DELIMITED', data['columnSeperator']  )]
        print(filemetadataInsertSet)
        feedcontrolSet = [(data['feedName'], os.path.basename(data['inputFileLocation']), 'DELIMITED', os.path.dirname(data['inputFileLocation']), 0, '', 'N', '' )]
        print(mysqlInsert(mapperSet, feedcontrolSet, filemetadataInsertSet)  )
        retValue=mysqlInsert(mapperSet, feedcontrolSet, filemetadataInsertSet)        
    return retValue


def create_new_fixed_file_rules(body):  # noqa: E501
    """Create New rules for Fixed Width File data ingestion

     # noqa: E501

    :param body: New Fixed File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        #body = FixedFile.from_dict(connexion.request.get_json())  # noqa: E501
        data = connexion.request.get_json()
        print(data)
        mapperSet = [ ]
        filemetadataInsertSet = [(data['feedName'], os.path.basename(data['inputFileLocation']), 'FIXED', ''  )]
        print(filemetadataInsertSet)
        feedcontrolSet = [(data['feedName'], os.path.basename(data['inputFileLocation']), 'FIXED', os.path.dirname(data['inputFileLocation']), 0, '', 'N', '' )]
        print(mysqlInsert(mapperSet, feedcontrolSet, filemetadataInsertSet)  )
        retValue=mysqlInsert(mapperSet, feedcontrolSet, filemetadataInsertSet)        
    return retValue


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
        #body = AvroFile.from_dict(connexion.request.get_json())  # noqa: E501
        data = connexion.request.get_json()
        print(data)
        mapperSet = [ ]
        filemetadataInsertSet = []
        feedcontrolSet = [(data['feedName'], os.path.basename(data['inputFileLocation']), 'JSON', os.path.dirname(data['inputFileLocation']), 0, '', 'N', '' )]
        retValue=mysqlInsert(mapperSet, feedcontrolSet, filemetadataInsertSet)
    return retValue


def create_new_my_sql_rules(body):  # noqa: E501
    """Create New rules for MYSQL data ingestion

     # noqa: E501

    :param body: New MYSQL data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        #body = MySQL.from_dict(connexion.request.get_json())  # noqa: E501
        data = connexion.request.get_json()
        print(data)
        mapperSet = [ (data['feedName'],'USER',data['user']) , (data['feedName'],'DBNAME',data['dbName']),(data['feedName'],'SQLQUERY',data['sqlQuery']),(data['feedName'],'PASSWORD',data['password']) ]
        feedcontrolSet = [(data['feedName'], '', 'MYSQL', '', 0, '', 'N', '' )]
        filemetadataInsertSet = []
        retValue=mysqlInsert(mapperSet, feedcontrolSet, filemetadataInsertSet)
    
    return retValue


def create_new_orc_file_rules(body):  # noqa: E501
    """Create New rules for ORC File data ingestion

     # noqa: E501

    :param body: New ORC File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        #body = AvroFile.from_dict(connexion.request.get_json())  # noqa: E501
        data = connexion.request.get_json()
        print(data)
        mapperSet = [ ]
        filemetadataInsertSet = []
        feedcontrolSet = [(data['feedName'], os.path.basename(data['inputFileLocation']), 'ORC', os.path.dirname(data['inputFileLocation']), 0, '', 'N', '' )]
        retValue=mysqlInsert(mapperSet, feedcontrolSet, filemetadataInsertSet)
    return retValue


def create_new_parquet_file_rules(body):  # noqa: E501
    """Create New rules for PARQUET File data ingestion

     # noqa: E501

    :param body: New PARQUET File data source to be configured
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        #body = AvroFile.from_dict(connexion.request.get_json())  # noqa: E501
        data = connexion.request.get_json()
        print(data)
        mapperSet = [ ]
        filemetadataInsertSet = []
        feedcontrolSet = [(data['feedName'], os.path.basename(data['inputFileLocation']), 'PARQUET', os.path.dirname(data['inputFileLocation']), 0, '', 'N', '' )]
        retValue=mysqlInsert(mapperSet, feedcontrolSet, filemetadataInsertSet)
    return retValue
