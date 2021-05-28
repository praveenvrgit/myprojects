#!/usr/bin/env python
# coding: utf-8

# In[55]:


import sys
import logging
import json
import os, time  #import statements
from datetime import datetime
import mysql.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from stat import *
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.types import StringType
import pyspark.sql.functions as f

os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"
startTime = datetime.now()

import datetime
ts = time.time()
currentTimeStamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

logging.basicConfig(filename='Ingestion_'+currentTimeStamp+'.log', filemode='w',format='%(asctime)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logging.info('Job Started : %s',currentTimeStamp)
SparkContext.setSystemProperty("hive.metastore.uris", "thrift://10.3.2.20:9083")
sparkSession = (SparkSession.builder.appName('pyspark-to-load-tables-hive').enableHiveSupport().getOrCreate())
spark = SparkSession.builder.appName('changeColNames').getOrCreate()
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
logging.info('User logged in and created SparkContext and SparkSession')


# In[56]:


feedname = sys.argv[1]
jsonpath = "/home/etl/ETL/mysqlDetails.json"
data = pd.read_json(jsonpath,typ='series',orient='columns')
DB_name= data.DB_name
user=data.user
password=data.password
host=data.host
feedControlquery= 'SELECT * FROM etl.feedcontrol WHERE feedname' + " = '" + feedname + "'"

conn = mysql.connector.connect(
     host=host,
     database=DB_name,
     user=user,
     password=password) 
feedcontrol = pd.read_sql(feedControlquery, conn)
feed = spark.createDataFrame(feedcontrol)

calenderquery = 'SELECT * FROM etl.calender where openindicator ="Y" '
calender_pd = pd.read_sql(calenderquery, conn)
calender = spark.createDataFrame(calender_pd)

filemetadataquery = 'SELECT * FROM etl.filemetadata' + ' WHERE feedname' + " ='" + feedname + "'"
filemetadata_pd = pd.read_sql(filemetadataquery, conn)
filemetadata = spark.createDataFrame(filemetadata_pd)
    
    





try:
    df_ATracker = sparkSession.sql('SELECT max(trackingid) as attendence_id FROM ETL.Attendence_Tracker order by attendence_id ')  #Attendence_tracker
    if(df_ATracker.collect()[0]['attendence_id']== None):
       attendence_id = 1
    else:
       attendence_id = int(df_ATracker.collect()[0]['attendence_id']) 
       attendence_id+=1 
    logging.info('value of attendence_id is %s', attendence_id)
    print(df_ATracker.collect()[0]['attendence_id'])
    print(attendence_id)
except Exception as e:
    logging.error('Error occured while creating dataframe for attendence_tarcker tables', exc_info=True)
feedname = feed.collect()[0]['feedname']
print(feedname)
filename = feed.collect()[0]['filename']
print(filename)
landingpath = feed.collect()[0]['landingpath']
print(landingpath)
sourceTableName = feed.collect()[0]['sourcetablename']
print(sourceTableName)
header_trailer_flag = feed.collect()[0]['headertrailerflag']
print(header_trailer_flag)
fileformat = feed.collect()[0]['dataformat']
print(fileformat)
rawzonepath = feed.collect()[0]['rawzonepath']
print(rawzonepath)
processType = feed.collect()[0]['processtype']
print(processType)
busdate_calender = calender.collect()[0]['busdate']
print(busdate_calender)
if(fileformat == 'DELIMITED' or fileformat == 'FIXED'):
    filedelimiter = filemetadata.collect()[0]['filedelimiter']
    path = '/home/etl/ETL/inputdata/WA_Fn-UseC_-Telco-Customer-Churn.csv' #str("%s/%s" % (landingpath, filename))
    print(path)
else:
    path = str("%s/%s%s%s" % (landingpath, filename,".",fileformat))
    print(path)
if(fileformat == 'database'):
    jsonpath = "/home/etl/ETL/mysqlDetails.json"
    ArrivalTimeStamp=currentTimeStamp
    FileSize = 0
    logging.info('ArrivalTimestamp for MysqlDB fileformat %s',ArrivalTimeStamp)
else:     #extracting arrival time stamp of file from system
    ArrivalTimeStamp = time.ctime(os.path.getctime(path))
    st = os.stat(path)
    FileSize = st[ST_SIZE]
rawzonepath = 'hdfs://10.3.2.13:8020/user/hadoop/rawzone/WA_Fn-UseC_-Telco-Customer-Churn.csv' #str("%s%s" % (rawzonepath, filename))
print("rawpath:%s " %rawzonepath)

    


# In[59]:


def WithHeaderTrailer(attendence_id,feedname, filename, path, filedelimiter, ArrivalTimeStamp, FileSize, busdateposition, numofrowspos, busdate_calender,processType,fileformat,headerIdentifier,detailIdentifier,trailerIdentifier):
        logging.info('Job is in function WithHeaderTrailer')
        lines = sc.textFile(path)
        H = lines.filter(lambda l: l.startswith(headerIdentifier)) 
        H.collect()
        header = H.take(1)  #extracting header data
        Header = ''.join(header)    
        Date = H.map(lambda l: l.split(filedelimiter)[busdateposition-1]) #extracting business date
        DateOfExtract = Date.collect().pop(0)
        Detail = lines.filter(lambda l: l.startswith(detailIdentifier))  #separating detail data
        Detail.collect()
        Detail_Count = Detail.count() #counting no of rows
        print(Detail_Count)
        T = lines.filter(lambda l: l.startswith(trailerIdentifier))  #separating trailer data
        T.collect()
        trailer = T.take(1)  #extracting trailer data
        Trailer = ''.join(trailer)
        NOR = T.map(lambda l: l.split(filedelimiter)[numofrowspos-1]) #extracting date of number of rows
        NoOfRecords = int(NOR.collect().pop(0))
        print(NoOfRecords) #validating date of extract
        if DateOfExtract == busdate_calender:
            HeaderVldFlag = 'Y'
            ErrorCodeList = 'No Error'
        else:
            HeaderVldFlag = 'N'
            ErrorCodeList = 'Invalid Busdate'
        if NoOfRecords == Detail_Count: 
    #validating number of rows
            TrailerVldFlag = 'Y'
            ErrorCodeList = 'No Error'
        else:
            TrailerVldFlag = 'N'
            ErrorCodeList = 'Invalid NoOfRecords'          
        
        
        dataframe = spark.createDataFrame(Detail, StringType())
        data = dataframe.replace("\"","")                   #.map( lambda elem: elem.strip('\"'))
        data.show()
        data.write.format('csv').save('/home/etl/ETL/dataframecheck')
        #Ingestion Into Rawzone
        #rawzonelocation = IngestionIntoRawzone(attendence_id,processType,fileformat,DateOfExtract,dataframe)
        #attendence tracker table
        #print(rawzonelocation)
        #Attendence_tracking(attendence_id, feedname, filename, path, ArrivalTimeStamp, currentTimeStamp,FileSize, DateOfExtract,rawzonelocation, Header, Trailer, HeaderVldFlag, TrailerVldFlag, ErrorCodeList)
        #logging.info('Data has been entered into Attendence_tracker')
            
            
    
            
    


# In[ ]:





# In[60]:


def WithOutHeaderTrailer(attendence_id,feedname, filename, path,filedelimiter, ArrivalTimeStamp, FileSize,busdate_calender,processType):
        logging.info('Job is in function WithOutHeaderTrailer')
        dataframe = spark.read.load('/home/etl/ETL/inputdata/WA_Fn-UseC_-Telco-Customer-Churn.csv', format='csv', sep=filedelimiter)
        #Ingestion Into Rawzone
        rawzonelocation = IngestionIntoRawzone(attendence_id,processType,fileformat,busdate_calender,dataframe)
        #attendence tracker table
        logging.info(rawzonelocation)
        Attendence_tracking(attendence_id,feedname, filename, path, ArrivalTimeStamp,currentTimeStamp, FileSize, busdate_calender,rawzonelocation,"null","null","null","null","null")
        logging.info('Data has been entered into Attendence_tracker')
            
            
         
            
    


# In[61]:


def MySqlDB(attendence_id,feedname, filename, jsonpath,processType,busdate_calender):
    logging.info('Job is in function MySqlDB')
    data = pd.read_json(jsonpath,typ='series',orient='columns')
    DB_name= data.DB_name
    user=data.user
    password=data.password
    host=data.host
    query=data.query
    conn = mysql.connector.connect(
         host=host,
         database=DB_name,
         user=user,
         password=password)
    #attendence tracker table 
    pd_df = pd.read_sql(query, conn)
    dataframe = spark.createDataFrame(pd_df)
    dataframe.show()
    rawzonelocation = IngestionIntoRawzone(attendence_id,processType,fileformat,busdate_calender,dataframe)
    print(rawzonelocation)
    Attendence_tracking(attendence_id,feedname, filename, "", ArrivalTimeStamp,currentTimeStamp, FileSize, busdate_calender,rawzonelocation,"","","","","")    
        

 


# In[62]:


def Attendence_tracking(attendence_id, feedname, filename, path, ArrivalTimeStamp, currentTimeStamp,FileSize, DateOfExtract,rawzonepath, Header, Trailer, HeaderVldFlag, TrailerVldFlag, ErrorCodeList):
    sparkSession.sql("insert into table etl.attendence_tracker select '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'".format(attendence_id, feedname, filename, path, ArrivalTimeStamp, currentTimeStamp,FileSize, DateOfExtract,rawzonepath, Header, Trailer, HeaderVldFlag, TrailerVldFlag, ErrorCodeList))

def IngestionIntoRawzone(attendence_id,processType,fileformat,busdate_calender,dataframe):
    try:
        dataframe = dataframe.withColumn('attendence_id',f.lit(attendence_id))
        #Date_date=datetime.datetime.strptime(busdate_calender, '%d/%m/%Y')
        #print(busdate_calender)
        Date=busdate_calender
        print(Date)
        df = dataframe.withColumn('BusDate',f.lit(Date))
        if(fileformat == 'DELIMITED'):
            df.write.format('csv').partitionBy('BusDate','attendence_id').option('delimiter', filedelimiter).save(rawzonepath, mode='append')
            partitionAlterSql = 'ALTER TABLE etl.' + sourceTableName + ' ADD IF NOT EXISTS PARTITION (' + 'BusDate' + "='" + str(Date) + "'," + 'attendence_id' + "=" + str(attendence_id) +") LOCATION '" + rawzonepath + '/' + 'BusDate' + "=" + str(Date) + '/' + 'attendence_id' + "=" + str(attendence_id) +"'"
            logging.info(partitionAlterSql)
            sparkSession.sql(partitionAlterSql)
        elif(fileformat == 'database'):
            df.write.format('parquet').partitionBy('BusDate','attendence_id').save(rawzonepath,mode='append')
            partitionAlterSql = 'ALTER TABLE etl.' + sourceTableName + ' ADD IF NOT EXISTS PARTITION (' + 'BusDate' + "=" + str(Date) + "," + 'attendence_id' + "=" + str(attendence_id) +") LOCATION '" + rawzonepath + '/' + 'BusDate' + "=" + str(Date) + '/' + 'attendence_id' + "=" + str(attendence_id) +"'"
            logging.info(partitionAlterSql)
        elif(fileformat == 'json' or fileformat == 'avro' or fileformat == 'parquet' or fileformat == 'orc'):
                    df.write.format(fileformat).partitionBy('BusDate','attendence_id').option('delimiter', filedelimiter).save(rawzonepath,mode='append')
                    partitionAlterSql = 'ALTER TABLE etl.' + sourceTableName + ' ADD IF NOT EXISTS PARTITION (' + 'BusDate' + "=" + str(Date) + "," + 'attendence_id' + "=" + str(attendence_id) +") LOCATION '" + rawzonepath + '/' + 'BusDate' + "=" + str(Date) + '/' + 'attendence_id' + "=" + str(attendence_id) +"'"
                    print(partitionAlterSql)
        rawzonelocation = rawzonepath + '/' + 'BusDate' + "=" + str(Date) + '/' + 'attendence_id' + "=" + str(attendence_id)
        logging.info('path to rawzone with partitions : %s',rawzonelocation)
        return rawzonelocation
        
    except Exception as e:
        logging.error('Error occured while in function IngestionIntoRawzone', exc_info=True)


# In[63]:


logging.info('fileformat check')
if(fileformat == 'database'):
    try:
        logging.info('header_trailer_flag is N and fileformat is %s , hence calling MySqlDB',fileformat)
        MySqlDB(attendence_id,feedname, filename, jsonpath,processType,busdate_calender) 
        logging.info('Job is SUCCESS')
        from datetime import datetime
        logging.info('Time taken is: %s',datetime.now() - startTime) 
    except Exception as e:
        logging.error('Error occured while in function MySqlDB', exc_info=True) 
        logging.info('Job is Failed')
        from datetime import datetime
        logging.info('Time taken is: %s',datetime.now() - startTime)
elif(fileformat == "DELIMITED"):
    logging.info('Job is at delimiter check')
    if(header_trailer_flag == 'Y'):
        busdateposition = filemetadata.collect()[0]['busdateposition']
        busdateposition= int(busdateposition)
        numofrows = filemetadata.collect()[0]['trailercountpos']
        numofrows = int(numofrows)
        headerIdentifier = filemetadata.collect()[0]['headeridentifier']
        detailIdentifier = filemetadata.collect()[0]['detailidentifier']
        tailerIdentifier = filemetadata.collect()[0]['traileridentifier']
        WithHeaderTrailer(attendence_id,feedname, filename, path, filedelimiter, ArrivalTimeStamp, FileSize, busdateposition, numofrows, busdate_calender,processType,fileformat,hId,dId,tId)
    elif(header_trailer_flag == 'N'):
        WithOutHeaderTrailer(attendence_id,feedname, filename, path,filedelimiter, ArrivalTimeStamp, FileSize,busdate_calender,processType)   

elif(fileformat == 'json' or fileformat == 'avro' or fileformat == 'parquet' or fileformat == 'orc'):
    try:
        logging.info('header_trailer_flag is N and fileformat is %s , hence calling WithOutHeaderTrailer',fileformat)
        WithOutHeaderTrailer(attendence_id,feedname, filename, path, filetype, filedelimiter, ArrivalTimeStamp, FileSize, busdate_calender,processType,fileformat)
    except Exception as e:
        logging.error('Error occured while in function WithOutHeaderTrailer', exc_info=True)    






