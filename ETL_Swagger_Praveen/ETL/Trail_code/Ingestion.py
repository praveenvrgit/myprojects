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


startTime = datetime.now()

import datetime
ts = time.time()
currentTimeStamp= datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

logging.basicConfig(filename='Ingestion_'+currentTimeStamp+'.log', filemode='w',format='%(asctime)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logging.info('Job Started : %s',currentTimeStamp)
SparkContext.setSystemProperty("hive.metastore.uris", "thrift://10.100.101.30:9083")
sparkSession = (SparkSession.builder.appName('pyspark-to-load-tables-hive').enableHiveSupport().getOrCreate())
spark = SparkSession.builder.appName('changeColNames').getOrCreate()
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
logging.info('User logged in and created SparkContext and SparkSession')


feedname = sys.argv[1]
filename = sys.argv[2]
logging.info('Feteched command line arguments %s and %s',feedname,filename)
try:
    selectSql = 'SELECT * FROM etl.feedcontrol' + ' WHERE feedname' + " ='" + feedname + "' AND filename" + " ='" + filename +"'"
    logging.info('%s',selectSql)
    feed = sparkSession.sql(selectSql)
    feed.show()
    cal = sparkSession.sql('SELECT * FROM etl.calender where openindicator ="Y" ')  #calender table
    cal.show()
    filemetadata_selectSql = 'SELECT * FROM etl.filemetadata' + ' WHERE feedname' + " ='" + feedname + "' AND filename" + " ='" + filename +"'"
    filemetadata = sparkSession.sql(filemetadata_selectSql)
    filemetadata.show()
    try:
        df_ATracker = sparkSession.sql('SELECT max(attendenceid) as attendence_id FROM ETL.Attendence_Tracker order by attendence_id ')  #Attendence_tracker
        attendenceid = df_ATracker.collect()[0]['attendence_id']
        attendence_id = int(attendenceid) + 1
        logging.info('value of attendence_id is %s', attendence_id)
    except Exception as e:
        logging.error('Error occured while creating dataframe for attendence_tarcker tables', exc_info=True)
    feedname = feed.collect()[0]['feedname']
    filename = feed.collect()[0]['filename']
    landingpath = feed.collect()[0]['landingpath']
    sourceTableName = feed.collect()[0]['sourcetablename']
    header_trailer_flag = feed.collect()[0]['header_trailer_flag']
    fileformat = feed.collect()[0]['fileformat']
    rawzonepath = feed.collect()[0]['rawzonepath']
    processType = feed.collect()[0]['processtype']
    busdate_calender = cal.collect()[0]['busdate']
    if(fileformat == 'DELIMITED' or fileformat == 'FIXED'):
        filedelimiter = filemetadata.collect()[0]['filedelimiter']
    if(fileformat == 'database'):
        jsonpath = "/home/developer/Vedashree/MysqlTestTable.json"
    if(fileformat == 'DELIMITED' or 'FIXED'):
        path = str("%s/%s" % (landingpath, filename))
        print(filename)
    else:
        path = str("%s/%s%s%s" % (landingpath, filename,".",fileformat))
    rawzonepath = str("%s%s" % (rawzonepath, filename))
    print(rawzonepath)
    
    if(fileformat == 'database'):
        ArrivalTimeStamp=currentTimeStamp
        logging.info('ArrivalTimestamp for MysqlDB fileformat %s',ArrivalTimeStamp)
    else:                                                                                       #extracting arrival time stamp of file from system
        ArrivalTimeStamp = time.ctime(os.path.getctime(path))
    if(fileformat == 'database'):  #extracting file size
        FileSize = 0
    else :
        st = os.stat(path)
        FileSize = st[ST_SIZE]
except Exception as e:
    logging.info('Job is FAILED')
    logging.error('Error occured while creating dataframe or extracting data from feedcontrol and calender tables', exc_info=True)

    
def WithHeaderTrailer(attendence_id,feedname, filename, path, filedelimiter, ArrivalTimeStamp, FileSize, busdateposition, numofrowspos, busdate_calender,processType,fileformat):
        logging.info('Job is in function WithHeaderTrailer')
        lines = sc.textFile(path)
        H = lines.filter(lambda l: l.startswith('1')) 
        H.collect()
        header = H.take(1)  #extracting header data
        Header = ''.join(header)    
        Date = H.map(lambda l: l.split(filedelimiter)[busdateposition-1]) #extracting business date
        DateOfExtract = Date.collect().pop(0)
        Detail = lines.filter(lambda l: l.startswith('2'))  #separating detail data
        Detail.collect()
        Detail_Count = Detail.count() #counting no of rows
        print(Detail_Count)
        T = lines.filter(lambda l: l.startswith('3'))  #separating trailer data
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
        if NoOfRecords == Detail_Count: #validating number of rows
            TrailerVldFlag = 'Y'
            ErrorCodeList = 'No Error'
        else:
            TrailerVldFlag = 'N'
            ErrorCodeList = 'Invalid NoOfRecords'          
        
        
        dataframe = spark.createDataFrame(Detail, StringType())
        #Ingestion Into Rawzone
        IngestionIntoRawzone(attendence_id,processType,fileformat,DateOfExtract,dataframe)
        #attendence tracker table
        Attendence_tracking(attendence_id, feedname, filename, path, ArrivalTimeStamp, currentTimeStamp,FileSize, DateOfExtract,rawzonepath, Header, Trailer, HeaderVldFlag, TrailerVldFlag, ErrorCodeList, 1)
        logging.info('Data has been entered into Attendence_tracker')
            

def WithOutHeaderTrailer(attendence_id,feedname, filename, path,filedelimiter, ArrivalTimeStamp, FileSize,busdate_calender,processType):
        logging.info('Job is in function WithOutHeaderTrailer')
        dataframe = spark.read.load(path, format='csv', sep=filedelimiter)
        #Ingestion Into Rawzone
        IngestionIntoRawzone(attendence_id,processType,fileformat,busdate_calender,dataframe)
        #attendence tracker table
        Attendence_tracking(attendence_id,feedname, filename, path, ArrivalTimeStamp,currentTimeStamp, FileSize, busdate_calender,rawzonepath,"null","null","null","null","null",1)
        logging.info('Data has been entered into Attendence_tracker')
            
            
         
            
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
    IngestionIntoRawzone(attendence_id,processType,fileformat,busdate_calender,dataframe)
    Attendence_tracking(attendence_id,feedname, filename, "", ArrivalTimeStamp,currentTimeStamp, FileSize, busdate_calender,rawzonepath,"","","","","",1)    
        

def Attendence_tracking(attendence_id, feedname, filename, path, ArrivalTimeStamp, currentTimeStamp,FileSize, DateOfExtract,rawzonepath, Header, Trailer, HeaderVldFlag, TrailerVldFlag, ErrorCodeList, audit_id):
    sparkSession.sql("insert into table etl.attendence_tracker select '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}'".format(attendence_id, feedname, filename, path, ArrivalTimeStamp, currentTimeStamp,FileSize, DateOfExtract,rawzonepath, Header, Trailer, HeaderVldFlag, TrailerVldFlag, ErrorCodeList, audit_id))

def IngestionIntoRawzone(attendence_id,processType,fileformat,busdate_calender,dataframe):
    try:
        dataframe = dataframe.withColumn('attendence_id',f.lit(attendence_id))
        if(processType == 'ETL'):  
            Date_date=datetime.datetime.strptime(busdate_calender, '%d/%m/%Y')
            Date=Date_date.date()
            df = dataframe.withColumn('BusDate',f.lit(Date))
            if(fileformat == 'DELIMITED'):
                df.write.format('csv').partitionBy('attendence_id','BusDate').option('delimiter', filedelimiter).save(rawzonepath, mode='append')
                partitionAlterSql = 'ALTER TABLE etl.' + sourceTableName + ' ADD IF NOT EXISTS PARTITION (' + 'attendence_id' + "=" + str(attendence_id) + "," + 'BusDate' + "='" + str(Date) +"') LOCATION '" + rawzonepath + '/' + 'attendence_id' + "=" + str(attendence_id) + '/' + 'BusDate' + "=" + str(Date) +"'"
                logging.info(partitionAlterSql)
                sparkSession.sql(partitionAlterSql)
            elif(fileformat == 'database'):
                df.write.format('parquet').partitionBy('attendence_id','BusDate').mode("overwrite").save(rawzonepath,mode='append')
                partitionAlterSql = 'ALTER TABLE etl.' + sourceTableName + ' ADD IF NOT EXISTS PARTITION (' + 'attendence_id' + "=" + str(attendence_id) + "," + 'BusDate' + "='" + str(Date) +"') LOCATION '" + rawzonepath + '/' + 'attendence_id' + "=" + str(attendence_id) + '/' + 'BusDate' + "=" + str(Date) +"'"
                logging.info(partitionAlterSql)
            elif(fileformat == 'json' or fileformat == 'avro' or fileformat == 'parquet' or fileformat == 'orc'):
                    df.write.format(fileformat).partitionBy('attendence_id','BusDate').option('delimiter', filedelimiter).save(rawzonepath,mode='append')
                    partitionAlterSql = 'ALTER TABLE etl.' + sourceTableName + ' ADD IF NOT EXISTS PARTITION (' + 'attendence_id' + "=" + str(attendence_id) + "," + 'BusDate' + "='" + str(Date) +"') LOCATION '" + rawzonepath + '/' + 'attendence_id' + "=" + str(attendence_id) + '/' + 'BusDate' + "=" + str(Date) +"'"
                    print(partitionAlterSql)
                
        elif(processType == 'passthrough'):
            if(fileformat == 'DELIMITED'):
                dataframe.write.format('csv').partitionBy('attendence_id').option("delimiter",filedelimiter).save(rawzonepath,mode='append')
            elif(fileformat == 'database'):
                dataframe.write.format('parquet').partitionBy('attendence_id').mode("overwrite").save(rawzonepath,mode='append')
                  
            elif(fileformat == 'json' or fileformat == 'avro' or fileformat == 'parquet' or fileformat == 'orc'):
                dataframe.write.format(fileformat).partitionBy('attendence_id').mode("overwrite").option.save(rawzonepath,mode='append')
    except Exception as e:
        logging.error('Error occured while in function IngestionIntoRawzone', exc_info=True)             
        
logging.info('fileformat check')
try :
    if(fileformat == 'database'):
        try:
            logging.info('header_trailer_flag is N and fileformat is %s , hence calling MySqlDB',fileformat)
            MySqlDB(attendence_id,feedname, filename, jsonpath,processType,busdate_calender)        
        except Exception as e:
            logging.error('Error occured while in function MySqlDB', exc_info=True) 
    elif(fileformat == "DELIMITED"):
        logging.info('Job is at delimiter check')
        if(header_trailer_flag == 'Y'):
            busdateposition = filemetadata.collect()[0]['busdateposition']
            busdateposition= int(busdateposition)
            numofrows = filemetadata.collect()[0]['trailercountpos']
            numofrows = int(numofrows)
            try:
                logging.info('header_trailer_flag is Y and fileformat is %s , hence calling WithHeaderTrailer',fileformat)
                WithHeaderTrailer(attendence_id,feedname, filename, path, filedelimiter, ArrivalTimeStamp, FileSize, busdateposition, numofrows, busdate_calender,processType,fileformat)
            except Exception as e:
                logging.error('Error occured while in function WithHeaderTrailer with header_trailer_flag is N and fileformat is %s', exc_info=True)
        elif(header_trailer_flag == 'N'):
            try:
                logging.info('header_trailer_flag is N and fileformat is %s , hence calling WithOutHeaderTrailer',fileformat)
                WithOutHeaderTrailer(attendence_id,feedname, filename, path,filedelimiter, ArrivalTimeStamp, FileSize,busdate_calender,processType)
            except Exception as e:
                logging.error('Error occured while in function WithOutHeaderTrailer', exc_info=True)    
    
    elif(fileformat == 'json' or fileformat == 'avro' or fileformat == 'parquet' or fileformat == 'orc'):
        try:
            logging.info('header_trailer_flag is N and fileformat is %s , hence calling WithOutHeaderTrailer',fileformat)
            WithOutHeaderTrailer(attendence_id,feedname, filename, path, filetype, filedelimiter, ArrivalTimeStamp, FileSize, busdate_calender,processType,fileformat)
        except Exception as e:
            logging.error('Error occured while in function WithOutHeaderTrailer', exc_info=True)    
    logging.info('Job is SUCCESS')
    from datetime import datetime
    logging.info('Time taken is: %s',datetime.now() - startTime)
except Exception as e:
    logging.error('Error occured while checking the fileformat and calling the respective functions',exc_info=True)
    logging.info('Job is Failed')
    from datetime import datetime
    logging.info('Time taken is: %s',datetime.now() - startTime)