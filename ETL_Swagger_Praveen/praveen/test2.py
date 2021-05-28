from os import sys
from flask import Flask
from flask import request
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
SparkContext.setSystemProperty("hive.metastore.uris", "thrift://10.3.2.20:9083")
sparkSession = (SparkSession.builder.appName('pyspark-to-load-tables-hive').enableHiveSupport().getOrCreate())
spark = SparkSession.builder.appName('changeColNames').getOrCreate()

selectSql = 'SELECT * FROM etl.feedcontrol' + ' WHERE feedname' + " ='" + 'Bank' + "'"
feed = sparkSession.sql(selectSql)
feed.show()
