#Imports
from flask import Flask
from flask import request
from pyspark.sql import SparkSession
import os
from os import sys
from flask import Flask
from flask import request
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
SparkContext.setSystemProperty("hive.metastore.uris", "thrift://10.3.2.20:9083")
sparkSession = (SparkSession.builder.appName('pyspark-to-load-tables-hive').enableHiveSupport().getOrCreate())
spark = SparkSession.builder.appName('changeColNames').getOrCreate()

app = Flask(__name__)


@app.route("/load", methods=['POST'])
def FileIngestion():
        data = request.get_json()
        print(data)
        selectSql = 'SELECT * FROM etl.feedcontrol' + ' WHERE feedname' + " ='" + data['feedname'] + "'"
        feed = sparkSession.sql(selectSql)
        feed.show()
        return data

#Test Server
@app.route("/get", methods=['GET'])
def testGet():
        return "Get works"

if __name__ == "__main__":
    app.run(host="10.3.2.16",port="7777")

