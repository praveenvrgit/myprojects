#Imports
from flask import Flask
from flask import request
from pyspark.sql import SparkSession
import subprocess
app = Flask(__name__)
def run_cmd(args_list):
  """
  run linux commands
  """
  temp = args_list.split(" ")
  temp = [ i for i in temp if i!='']
  print('Running system command   :  {0}'.format(' '.join(temp)))
  proc = subprocess.Popen(temp, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  s_output, s_err = proc.communicate()
  s_return =  proc.returncode
  if s_return == 0:
    print ("Command executed successfully ")
  else:
    print(s_output)
  return s_return, s_output, s_err


@app.route("/load", methods=['POST'])
def FileIngestion():
        data = request.get_json()
        print(data)
        command = "python3 /home/etl/praveen/test2.py"
        ret , out , err = run_cmd(command)
        return data

#Test Server
@app.route("/get", methods=['GET'])
def testGet():
        return "Get works"

if __name__ == "__main__":
    app.run(host="10.3.2.16",port="7777")

