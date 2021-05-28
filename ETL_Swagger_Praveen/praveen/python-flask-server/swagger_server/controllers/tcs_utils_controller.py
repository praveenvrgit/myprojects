import connexion
import six
import flask
from swagger_server import util
import subprocess
from connexion import NoContent
from flask import render_template, send_file
import zipfile
import io
import pathlib
import os
from os.path import basename

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
    retVal=400
  else:
    print(s_output)
    retVal=401
  return retVal

def predictiveAnalysis():  # noqa: E501
    """Predictive Analysis Usecase for Telcom &amp; Manufacturing industry

     # noqa: E501

    :param feedname: Feedname for which data ingestion needs to be triggered
    :type feedname: str

    :rtype: None
    """
    command = "python3 /home/etl/ETL/churn.py "
    ret_val = run_cmd(command)
    #return 'Churn Prediction process completed', ret_val
    #return send_file('/home/etl/ETL/graphs/report.pdf', attachment_filename='report.pdf',as_attachment=True)
    #return send_file('/home/etl/ETL/graphs/image1.html', attachment_filename='image1.html',as_attachment=True)
    base_path = pathlib.Path('/home/etl/ETL/graphs/')
    data = io.BytesIO()
    with zipfile.ZipFile(data, mode='w') as z:
        for f_name in base_path.iterdir():
                z.write(f_name, basename(f_name))
    data.seek(0)
    return send_file(data,mimetype='application/zip',as_attachment=True,attachment_filename='data.zip')
