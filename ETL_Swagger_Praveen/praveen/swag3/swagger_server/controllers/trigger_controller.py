import connexion
import six

from swagger_server import util
import subprocess
from connexion import NoContent

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


def trigger_ingestion(feedname):  # noqa: E501
    """Trigger a data ingestion rule

     # noqa: E501

    :param feedname: Feedname for which data ingestion needs to be triggered
    :type feedname: str

    :rtype: None
    """
    command = "python3 /home/etl/ETL/Trail_code/ETL_Ingestion.py " + feedname
    ret_val = run_cmd(command)
    return 'data ingestion completed', ret_val
