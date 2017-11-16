import os
import sys
os.environ["PYTHONPATH"] = "/home/ubuntu/CMM-CoreStrategy-dev/"
sys.path.append('/home/ubuntu/CMM-CoreStrategy-dev')
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
# from CRUD.ods.ods_configs import get_script_configs


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@airflow.com'],
    'start_date': datetime(2015, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'custom': {"name": "kamilia"}
}

exm_dag = DAG('set_env_var', default_args=default_args, schedule_interval='@daily')

accounts = ['ATKN', 'NICK']

for account in accounts:
    os.environ["Var_{}".format(account)] = account
    get_env_var = BashOperator(
        task_id='get_env_var_{}'.format(account),
        bash_command="echo $Var_{}".format(account),
        dag=exm_dag)


