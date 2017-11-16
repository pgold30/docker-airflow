"""import bingads
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
"""
"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
import os
import sys
parent_path = "/usr/local/airflow/dags//CMM-CoreStrategy/"
os.environ["PYTHONPATH"] = parent_path
os.environ["ENV"] = "awsdev"
sys.path.append(parent_path)
import pandas as pd
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
from utils.google_drive import create_sheet
from CRUD.ods.ods_configs import get_script_configs, brand_gs_folder_check_and_create
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@airflow.com'],
    'start_date': datetime(2017, 11, 16),
    'email_on_failure': False,
    'email_on_retry': False
}

ods_dag = DAG('km_dag_test', default_args=default_args)


def create_gsheet_and_folder(client_code, brand_code, **kwargs):
    chicago = timezone('America/Chicago')
    cst_time = datetime.now(chicago)
    file_name = brand_code + '_' + str(cst_time.strftime("%Y-%m-%d_%H:%M:%S"))
    folder_id = brand_gs_folder_check_and_create("active_ads", client_code, brand_code)
    if folder_id:
        logging.info("Creat spreadsheet: {0} for brand: {1}".format(file_name, brand_code))
        spreadsheet_id = create_sheet(parent_folder=folder_id, sheet_name=file_name, tab_names=["Adwords", "BingAds"])
        logging.info("Folder/sheet creation completed.")
        kwargs['ti'].xcom_push(key='spreadsheet_id', value=spreadsheet_id)


ods_config = get_script_configs("active_ads")
brand_codes = ods_config["BrandCode"].drop_duplicates().tolist()


for brand_code in brand_codes:
    if brand_code in ['ATHL']:
        # Get brand config, client code and engines.
        task_brand_name = brand_code.replace(" ", "_").replace("(", "_").replace(")", "_").replace("'", "_")
        brand_config = ods_config.loc[(ods_config['BrandCode'] == brand_code)]
        client_code = ods_config["ClientCode"].drop_duplicates().tolist()[0]
        engines = brand_config["EngineName"].drop_duplicates().tolist()

        # Define the dynamic task ids
        create_sheet_tid = 'create_sheet_{}'.format(task_brand_name)
        send_email_tid = 'send_email_{}'.format(task_brand_name)
        extract_adwords_ad_group_tid = 'download_adwords_adgroup_{}'.format(task_brand_name)
        extract_adwords_ad_tid = 'download_adwords_ad_{}'.format(task_brand_name)
        extract_bing_tid = 'download_bing_{}'.format(task_brand_name)
        mastering_adwords_tid = 'mastering_adwords_{}'.format(task_brand_name)
        mastering_bing_tid = 'mastering_bing_{}'.format(task_brand_name)

        create_sheet = PythonOperator(
            task_id=create_sheet_tid,
            python_callable=create_gsheet_and_folder,
            op_kwargs={"client_code": client_code,
                       "brand_code": brand_code},
            dag=ods_dag)

        send_email = BashOperator(
            task_id=send_email_tid,
            bash_command="echo aaa",
            dag=ods_dag)

        google_sheet_id_xcoms = "{{ti.xcom_pull(task_ids='{}', key='spreadsheet_id')}}".format(create_sheet_tid)
        for engine in engines:
            if engine == "adwords":
                download_adwords_adgroup = BashOperator(
                    task_id=extract_adwords_ad_group_tid,
                    bash_command="python " + parent_path + "CRUD/adwords/read_engine.py -c {0} -b {1} -r AdGroupPerformanceReport".format(client_code, brand_code),
                    dag=ods_dag)
                download_adwords_ads = BashOperator(
                    task_id=extract_adwords_ad_tid,
                    bash_command="python " + parent_path + "CRUD/adwords/read_engine.py -c {0} -b {1} -r AdPerformanceReport".format(client_code, brand_code),
                    dag=ods_dag)
                mastering_adwords = BashOperator(
                    task_id=mastering_adwords_tid,
                    bash_command="python " + parent_path + "strategies/active_ads/active_ads_check.py -c {0} -b {1} -e {2} -i {3}".format(client_code, brand_code, google_sheet_id_xcoms, "Adwords"),
                    dag=ods_dag)
                download_adwords_adgroup.set_upstream(create_sheet)
                download_adwords_ads.set_upstream(create_sheet)
                mastering_adwords.set_upstream(download_adwords_adgroup)
                mastering_adwords.set_upstream(download_adwords_ads)
                send_email.set_upstream(mastering_adwords)

            if engine == "bing":
                download_bing = BashOperator(
                    task_id=extract_bing_tid,
                    bash_command="echo aaa",
                    dag=ods_dag)

                mastering_bing = BashOperator(
                    task_id=mastering_bing_tid,
                    bash_command="python " + parent_path + "strategies/active_ads/active_ads_check.py -c {0} -b {1} -e {2} -i {3}".format(client_code, brand_code, google_sheet_id_xcoms, "BingAds"),
                    dag=ods_dag)
                download_bing.set_upstream(create_sheet)
                mastering_bing.set_upstream(download_bing)
                send_email.set_upstream(mastering_bing)


