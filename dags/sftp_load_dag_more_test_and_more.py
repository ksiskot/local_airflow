from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.base_hook import BaseHook

from airflow.utils import dates
from airflow.models import Variable
import os, glob, subprocess, boto3, ast
import pandas as pd
from io import StringIO, BytesIO
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime
from airflow.contrib.operators.sftp_to_s3_operator import SFTPToS3Operator


#this if from AIRFLOW -> ADMIN/Connections menu
envista_sftp_id = 'sftp_envista_corp'
#postrgre connections
postgre_conn_id='envista_ods'

#timestamp
rundt = datetime.now().strftime("%Y%m%d_%H%M%S")

#ftp source directory
source="/lululemon/"
source_archive="/lululemon/Archive/"


envista_s3_id="envista_s3"
envista_s3_bucket="ll-trade-wms-dev"


args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(1),
    'email_on_failure': True,
    'email_on_success': True,
    #'schedule_interval': '0 1 * * *',
}





def GetFiles(**kwargs):
    """
    this function downloads the files from the source host and writes it into the DB
    """
    ftp = FTPHook(ftp_conn_id=envista_sftp_id)
    #ftp.get_conn()

    #create a list from all files on the destination what ends with .csv
    files = [x for x in ftp.list_directory(source) if str(x).endswith('.csv')]
    #ftp.close_conn()
    for filename in files:
        
        if filename.startswith("Accrual Deliverable-Paid_"):
                task_name = "accrual_deliverable_paid"
        if filename.startswith("Accrual Deliverable-Unpaid_"):
                task_name = "accrual_deliverable_unpaid"
        
        
        sftptoaws = SFTPToS3Operator(
            task_id=task_name,
            sftp_conn_id=envista_sftp_id,
            sftp_path=source+filename,
            s3_conn_id=envista_s3_id,
            s3_bucket=envista_s3_bucket,
            s3_key = "Envista/"+filename+'.'+rundt,
            dag=dag)


        sftptoaws.execute(context=kwargs)


    return files


def load_files(**context):
    files = context['templates_dict']['file_list']
    

    conn = BaseHook.get_connection(envista_s3_id)
    cred_dict= ast.literal_eval(conn.extra)
    #print(conn.extra['aws_access_key_id'])
    """
    s3_resource = boto3.resource('s3')
    my_bucket = s3_resource.Bucket(envista_s3_bucket)
    objects = my_bucket.objects.filter(Prefix='Envista/')
    for obj in objects:
        path, filename = os.path.split(obj.key)
        my_bucket.download_file(obj.key, filename)
    """
    s3_conn = boto3.client('s3',aws_access_key_id=cred_dict['aws_access_key_id'], aws_secret_access_key=cred_dict['aws_secret_access_key'])

    

    
    for filename in files:
        file = filename+'.'+rundt
        obj = s3_conn.get_object(Bucket=envista_s3_bucket, Key=file)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        print(df)
        
  

    



dag = DAG(
    dag_id='sftp_s3_load',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)



sftp_s3 = PythonOperator(
    task_id='sftp_s3',
    provide_context=True,
    python_callable=GetFiles,
    dag=dag
)

s3_ods = PythonOperator(
    task_id='s3_ods',
    provide_context=True,
    python_callable=load_files,
    templates_dict={'file_list': "{{ ti.xcom_pull(task_ids='sftp_s3') }}" },
    dag=dag
)


sftp_s3>>s3_ods