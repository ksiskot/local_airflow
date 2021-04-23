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
#from trade_airflow.utils.constants import FilePrefix, FileHeaders, EnvVar
#from trade_airflow.utils import dag_utils


name = "S3_CONN"
description = ""

args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(1),
    'email_on_failure': True,
    'email_on_success': True,
    #'schedule_interval': '0 1 * * *',
}



dag = DAG(
    dag_id='prod_to_nonprod',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)


prod_s3="S3_CONN"
prod_s3_bucket="afksiskot"

non_prod_s3="S3_CONN"
non_prod_s3_bucket="afksiskot"


def GetFiles(**kwargs):

    conn_prod = BaseHook.get_connection(prod_s3)
    cred_dict_prod= ast.literal_eval(conn_prod.extra)

    conn_nonprod = BaseHook.get_connection(non_prod_s3)
    cred_dict_nonprod = ast.literal_eval(conn_nonprod.extra)

    session_src = boto3.session.Session(aws_access_key_id=cred_dict_prod['aws_access_key_id'], aws_secret_access_key=cred_dict_prod['aws_secret_access_key'])
    source_s3_r = session_src.resource('s3')

    session_dest = boto3.session.Session(aws_access_key_id=cred_dict_nonprod['aws_access_key_id'], aws_secret_access_key=cred_dict_nonprod['aws_secret_access_key'])
    dest_s3_r = session_dest.resource('s3')

    bucket = source_s3_r.Bucket(prod_s3_bucket)


    for prefix in prefix_list:
        for obj in bucket.objects.filter(Delimiter='/', Prefix=prefix):
            old_obj = source_s3_r.Object(prod_s3_bucket, obj.key)
            new_obj = dest_s3_r.Object(non_prod_s3_bucket, obj.key)
            new_obj.put(Body=old_obj.get()['Body'].read())

["COL/","DEL/","SUM/","TOR/"]


def GetFiles_ks(**kwargs):

    conn_prod = BaseHook.get_connection(prod_s3)
    cred_dict_prod= ast.literal_eval(conn_prod.extra)

    conn_nonprod = BaseHook.get_connection(non_prod_s3)
    cred_dict_nonprod = ast.literal_eval(conn_nonprod.extra)

    session_src = boto3.session.Session(aws_access_key_id=cred_dict_prod['aws_access_key_id'], aws_secret_access_key=cred_dict_prod['aws_secret_access_key'])
    source_s3_r = session_src.resource('s3')

    session_dest = boto3.session.Session(aws_access_key_id=cred_dict_nonprod['aws_access_key_id'], aws_secret_access_key=cred_dict_nonprod['aws_secret_access_key'])
    dest_s3_r = session_dest.resource('s3')

    # create a reference to source image
    old_obj = source_s3_r.Object(prod_s3_bucket, "COL_CANCELLATION_EOD_20210409020004.csv")

    # create a reference for destination image
    new_obj = dest_s3_r.Object(non_prod_s3_bucket, "COL_CANCELLATION_EOD_20210409020004.csv")

    # upload the image to destination S3 object
    new_obj.put(Body=old_obj.get()['Body'].read())


prod_to_nonprod = PythonOperator(
    task_id='prod_to_nonprod',
    provide_context=True,
    python_callable=GetFiles,
    dag=dag
)

prod_to_nonprod
