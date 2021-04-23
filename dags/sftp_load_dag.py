from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.sftp_to_s3_operator import SFTPToS3Operator
from airflow.hooks.S3_hook import S3Hook

from airflow.utils import dates
from airflow.models import Variable
import os, glob, subprocess
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime

#this if from AIRFLOW -> ADMIN/Connections menu
af_conn_id = 'SFTP_CONN_ID'
#postrgre connections
postgre_conn_id='POSTGRE_CONN'

s3_conn_id = 'S3_CONN'

#ftp source directory
source="/homes/airflow_test/"

rundt = datetime.now().strftime("%Y%m%d_%H%M%S")

args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(1),
    'email_on_failure': True,
    'email_on_success': True,
    #'schedule_interval': '0 1 * * *',
}


def get_postgre_connection(conn_id):
    """ Reads airflow connection and returns an sqlalchemy-style connection string suitable for pd.read_sql() function. """
    conn = BaseHook.get_connection(conn_id)

    template = 'postgresql://{db_user}:{db_pass}@{host}:{port}/{db}'
    return template.format(db_user=conn.login,
                           db_pass=conn.get_password(),
                           host=conn.host,
                           port=conn.port,
                           db=conn.schema)



def upload_data(conn,filename):
    """
    FTP is not a stable connection so we need to store the data in a dictionery before process it
    """
    files_dict = {}
    def create_datadict(data):
        text = data.decode('utf-8') #this depends on the source files

        if filename in files_dict.keys():
            files_dict[filename] += text
        else:
            files_dict[filename] = text

    #ftp = FTPHook(ftp_conn_id=af_conn_id)
    #ftp.get_conn()
    #download the file data from the source
    conn.retrieve_file(source+filename, None, callback=create_datadict)
    #ftp.close_conn()

    return files_dict

def GetFiles(**kwargs):
    """
    this function downloads the files from the source host and writes it into the DB
    """
    ftp = FTPHook(ftp_conn_id=af_conn_id)
    #ftp.get_conn()

    #create a list from all files on the destination what ends with .csv
    files = [x for x in ftp.list_directory(source) if str(x).endswith('.csv')]

    #ftp.close_conn()
    for file in files:
        data_dict = upload_data(ftp, file)
        for filename in data_dict:
            #df = pd.read_csv(StringIO(data_dict[filename]), names=['','','','',''])
            #based on the file names the destination table has to be set
            if filename.startswith("location"):
                table = "location_details_test"
                column_list = 'load from constans'
                #here data modifications could aply
                #df['UPDATE_DATE'] = pd.to_datetime(df['UPDATE_DATE'], format='%Y%m%d')
            if filename.startswith("product"):
                table = "product_details"
                column_list = 'load from constans'


            df = pd.read_csv(StringIO(data_dict[filename]), skiprows=1, names=column_list)
            df['FILE_NAME'] = filename

            db = create_engine(get_postgre_connection(postgre_conn_id))
            db_conn = db.connect()
            try:
                df.to_sql(name=table, con=db_conn, schema='public', if_exists='append', index=False)
            except Exception as error:
                print("An exception occurred:", error)
            db_conn.close()


        taks_id = "S3_"+table
        sftptoaws = SFTPToS3Operator(
            task_id=taks_id,
            sftp_conn_id=af_conn_id,
            sftp_path="/root/airflow_test/"+file,
            s3_conn_id=s3_conn_id,
            s3_bucket='afksiskot',
            s3_key = file+'.'+rundt,
            dag=dag)

        sftptoaws.execute(context=kwargs)


dag = DAG(
    dag_id='sftp_load_dag',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)


load_file = PythonOperator(
        task_id='Check_FTP_and_Download',
        provide_context=True,
        python_callable=GetFiles,
        dag=dag
    )



load_file
