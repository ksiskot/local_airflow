from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.base_hook import BaseHook
#from airflow.hooks.S3_hook import S3Hook

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
#ftp source directory
s3_conn_id = 'S3_CONN'


source="/homes/airflow_test/"
archive_dir = "/homes/airflow_test/Archive/"
sshsource="/share/homes/airflow_test/"
ssharchive_dir = "/share/homes/airflow_test/Archive/"



archive_command='mv {}{} {}{}_{}'
archive_command_ftp = 'rename .{}{} .{}{}_{}'

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
    #conn.retrieve_file(source+filename, '/downloads/'+filename, callback=create_datadict)
    conn.retrieve_file(source+filename, './downloads/'+filename)
    print('download ok')
    os.remove('./downloads/'+filename)
    #ftp.close_conn()

    return files_dict

def GetFiles(**kwargs):
    """
    this function downloads the files from the source host and writes it into the DB
    """
    ftp = FTPHook(ftp_conn_id=af_conn_id)


    #create a list from all files on the destination what ends with .csv
    files = [x for x in ftp.list_directory(source) if str(x).endswith('.csv')]

    #ftp.close_conn()
    for file in files:
        data_dict = upload_data(ftp, file)
        for filename in data_dict:
            df = pd.read_csv(StringIO(data_dict[filename]))
            #based on the file names the destination table has to be set
            if filename.startswith("location"):
                table = "location_details"
                #here data modifications could aply
                #df['UPDATE_DATE'] = pd.to_datetime(df['UPDATE_DATE'], format='%Y%m%d')
            if filename.startswith("product"):
                table = "product_details"

            #db = create_engine(get_postgre_connection(postgre_conn_id))
            #db_conn = db.connect()
            #try:
                #df.to_sql(name=table, con=db_conn, schema='public', if_exists='append', index=False)
            #except Exception as error:
                #print("An exception occurred:", error)
            #db_conn.close()

        ssh_hook = SSHHook(af_conn_id)
        s3_hook = S3Hook(s3_conn_id)

        sftp_client = ssh_hook.get_conn().open_sftp()

        with NamedTemporaryFile("w") as f:
            sftp_client.get(self.sftp_path, f.name)

            s3_hook.load_file(
                filename=f.name,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True

            )


"""        ssh = SSHHook(af_conn_id)
        ssh_client = None
        try:
            ssh_client = ssh.get_conn()
            command = archive_command.format(sshsource,file,ssharchive_dir,rundt,file)
            print(command)
            #ssh_client.exec_command(command)
        finally:
            if ssh_client:
                ssh_client.close()
"""

dag = DAG(
    dag_id='sftp_load_local_dag',
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
