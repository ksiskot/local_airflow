import logging
import airflow
from airflow.utils import email
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import SqlSensor
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from os import path
from airflow.utils import dates
"""
from utils.database_to_csv_operator import DatabaseToCSVOperator
from utils import emailAlerts
from utils import dag_utilsy
from utils.constants import EnvVar
"""
postgres_conn_id = 'ods'

args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(1),
    'email_on_failure': True,
    'email_on_success': True,
    #'schedule_interval': '0 1 * * *',
}

dag = DAG(
    dag_id='ODS_STALKER',
    default_args=args,
    description='DAG to monitor ods process id and send an e-mail with the results',
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

def run_monitor_query(**kwargs):
    query = kwargs['sqlQuery']
    environment = "Dev"
    to_email = "email"
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id, schema=None)


    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    if cursor.rowcount > 0:
        result_df = pd.DataFrame(result)
        email_body = result_df.to_html()
        subject = "***Monitor " + environment.upper() + " ODS Process ID - " + datetime.today().strftime('%m/%d/%Y') + " @" + datetime.today().strftime('%H:%M:%S') + "***"
        html_fmt = """
        <br>
        <p><em>Please reach out to <a href="mailto:trade-alerts@lululemon.com">trade_alerts</a> DL for any questions.
        <br>
        <br>Note: This is an automated email
        </em></p>"""

        html_content = email_body + html_fmt
        email.send_email_smtp(to_email, subject, html_content)


postgresql = PythonOperator(
        task_id='run_monitor_query',
        provide_context=True,
        python_callable=run_monitor_query,
        op_kwargs={'sqlQuery': "select current_time"},
        dag=dag
    )
