import csv
import logging
from pathlib import Path
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults

from trade_airflow.utils import constants


class DatabaseToCSVOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self, filePath : str, fileName : str, sqlQuery : str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = 'ods'
        self.autocommit = False
        self.parameters = None
        self.database = None        
        self.file_path = filePath
        self.file_name = fileName
        self.sql = sqlQuery
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        Path(self.file_path).mkdir(parents=True, exist_ok=True)

    """
    Extract data from the database to CSV file
    """

    def execute(self, context):
        logging.info("execute initializing")             
        dag_id = context['dag'].dag_id
        environment = Variable.get(constants.variable_environment)
        task_instance = context['ti']
        task_id = task_instance.task_id
        task_try_number = task_instance.try_number
        task_start_date = task_instance.start_date
        task_end_date = task_instance.end_date
        task_operator = task_instance.operator
        log_url = task_instance.log_url
        logging.info("Running query against database")
        self.log.info('Executing: %s', self.sql)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        result = cursor.fetchall()
                       
        if cursor.rowcount > 0:
           # Write to CSV file
           temp_path = self.file_path + self.file_name
           tmp_path = self.file_path + '-' + self.file_name
           with open(temp_path, 'w') as fp:
               a = csv.writer(fp, quoting=csv.QUOTE_MINIMAL, delimiter=',')
               a.writerow([i[0] for i in cursor.description])
               a.writerows(result)               
           
           with open(temp_path, 'rb') as f:
               data = f.read()
           f.close()
           logging.info("file created")
           self.hook.copy_expert(self.sql, filename=tmp_path)
        else:
           logging.info('File was not created because there was not result from the query')
