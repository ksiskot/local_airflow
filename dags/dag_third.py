dag = DAG(
    dag_id='THIRD_DAG',
    default_args=args,
    description='DAG to monitor ods process id and send an e-mail with the results',
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1
)
