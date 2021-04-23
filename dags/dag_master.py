import pprint

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago

pp = pprint.PrettyPrinter(indent=4)

'''
def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        pp.pprint(dag_run_obj.payload)
        return dag_run_obj
'''

# Define the DAG
dag = DAG(
    dag_id="example_trigger_controller_dag",
    default_args={"owner": "airflow", "start_date": days_ago(2)},
    schedule_interval=None,
    tags=['ksiskot']
)

# Define the single task in this controller example DAG
trigger = TriggerDagRunOperator(
    task_id='test_trigger_dagrun',
    trigger_dag_id="FIRST_DAG",
    #python_callable=conditionally_trigger,
    params={'condition_param': True, 'message': 'Hello World'},
    dag=dag,
)

trigger
