from datetime import datetime
from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook
from dag3_plugin.dag3_operator import ExampleOperator

connection = BaseHook.get_connection("main_postgresql_connection")

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 8, 29),
}

dag = DAG('dag3', default_args=default_args, schedule_interval='0 * * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["Test"])

task1 = ExampleOperator(
    task_id='task1',
    postgre_conn=connection,
    dag=dag)

# task2 = ExampleOperator(
#     task_id='task2',
#     postgre_conn=connection,
#     dag=dag)

# task1 >> task2