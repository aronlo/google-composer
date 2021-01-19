from datetime import timedelta

import airflow
from airflow import DAG
from airflow.utils import dates
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.functions import (
    CloudFunctionDeleteFunctionOperator,
    CloudFunctionDeployFunctionOperator,
    CloudFunctionInvokeFunctionOperator
)


default_args = {
    'owner': 'aronlo',
    'start_date': airflow.utils.dates.days_ago(0),
    # 'end_date': datetime(2018, 12, 30),
    'depends_on_past': False,
    'email': ['aron.lo.li@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test-dag-cloud-function',
    default_args=default_args,
    description='A simple test of DAG Cloud function',
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1),
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id ='execute_function',
    bash_command='gcloud functions call hello --region us-central1',
    dag=dag)


t3 =  CloudFunctionInvokeFunctionOperator(
        task_id="invoke_function",
        project_id="alicorp-test",
        location="us-central1",
        input_data={},
        function_id="hello",
        dag=dag)



t1 >> t2 >> t3