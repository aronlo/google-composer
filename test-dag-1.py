from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator


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
    'test-dag-1',
    default_args=default_args,
    description='A simple test of DAG 1',
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1),
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag)


t1 >> [t2, t3] 