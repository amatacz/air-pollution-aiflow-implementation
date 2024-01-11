# Import necessary modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # The DAG does not depend on past runs
    'start_date': datetime(2023, 1, 1),  # Start date of the DAG
    'email': ['example@example.com'],  # Email for alerts
    'email_on_failure': False,  # Email sent on task failure
    'email_on_retry': False,  # Email sent on task retry
    'retries': 1,  # Number of retries
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'example_dag',  # DAG ID
    default_args=default_args,  # Apply the default arguments
    description='An example DAG',  # Description of the DAG
    schedule_interval=timedelta(days=1),  # Interval at which the DAG will run
    catchup=False,  # Prevents catch-up runs for past dates after deployment
)


def print_message():
    print('Hello World.')


print_task = PythonOperator(
    task_id='print_message',  # Task ID
    python_callable=print_message,
    dag=dag
)


def perform_calculation():
    result = 2 + 2
    print('The result is', result)


calculation_task = PythonOperator(
    task_id='perform_calculation',
    python_callable=perform_calculation,
    dag=dag
)

print_task >> calculation_task
