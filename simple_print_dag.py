from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the Python function to be executed
def print_hello():
    print("Hello, this is a simple test for an Airflow DAG!")

# Define the DAG
with DAG(
    dag_id='simple_print_dag',
    start_date=datetime(2024, 9, 5),  # Replace with the current date or desired start date
    schedule_interval='@daily',  # Adjust the schedule as needed (e.g., @daily, @hourly, etc.)
    catchup=False  # Prevent backfilling of DAG runs
) as dag:
    # Define the task using the PythonOperator
    print_task = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello
    )

    # Set task dependencies (if there are multiple tasks, you can define the execution order)
    print_task
