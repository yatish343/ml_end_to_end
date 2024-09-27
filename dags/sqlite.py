from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sqlite3

def insert_data():
    conn = sqlite3.connect('/mnt/c/Users/yatish.v/Desktop/ubuntu_airflow/airflow.db')
    cursor = conn.cursor()

    # Create a table if it doesn't exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS my_table (
                       id INTEGER PRIMARY KEY, 
                       name TEXT, 
                       value REAL)''')

    # Insert data
    cursor.execute("INSERT INTO my_table (name, value) VALUES ('example_name', 123.45)")
    conn.commit()
    conn.close()

# Define the DAG
with DAG(dag_id='example_sqlite_dag', start_date=days_ago(1), schedule_interval=None,catchup=False) as dag:

    insert_task = PythonOperator(
        task_id='insert_data_task',
        python_callable=insert_data
    )

    insert_task
