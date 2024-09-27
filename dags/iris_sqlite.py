import sqlite3
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sqlite3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import pandas as pd
import mlflow
import mlflow.sklearn
mlflow.set_tracking_uri("http://127.0.0.1:5000") 
# def create_table():
#     conn = sqlite3.connect('/mnt/c/Users/yatish.v/Desktop/ubuntu_airflow/airflow.db')
#     cursor = conn.cursor()

#     cursor.execute('''
#     CREATE TABLE IF NOT EXISTS airflow_data (
#         key TEXT PRIMARY KEY,
#         value TEXT
#     )
#     ''')

#     conn.commit()
#     conn.close()

#     create_table()  
# def load_data(**kwargs):
#     # Load the Iris dataset
#     data = load_iris(as_frame=True)
#     df = pd.concat([data.data, data.target.rename('target')], axis=1)

#     # Save DataFrame to SQLite
#     conn = sqlite3.connect('/mnt/c/Users/yatish.v/Desktop/ubuntu_airflow/airflow.db')
#     cursor = conn.cursor()

#     # Convert DataFrame to a JSON string
#     df_json = df.to_json()

#     # Insert data into SQLite table
#     cursor.execute("REPLACE INTO airflow_data (key, value) VALUES (?, ?)", ('dataframe', df_json))

#     conn.commit()
#     conn.close()

def load_data(**kwargs):
    # Connect to the SQLite database
    conn = sqlite3.connect('/mnt/c/Users/yatish.v/Desktop/ubuntu_airflow/airflow.db')  # Ensure this is the correct path to your SQLite DB
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS airflow_data (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    ''')
    conn.commit()

    # Load the Iris dataset
    data = load_iris(as_frame=True)
    df = pd.concat([data.data, data.target.rename('target')], axis=1)

    # Convert DataFrame to JSON string and insert into SQLite
    df_json = df.to_json()
    cursor.execute("REPLACE INTO airflow_data (key, value) VALUES (?, ?)", ('dataframe', df_json))

    # Commit and close the connection
    conn.commit()
    conn.close()


def preprocess_data(**kwargs):
    conn = sqlite3.connect('/mnt/c/Users/yatish.v/Desktop/ubuntu_airflow/airflow.db')
    cursor = conn.cursor()

    # Retrieve data from SQLite
    cursor.execute("SELECT value FROM airflow_data WHERE key = ?", ('dataframe',))
    df_json = cursor.fetchone()[0]

    conn.close()

    # Convert JSON string back to DataFrame
    df = pd.read_json(df_json)

    # Preprocess data
    X = df.drop(columns=['target'])
    y = df['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Save the preprocessed data to SQLite
    conn = sqlite3.connect('/mnt/c/Users/yatish.v/Desktop/ubuntu_airflow/airflow.db')
    cursor = conn.cursor()

    cursor.execute("REPLACE INTO airflow_data (key, value) VALUES (?, ?)", ('X_train', X_train.to_json()))
    cursor.execute("REPLACE INTO airflow_data (key, value) VALUES (?, ?)", ('X_test', X_test.to_json()))
    cursor.execute("REPLACE INTO airflow_data (key, value) VALUES (?, ?)", ('y_train', json.dumps(y_train.tolist())))
    cursor.execute("REPLACE INTO airflow_data (key, value) VALUES (?, ?)", ('y_test', json.dumps(y_test.tolist())))

    conn.commit()
    conn.close()

def train_model(**kwargs):
    mlflow.set_experiment("MLflow iris")
    conn = sqlite3.connect('/mnt/c/Users/yatish.v/Desktop/ubuntu_airflow/airflow.db')
    cursor = conn.cursor()

    # Retrieve preprocessed data from SQLite
    cursor.execute("SELECT value FROM airflow_data WHERE key = ?", ('X_train',))
    X_train_json = cursor.fetchone()[0]

    cursor.execute("SELECT value FROM airflow_data WHERE key = ?", ('y_train',))
    y_train_list = json.loads(cursor.fetchone()[0])

    conn.close()

    # Convert JSON string and list back to DataFrame and Series
    X_train = pd.read_json(X_train_json)
    y_train = pd.Series(y_train_list)

    # Train model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Save the model and push run ID to the SQLite database
    with mlflow.start_run() as run:
        mlflow.sklearn.log_model(model, "model")
        run_id = run.info.run_id

    # Save the model path and run ID to SQLite
    conn = sqlite3.connect('/mnt/c/Users/yatish.v/Desktop/ubuntu_airflow/airflow.db')
    cursor = conn.cursor()

    cursor.execute("REPLACE INTO airflow_data (key, value) VALUES (?, ?)", ('run_id', run_id))

    conn.commit()
    conn.close()
def evaluate_model(**kwargs):
    conn = sqlite3.connect('/mnt/c/Users/yatish.v/Desktop/ubuntu_airflow/airflow.db')
    cursor = conn.cursor()

    # Retrieve test data and run ID from SQLite
    cursor.execute("SELECT value FROM airflow_data WHERE key = git  ('X_test',))
    X_test_json = cursor.fetchone()[0]

    cursor.execute("SELECT value FROM airflow_data WHERE key = ?", ('y_test',))
    y_test_list = json.loads(cursor.fetchone()[0])

    cursor.execute("SELECT value FROM airflow_data WHERE key = ?", ('run_id',))
    run_id = cursor.fetchone()[0]

    conn.close()

    # Convert JSON and list back to DataFrame and Series
    X_test = pd.read_json(X_test_json)
    y_test = pd.Series(y_test_list)

    # Load the model
    model = mlflow.sklearn.load_model(f"runs:/{run_id}/model")

    # Predict and evaluate
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    # Log the accuracy
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metric("accuracy", accuracy)

    print(f"Model Accuracy: {accuracy}")

default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 9, 3),
    'retries': 1,
}
with DAG(dag_id='pipeline21_sqlite',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    load_data_task = PythonOperator(task_id='load_data', python_callable=load_data, provide_context=True)
    preprocess_data_task = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data, provide_context=True)
    train_model_task = PythonOperator(task_id='train_model', python_callable=train_model, provide_context=True)
    evaluate_model_task = PythonOperator(task_id='evaluate_model', python_callable=evaluate_model, provide_context=True)

    load_data_task >> preprocess_data_task >> train_model_task >> evaluate_model_task
