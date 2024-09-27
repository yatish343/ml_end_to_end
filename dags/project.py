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

import joblib
import os
mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Replace with your MLflow server IP and port

def load_data(**kwargs):
    data = load_iris(as_frame=True)
    df = pd.concat([data.data, data.target.rename('target')], axis=1)
    kwargs['ti'].xcom_push(key='dataframe', value=df.to_dict())

def preprocess_data(**kwargs):
    df_dict = kwargs['ti'].xcom_pull(key='dataframe')
    df = pd.DataFrame.from_dict(df_dict)
    X = df.drop(columns=['target'])
    y = df['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    kwargs['ti'].xcom_push(key='X_train', value=X_train.to_dict())
    kwargs['ti'].xcom_push(key='X_test', value=X_test.to_dict())
    kwargs['ti'].xcom_push(key='y_train', value=y_train.tolist())
    kwargs['ti'].xcom_push(key='y_test', value=y_test.tolist())

def train_model(**kwargs):
    mlflow.set_experiment("MLflow Quickstart")


    # Set MLflow tracking URI
    mlflow.set_tracking_uri("http://127.0.0.1:5000")  # Replace with your MLflow server IP and port

    # Retrieve data
    X_train_dict = kwargs['ti'].xcom_pull(key='X_train')
    y_train_list = kwargs['ti'].xcom_pull(key='y_train')

    # Convert dictionary and list back to DataFrame and Series
    X_train = pd.DataFrame.from_dict(X_train_dict)
    y_train = pd.Series(y_train_list)

    # Train model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Log model with MLflow
    with mlflow.start_run() as run:
        mlflow.sklearn.log_model(model, "model")
        kwargs['ti'].xcom_push(key='run_id', value=run.info.run_id)

    # Ensure the directory exists
    model_dir = "models"  # Directory to save the model
    os.makedirs(model_dir, exist_ok=True)

    # Save model to a file
    model_path = os.path.join(model_dir, "random_forest.pkl")
    joblib.dump(model, model_path)

    # Push the model path to XCom
    kwargs['ti'].xcom_push(key='model_path', value=model_path)

def evaluate_model(**kwargs):
    X_test_dict = kwargs['ti'].xcom_pull(key='X_test')
    y_test_list = kwargs['ti'].xcom_pull(key='y_test')
    model = kwargs['ti'].xcom_pull(key='model')
    X_test = pd.DataFrame.from_dict(X_test_dict)
    y_test = pd.Series(y_test_list)

    # Predict and evaluate
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    # Log metrics
    run_id = kwargs['ti'].xcom_pull(key='run_id')
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metric("accuracy", accuracy)

    print(f"Model Accuracy: {accuracy}")

default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 9, 3),
    'retries': 1,
}

with DAG(dag_id='pipeline21',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    load_data_task = PythonOperator(task_id='load_data', python_callable=load_data, provide_context=True)
    preprocess_data_task = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data, provide_context=True)
    train_model_task = PythonOperator(task_id='train_model', python_callable=train_model, provide_context=True)
    evaluate_model_task = PythonOperator(task_id='evaluate_model', python_callable=evaluate_model, provide_context=True)

    load_data_task >> preprocess_data_task >> train_model_task >> evaluate_model_task
