from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import pandas as pd

def load_data(**kwargs):
    data = load_iris(as_frame=True)
    df = pd.concat([data.data, data.target.rename('target')], axis=1)
    kwargs['ti'].xcom_push(key='dataframe', value=df)

def preprocess_data(**kwargs):
    df = kwargs['ti'].xcom_pull(key='dataframe')
    X = df.drop(columns=['target'])
    y = df['target']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    kwargs['ti'].xcom_push(key='X_train', value=X_train)
    kwargs['ti'].xcom_push(key='X_test', value=X_test)
    kwargs['ti'].xcom_push(key='y_train', value=y_train)
    kwargs['ti'].xcom_push(key='y_test', value=y_test)

def train_model(**kwargs):
    X_train = kwargs['ti'].xcom_pull(key='X_train')
    y_train = kwargs['ti'].xcom_pull(key='y_train')
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    kwargs['ti'].xcom_push(key='model', value=model)

def evaluate_model(**kwargs):
    X_test = kwargs['ti'].xcom_pull(key='X_test')
    y_test = kwargs['ti'].xcom_pull(key='y_test')
    model = kwargs['ti'].xcom_pull(key='model')
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Model Accuracy: {accuracy}")

default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 9, 3),
    'retries': 1,
}

with DAG(dag_id='ml_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    load_data_task = PythonOperator(task_id='load_data', python_callable=load_data, provide_context=True)
    preprocess_data_task = PythonOperator(task_id='preprocess_data', python_callable=preprocess_data, provide_context=True)
    train_model_task = PythonOperator(task_id='train_model', python_callable=train_model, provide_context=True)
    evaluate_model_task = PythonOperator(task_id='evaluate_model', python_callable=evaluate_model, provide_context=True)

    load_data_task >> preprocess_data_task >> train_model_task >> evaluate_model_task
