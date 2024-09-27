from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime  import datetime 
from datetime  import timedelta
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
import mlflow
# Define the functions for each task

def load_data(**kwargs):
    # Load the dataset
    df = pd.read_csv("HCPCS_code.csv")
    kwargs['ti'].xcom_push(key='dataframe', value=df)
    print(df.columns) 

def transform_data(**kwargs):
    # Extract the dataframe from XCom
    df = kwargs['ti'].xcom_pull(key='dataframe')
    print(df.columns) 
    # Separate 'code' column as target variable
    X = df.drop(columns=['Code'])
    y = df['Code']
    
    # Perform TF-IDF transformation
    tfidf = TfidfVectorizer()
    X_tfidf = tfidf.fit_transform(X.apply(lambda x: ' '.join(x.astype(str)), axis=1))
    
    kwargs['ti'].xcom_push(key='features', value=X_tfidf)
    kwargs['ti'].xcom_push(key='target', value=y)

def train_model(**kwargs):
    # Extract the features and target from XCom
    X_tfidf = kwargs['ti'].xcom_pull(key='features')
    y = kwargs['ti'].xcom_pull(key='target')
    
    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(X_tfidf, y, test_size=0.2, random_state=42)
    
    # Train a Logistic Regression model for multi-class classification
    model = LogisticRegression(multi_class='multinomial', solver='lbfgs')
    model.fit(X_train, y_train)
    
    # Predict and evaluate the model
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f'Model Accuracy: {accuracy}')

# Define the DAG

default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 8, 27),
    'retries': 1,
}

with DAG(dag_id='ab_code_classification',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    train_model_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        provide_context=True
    )

    # Define the task dependencies
    load_data_task >> transform_data_task >> train_model_task
