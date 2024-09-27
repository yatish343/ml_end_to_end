from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
import os
import mlflow
import mlflow.sklearn
import scipy.sparse

# Define file paths
RAW_DATA_PATH = 'dags/raw_data.csv'
TRANSFORMED_FEATURES_PATH = 'dags/features.npz'
TRANSFORMED_TARGET_PATH = 'dags/target.csv'

# Ensure the directory exists
os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)

# Define the functions for each task
def load_data(**kwargs):
    # Load the dataset
    df = pd.read_csv("HCPCS_code.csv")
    # Save DataFrame to CSV
    df.to_csv(RAW_DATA_PATH, index=False)
    print(df.columns)

def transform_data(**kwargs):
    # Load DataFrame from CSV
    df = pd.read_csv(RAW_DATA_PATH)
    print(df.columns)
    # Separate 'Code' column as target variable
    X = df.drop(columns=['Code'])
    y = df['Code']
    
    # Perform TF-IDF transformation
    tfidf = TfidfVectorizer()
    X_tfidf = tfidf.fit_transform(X.apply(lambda x: ' '.join(x.astype(str)), axis=1))
    
    # Save transformed features and target
    scipy.sparse.save_npz(TRANSFORMED_FEATURES_PATH, X_tfidf)
    y.to_csv(TRANSFORMED_TARGET_PATH, index=False)

def train_model(**kwargs):
    # Start MLflow run
    with mlflow.start_run():
        # Load transformed data from CSV
        X_tfidf = pd.read_csv(TRANSFORMED_FEATURES_PATH)
        y = pd.read_csv(TRANSFORMED_TARGET_PATH)
        
        # Convert back to sparse matrix
        X_tfidf = pd.DataFrame.sparse.from_spmatrix(X_tfidf).sparse.to_coo()
        
        # Split the data
        X_train, X_test, y_train, y_test = train_test_split(X_tfidf, y, test_size=0.2, random_state=42)
        
        # Train a Logistic Regression model for multi-class classification
        model = LogisticRegression(multi_class='multinomial', solver='lbfgs')
        model.fit(X_train, y_train)
        
        # Predict and evaluate the model
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        
        # Log metrics and model
        mlflow.log_metric("accuracy", accuracy)
        mlflow.sklearn.log_model(model, "model")
        
        print(f'Model Accuracy: {accuracy}')

# Define the DAG
default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 8, 27),
    'retries': 1,
}

with DAG(dag_id='classification',
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
