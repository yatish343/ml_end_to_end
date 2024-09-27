from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import mlflow
import mlflow.sklearn
mlflow.set_tracking_uri("http://127.0.0.1:5000")
# Define the function that will train and log the model
def train_iris_model():
    # Load the Iris dataset
    iris = load_iris()
    X = iris.data
    y = iris.target

    # Split the data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Start an MLflow run
    with mlflow.start_run():
        # Define model parameters
        n_estimators = 100
        max_depth = 3

        # Log parameters
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("max_depth", max_depth)

        # Train a RandomForest classifier
        clf = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth, random_state=42)
        clf.fit(X_train, y_train)

        # Make predictions
        y_pred = clf.predict(X_test)

        # Log accuracy metric
        accuracy = accuracy_score(y_test, y_pred)
        mlflow.log_metric("accuracy", accuracy)

        # Log the trained model
        mlflow.sklearn.log_model(clf, "random_forest_model")

        # Log the test data
        pd.DataFrame(X_test).to_csv("iris_test_data.csv", index=False)
        mlflow.log_artifact("iris_test_data.csv")

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,9,18),
    'retries': 1,
}

# Define the Airflow DAG
dag = DAG(
    'iris_mlflow_dag',
    default_args=default_args,
    description='Train Iris Dataset with MLflow logging',
    schedule_interval='@daily',  
    catchup=False
)

# Define the PythonOperator to execute the model training function
train_model_task = PythonOperator(
    task_id='train_iris_model',
    python_callable=train_iris_model,
    dag=dag,
)

# Set the task in the DAG
train_model_task

