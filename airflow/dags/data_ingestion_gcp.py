import os
from google.cloud import storage
import glob
import shutil
import tarfile
from pathlib import Path

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator,BigQueryCreateEmptyDatasetOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "snappy-premise-456414-i7")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "my-bucket-snappy-premise-456414-i7")

dataset_url = f"https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
path_to_creds = f"/.google/credentials/google_credentials.json"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'noaa_data')


def untar_file(pathToData):
    path=Path(pathToData)
    for file in path.rglob("*.tar.gz"):
        year=file.stem
        year=Path(year).stem
        output_dir=path/year
        output_dir.mkdir(exist_ok=True)

        with tarfile.open(file, 'r:gz') as tar:
            tar.extractall(path=output_dir)
            print(f"Extracted {file.name} to {output_dir}")



default_args = {
 "owner": "airflow",
 "start_date": days_ago(1),
 "depends_on_past": False,
 "retries": 1,
}

with DAG(
 dag_id="data_ingestion_gcp_dag",
 schedule_interval="@daily",
 default_args=default_args,
 catchup=False,
 max_active_runs=1,
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f'wget -r -l1 -nd -A "19*.tar.gz" {dataset_url} -P {path_to_local_home}/noaa'
            )

    untar_dataset_task = PythonOperator(
        task_id="untar_dataset_task",
        python_callable=untar_file,
        op_kwargs={"pathToData":"/opt/airflow/noaa"}
    )
    
    run_spark_task = SparkSubmitOperator(
        task_id="run_spark_task",
        application=f"{path_to_local_home}/scripts/noaa_data_process.py",
        conn_id="my_spark"
    )

    upload_to_gcs_task = BashOperator(
        task_id="upload_to_gcs_task",
        bash_command=f"gcloud auth activate-service-account --key-file={path_to_creds} && \
                        gsutil -m cp -r {path_to_local_home}/noaa/processed gs://{BUCKET}/processed"
    ) 

    bigquery_create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="bigquery_create_dataset_task",
        dataset_id="weather_all",
        project_id="snappy-premise-456414-i7",
        exists_ok=True
    )  


    download_dataset_task>>untar_dataset_task>>run_spark_task>>upload_to_gcs_task>>bigquery_create_dataset_task
