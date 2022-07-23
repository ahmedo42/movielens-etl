import logging
import os

import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateExternalTableOperator
from airflow.utils.dates import days_ago
from google.cloud import storage

from airflow import DAG

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_url = "https://files.grouplens.org/datasets/movielens/ml-25m.zip"
dataset_file = "ml-25m.zip"
dataset_folder = "ml-25m"
csv_files = ["movies.csv", "tags.csv", "ratings.csv"]
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "movielens_25m")


def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@yearly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["de-project"],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}",
    )

    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset",
        bash_command=f"unzip {path_to_local_home}/{dataset_file} -d {path_to_local_home}",
    )

    for csv_file in csv_files:
        parquet_format = csv_file.replace(".csv", ".parquet")

        format_to_parquet_task = PythonOperator(
            task_id=f"format_to_parquet_task_{os.path.splitext(parquet_format)[0]}",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{path_to_local_home}/{dataset_folder}/{csv_file}",
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"local_to_gcs_task_{os.path.splitext(parquet_format)[0]}",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{parquet_format}",
                "local_file": f"{path_to_local_home}/{dataset_folder}/{parquet_format}",
            },
        )

        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"bigquery_external_table_task_{os.path.splitext(parquet_format)[0]}",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{os.path.splitext(parquet_format)[0]}",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/{parquet_format}"],
                },
            },
        )

        (
            download_dataset_task
            >> unzip_dataset_task
            >> format_to_parquet_task
            >> local_to_gcs_task
            >> bigquery_external_table_task
        )
