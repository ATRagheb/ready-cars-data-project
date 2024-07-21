from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCS_BUCKET = 'ready-project-dataset'
GCS_PREFIX = 'cars-com_dataset/'
BIGQUERY_LANDING_TABLE = 'cars_raw_06.cars'

dag = DAG(
    dag_id='cars_transfers',
    start_date=datetime(2021, 1, 1), 
    schedule_interval=None,
)


start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)


load_from_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_from_gcs_to_bq',
    bucket="chicago-taxi-test-de24",
    source_objects=[f"{GCS_PREFIX}/*.csv"],
    source_format="CSV",
    destination_project_dataset_table=BIGQUERY_LANDING_TABLE,
    autodetect=True,
    gcp_conn_id="gcp_conn",
    field_delimiter=',',
    max_bad_records=1000000,
    skip_leading_rows=1,
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    encoding='UTF-8',
    dag=dag
)


start >> load_from_gcs_to_bq >> end