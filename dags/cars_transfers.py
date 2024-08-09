from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GCS_BUCKET = 'ready-project-dataset'
GCS_PREFIX = 'cars-com_dataset/'
BIGQUERY_LANDING_TABLE = 'ready-data-de24.cars_raw_06.cars'

dag = DAG(
    dag_id='cars_transfers',
    start_date=datetime(2021, 1, 1), 
    schedule_interval=None,
)

schema = [
        {"name": "brand", "type": "STRING", "mode": "NULLABLE"},
        {"name": "model", "type": "STRING", "mode": "NULLABLE"},
        {"name": "year", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "mileage", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "engine", "type": "STRING", "mode": "NULLABLE"},
        {"name": "engine_size", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "transmission", "type": "STRING", "mode": "NULLABLE"},
        {"name": "automatic_transmission", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "fuel_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "drive_train", "type": "STRING", "mode": "NULLABLE"},
        {"name": "min_mpg", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "max_mpg", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "damaged", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "first_owner", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "personal_using", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "turbo", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "alloy_wheels", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "adaptive_cruise_control", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "navigation_system", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "power_liftgate", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "backup_camera", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "keyless_start", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "remote_start", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "sunroof_moonroof", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "automatic_emergency_braking", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "stability_control", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "leather_seats", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "memory_seat", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "third_row_seating", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "apple_car_play_android_auto", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "bluetooth", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "usb_port", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "heated_seats", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "interior_color", "type": "STRING", "mode": "NULLABLE"},
        {"name": "exterior_color", "type": "STRING", "mode": "NULLABLE"},
        {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
    ]


start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)


load_from_gcs_to_bq = GCSToBigQueryOperator(
    task_id='load_from_gcs_to_bq',
    bucket=GCS_BUCKET,
    source_objects=[f"{GCS_PREFIX}*.csv"],
    source_format="CSV",
    destination_project_dataset_table=BIGQUERY_LANDING_TABLE,
    schema_fields=schema,
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