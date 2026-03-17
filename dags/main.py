from airflow import DAG
from dataquality.soda import yt_elt_data_quality
from datawarehouse.dwh import core_table, staging_table
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "test@example.com",
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(minutes=60),
    "start_date": datetime(2026, 2, 24, tzinfo=local_tz)
}

with DAG(
    dag_id = "produce_json",
    default_args=default_args,
    description="A DAG to extract video stats from YouTube and save to JSON",
    schedule= "0 14 * * *",
    catchup=False,
) as dag_produce:
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id="update_db", 
    )

    playlist_id >> video_ids >> extracted_data >> save_to_json_task >> trigger_update_db



with DAG(
    dag_id = "update_db",
    default_args=default_args,
    description="Dag to process json data and update staging and core tables in the data warehouse",
    schedule= None,
    catchup=False,
) as dag_update:
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality",
    )
    
    update_staging >> update_core >> trigger_data_quality


with DAG(
    dag_id = "data_quality",
    default_args=default_args,
    description="Dag to check data quality of staging and core tables using Soda",
    schedule= None,
    catchup=False,
) as dag_quality:
    soda_validate_staging = yt_elt_data_quality("staging")
    soda_validate_core = yt_elt_data_quality("core")

    soda_validate_staging >> soda_validate_core