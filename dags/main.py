from airflow import DAG
from datawarehouse.dwh import core_table, staging_table
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

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
) as dag:
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    playlist_id >> video_ids >> extracted_data >> save_to_json_task



with DAG(
    dag_id = "update_db",
    default_args=default_args,
    description="Dag to process json data and update staging and core tables in the data warehouse",
    schedule= "0 15 * * *",
    catchup=False,
) as dag:
    update_staging = staging_table()
    update_core = core_table()

    update_staging >> update_core