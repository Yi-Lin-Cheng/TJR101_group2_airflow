from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from spot import (e03_craw_googlemap_info, l01_insert_into_and_update_mysql,
                  t03_clean_openhours_name)
from utils.airflow_notify import line_notify_failure

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": [Variable.get("EMAIL")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": line_notify_failure,
}


@dag(
    dag_id="d_spot_etl_03",
    default_args=default_args,
    description="ETL: extract Google Map data, clean it, and load to MySQL.",
    schedule_interval="0 2 * * 4",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Taipei"),
    catchup=False,
    default_args=default_args,
    tags=["spot", "etl", "mysql", "google"],
)
def d_spot_etl_03():
    @task
    def extract_googlemap():
        e03_craw_googlemap_info()
    @task
    def clean_openhours_name():
        t03_clean_openhours_name()
    @task
    def load_mysql():
        l01_insert_into_and_update_mysql()

    extract_googlemap() >> clean_openhours_name() >> load_mysql()

d_spot_etl_03()
