from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from food import (e01_get_opensource, e02_googleapi_text_search,
                  t01_filter_new_row, t02_compare_name_and_add)
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
    dag_id="d_food_etl_01_02",
    default_args=default_args,
    description="Extract and transform from open source and Google Place, then match and add new rows.",
    schedule_interval="0 1 * * 6",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["food", "etl", "open_data", "google"],
)
def d_food_etl_01_02():
    @task
    def extract_opensource():
        e01_get_opensource()

    @task
    def filter_new_rows():
        t01_filter_new_row()

    @task
    def googleapi_search():
        e02_googleapi_text_search()

    @task
    def compare_and_add():
        t02_compare_name_and_add()

    extract_opensource() >> filter_new_rows() >> googleapi_search() >> compare_and_add()


d_food_etl_01_02()
