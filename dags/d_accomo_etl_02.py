from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from accomo import (e03_update_rate, l02_update_rate)
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
    dag_id="d_accomo_etl_02",
    default_args=default_args,
    description="Update data from Booking.com.",
    schedule_interval="0 2 * * 1,4",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["accomo", "booking", "mysql"],
)
def d_accomo_etl_02():
    @task
    def update_rate():
        e03_update_rate()
    @task
    def load_mysql():
        l02_update_rate()

    update_rate() >> load_mysql()


d_accomo_etl_02()
