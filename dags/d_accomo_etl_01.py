from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from accomo import (e01_get_opensource, e02_crawler_booking,
                    l01_add_row_to_mysql, t01_filter_new_row,
                    t02_clean_name_and_add, t03_match_name_and_disc,
                    t04_select_column)
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
    dag_id="d_accomo_etl_01",
    default_args=default_args,
    description="Extract and transform from open source and Booking.com, then match and add new rows.",
    schedule_interval="0 1 * * 1,4",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["accomo", "etl", "open_data", "booking", "mysql"],
)
def d_accomo_etl_01():
    @task
    def extract_opensource():
        e01_get_opensource()
    @task
    def filter_new_rows():
        t01_filter_new_row()
    @task
    def crawler_booking():
        e02_crawler_booking()
    @task
    def clean_name_and_add():
        t02_clean_name_and_add()
    @task
    def match_name_and_disc():
        t03_match_name_and_disc()
    @task
    def select_column():
        t04_select_column()
    @task
    def load_mysql():
        l01_add_row_to_mysql()

    t1 = extract_opensource()
    t2 = filter_new_rows()
    t3 = crawler_booking()
    t4 = clean_name_and_add()
    t5 = match_name_and_disc()
    t6 = select_column()
    t7 = load_mysql()

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7


d_accomo_etl_01()
