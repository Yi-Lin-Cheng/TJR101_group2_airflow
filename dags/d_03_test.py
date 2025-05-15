from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from spot.e03_spot_craw_googlemap_info import main as craw_googlemap
from spot.t03_spot_clean_openhours_name import main as clean_openhours
from spot.l_spot_insert_into_and_update_mysql import main as load_mysql

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="d_03_test",
    default_args=default_args,
    description="An example DAG with Python operators",
    schedule_interval="10 13 * * 1",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["example", "decorator"],  # Optional: Add tags for better filtering in the UI
)
def d_03_test():
    @task
    def test1():
        craw_googlemap()
    @task
    def test2():
        clean_openhours()
    @task
    def test3():
        load_mysql()

    test1() >> test2() >> test3()


d_03_test()
