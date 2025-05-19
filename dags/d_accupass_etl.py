from datetime import datetime, timedelta


from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from accupass.e01_accupass_crawler import e_accupass_crawler
from accupass.l01_accupass_mysql_con import l_accupass_mysql_con
from accupass.t01_accupass_data_clean import t_accupass_data_clean
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

with DAG(
    dag_id="d_accupass_etl",
    default_args=default_args,
    description="ETL: extract accupass data, clean it, and load to MySQL.",
    schedule_interval="0 4 * * 0",
    start_date=datetime(2025, 5, 10),
    catchup=False,
    tags=["accupass", "etl", "mysql"]
) as dag:

    task1 = PythonOperator(
        task_id="crawler",
        python_callable=e_accupass_crawler,
    )

    task2 = PythonOperator(
        task_id="data_clean",
        python_callable=t_accupass_data_clean,
    )

    task3 = PythonOperator(
        task_id="to_sql",
        python_callable=l_accupass_mysql_con,
    )

    task1 >> task2 >> task3
