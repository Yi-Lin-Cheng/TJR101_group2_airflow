from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from accupass.e01_accupass_crawler import e_accupass_crawler
from accupass.t01_accupass_data_clean import t_accupass_data_clean
from accupass.l01_accupass_mysql_con import l_accupass_mysql_con

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="d_accupass_etl",
    default_args=default_args,
    description="ETL: extract accupass data, clean it, and load to MySQL.",
    schedule_interval="30 12 * * 7",
    start_date=datetime(2025, 5, 10),
    catchup=False,
    tags=["crawler", "clean", "to_mySQL"]
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
