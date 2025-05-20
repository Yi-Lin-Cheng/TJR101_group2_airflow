from datetime import datetime, timedelta


from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


from klook.e_data_list import main as e_data_list_task
from klook.e_data_detail import main as e_data_detail_task
from klook.t_address import main as t_address_task
from klook.e_coordinate import main as e_coordinate_task
from klook.t_county import main as t_county_task
from klook.t_date import main as t_date_task
from klook.t_title import main as t_title_task
from klook.l_data_to_db import main as l_data_to_db_task

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
    dag_id="d_klook_etl_02",
    default_args=default_args,
    description="ETL: extract Klook data, clean it, and load to MySQL.",
    schedule_interval="*/30 5-23 * * *",
    start_date=datetime(2025, 5, 10),
    catchup=False,
    tags=["klook", "etl", "mysql"]
) as dag:

    # Define the tasks
    task_e_data_detail_obj = PythonOperator(
        task_id='e_data_detail_task',
        python_callable=e_data_detail_task,
        op_kwargs={
            'source_file': 'final_data.csv',
            'save_file': 'final_data.csv'
        },
        dag=dag,
    )

    task_t_address_obj = PythonOperator(
        task_id='t_address_task',
        python_callable=t_address_task,
        op_kwargs={
            'source_file': 'final_data.csv',
            'save_file': 'final_data.csv'
        },    
        dag=dag,
    )

    task_e_coordinate_obj = PythonOperator(
        task_id='e_coordinate_task',
        python_callable=e_coordinate_task,
        op_kwargs={
            'source_file': 'final_data.csv',
            'save_file': 'final_data.csv'
        },       
        dag=dag,
    )
    
    task_l_data_to_db_obj = PythonOperator(
        task_id='l_data_to_db_task',
        python_callable=l_data_to_db_task,
        op_kwargs={
            'source_file': 'final_data.csv',
        },       
        dag=dag,
    )    


    # Task dependencies
    task_e_data_detail_obj >> task_t_address_obj >> task_e_coordinate_obj >> task_l_data_to_db_obj
    # task1_obj >> task2_obj