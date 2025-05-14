from datetime import timedelta

import pendulum

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils import get_connection,close_connection





# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "email": ["your_email@example.com"],
    # "email_on_failure": False,
    # "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="d_01_test",
    default_args=default_args,
    description="An example DAG with Python operators",
    schedule_interval="10 13 * * 1",
    start_date=pendulum.datetime(2025, 5, 1, tz="Asia/Taipei"),
    catchup=False,
    tags=["example", "decorator"],  # Optional: Add tags for better filtering in the UI
)
def d_01_test():
    @task
    def test1():
        # 開啟連線
        conn,cursor = get_connection()

        # 關閉連線
        close_connection(conn,cursor)

    # trigger_next = TriggerDagRunOperator(
    #     task_id="test1",
    #     trigger_dag_id="d_02_test",  # 要觸發的 DAG ID
    #     wait_for_completion=True,  # 或 True（等它跑完）
    # )

    test1() 

d_01_test()

# @task
# def e_data_source_1() -> pd.DataFrame:
#     print("Getting df1.")
#     return pd.DataFrame(data=[[1], [2]], columns=["col"])

# @task
# def e_data_source_2() -> pd.DataFrame:
#     print("Getting df2.")
#     return pd.DataFrame(data=[[3], [4]], columns=["col"])

# @task
# def t_concat(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
#     print("Concating df1 and df2.")
#     return pd.concat([df1, df2]).reset_index(drop=True)

# @task
# def l_db1(df: pd.DataFrame) -> None:
#     print("Loading df to db1.")
#     print(df)
#     print("===============")

# @task
# def l_db2(df: pd.DataFrame) -> None:
#     print("Loading df to db2.")
#     print(df)
#     print("===============")

# # Task dependencies defined by calling the tasks in sequence
# df1 = e_data_source_1()
# df2 = e_data_source_2()
# df = t_concat(df1, df2)
# l_db1(df)
# l_db2(df)

# Instantiate the DAG
