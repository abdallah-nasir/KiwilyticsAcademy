from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

PG_CONN_ID = "pg_default"

default_args = {
    "owner": "admin",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def get_conn():
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    return hook.get_conn()

# PostgreSQL
# Total daily revenue
#     - task1 -> read the data from tables
#     - task2 -> load the data to csv
#     - task3 -> calculate the total daily revenue and export it to a file
#     - task4 -> plot the daily revenue


READ_DATA_SQL = """
    select o.orderdate::date, od.productid, p.productname, od.quantity, p.price
    from orders o
    join order_details od on o.orderid = od.orderid
    join products p on od.productid = p.productid
"""
# task1
def read_data():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(READ_DATA_SQL)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    print("data", data)
    return data

# task2
def load_data_to_csv(data: list):
    data = pd.DataFrame(data, columns=["orderdate", "productid", "productname", "quantity", "price"])
    data.to_csv("/home/airflow/output.csv", index=False)

# task3
def calculate_daily_revenue():
    data = pd.read_csv("/home/airflow/output.csv")
    data["revenue"] = data["quantity"] * data["price"]
    daily_revenue = data.groupby("orderdate")["revenue"].sum().reset_index()
    daily_revenue.to_csv("/home/airflow/daily_revenue.csv", index=False)
    return daily_revenue.to_dict("records")

with DAG(
    dag_id="daily_revenue",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    description="Daily revenue calculation",
) as dag:
    t1 = PythonOperator(
        task_id="read_data",
        python_callable=read_data,
    )

    t2 = PythonOperator(
        task_id="load_data_to_csv",
        python_callable=load_data_to_csv,
        op_kwargs={"data": t1.output},
    )

    t3 = PythonOperator(
        task_id="calculate_daily_revenue",
        python_callable=calculate_daily_revenue,
    )

    t1 >> t2 >> t3

