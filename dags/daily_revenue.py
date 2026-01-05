from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from matplotlib import pyplot as plt

PG_CONN_ID = "pg_default"

# make the current project folder path
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
--  filter with year 1996 only
where o.orderdate::date between '1996-08-01' and '1996-08-31'
"""
# task1
def read_data():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(READ_DATA_SQL)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
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
    return data

# task 4 export revenue plot to png
def plot_daily_revenue(data: list):
    data = pd.DataFrame(data, columns=["orderdate", "revenue"])
    data["orderdate"] = pd.to_datetime(data["orderdate"]).dt.date
    data.plot(x="orderdate", y="revenue", kind="line")
    plt.savefig("/home/airflow/daily_revenue.png")
    plt.show()

# task 5 total revenue on specific date 1996-08-08
def calculate_total_revenue_on_date(date: str="1996-08-08"):
    data = pd.read_csv("/home/airflow/daily_revenue.csv")
    total_revenue = data[data["orderdate"] == date]["revenue"].sum()
    return total_revenue

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
    t4 = PythonOperator(
        task_id="plot_daily_revenue",
        python_callable=plot_daily_revenue,
        op_kwargs={"data": t3.output},
    )
    t5 = PythonOperator(
        task_id="calculate_total_revenue_on_date",
        python_callable=calculate_total_revenue_on_date,
    )

    t1 >> t2 >> t3 >> t4 >> t5

