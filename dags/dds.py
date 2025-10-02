from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import os

SQL_DIR = "/opt/airflow/sql/dds"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="dds_load",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["dds", "snowflake", "scd1"],
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    def read_sql(file_name):
        path = os.path.join(SQL_DIR, file_name)
        if not os.path.exists(path):
            raise FileNotFoundError(f"SQL файл не найден: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    create_tables = PostgresOperator(
        task_id="dds_create_tables",
        postgres_conn_id="airflow_postgres",
        sql=read_sql("create_dds_tables.sql"),
    )

    check_tables = PostgresOperator(
        task_id="dds_check_tables",
        postgres_conn_id="airflow_postgres",
        sql="""
        SELECT 
            table_name,
            CASE 
                WHEN EXISTS (SELECT FROM information_schema.tables 
                            WHERE table_name = 'brand' AND table_schema = 'public') 
                THEN 'SUCCESS' 
                ELSE 'MISSING' 
            END as status
        FROM (VALUES ('brand')) AS t(table_name);
        """,
    )

    sql_files = [
        "load_manufacturer.sql",
        "load_brand.sql",
        "load_product_group.sql",
        "load_product_line.sql",
        "load_product.sql",
        "load_city.sql",
        "load_counterparty.sql",
        "load_store.sql",
        "load_contract.sql",
        "load_supplier.sql",
        "load_branch.sql",
        "load_manager.sql",
        "load_deal_type.sql",
        "load_delivery_type.sql",
        "load_transaction_type.sql",
        "load_orders.sql",
        "load_transactions.sql"
    ]

    first_load_task = None
    load_tasks = []

    for file in sql_files:
        sql_content = read_sql(file)
        task_id = f"dds_{file.replace('load_', '').replace('.sql', '')}"
        task = PostgresOperator(
            task_id=task_id,
            postgres_conn_id="airflow_postgres",
            sql=sql_content,
        )
        load_tasks.append(task)
        
        if first_load_task is None:
            first_load_task = task

    start >> create_tables >> check_tables
    check_tables >> first_load_task
    
    for i in range(len(load_tasks) - 1):
        load_tasks[i] >> load_tasks[i + 1]
    
    load_tasks[-1] >> end