from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pandas as pd
import sqlalchemy
import hashlib
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 4),
    'retries': 1
}

dag = DAG(
    'stg_load_with_verification',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['stg', 'load', 'verification']
)

SOURCE_FILES = {
    'Товары': '/opt/airflow/data/input/Товары.xlsx',
    'Транзакции': '/opt/airflow/data/input/Транзакции.xlsx', 
    'Торговые Точки': '/opt/airflow/data/input/Торговые Точки.xlsx',
    'Контрагенты': '/opt/airflow/data/input/Контрагенты.xlsx',
    'Договор': '/opt/airflow/data/input/Договор.xls'
}

STG_TABLES = {
    'Товары': 'stg_products',
    'Транзакции': 'stg_transactions',
    'Торговые Точки': 'stg_stores',
    'Контрагенты': 'stg_counterparties',
    'Договор': 'stg_contracts'
}

engine = sqlalchemy.create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')

def create_stg_tables():
    """Создает STG таблицы из SQL файла"""
    
    try:
        with open('/opt/airflow/sql/create_tables.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
    except FileNotFoundError:
        with open('/opt/airflow/dags/sql/create_tables.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
    
    sql_commands = sql_script.split(';')
    
    with engine.begin() as conn:
        for command in sql_commands:
            command = command.strip()
            if command:
                print(f"Выполняется: {command[:50]}...")
                conn.execute(sqlalchemy.text(command))
    
    print("Все STG таблицы созданы или уже существуют")

def hash_dataframe(df):
    """Улучшенное хэширование с нормализацией для сравнения"""

    df_sorted = df.sort_values(by=df.columns.tolist()).reset_index(drop=True)
    
    normalized_data = ''
    for _, row in df_sorted.iterrows():
        normalized_row = ''
        for value in row:
            if pd.isna(value):
                normalized_value = ''
            else:
                normalized_value = str(value).strip()
            normalized_row += normalized_value
        
        normalized_data += normalized_row
    
    return hashlib.md5(normalized_data.encode()).hexdigest()

def load_table(table_name, **kwargs):
    source_file = SOURCE_FILES[table_name]
    stg_table = STG_TABLES[table_name]
    
    print(f"Загрузка {table_name} из {source_file} в {stg_table}")
    
    try:
        if source_file.endswith('.xls'):
            df = pd.read_excel(source_file, engine='xlrd')
        else:
            df = pd.read_excel(source_file, engine='openpyxl')
        
        print(f"Прочитано {len(df)} строк из {source_file}")
        
        source_hash = hash_dataframe(df)
        print(f"Хэш исходных данных: {source_hash}")
        
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(f"DELETE FROM {stg_table};"))
            print(f"Таблица {stg_table} очищена")
        
        df.to_sql(stg_table, engine, if_exists='append', index=False)
        print(f"Данные записаны в {stg_table}")
        

        stg_df = pd.read_sql(f"SELECT * FROM {stg_table}", engine)
        
        stg_hash = hash_dataframe(stg_df)
        print(f"Хэш данных в БД: {stg_hash}")
        
        if source_hash != stg_hash:
            raise ValueError(f"Данные в {stg_table} не совпадают с источником!")
        
        print(f"Проверка пройдена: {table_name} загружено успешно!")
        return f"Успешно загружено {len(df)} строк в {stg_table}"
    
    except Exception as e:
        print(f"Ошибка при загрузке {table_name}: {str(e)}")
        raise

start_task = DummyOperator(task_id='start', dag=dag)

create_tables_task = PythonOperator(
    task_id='create_stg_tables',
    python_callable=create_stg_tables,
    dag=dag
)

load_tasks = {}

for table_name in SOURCE_FILES.keys():
    task_id = f'load_{table_name.lower().replace(" ", "_")}'
    
    load_task = PythonOperator(
        task_id=task_id,
        python_callable=load_table,
        op_kwargs={'table_name': table_name},
        dag=dag
    )
    
    load_tasks[table_name] = load_task

trigger_ods_dag = TriggerDagRunOperator(
    task_id='trigger_ods_load',
    trigger_dag_id='ods_scd2_load',
    wait_for_completion=False,  # Не ждать завершения ODS DAG
    dag=dag
)

start_task >> create_tables_task >> load_tasks['Товары'] >> load_tasks['Торговые Точки'] >> load_tasks['Контрагенты'] >> load_tasks['Договор'] >> load_tasks['Транзакции'] >> trigger_ods_dag