from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pandas as pd
import sqlalchemy

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

dag = DAG(
    'ods_scd2_load',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['ods', 'scd2', 'historical']
)

ODS_TABLES = {
    'stg_products': 'ods_products',
    'stg_transactions': 'ods_transactions',
    'stg_stores': 'ods_stores', 
    'stg_counterparties': 'ods_counterparties',
    'stg_contracts': 'ods_contracts'
}

BUSINESS_KEYS = {
    'stg_products': 'КодТовара',
    'stg_transactions': ['КодЗаказа', 'КодТовара', 'ТорговаяТочка', 'Дата'], 
    'stg_stores': 'ДоговорКонтрагента',
    'stg_counterparties': ['КодКонтрагента', 'Контрагент', 'Менеджер', 'Город', 'Область', 'АдресТТ', 'Филиал'],
    'stg_contracts': ['КодТорговойТочки', 'ДоговорКонтрагента'] 
}

engine = sqlalchemy.create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/airflow')


def create_ods_tables():
    """Создает ODS таблицы из SQL файла"""
    
    # Читаем SQL файл
    try:
        with open('/opt/airflow/sql/ods_tables.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
    except FileNotFoundError:
        with open('/opt/airflow/dags/sql/ods_tables.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()
    

    sql_commands = sql_script.split(';')
    
    with engine.begin() as conn:
        for command in sql_commands:
            command = command.strip()
            if command:
                print(f"Выполняется: {command[:50]}...")
                conn.execute(sqlalchemy.text(command))
    
    print("Все ODS таблицы созданы")

def values_equal(v1, v2):
    if pd.isna(v1) and pd.isna(v2):
        return True
    if pd.isna(v1) or pd.isna(v2):
        return False
    
    str_v1 = str(v1).strip().lower() if v1 is not None else ""
    str_v2 = str(v2).strip().lower() if v2 is not None else ""
    
    try:
        num_v1 = float(v1) if v1 is not None else None
        num_v2 = float(v2) if v2 is not None else None
        if num_v1 is not None and num_v2 is not None:
            return abs(num_v1 - num_v2) < 0.000001
    except (ValueError, TypeError):
        pass
    
    return str_v1 == str_v2

def create_composite_key(row, keys):
    """Создает составной ключ с обработкой пустых строк"""
    key_parts = []
    for key in keys:
        value = row[key]
        if pd.isna(value) or str(value).strip() == '':
            key_parts.append('EMPTY')
        else:
            key_parts.append(str(value).strip().lower())
    return '|'.join(key_parts)

def scd2_load_final_fixed(table_name, **kwargs):
    stg_table = table_name
    ods_table = ODS_TABLES[table_name]
    business_keys = BUSINESS_KEYS[table_name]
    
    if not isinstance(business_keys, list):
        business_keys = [business_keys]
    
    print(f"SCD2 загрузка: {stg_table} -> {ods_table}")
    print(f"Бизнес-ключ: {business_keys}")
    
    try:
        stg_df = pd.read_sql(f"SELECT * FROM {stg_table}", engine)
        current_ods_df = pd.read_sql(f"SELECT * FROM {ods_table} WHERE is_current = TRUE", engine)
        
        print(f"Прочитано {len(stg_df)} строк из {stg_table}")
        print(f"Прочитано {len(current_ods_df)} актуальных строк из {ods_table}")
        
        for key in business_keys:
            if key not in stg_df.columns:
                raise KeyError(f"Бизнес-ключ '{key}' не найден в таблице {stg_table}")
            if key not in current_ods_df.columns:
                raise KeyError(f"Бизнес-ключ '{key}' не найден в таблице {ods_table}")
            
            empty_count = stg_df[key].apply(lambda x: str(x).strip() == '').sum()
            if empty_count > 0:
                print(f"ВНИМАНИЕ: Найдено {empty_count} пустых строк в поле {key}")
        

        stg_df['composite_key'] = stg_df.apply(lambda row: create_composite_key(row, business_keys), axis=1)
        current_ods_df['composite_key'] = current_ods_df.apply(lambda row: create_composite_key(row, business_keys), axis=1)
        

        unique_stg_keys = stg_df['composite_key'].nunique()
        if unique_stg_keys != len(stg_df):
            print(f"ВНИМАНИЕ: Найдено {len(stg_df) - unique_stg_keys} дубликатов в STG")
            # Логируем примеры дубликатов
            duplicates = stg_df[stg_df.duplicated(['composite_key'], keep=False)]
            if len(duplicates) > 0:
                print("Примеры дубликатов:")
                for key in duplicates['composite_key'].unique()[:3]:
                    sample = duplicates[duplicates['composite_key'] == key][business_keys].head(2)
                    print(f"  Ключ: {key}")
                    print(f"    Данные: {sample.to_dict('records')}")
            
        

        unique_ods_keys = current_ods_df['composite_key'].nunique()
        if unique_ods_keys != len(current_ods_df):
            print(f"ВНИМАНИЕ: Найдено {len(current_ods_df) - unique_ods_keys} дубликатов в ODS")
            current_ods_df = current_ods_df.sort_values('effective_from', ascending=False)
            current_ods_df = current_ods_df.drop_duplicates(subset=['composite_key'], keep='first')
            print(f"В ODS оставлено {len(current_ods_df)} уникальных записей")
        
        changes_found = 0
        new_records = 0
        deleted_records = 0
        
        with engine.begin() as conn:
            for index, stg_record in stg_df.iterrows():
                composite_key = stg_record['composite_key']
                
                matching_ods = current_ods_df[current_ods_df['composite_key'] == composite_key]
                
                if len(matching_ods) > 0:
                    ods_record = matching_ods.iloc[0]
                    has_changes = False
                    

                    compare_columns = [col for col in stg_df.columns 
                                     if col not in ['composite_key', 'surrogate_key', 'effective_from', 'effective_to', 'is_current'] + business_keys]
                    
                    for column in compare_columns:
                        if column in ods_record:
                            if not values_equal(stg_record[column], ods_record[column]):
                                has_changes = True
                                break
                    
                    if has_changes:
                        where_conditions = ' AND '.join([f'"{key}" = %s' for key in business_keys])
                        update_query = f"""
                            UPDATE {ods_table} 
                            SET effective_to = NOW(), is_current = FALSE 
                            WHERE {where_conditions} AND is_current = TRUE
                        """
                        params = [stg_record[key] for key in business_keys]
                        conn.execute(update_query, params)
                        
                        new_record = {}
                        for col in compare_columns + business_keys:
                            new_record[col] = stg_record[col]
                        new_record['effective_from'] = datetime.now()
                        new_record['effective_to'] = datetime(9999, 12, 31)
                        new_record['is_current'] = True
                        
                        pd.DataFrame([new_record]).to_sql(ods_table, conn, if_exists='append', index=False)
                        changes_found += 1
                
                else:
                    compare_columns = [col for col in stg_df.columns 
                                     if col not in ['composite_key', 'surrogate_key', 'effective_from', 'effective_to', 'is_current'] + business_keys]
                    
                    new_record = {}
                    for col in compare_columns + business_keys:
                        new_record[col] = stg_record[col]
                    new_record['effective_from'] = datetime.now()
                    new_record['effective_to'] = datetime(9999, 12, 31)
                    new_record['is_current'] = True
                    
                    pd.DataFrame([new_record]).to_sql(ods_table, conn, if_exists='append', index=False)
                    new_records += 1
            

            for index, ods_record in current_ods_df.iterrows():
                composite_key = ods_record['composite_key']
                if composite_key not in stg_df['composite_key'].values:
                    where_conditions = ' AND '.join([f'"{key}" = %s' for key in business_keys])
                    update_query = f"""
                        UPDATE {ods_table} 
                        SET effective_to = NOW(), is_current = FALSE 
                        WHERE {where_conditions} AND is_current = TRUE
                    """
                    params = [ods_record[key] for key in business_keys]
                    conn.execute(update_query, params)
                    deleted_records += 1
        
        print(f"РЕЗУЛЬТАТ: Изменений: {changes_found}, Новых: {new_records}, Удалено: {deleted_records}")
        
        final_count = pd.read_sql(f"SELECT COUNT(*) as count FROM {ods_table} WHERE is_current = TRUE", engine).iloc[0]['count']
        print(f"Проверка: {final_count} актуальных записей в ODS")
        
        if final_count == len(stg_df):
            print("SCD2 загрузка завершена успешно")
        else:
            print(f"Расхождение: ODS ({final_count}) записей != STG ({len(stg_df)}) записей")
        
    except Exception as e:
        print(f"ОШИБКА при SCD2 загрузке {ods_table}: {str(e)}")
        import traceback
        print(f"Трассировка: {traceback.format_exc()}")
        raise

# Создаем задачи
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)


create_ods_tables_task = PythonOperator(
    task_id='create_ods_tables',
    python_callable=create_ods_tables,
    dag=dag
)

scd2_tasks = []

for stg_table in ODS_TABLES.keys():
    task_id = f'scd2_load_{stg_table}'
    
    scd2_task = PythonOperator(
        task_id=task_id,
        python_callable=scd2_load_final_fixed,
        op_kwargs={'table_name': stg_table},
        dag=dag
    )
    
    scd2_tasks.append(scd2_task)


start_task >> create_ods_tables_task >> scd2_tasks >> end_task