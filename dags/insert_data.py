from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from sqlalchemy import inspect
import pandas as pd

# Получаем текущую дату и время
date_now = datetime.now()

# Получаем путь из переменных Airflow
PATH = Variable.get('my_path')
# Устанавливаем путь для поиска шаблонов
conf.set('core', 'template_searchpath', PATH)

# Список таблиц для обработки
tables = [
    'ft_balance_f',
    'ft_posting_f',
    'md_account_d',
    'md_currency_d',
    'md_exchange_rate_d',
    'md_ledger_account_s'
]


# Функция для получения первичных ключей таблицы
def get_primary_key(table_name, schema='DS', conn_id='postgres-db'):
    # Создаем подключение к базе данных с использованием PostgresHook
    hook = PostgresHook(conn_id)
    # Получаем SQLAlchemy engine для работы с базой данных
    engine = hook.get_sqlalchemy_engine()
    # Создаем инспектор для получения метаданных таблицы
    inspector = inspect(engine)
    # Получаем список столбцов, которые являются первичными ключами
    pk_columns = inspector.get_pk_constraint(table_name, schema=schema)['constrained_columns']
    # Возвращаем список первичных ключей
    return pk_columns


def get_table_columns(table_name, schema='DS', conn_id='postgres-db'):
    # Создаем подключение к базе данных с использованием PostgresHook
    hook = PostgresHook(conn_id)
    # Получаем SQLAlchemy engine для работы с базой данных
    engine = hook.get_sqlalchemy_engine()
    # Создаем инспектор для получения метаданных таблицы
    inspector = inspect(engine)
    # Получаем список столбцов таблицы
    columns = [col['name'] for col in inspector.get_columns(table_name, schema=schema)]
    return columns

# Функция для записи логов выполнения задач
def uploading_logs(context):
    # Получаем имя текущей задачи
    task_instance = context['task_instance'].task_id
    # Получаем статус выполнения задачи
    status = context['task_instance'].state
    # Получаем время выполнения задачи и преобразуем его в timestamp
    ts = context['task_instance'].execution_date.timestamp()
    # Преобразуем timestamp в строку в формате ISO
    ts = datetime.fromtimestamp(ts).isoformat(sep='T')

    # Указываем схему и таблицу для записи логов
    logs_schema = 'logs'
    logs_table = 'csv_to_dag'

    # Формируем SQL-запрос для вставки данных в таблицу логов
    query = f"""
        INSERT INTO {logs_schema}.{logs_table} (execution_datetime, event_datetime, event_name, event_status)
        VALUES ('{ts}', '{datetime.now().isoformat(sep='T')}', '{task_instance}', '{status}');
    """

    # Используем метод run для выполнения запроса
    pg_hook = PostgresHook(postgres_conn_id='postgres-db')

    # Выполняем SQL-запрос
    pg_hook.run(query)


# Функция для загрузки данных с использованием COPY
def load_data_with_copy(table_name, schema='DS', conn_id='postgres-db'):
    pg_hook = PostgresHook(conn_id)

    # Получаем первичные ключи
    primary_keys = get_primary_key(table_name, schema, conn_id)

    # Читаем CSV-файл
    csv_path = f'{PATH}{table_name}.csv'
    try:
        df = pd.read_csv(csv_path, delimiter=';', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, delimiter=';', encoding='Windows-1252')

    # Преобразуем имена столбцов в нижний регистр
    df.columns = [col.lower() for col in df.columns]

    # Преобразуем даты из формата DD.MM.YYYY в YYYY-MM-DD
    for col in df.columns:
        if df[col].dtype == 'object':  # Проверяем, является ли столбец строковым
            try:
                # Пытаемся преобразовать в дату (формат DD.MM.YYYY)
                df[col] = pd.to_datetime(df[col], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')
            except ValueError:
                continue  # Если не удалось преобразовать, оставляем как есть

    # Удаляем дубликаты по первичному ключу
    df = df.drop_duplicates(subset=primary_keys)

    # Получаем имена всех столбцов
    column_names = df.columns.tolist()

    # Определяем столбцы для обновления
    update_columns = [col for col in column_names if col not in primary_keys]
    update_set = ", ".join([f'"{col}"=EXCLUDED."{col}"' for col in update_columns])
    primary_keys = ', '.join([f'"{pk}"' for pk in primary_keys])

    sql = f"""
        begin;
        create temporary table tmp_table 
        (like "{schema}"."{table_name}" including defaults)
        on commit drop;

        copy tmp_table from stdin delimiter ';' csv header;

        insert into "{schema}"."{table_name}"
        select *
        from tmp_table
        on conflict ({primary_keys}) do update
        set {update_set};
        commit;
    """

    # Сохраняем DataFrame во временный CSV-файл с преобразованными датами
    temp_csv_path = f'{PATH}{table_name}_temp.csv'
    df.to_csv(temp_csv_path, index=False, sep=';', encoding='utf-8')

    # Выполняем SQL-запрос
    pg_hook.copy_expert(sql, temp_csv_path)

    # Удаляем временный файл
    import os
    os.remove(temp_csv_path)


def load_md_ledger_account_s(table_name, schema='DS', conn_id='postgres-db'):
    pg_hook = PostgresHook(conn_id)

    # Получаем первичные ключи
    primary_keys = get_primary_key(table_name, schema, conn_id)

    # Читаем CSV-файл
    csv_path = f'{PATH}{table_name}.csv'
    try:
        df = pd.read_csv(csv_path, delimiter=';', encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, delimiter=';', encoding='Windows-1252')

    # Преобразуем имена столбцов в нижний регистр
    df.columns = [col.lower() for col in df.columns]

    # Получаем список столбцов таблицы
    columns_in_table = get_table_columns(table_name, schema, conn_id)

    # Фильтруем столбцы, которые есть в CSV
    columns_to_load = [col for col in columns_in_table if col in df.columns]
    columns_str = ', '.join([f'"{col}"' for col in columns_to_load])

    # Сохраняем DataFrame во временный CSV-файл
    temp_csv_path = f'{PATH}{table_name}_temp.csv'
    df.to_csv(temp_csv_path, index=False, sep=';', encoding='utf-8')

    # Формируем SQL-запрос
    sql = f"""
        BEGIN;
        CREATE TEMPORARY TABLE tmp_table 
        (LIKE "{schema}"."{table_name}" INCLUDING DEFAULTS)
        ON COMMIT DROP;

        COPY tmp_table ({columns_str}) FROM STDIN DELIMITER ';' CSV HEADER;

        INSERT INTO "{schema}"."{table_name}" ({columns_str})
        SELECT {columns_str}
        FROM tmp_table
        ON CONFLICT ({', '.join([f'"{pk}"' for pk in primary_keys])}) DO UPDATE
        SET {', '.join([f'"{col}"=EXCLUDED."{col}"' for col in columns_to_load if col not in primary_keys])};
        COMMIT;
    """
    # Выполняем SQL-запрос
    pg_hook.copy_expert(sql, temp_csv_path)

    # Удаляем временный файл
    import os
    os.remove(temp_csv_path)


# Функция для загрузки данных в таблицу ft_posting_f
def load_ft_posting_f():
    table_name = 'ft_posting_f'
    schema = 'DS'
    conn_id = 'postgres-db'

    # Подключаемся к базе данных
    pg_hook = PostgresHook(conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Очищаем таблицу перед вставкой данных
    cursor.execute(f'TRUNCATE TABLE "{schema}"."{table_name}";')

    # Читаем CSV-файл
    df = pd.read_csv(f'{PATH}{table_name}.csv', delimiter=';')

    # Приводим имена столбцов к нижнему регистру
    df.columns = [col.lower() for col in df.columns]

    # Преобразуем дату из формата DD-MM-YYYY в YYYY-MM-DD
    df['oper_date'] = pd.to_datetime(df['oper_date'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')

    # Формируем SQL-запрос для вставки данных
    columns = ', '.join([f'"{col}"' for col in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))

    query = f"""
        INSERT INTO "{schema}"."{table_name}" ({columns})
        VALUES ({placeholders});
    """

    # Вставляем данные массово
    data_tuples = [tuple(row) for row in df.to_numpy()]
    cursor.executemany(query, data_tuples)

    # Фиксируем изменения
    conn.commit()

    # Закрываем соединение
    conn.close()


# Аргументы по умолчанию для DAG
default_args = {
    'owner': 'dima',
    'start_date': date_now,
    'retries': 2,
}

# Создание DAG
with DAG(
        'insert_data',
        default_args=default_args,
        description='Мой первый DAG',
        catchup=False,
        max_active_tasks=32,
        template_searchpath=[PATH],
        schedule='0 0 * * *',
) as dag:
    # Задача для логирования начала ETL
    logs_etl_started = DummyOperator(
        task_id='logs_etl_started',
        on_success_callback=uploading_logs
    )

    # Задача для создания схемы DS
    create_sheme = SQLExecuteQueryOperator(
        task_id='create_sheme',
        conn_id='postgres-db',
        sql='sql/create_sheme_ds.sql',
        on_success_callback=uploading_logs
    )

    # Задача для создания схемы logs
    create_logs_sheme = SQLExecuteQueryOperator(
        task_id='create_logs_sheme',
        conn_id='postgres-db',
        sql='sql/create_logs_sheme.sql',
        on_success_callback=uploading_logs
    )

    # Пустая задача для разделения этапов
    pause = EmptyOperator(
        task_id='pause',
        on_success_callback=uploading_logs

    )

    # Создаем задачи для каждой таблицы
    tasks = []
    for table in tables:
        if table == 'ft_posting_f':
            task = PythonOperator(
                task_id=table,
                python_callable=load_ft_posting_f,
                on_success_callback=uploading_logs
            )
        elif table == 'md_ledger_account_s':
            task = PythonOperator(
                task_id=table,
                python_callable=load_md_ledger_account_s,
                op_kwargs={'table_name': table},  # Передаем имя таблицы
                on_success_callback=uploading_logs
            )
        else:
            task = PythonOperator(
                task_id=table,
                python_callable=load_data_with_copy,
                op_kwargs={'table_name': table},
                on_success_callback=uploading_logs
            )
        tasks.append(task)

    # Задача для логирования начала ETL
    logs_etl_end = DummyOperator(
        task_id='logs_etl_end',
        on_success_callback=uploading_logs
    )

    # Определение порядка выполнения задач
    (
            logs_etl_started
            >> [create_sheme, create_logs_sheme]
            >> pause
            >> tasks
            >> logs_etl_end
    )