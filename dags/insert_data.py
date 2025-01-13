from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from sqlalchemy import inspect
import pandas as pd
import os
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


# Функция для получения столбцов
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
    task_instance = context['task_instance'].task_id
    status = context['task_instance'].state
    execution_ts = context['task_instance'].start_date
    event_ts = datetime.now()

    exception = context.get('exception', None)
    error_message = str(exception) if exception else None

    logs_schema = 'logs'
    logs_table = 'csv_to_dag'

    query = f"""
        INSERT INTO {logs_schema}.{logs_table} 
            (execution_datetime, event_datetime, event_name, event_status, error_message)
        VALUES 
            ('{execution_ts}', '{event_ts}', '{task_instance}', '{status}', '{error_message}');
    """

    print(f"Executing SQL query: {query}")

    pg_hook = PostgresHook(postgres_conn_id='postgres-db')
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

    df = pd.read_csv(csv_path, delimiter=';', encoding='utf-8')

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


# Определение функции для расчета оборотов за январь 2018 года
def calculate_turnover_january_task(conn_id='postgres-db'):
    # Установка начальной даты (1 января 2018 года)
    start_date = datetime(2018, 1, 1).date()
    # Установка конечной даты (31 января 2018 года)
    end_date = datetime(2018, 1, 31).date()

    # Создание подключения к базе данных PostgreSQL с использованием переданного идентификатора соединения
    pg_hook = PostgresHook(conn_id)
    # Получение объекта соединения с базой данных
    conn = pg_hook.get_conn()
    # Создание курсора для выполнения SQL-запросов
    cursor = conn.cursor()

    # Инициализация переменной текущей даты, начиная с начальной даты
    current_date = start_date
    # Цикл, который выполняется, пока текущая дата не превысит конечную дату
    while current_date <= end_date:
        # Выполнение SQL-запроса для вызова функции fill_account_turnover_f с текущей датой в качестве аргумента
        cursor.execute('SELECT "DM".fill_account_turnover_f(%s);', (current_date,))
        # Фиксация изменений в базе данных
        conn.commit()
        # Увеличение текущей даты на один день
        current_date += timedelta(days=1)

    # Закрытие соединения с базой данных
    conn.close()


def calculate_balance_january_task(conn_id='postgres-db'):
    start_date = datetime(2018, 1, 1).date()
    end_date = datetime(2018, 1, 31).date()

    pg_hook = PostgresHook(conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    current_date = start_date
    while current_date <= end_date:
        cursor.execute('SELECT "DM".fill_account_balance_f(%s);', (current_date,))
        conn.commit()
        current_date += timedelta(days=1)

    conn.close


def export_to_csv():
    # Подключение к базе данных
    pg_hook = PostgresHook(postgres_conn_id='postgres-db')
    conn = pg_hook.get_conn()

    # SQL-запрос для выборки данных из таблицы
    query = 'SELECT * FROM "DM".dm_f101_round_f;'

    # Чтение данных в DataFrame
    df = pd.read_sql(query, conn)

    # Путь к папке reports
    reports_path = 'reports'

    # Проверка, существует ли папка reports, и создание, если её нет
    if not os.path.exists(reports_path):
        os.makedirs(reports_path)

    # Сохранение данных в CSV-файл
    csv_path = os.path.join(reports_path, 'dm_f101_round_f.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8')

    # Закрытие соединения
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
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs
    )

    create_tables_and_functions = SQLExecuteQueryOperator(
        task_id='create_tables_and_functions',
        conn_id='postgres-db',
        sql='sql/create_vitrin.sql',
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs
    )
    # Задача для создания схемы DS
    create_sheme = SQLExecuteQueryOperator(
        task_id='create_sheme',
        conn_id='postgres-db',
        sql='sql/create_sheme_ds.sql',
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs
    )

    # Задача для создания схемы logs
    create_logs_sheme = SQLExecuteQueryOperator(
        task_id='create_logs_sheme',
        conn_id='postgres-db',
        sql='sql/create_logs_sheme.sql',
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs
    )

    # Задача для создания таблицы форма 101
    create_101_form = SQLExecuteQueryOperator(
        task_id='create_101_form',
        conn_id='postgres-db',
        sql='sql/101_forms.sql',
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs
    )
    #Задача для заполнение данными формы 101
    fill_101_form = SQLExecuteQueryOperator(
        task_id='fill_101_form',
        conn_id='postgres-db',
        sql="CALL \"DM\".fill_f101_round_f('2018-02-01');",
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs
    )

    # Пустая задача для разделения этапов
    pause = EmptyOperator(
        task_id='pause',
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs

    )

    calculate_turnover_january_task = PythonOperator(
        task_id='calculate_turnover_january_task',
        python_callable=calculate_turnover_january_task,
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs
    )

    calculate_balance_january_task = PythonOperator(
        task_id='calculate_balance_january_task',
        python_callable=calculate_balance_january_task,
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs
    )

    # Создаем задачи для каждой таблицы
    tasks = []
    for table in tables:
        if table == 'ft_posting_f':
            task = PythonOperator(
                task_id=table,
                python_callable=load_ft_posting_f,
                on_success_callback=uploading_logs,
                on_failure_callback=uploading_logs
            )
        elif table == 'md_ledger_account_s':
            task = PythonOperator(
                task_id=table,
                python_callable=load_md_ledger_account_s,
                op_kwargs={'table_name': table},  # Передаем имя таблицы
                on_success_callback=uploading_logs,
                on_failure_callback=uploading_logs
            )
        else:
            task = PythonOperator(
                task_id=table,
                python_callable=load_data_with_copy,
                op_kwargs={'table_name': table},
                on_success_callback=uploading_logs,
                on_failure_callback=uploading_logs
            )
        tasks.append(task)

    # Задача для логирования начала ETL
    logs_etl_end = DummyOperator(
        task_id='logs_etl_end',
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs
    )

    # Пустая задача для разделения этапов
    pause2 = EmptyOperator(
        task_id='pause2',
        on_success_callback=uploading_logs,
        on_failure_callback=uploading_logs

    )

    #Экпорт отчёта 101 в csv
    export_reports_101 = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_to_csv,
        on_failure_callback=uploading_logs
    )

    # Определение порядка выполнения задач
    (
            logs_etl_started
            >> [create_sheme, create_logs_sheme]
            >> pause
            >> tasks
            >> pause2
            >> create_tables_and_functions
            >> calculate_turnover_january_task
            >> calculate_balance_january_task
            >> create_101_form
            >> fill_101_form
            >> export_reports_101
            >> logs_etl_end

    )
