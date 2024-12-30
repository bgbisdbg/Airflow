import psycopg2

# Параметры подключения
conn_params = {
    "host": "localhost",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

# Подключение к PostgreSQL
try:
    conn = psycopg2.connect(**conn_params)
    print("Подключение успешно!")
    conn.close()
except Exception as e:
    print(f"Ошибка подключения: {e}")