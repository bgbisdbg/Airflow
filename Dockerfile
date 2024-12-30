# Используем базовый образ Apache Airflow
FROM apache/airflow:2.6.3

# Копируем папку files в корневую директорию контейнера
COPY ./files /files

# Устанавливаем владельца для скопированных файлов и папок
USER root
RUN chown -R airflow:root /files
USER airflow

# Команда по умолчанию (может быть переопределена в docker-compose.yml)
CMD ["celery", "worker"]