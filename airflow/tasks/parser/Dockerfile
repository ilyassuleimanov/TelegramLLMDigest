# airflow/tasks/parser/Dockerfile

FROM python:3.9-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл с зависимостями в рабочую директорию
COPY requirements.txt .

# Устанавливаем зависимости Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем скрипт парсера. Предполагается, что telegram_parser.py лежит в airflow/tasks/parser/
COPY telegram_parser.py .

# Создаем директорию для сессии внутри образа,
# Права на эту папку будут переопределены при монтировании тома из DockerOperator.
RUN mkdir -p /app/session