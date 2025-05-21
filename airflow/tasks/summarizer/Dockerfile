# airflow/tasks/summarizer/Dockerfile

FROM python:3.9-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл с зависимостями в рабочую директорию
# Предполагается, что requirements.txt лежит в airflow/tasks/summarizer/
COPY requirements.txt .

# 4. Устанавливаем зависимости Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 5. Копируем скрипт суммаризатора
# Предполагается, что summarizer.py лежит в airflow/tasks/summarizer/
COPY summarizer.py .
