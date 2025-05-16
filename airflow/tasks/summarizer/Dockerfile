# airflow/tasks/summarizer/Dockerfile

# 1. Выбираем базовый образ с Python нужной версии
FROM python:3.9-slim

# 2. Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# 3. Копируем файл с зависимостями в рабочую директорию
# Предполагается, что requirements.txt лежит в airflow/tasks/summarizer/
COPY requirements.txt .

# 4. Устанавливаем зависимости Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 5. Копируем адаптированный скрипт суммаризатора
# Предполагается, что summarizer.py лежит в airflow/tasks/summarizer/
COPY summarizer.py .

# USER nobody -- УБРАТЬ (если был, здесь не было, но на всякий случай напоминаю)
#             Пусть запускается от пользователя по умолчанию в образе.

# CMD ["python", "summarizer.py"] -- УБРАТЬ или ЗАКОММЕНТИРОВАТЬ.
#                                    Команду задает DockerOperator.