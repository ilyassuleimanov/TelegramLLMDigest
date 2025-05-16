# airflow/tasks/parser/Dockerfile

# 1. Выбираем базовый образ с Python нужной версии
FROM python:3.9-slim

# 2. Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# 3. Копируем файл с зависимостями в рабочую директорию
# Предполагается, что requirements.txt лежит в airflow/tasks/parser/
COPY requirements.txt .

# 4. Устанавливаем зависимости Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 5. Копируем адаптированный скрипт парсера
# Предполагается, что telegram_parser.py лежит в airflow/tasks/parser/
COPY telegram_parser.py .

# 6. (Опционально) Создаем директорию для сессии внутри образа,
# если скрипт ожидает ее существования даже без монтирования тома.
# Права на эту папку будут переопределены при монтировании тома из DockerOperator.
RUN mkdir -p /app/session

# USER nobody -- УБРАТЬ. Пусть запускается от пользователя по умолчанию в образе (обычно root)
#             или того, которого может передать DockerOperator/Airflow.
#             Это упростит работу с томами и правами.

# CMD ["python", "telegram_parser.py"] -- УБРАТЬ или ЗАКОММЕНТИРОВАТЬ.
#                                      Команду задает DockerOperator.