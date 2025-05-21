# fastAPI_app/Dockerfile

FROM python:3.9-slim

# Обновляем список пакетов и устанавливаем curl.
RUN apt-get update && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*

# Установка рабочей директории
WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

# Обновляем pip и устанавливаем зависимости.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Копируем все содержимое текущей директории в рабочую директорию  внутри контейнера.
COPY . /app

# Открытие порта
EXPOSE 8000

# --- Команда для запуска приложения ---
# Запускаем Uvicorn ASGI сервер для обслуживания FastAPI приложения.
# main:app - указывает на объект 'app' в файле 'main.py'.
# --host 0.0.0.0 - делает сервер доступным извне контейнера (внутри сети Docker).
# --port 8000 - порт, на котором Uvicorn будет слушать внутри контейнера.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]