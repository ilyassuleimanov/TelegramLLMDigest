# streamlit_app/Dockerfile

# Используем официальный образ Python (выберите ту же версию, что и для FastAPI, для консистентности)
FROM python:3.9-slim

# Установка переменных окружения
ENV PYTHONUNBUFFERED 1
ENV PIP_NO_CACHE_DIR off
ENV PYTHONDONTWRITEBYTECODE 1

# Создаем пользователя и группу appuser для запуска приложения не от рута (хорошая практика безопасности)
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Установка рабочей директории
WORKDIR /app

# Копируем файл зависимостей и устанавливаем их
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Копируем код приложения Streamlit
COPY ./app.py /app/app.py
# Если у тебя будут другие файлы в streamlit_app (например, assets, utils.py), скопируй их тоже
# COPY . /app # Если хочешь скопировать все из текущей папки

# Меняем владельца файлов на appuser
RUN chown -R appuser:appuser /app

# Переключаемся на пользователя appuser
USER appuser

# Открываем порт, который Streamlit использует по умолчанию
EXPOSE 8501

# Команда для запуска Streamlit приложения
# streamlit run app.py - запускает приложение
# --server.port 8501 - указывает порт (хотя это и так по умолчанию)
# --server.address 0.0.0.0 - делает приложение доступным извне контейнера
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]