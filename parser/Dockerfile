# 1. Выбираем базовый образ с Python нужной версии
FROM python:3.9-slim

# 2. Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# 3. Копируем файл с зависимостями в рабочую директорию
COPY requirements.txt .

# 4. Устанавливаем зависимости Python
# --no-cache-dir экономит место в образе
# --trusted-host pypi.python.org --trusted-host pypi.org --trusted-host files.pythonhosted.org - могут понадобиться, если есть проблемы с доступом к PyPI из-за сети/SSL
RUN pip install --no-cache-dir -r requirements.txt

# 5. Копируем остальной код приложения (скрипт) в рабочую директорию
COPY tg_parser.py .

# 6. Создаем директорию для сессии внутри контейнера (если нужна)
# Убедимся, что она существует и у Python есть права на запись
RUN mkdir -p /app/session && chown -R nobody:nogroup /app/session

# Запускаем от имени непривилегированного пользователя для безопасности
USER nobody

# 7. Указываем команду, которая будет выполняться при запуске контейнера
CMD ["python", "tg_parser.py"]