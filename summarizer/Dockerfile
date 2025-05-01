# Используем официальный базовый образ Python 3.9 (слим-версия)
FROM python:3.9-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Копируем файл с зависимостями в рабочую директорию
COPY requirements.txt .

# Устанавливаем зависимости, не сохраняя кеш pip для уменьшения размера образа
# Добавляем --no-cache-dir и --upgrade pip
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем скрипт суммаризатора в рабочую директорию
COPY summarizer.py .

# Команда, которая будет выполняться при запуске контейнера
CMD ["python", "summarizer.py"]