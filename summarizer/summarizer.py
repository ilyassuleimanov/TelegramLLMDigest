import os
import sys
import psycopg2
import logging
from datetime import datetime
from dotenv import load_dotenv
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole

# --- Настройка Логгирования ---
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] - %(message)s')

# --- Загрузка Переменных Окружения ---
load_dotenv() # Загружаем переменные из .env файла (если он есть)

GIGACHAT_API_KEY = os.getenv('GIGACHAT_API_KEY')
DB_HOST = os.getenv('DB_HOST', 'postgres_db') # Имя сервиса БД в Docker Compose
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
START_DATE_STR = os.getenv('START_DATE_STR') # 'YYYY-MM-DD HH:MM:SS'
# Можно добавить настройку для максимального кол-ва новостей
MAX_NEWS_ITEMS_FOR_SUMMARY = int(os.getenv('MAX_NEWS_ITEMS_FOR_SUMMARY', 10))

# --- Проверка Обязательных Переменных ---
required_vars = {
    'GIGACHAT_API_KEY': GIGACHAT_API_KEY,
    'DB_NAME': DB_NAME,
    'DB_USER': DB_USER,
    'DB_PASSWORD': DB_PASSWORD,
    'START_DATE_STR': START_DATE_STR
}

missing_vars = [name for name, value in required_vars.items() if not value]
if missing_vars:
    logging.error(f"Ошибка: Не установлены обязательные переменные окружения: {', '.join(missing_vars)}")
    sys.exit(1) # Выход с ошибкой

# --- Функции Работы с Базой Данных ---

def get_db_connection():
    """Устанавливает соединение с базой данных PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logging.info(f"[*] Успешное подключение к БД {DB_NAME} на {DB_HOST}:{DB_PORT}")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"[!] Ошибка подключения к БД: {e}")
        return None

def setup_summaries_table(conn):
    """Создает таблицу для саммари, если она не существует."""
    if not conn: return False
    try:
        with conn.cursor() as cur:
            # Создаем таблицу для хранения батч-саммари
            cur.execute("""
                CREATE TABLE IF NOT EXISTS summaries (
                    summary_id SERIAL PRIMARY KEY,
                    summary_text TEXT NOT NULL,
                    start_date_criteria TIMESTAMPTZ NOT NULL, -- Дата, после которой брались посты
                    end_date_processed TIMESTAMPTZ,         -- Дата самого последнего поста в батче
                    processed_posts_count INTEGER,         -- Кол-во постов в батче
                    llm_model_used VARCHAR(100),           -- Какая модель LLM использовалась (опционально)
                    generated_at TIMESTAMPTZ DEFAULT NOW() -- Время генерации саммари
                );
            """)
            # Можно добавить индексы при необходимости
            # cur.execute("CREATE INDEX IF NOT EXISTS idx_summaries_generated_at ON summaries (generated_at);")
            conn.commit()
            logging.info("[*] Таблица 'summaries' проверена/создана.")
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[!] Ошибка при настройке таблицы 'summaries': {error}")
        conn.rollback()
        return False

def fetch_posts_for_summary(conn, start_date):
    """Извлекает тексты постов из БД после указанной даты."""
    if not conn: return [], None
    posts_data = []
    latest_post_date = None
    try:
        with conn.cursor() as cur:
            # Выбираем только непустые тексты и дату сообщения
            # Сортируем по дате, чтобы получить самую позднюю
            cur.execute("""
                SELECT text, message_date
                FROM telegram_messages
                WHERE message_date > %s AND text IS NOT NULL AND text <> ''
                ORDER BY message_date ASC;
            """, (start_date,))
            results = cur.fetchall()
            if results:
                # Последняя дата будет у последнего элемента из-за ORDER BY ASC
                latest_post_date = results[-1][1]
                # Сохраняем только тексты
                posts_data = [row[0] for row in results]
            logging.info(f"[*] Найдено {len(posts_data)} новых постов для саммаризации после {start_date}.")
            return posts_data, latest_post_date
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[!] Ошибка при извлечении постов из БД: {error}")
        return [], None

def save_summary(conn, summary_text, start_date, end_date, count, model="GigaChat"):
    """Сохраняет сгенерированное саммари в таблицу 'summaries'."""
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO summaries (summary_text, start_date_criteria, end_date_processed, processed_posts_count, llm_model_used)
                VALUES (%s, %s, %s, %s, %s);
            """, (summary_text, start_date, end_date, count, model))
            conn.commit()
            logging.info(f"[*] Саммари для {count} постов успешно сохранено в БД.")
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"[!] Ошибка при сохранении саммари в БД: {error}")
        conn.rollback()
        return False

# --- Логика Суммаризации ---

def build_llm_prompt(posts_texts):
    """Формирует промпт для LLM из текстов постов."""
    prompt_parts = []
    news_counter = 0

    for text in posts_texts:
        if news_counter >= MAX_NEWS_ITEMS_FOR_SUMMARY:
            logging.warning(f"Достигнут лимит в {MAX_NEWS_ITEMS_FOR_SUMMARY} новостей для промпта. Остальные пропускаются.")
            break
        news_counter += 1
        news_block = f"""--- НОВОСТЬ {news_counter} ---
{text}
--- КОНЕЦ НОВОСТИ {news_counter} ---"""
        prompt_parts.append(news_block)

    if news_counter == 0:
        return None # Нет новостей для промпта

    final_request = f"""--- КОНЕЦ ВСЕХ НОВОСТЕЙ ---

Пожалуйста, сделай саммари (краткое изложение) представленных выше {news_counter} новост(и/ей). Представь результат в виде маркированного списка ключевых тезисов. Саммари должно быть кратким, но охватывать основные моменты всех статей. Сохраняй нейтральный тон."""
    prompt_parts.append(final_request)

    final_llm_prompt = "\n\n".join(prompt_parts)
    return final_llm_prompt

def get_summary_from_gigachat(prompt):
    """Отправляет промпт в GigaChat API и получает саммари."""
    if not prompt:
        return None

    logging.info("[*] Отправка запроса к GigaChat API...")
    try:
        # Настройки модели (можно вынести в переменные окружения)
        # Системный промпт задает общую роль и формат вывода
        payload = Chat(
            messages=[
                Messages(
                    role=MessagesRole.SYSTEM,
                    content="Ты - ассистент, который генерирует краткое изложение новостных статей в виде маркированного списка."
                ),
                Messages(role=MessagesRole.USER, content=prompt)
            ],
            temperature=1.0, # Уровень случайности
            max_tokens=512, # Максимальная длина ответа (увеличено для саммари)
            # repetition_penalty=1.1 # Штраф за повторения (опционально)
        )

        # Используем токен/ключ авторизации
        # verify_ssl_certs=False - НЕ рекомендуется для продакшена, но может быть нужно для локальной отладки
        with GigaChat(credentials=GIGACHAT_API_KEY, verify_ssl_certs=False, model="GigaChat-Pro") as giga: # Используем Pro модель
            response = giga.chat(payload)
            summary = response.choices[0].message.content
            logging.info("[*] Саммари успешно получено от GigaChat.")
            return summary
    except Exception as e:
        logging.error(f"[!] Ошибка при взаимодействии с GigaChat API: {e}")
        return None

# --- Основной Блок Выполнения ---

def main():
    logging.info("--- Запуск скрипта суммризации ---")

    # 1. Преобразование даты начала
    try:
        start_date = datetime.strptime(START_DATE_STR, '%Y-%m-%d %H:%M:%S')
        # Можно добавить обработку часовых поясов, если необходимо
    except ValueError:
        logging.error(f"Ошибка: Неверный формат START_DATE_STR ('{START_DATE_STR}'). Ожидается 'YYYY-MM-DD HH:MM:SS'.")
        sys.exit(1)

    # 2. Подключение к БД и настройка схемы
    connection = get_db_connection()
    if not connection:
        sys.exit(1) # Выход, если не удалось подключиться

    if not setup_summaries_table(connection):
        connection.close()
        sys.exit(1) # Выход, если не удалось настроить таблицу

    # 3. Извлечение постов
    posts_texts, latest_post_date = fetch_posts_for_summary(connection, start_date)

    if not posts_texts:
        logging.info("[-] Новых постов для саммаризации не найдено.")
        connection.close()
        logging.info("--- Скрипт суммризации завершен ---")
        sys.exit(0) # Успешный выход, т.к. нет работы

    # 4. Формирование промпта
    final_prompt = build_llm_prompt(posts_texts)
    if not final_prompt:
        # Это не должно произойти, если posts_texts не пустой, но на всякий случай
        logging.error("[!] Не удалось сформировать промпт, хотя посты были найдены.")
        connection.close()
        sys.exit(1)

    logging.info(f"--- Сформированный Промпт ---\n{final_prompt}\n--------------------------") # Для отладки

    # 5. Получение саммари
    summary = get_summary_from_gigachat(final_prompt)

    # 6. Сохранение саммари
    if summary:
        save_summary(
            conn=connection,
            summary_text=summary,
            start_date=start_date,
            end_date=latest_post_date, # Сохраняем дату последнего поста в батче
            count=min(len(posts_texts), MAX_NEWS_ITEMS_FOR_SUMMARY), # Кол-во постов, реально ушедших в промпт
            model="GigaChat" # Можно указать модель
        )
    else:
        logging.error("[!] Не удалось получить или сохранить саммари.")

    # 7. Закрытие соединения
    if connection:
        connection.close()
        logging.info("[*] Соединение с БД закрыто.")

    logging.info("--- Скрипт суммризации завершен ---")

if __name__ == "__main__":
    main()