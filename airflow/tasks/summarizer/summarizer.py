import os
import sys
import psycopg2
import logging
import json
from datetime import datetime, date, time, timedelta, timezone # Добавлены date, time, timedelta, timezone
from dotenv import load_dotenv
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole

# --- Настройка Логгирования ---
# Устанавливаем базовую конфигурацию логирования
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [Summarizer] - %(message)s', # Добавлен префикс [Summarizer]
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Загрузка Переменных Окружения (для локального запуска/отладки и для Airflow) ---
# load_dotenv() будет работать, если .env файл есть рядом со скриптом при локальном запуске.
# При запуске из DockerOperator переменные будут переданы самим Airflow.
load_dotenv()

# --- Чтение Конфигурации из Переменных Окружения ---
GIGACHAT_API_KEY = os.getenv('GIGACHAT_API_KEY')
DB_HOST = os.getenv('DB_HOST', 'postgres_results_db_service') # Имя сервиса БД результатов
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Настройки для LLM
MAX_NEWS_ITEMS_FOR_SUMMARY = int(os.getenv('MAX_NEWS_ITEMS_FOR_SUMMARY', 15)) # Увеличим лимит по умолчанию
LLM_MODEL_NAME = os.getenv('LLM_MODEL_NAME', "GigaChat-Pro")
LLM_TEMPERATURE = float(os.getenv('LLM_TEMPERATURE', 0.7))
LLM_MAX_TOKENS = int(os.getenv('LLM_MAX_TOKENS', 700)) # Увеличим лимит токенов для саммари за день

# --- Путь к файлу XCom (стандартный для DockerOperator) ---
XCOM_PATH = "/airflow/xcom/return.json"

# --- Проверка Обязательных Переменных Окружения ---
# START_DATE_STR больше не нужен, данные придут из XCom
required_env_vars = {
    'GIGACHAT_API_KEY': GIGACHAT_API_KEY,
    'DB_NAME': DB_NAME,
    'DB_USER': DB_USER,
    'DB_PASSWORD': DB_PASSWORD,
    # DB_HOST будет использоваться по умолчанию, если не задан
}

missing_env_vars = [name for name, value in required_env_vars.items() if not value]
if missing_env_vars:
    logging.error(f"Ошибка: Не установлены обязательные переменные окружения: {', '.join(missing_env_vars)}")
    # При запуске в Airflow это приведет к падению задачи, что правильно
    sys.exit(1)

# --- Функции Работы с Базой Данных ---

def get_db_connection():
    """Устанавливает соединение с базой данных PostgreSQL."""
    conn = None # Инициализируем conn как None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            connect_timeout=10 # Добавляем таймаут подключения
        )
        conn.autocommit = False # Управляем транзакциями вручную
        logging.info(f"Успешное подключение к БД {DB_NAME} на {DB_HOST}:{DB_PORT}")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Ошибка подключения к БД: {e}")
        if conn: # Закрываем соединение, если оно было частично открыто
             conn.close()
        return None
    except Exception as e: # Ловим другие возможные ошибки при подключении
        logging.error(f"Неожиданная ошибка при подключении к БД: {e}")
        if conn:
             conn.close()
        return None


def setup_summaries_table(conn):
    """Создает или проверяет таблицу summaries с новой структурой."""
    # Эта функция теперь менее критична, если таблица создается через SQL или другим скриптом,
    # но полезна для идемпотентности и локальных тестов.
    if not conn:
        logging.error("Невозможно настроить таблицу: нет соединения с БД.")
        return False
    try:
        with conn.cursor() as cur:
            logging.info("Проверка/создание таблицы 'summaries'...")
            # Используем BIGINT для channel_id, DATE для summary_date
            # Добавляем UNIQUE constraint
            cur.execute("""
                CREATE TABLE IF NOT EXISTS summaries (
                    summary_id SERIAL PRIMARY KEY,
                    channel_id BIGINT NOT NULL,          -- ID канала (числовой)
                    summary_date DATE NOT NULL,            -- Дата саммари
                    summary_text TEXT NOT NULL,
                    llm_model_used VARCHAR(100),
                    generated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL, -- Добавлено NOT NULL
                    UNIQUE (channel_id, summary_date)   -- Уникальность
                );
            """)
            # Создаем индекс для быстрого поиска
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_summaries_channel_date
                ON summaries (channel_id, summary_date DESC);
            """)
            # Важно: коммит нужен только если были изменения схемы
            conn.commit()
            logging.info("Таблица 'summaries' проверена/создана.")
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Ошибка при настройке таблицы 'summaries': {error}")
        # Откатываем транзакцию в случае ошибки DDL
        try:
            conn.rollback()
        except Exception as rb_error:
            logging.error(f"Ошибка при откате транзакции: {rb_error}")
        return False

def fetch_posts_for_day(conn, channel_id: int, target_date: date):
    """Извлекает тексты постов из БД для заданного канала и даты."""
    if not conn:
        logging.error("Невозможно извлечь посты: нет соединения с БД.")
        return [] # Возвращаем пустой список

    posts_texts = []
    # Определяем начало и конец дня в UTC (PostgreSQL TIMESTAMPTZ работает с UTC по умолчанию)
    # Начало дня (включительно)
    day_start_utc = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    # Конец дня (не включительно - до начала следующего дня)
    day_after_utc = day_start_utc + timedelta(days=1)

    logging.info(f"Извлечение постов для channel_id={channel_id} за дату={target_date} (UTC: >= {day_start_utc} и < {day_after_utc})")

    try:
        with conn.cursor() as cur:
            # Выбираем непустые тексты за указанный день для указанного канала
            # Используем channel_id и временные рамки
            cur.execute("""
                SELECT text
                FROM telegram_messages
                WHERE channel_id = %s
                  AND message_date >= %s
                  AND message_date < %s
                  AND text IS NOT NULL AND text <> ''
                ORDER BY message_date ASC;
            """, (channel_id, day_start_utc, day_after_utc))
            results = cur.fetchall()
            # Преобразуем кортежи в список строк
            posts_texts = [row[0] for row in results]
            logging.info(f"Найдено {len(posts_texts)} постов для саммаризации.")
            return posts_texts
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Ошибка при извлечении постов из БД: {error}")
        # Откатываем транзакцию, хотя SELECT обычно ее не меняет, но на всякий случай
        try:
            conn.rollback()
        except Exception as rb_error:
            logging.error(f"Ошибка при откате транзакции: {rb_error}")
        return [] # Возвращаем пустой список в случае ошибки

def save_or_update_summary(conn, channel_id: int, summary_date: date, summary_text: str, model: str = LLM_MODEL_NAME):
    """Сохраняет или обновляет саммари в таблице 'summaries', используя ON CONFLICT."""
    if not conn:
        logging.error("Невозможно сохранить саммари: нет соединения с БД.")
        return False

    logging.info(f"Попытка сохранения/обновления саммари для channel_id={channel_id} за дату={summary_date}")
    try:
        with conn.cursor() as cur:
            # Используем INSERT ... ON CONFLICT для атомарного обновления или вставки
            # Ключ конфликта - (channel_id, summary_date)
            # Обновляем текст, модель и время генерации при конфликте
            cur.execute("""
                INSERT INTO summaries (channel_id, summary_date, summary_text, llm_model_used, generated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (channel_id, summary_date) DO UPDATE SET
                    summary_text = EXCLUDED.summary_text,
                    llm_model_used = EXCLUDED.llm_model_used,
                    generated_at = NOW();
            """, (channel_id, summary_date, summary_text, model))
            # Коммитим изменения
            conn.commit()
            logging.info("Саммари успешно сохранено/обновлено в БД.")
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Ошибка при сохранении/обновлении саммари в БД: {error}")
        # Откатываем транзакцию
        try:
            conn.rollback()
        except Exception as rb_error:
            logging.error(f"Ошибка при откате транзакции: {rb_error}")
        return False

# --- Логика Суммаризации ---

def build_llm_prompt(posts_texts: list[str], target_date_str: str):
    """Формирует промпт для LLM из текстов постов за конкретную дату."""
    prompt_parts = []
    news_counter = 0

    if not posts_texts:
        logging.warning("Нет текстов для формирования промпта.")
        return None

    logging.info(f"Формирование промпта из {len(posts_texts)} текстов (макс. {MAX_NEWS_ITEMS_FOR_SUMMARY})...")

    for text in posts_texts:
        if news_counter >= MAX_NEWS_ITEMS_FOR_SUMMARY:
            logging.warning(f"Достигнут лимит в {MAX_NEWS_ITEMS_FOR_SUMMARY} новостей для промпта. Остальные пропускаются.")
            break

        # Проверка на пустой текст (на всякий случай, хотя выборка из БД уже должна была отфильтровать)
        if not text or not text.strip():
            continue

        news_counter += 1
        # Ограничиваем длину одного поста, чтобы не превысить лимиты контекста LLM
        # Можно сделать это значение настраиваемым
        max_post_length = 2000
        truncated_text = (text[:max_post_length] + '...' if len(text) > max_post_length else text).strip()

        news_block = f"""--- НОВОСТЬ {news_counter} ---
{truncated_text}
--- КОНЕЦ НОВОСТИ {news_counter} ---"""
        prompt_parts.append(news_block)

    if news_counter == 0:
        logging.warning("Не найдено непустых текстов для формирования промпта.")
        return None # Если все тексты были пустыми или отфильтрованы

    # Уточняем промпт, указывая дату
    final_request = f"""--- КОНЕЦ ВСЕХ НОВОСТЕЙ ---

Пожалуйста, сделай саммари (краткое изложение) представленных выше {news_counter} новост(и/ей) за дату {target_date_str}. Представь результат в виде маркированного списка (* или -) ключевых тезисов. Саммари должно быть кратким, но охватывать основные моменты всех статей. Сохраняй нейтральный, информационный тон."""
    prompt_parts.append(final_request)

    final_llm_prompt = "\n\n".join(prompt_parts)
    logging.info(f"Промпт успешно сформирован из {news_counter} новостей.")
    # logging.debug(f"Сформированный Промпт:\n{final_llm_prompt}") # Вывод промпта для отладки (уровень DEBUG)
    return final_llm_prompt

def get_summary_from_gigachat(prompt: str):
    """Отправляет промпт в GigaChat API и получает саммари."""
    if not prompt:
        logging.warning("Промпт для LLM пуст, запрос не будет отправлен.")
        return None

    logging.info(f"Отправка запроса к {LLM_MODEL_NAME} API...")
    try:
        payload = Chat(
            messages=[
                Messages(
                    role=MessagesRole.SYSTEM,
                    content="Ты - ассистент, создающий краткое изложение новостных статей за определенный день в виде маркированного списка."
                ),
                Messages(role=MessagesRole.USER, content=prompt)
            ],
            temperature=LLM_TEMPERATURE,
            max_tokens=LLM_MAX_TOKENS,
        )
        # verify_ssl_certs=False - ИСПОЛЬЗОВАТЬ С ОСТОРОЖНОСТЬЮ! Лучше настроить доверенные сертификаты.
        # Можно сделать значением переменной окружения: `VERIFY_SSL = os.getenv('GIGACHAT_VERIFY_SSL', 'true').lower() == 'true'`
        with GigaChat(credentials=GIGACHAT_API_KEY, verify_ssl_certs=False, model=LLM_MODEL_NAME) as giga:
            response = giga.chat(payload)
            # Проверка наличия ответа
            if response and response.choices and response.choices[0].message:
                 summary = response.choices[0].message.content
                 logging.info(f"Саммари успешно получено от {LLM_MODEL_NAME}.")
                 return summary.strip() # Убираем лишние пробелы по краям
            else:
                 logging.warning(f"Получен неожиданный ответ от GigaChat API: {response}")
                 return None
    # Ловим специфичные ошибки GigaChat, если они есть в SDK, или общие ошибки requests/API
    except Exception as e:
        logging.error(f"Ошибка при взаимодействии с GigaChat API: {e}", exc_info=True) # exc_info=True добавит traceback
        return None

# --- Основной Блок Выполнения ---

def main():
    """Основная логика скрипта суммризации, запускаемая как задача Airflow."""
    logging.info("--- Запуск скрипта суммризации (Airflow Task) ---")
    connection = None # Инициализация для блока finally
    exit_code = 0 # Код выхода по умолчанию - успех

    try:
        # 1. Чтение данных из XCom файла
        channel_id = None
        target_date_str = None
        logging.info(f"Попытка чтения XCom из {XCOM_PATH}")
        if not os.path.exists(XCOM_PATH):
             logging.error(f"Ошибка: Файл XCom '{XCOM_PATH}' не найден.")
             sys.exit(1) # Выход с ошибкой

        try:
            with open(XCOM_PATH, 'r', encoding='utf-8') as f: # Добавлено encoding='utf-8'
                xcom_data = json.load(f)
            # Ожидаем числовой ID канала (BIGINT)
            channel_id = int(xcom_data.get("channel_id")) # Используем числовой ID
            target_date_str = xcom_data.get("target_date_str") # 'YYYY-MM-DD'
            logging.info(f"Прочитаны данные XCom: channel_id={channel_id}, target_date_str={target_date_str}")
        except FileNotFoundError: # Хотя мы проверили exists, оставим для полноты
             logging.error(f"Ошибка: Файл XCom '{XCOM_PATH}' не найден (неожиданно после проверки).")
             sys.exit(1)
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            logging.error(f"Ошибка: Не удалось прочитать или распарсить данные из XCom файла '{XCOM_PATH}': {e}")
            sys.exit(1)

        if not all([isinstance(channel_id, int), target_date_str]):
             logging.error("Ошибка: Необходимые данные 'channel_id' (int) или 'target_date_str' отсутствуют или некорректны в XCom.")
             sys.exit(1)

        # 2. Преобразование даты
        try:
            target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        except ValueError:
            logging.error(f"Ошибка: Неверный формат target_date_str ('{target_date_str}') из XCom. Ожидается 'YYYY-MM-DD'.")
            sys.exit(1)

        # 3. Подключение к БД и проверка схемы
        connection = get_db_connection()
        if not connection:
            sys.exit(1) # Выход, если не удалось подключиться

        # Проверяем/создаем таблицу summaries при каждом запуске (идемпотентно)
        if not setup_summaries_table(connection):
            # Ошибка уже залогирована внутри функции
            sys.exit(1)

        # 4. Извлечение постов за указанный день и канал
        posts_texts = fetch_posts_for_day(connection, channel_id, target_date)

        if not posts_texts:
            logging.info(f"Новых постов для саммаризации не найдено для channel_id={channel_id} за дату={target_date}.")
            # Можно создать/обновить запись в summaries с пустым текстом или специальной пометкой,
            # чтобы UI знал, что обработка за день была, но новостей не было.
            # save_or_update_summary(connection, channel_id, target_date, "Новостей за этот день не найдено.", "System")
            logging.info("Скрипт суммризации завершен (нет постов).")
            # Выходим с кодом 0, так как это не ошибка приложения
            # Airflow отметит задачу как успешную
            exit_code = 0
        else:
            # 5. Формирование промпта
            final_prompt = build_llm_prompt(posts_texts, target_date_str)

            if not final_prompt:
                logging.error("Не удалось сформировать промпт (возможно, все посты были пустыми).")
                # Считаем это ошибкой? Или просто выходим? Зависит от требований.
                # Пока выйдем с ошибкой, чтобы Airflow пометил задачу как неуспешную.
                exit_code = 1
            else:
                # 6. Получение саммари
                summary = get_summary_from_gigachat(final_prompt)

                # 7. Сохранение/Обновление саммари
                if summary:
                    if not save_or_update_summary(connection, channel_id, target_date, summary, LLM_MODEL_NAME):
                        # Если сохранение не удалось, считаем это ошибкой
                        logging.error("Не удалось сохранить саммари в БД.")
                        exit_code = 1
                    # Если всё успешно сохранилось, код выхода остается 0
                else:
                    logging.error("Не удалось получить саммари от LLM. Саммари за день не будет обновлено.")
                    # Считать ли это ошибкой задачи Airflow? Зависит от требований.
                    # Если нужно обязательно иметь саммари, то exit_code = 1.
                    # Если допустимо пропустить обновление при ошибке LLM, то exit_code = 0.
                    # Установим пока как ошибку:
                    exit_code = 1

    except SystemExit as e:
         # Ловим sys.exit(), чтобы корректно закрыть соединение
         exit_code = e.code if isinstance(e.code, int) else 1
         logging.warning(f"Скрипт завершается с кодом: {exit_code}")
    except Exception as e:
        # Ловим любые другие непредвиденные ошибки
        logging.error(f"Непредвиденная ошибка в main: {e}", exc_info=True)
        exit_code = 1 # Выход с ошибкой
    finally:
        # 8. Закрытие соединения в блоке finally, чтобы оно закрылось в любом случае
        if connection:
            try:
                connection.close()
                logging.info("Соединение с БД закрыто.")
            except Exception as close_error:
                logging.error(f"Ошибка при закрытии соединения с БД: {close_error}")

        logging.info(f"--- Скрипт суммризации завершен с кодом выхода: {exit_code} ---")
        # Явно выходим с накопленным кодом ошибки
        sys.exit(exit_code)


if __name__ == "__main__":
    # Этот блок позволяет запускать скрипт локально для тестов,
    # если передать переменные окружения и создать файл XCom вручную.
    # Пример ручного создания XCom для теста:
    # echo '{"channel_id": 123456789, "target_date_str": "2024-05-20"}' > /tmp/xcom_test.json
    # И затем изменить XCOM_PATH = "/tmp/xcom_test.json" для теста.
    main()