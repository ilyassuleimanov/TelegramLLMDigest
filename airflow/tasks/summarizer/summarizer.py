import os
import sys
import psycopg2
import logging
import json
from datetime import datetime, date, time, timedelta, timezone # Убедимся, что все импортированы
from dotenv import load_dotenv
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole
from typing import List, Optional, Dict, Any # Добавляем типизацию

# --- Настройка Логгирования ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [Summarizer] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__) # Используем именованный логгер

# --- Загрузка Переменных Окружения ---
load_dotenv()

# --- Чтение Конфигурации из Переменных Окружения ---
GIGACHAT_API_KEY = os.getenv('GIGACHAT_API_KEY')
DB_HOST = os.getenv('DB_HOST', 'postgres_results_db_service')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Настройки для LLM
MAX_NEWS_ITEMS_FOR_SUMMARY = int(os.getenv('MAX_NEWS_ITEMS_FOR_SUMMARY', 15))
LLM_MODEL_NAME = os.getenv('LLM_MODEL_NAME', "GigaChat")
LLM_TEMPERATURE = float(os.getenv('LLM_TEMPERATURE', 0.7))
LLM_MAX_TOKENS = int(os.getenv('LLM_MAX_TOKENS', 700))

# --- Переменная окружения для XCom данных (вместо пути к файлу) ---
XCOM_DATA_ENV_VAR = "XCOM_DATA_JSON" # Имя переменной окружения, которую будет устанавливать DAG

# --- Проверка Обязательных Переменных Окружения ---
required_env_vars = {
    'GIGACHAT_API_KEY': GIGACHAT_API_KEY,
    'DB_NAME': DB_NAME,
    'DB_USER': DB_USER,
    'DB_PASSWORD': DB_PASSWORD,
}

missing_env_vars = [name for name, value in required_env_vars.items() if not value]
if missing_env_vars:
    logger.error(f"Ошибка: Не установлены обязательные переменные окружения: {', '.join(missing_env_vars)}")
    sys.exit(1)

# --- Функции Работы с Базой Данных ---

def get_db_connection() -> Optional[psycopg2.extensions.connection]:
    """Устанавливает соединение с базой данных PostgreSQL."""
    conn: Optional[psycopg2.extensions.connection] = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            connect_timeout=10
        )
        conn.autocommit = False
        logger.info(f"Успешное подключение к БД {DB_NAME} на {DB_HOST}:{DB_PORT}")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        if conn: conn.close()
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при подключении к БД: {e}", exc_info=True)
        if conn: conn.close()
        return None

def setup_summaries_table(conn: psycopg2.extensions.connection) -> bool:
    """Создает или проверяет таблицу summaries с новой структурой."""
    if not conn:
        logger.error("Невозможно настроить таблицу: нет соединения с БД.")
        return False
    try:
        with conn.cursor() as cur:
            logger.info("Проверка/создание таблицы 'summaries'...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS summaries (
                    summary_id SERIAL PRIMARY KEY,
                    channel_id BIGINT NOT NULL,
                    summary_date DATE NOT NULL,
                    summary_text TEXT NOT NULL,
                    llm_model_used VARCHAR(100),
                    generated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
                    UNIQUE (channel_id, summary_date)
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_summaries_channel_date
                ON summaries (channel_id, summary_date DESC);
            """)
            conn.commit()
            logger.info("Таблица 'summaries' проверена/создана.")
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Ошибка при настройке таблицы 'summaries': {error}", exc_info=True)
        try:
            conn.rollback()
        except Exception as rb_error:
            logger.error(f"Ошибка при откате транзакции: {rb_error}")
        return False

def fetch_posts_for_day(conn: psycopg2.extensions.connection, channel_id: int, target_date: date) -> List[str]:
    """Извлекает тексты постов из БД для заданного канала и даты."""
    if not conn:
        logger.error("Невозможно извлечь посты: нет соединения с БД.")
        return []

    posts_texts: List[str] = []
    day_start_utc = datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    day_after_utc = day_start_utc + timedelta(days=1)

    logger.info(f"Извлечение постов для channel_id={channel_id} за дату={target_date} (UTC: >= {day_start_utc} и < {day_after_utc})")

    try:
        with conn.cursor() as cur:
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
            posts_texts = [row[0] for row in results]
            logger.info(f"Найдено {len(posts_texts)} постов для саммаризации.")
            return posts_texts
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Ошибка при извлечении постов из БД: {error}", exc_info=True)
        try:
            conn.rollback()
        except Exception as rb_error:
            logger.error(f"Ошибка при откате транзакции: {rb_error}")
        return []

def save_or_update_summary(conn: psycopg2.extensions.connection, channel_id: int, summary_date: date, summary_text: str, model: str = LLM_MODEL_NAME) -> bool:
    """Сохраняет или обновляет саммари в таблице 'summaries', используя ON CONFLICT."""
    if not conn:
        logger.error("Невозможно сохранить саммари: нет соединения с БД.")
        return False

    logger.info(f"Попытка сохранения/обновления саммари для channel_id={channel_id} за дату={summary_date}")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO summaries (channel_id, summary_date, summary_text, llm_model_used, generated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (channel_id, summary_date) DO UPDATE SET
                    summary_text = EXCLUDED.summary_text,
                    llm_model_used = EXCLUDED.llm_model_used,
                    generated_at = NOW();
            """, (channel_id, summary_date, summary_text, model))
            conn.commit()
            logger.info("Саммари успешно сохранено/обновлено в БД.")
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Ошибка при сохранении/обновлении саммари в БД: {error}", exc_info=True)
        try:
            conn.rollback()
        except Exception as rb_error:
            logger.error(f"Ошибка при откате транзакции: {rb_error}")
        return False

# --- Логика Суммаризации ---

def build_llm_prompt(posts_texts: List[str], target_date_str: str) -> Optional[str]:
    """Формирует промпт для LLM из текстов постов за конкретную дату."""
    prompt_parts = []
    news_counter = 0

    if not posts_texts:
        logger.warning("Нет текстов для формирования промпта.")
        return None

    logger.info(f"Формирование промпта из {len(posts_texts)} текстов (макс. {MAX_NEWS_ITEMS_FOR_SUMMARY})...")

    for text in posts_texts:
        if news_counter >= MAX_NEWS_ITEMS_FOR_SUMMARY:
            logger.warning(f"Достигнут лимит в {MAX_NEWS_ITEMS_FOR_SUMMARY} новостей для промпта. Остальные пропускаются.")
            break
        if not text or not text.strip():
            continue
        news_counter += 1
        max_post_length = 2000
        truncated_text = (text[:max_post_length] + '...' if len(text) > max_post_length else text).strip()
        news_block = f"""--- НОВОСТЬ {news_counter} ---
{truncated_text}
--- КОНЕЦ НОВОСТИ {news_counter} ---"""
        prompt_parts.append(news_block)

    if news_counter == 0:
        logger.warning("Не найдено непустых текстов для формирования промпта.")
        return None

    final_request = f"""--- КОНЕЦ ВСЕХ НОВОСТЕЙ ---

Пожалуйста, сделай саммари (краткое изложение) представленных выше {news_counter} новост(и/ей) за дату {target_date_str}. Представь результат в виде маркированного списка (* или -) ключевых тезисов. Саммари должно быть кратким, но охватывать основные моменты всех статей. Сохраняй нейтральный, информационный тон."""
    prompt_parts.append(final_request)
    final_llm_prompt = "\n\n".join(prompt_parts)
    logger.info(f"Промпт успешно сформирован из {news_counter} новостей.")
    return final_llm_prompt

def get_summary_from_gigachat(prompt: str) -> Optional[str]:
    """Отправляет промпт в GigaChat API и получает саммари."""
    if not prompt:
        logger.warning("Промпт для LLM пуст, запрос не будет отправлен.")
        return None

    logger.info(f"Отправка запроса к {LLM_MODEL_NAME} API...")
    try:
        payload = Chat(
            messages=[
                Messages(
                    role=MessagesRole.SYSTEM,
                    content="Ты - ассистент, который генерирует краткое изложение новостных статей за определенный день в виде маркированного списка."
                ),
                Messages(role=MessagesRole.USER, content=prompt)
            ],
            temperature=LLM_TEMPERATURE,
            max_tokens=LLM_MAX_TOKENS,
        )
        with GigaChat(credentials=GIGACHAT_API_KEY, verify_ssl_certs=False, model=LLM_MODEL_NAME) as giga:
            response = giga.chat(payload)
            if response and response.choices and response.choices[0].message:
                 summary = response.choices[0].message.content
                 logger.info(f"Саммари успешно получено от {LLM_MODEL_NAME}.")
                 return summary.strip()
            else:
                 logger.warning(f"Получен неожиданный ответ от GigaChat API: {response}")
                 return None
    except Exception as e:
        logger.error(f"Ошибка при взаимодействии с GigaChat API: {e}", exc_info=True)
        return None

# --- Основной Блок Выполнения ---

def main():
    """Основная логика скрипта суммризации, запускаемая как задача Airflow."""
    logger.info("--- Запуск скрипта суммризации (Airflow Task) ---")
    connection: Optional[psycopg2.extensions.connection] = None
    exit_code = 0

    try:
        # 1. Чтение данных XCom из ПЕРЕМЕННОЙ ОКРУЖЕНИЯ
        xcom_data_json_str = os.getenv(XCOM_DATA_ENV_VAR)
        channel_id: Optional[int] = None
        target_date_str: Optional[str] = None

        if not xcom_data_json_str:
            logger.error(f"Ошибка: Переменная окружения '{XCOM_DATA_ENV_VAR}' с данными XCom не найдена.")
            sys.exit(1)

        try:
            logger.info(f"Попытка разбора XCom из переменной окружения '{XCOM_DATA_ENV_VAR}'...")
            xcom_data = json.loads(xcom_data_json_str)
            channel_id = int(xcom_data.get("channel_id"))
            target_date_str = xcom_data.get("target_date_str")
            # Проверяем статус из парсера (опционально)
            parser_status = xcom_data.get("status", "unknown")
            logger.info(f"Прочитаны данные XCom: channel_id={channel_id}, target_date_str={target_date_str}, parser_status={parser_status}")

            if parser_status != "success":
                logger.warning(f"Статус предыдущей задачи парсинга: '{parser_status}'. "
                               f"Саммаризация может быть неполной или основана на ошибочных данных.")
                # Можно решить, прерывать ли выполнение, если парсер не был полностью успешен.
                # Например, если parsed_count=0 и saved_count=0, то и суммировать нечего.
                if xcom_data.get("parsed_count", 0) == 0:
                    logger.info("Парсер не нашел или не сохранил сообщений. Суммаризация не требуется.")
                    sys.exit(0) # Успешный выход, т.к. нет работы

        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            logger.error(f"Ошибка: Не удалось прочитать или распарсить данные из '{XCOM_DATA_ENV_VAR}': {e}", exc_info=True)
            logger.error(f"Содержимое переменной '{XCOM_DATA_ENV_VAR}': {xcom_data_json_str}")
            sys.exit(1)

        if not all([isinstance(channel_id, int), target_date_str]):
             logger.error("Ошибка: Необходимые данные 'channel_id' (int) или 'target_date_str' отсутствуют или некорректны в XCom.")
             sys.exit(1)

        # 2. Преобразование даты
        try:
            target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Ошибка: Неверный формат target_date_str ('{target_date_str}') из XCom. Ожидается 'YYYY-MM-DD'.")
            sys.exit(1)

        # 3. Подключение к БД и проверка схемы
        connection = get_db_connection()
        if not connection:
            sys.exit(1)

        if not setup_summaries_table(connection):
            sys.exit(1)

        # 4. Извлечение постов за указанный день и канал
        posts_texts = fetch_posts_for_day(connection, channel_id, target_date)

        if not posts_texts:
            logger.info(f"Постов для саммаризации не найдено для channel_id={channel_id} за дату={target_date}.")
            # Решаем создать/обновить запись в summaries с пометкой, что новостей не было
            # Это важно для UI, чтобы показать, что обработка за день была.
            if not save_or_update_summary(connection, channel_id, target_date, "Новостей за этот день не найдено.", "System"):
                logger.error("Не удалось сохранить информацию об отсутствии новостей.")
                exit_code = 1 # Считаем ошибкой, если не смогли даже это записать
            else:
                exit_code = 0 # Успех, обработка завершена
        else:
            # 5. Формирование промпта
            final_prompt = build_llm_prompt(posts_texts, target_date_str)

            if not final_prompt:
                logger.error("Не удалось сформировать промпт.")
                exit_code = 1
            else:
                # 6. Получение саммари
                summary = get_summary_from_gigachat(final_prompt)

                # 7. Сохранение/Обновление саммари
                if summary:
                    if not save_or_update_summary(connection, channel_id, target_date, summary, LLM_MODEL_NAME):
                        logger.error("Не удалось сохранить саммари в БД.")
                        exit_code = 1
                else:
                    logger.error("Не удалось получить саммари от LLM. Саммари за день не будет обновлено/создано.")
                    # Если LLM не вернул саммари, но посты были, это может быть ошибкой
                    # или временной проблемой LLM. Можно создать запись "Саммари не сгенерировано".
                    if not save_or_update_summary(connection, channel_id, target_date, "Саммари не удалось сгенерировать.", "SystemError"):
                         logger.error("Не удалось сохранить информацию об ошибке генерации саммари.")
                    exit_code = 1 # Считаем это ошибкой задачи

    except SystemExit as e:
         exit_code = e.code if isinstance(e.code, int) else 1
         logger.warning(f"Скрипт суммризации завершается через sys.exit() с кодом: {exit_code}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка в main функции суммризатора: {e}", exc_info=True)
        exit_code = 1
    finally:
        if connection:
            try:
                connection.close()
                logger.info("Соединение с БД закрыто.")
            except Exception as close_error:
                logger.error(f"Ошибка при закрытии соединения с БД: {close_error}")

        logger.info(f"--- Скрипт суммризации завершен с кодом выхода: {exit_code} ---")
        sys.exit(exit_code)

if __name__ == "__main__":
    # Этот блок для локального тестирования.
    # Для теста нужно установить переменную окружения XCOM_DATA_JSON, например:
    # export XCOM_DATA_JSON='{"channel_id": 1137503644, "target_date_str": "2024-05-18", "status": "success", "parsed_count": 10, "saved_count": 10}'
    # и остальные переменные (DB_*, GIGACHAT_API_KEY)
    final_exit_code = 0
    try:
        logger.info("Запуск основного процесса суммризатора (локальный или прямой вызов)...")
        main()
    except KeyboardInterrupt:
         logger.warning("Программа суммризатора прервана пользователем (Ctrl+C).")
         final_exit_code = 130
    except SystemExit as e:
         logger.info(f"Суммризатор: Перехвачен SystemExit с кодом {e.code}. Завершение.")
         final_exit_code = e.code if isinstance(e.code, int) else 1
    except Exception as e:
         logger.critical(f"Суммризатор: Неперехваченная ошибка на самом верхнем уровне: {e}", exc_info=True)
         final_exit_code = 1
    finally:
        logger.info(f"Скрипт суммризатора окончательно завершается с кодом {final_exit_code} (из __main__ блока).")
        if final_exit_code != 0 and sys.exc_info()[0] is None:
            sys.exit(final_exit_code)