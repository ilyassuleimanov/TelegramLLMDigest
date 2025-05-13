import asyncio
import datetime
import os
import sys
import json
import time
import logging
from urllib.parse import urlparse
from dotenv import load_dotenv # Оставляем для удобства локального тестирования
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from psycopg2.extras import Json # Для удобной работы с JSONB

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError, AuthKeyError, UserDeactivatedBanError, UsernameNotOccupiedError, ChannelPrivateError
from telethon.tl.types import Channel

# --- Настройка Логгирования ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [Parser] - %(message)s', # Добавлен префикс [Parser]
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
# load_dotenv() здесь полезна в основном для локальных тестов.
# При запуске в Airflow/DockerOperator переменные будут переданы извне.
load_dotenv()

# --- ЧТЕНИЕ НАСТРОЕК ИЗ ОКРУЖЕНИЯ ---

# Telegram API Credentials
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
TELEGRAM_PHONE = os.getenv('TELEGRAM_PHONE') # Обязателен, если сессия не создана

# Telegram Session
TELEGRAM_SESSION_FOLDER = os.getenv('TELEGRAM_SESSION_FOLDER_IN_CONTAINER', '/app/session') # Путь ВНУТРИ контейнера
TELEGRAM_SESSION_NAME = os.getenv('TELEGRAM_SESSION_NAME', 'my_telegram_session')
TELEGRAM_SESSION_PATH = os.path.join(TELEGRAM_SESSION_FOLDER, TELEGRAM_SESSION_NAME)

# PostgreSQL Database Settings
DB_HOST = os.getenv('DB_HOST', 'postgres_results_db_service') # Имя сервиса БД результатов
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Параметры, передаваемые Airflow DockerOperator
# Идентификатор канала для Telethon (ссылка, username, id)
PARSER_CHANNEL_IDENTIFIER = os.getenv('PARSER_CHANNEL_IDENTIFIER')
# Числовой ID канала для записи в БД и передачи в XCom
PARSER_CHANNEL_ID = os.getenv('PARSER_CHANNEL_ID') # Ожидаем, что это будет передано как числовой ID
# Дата, за которую парсим ('YYYY-MM-DD')
PARSER_TARGET_DATE_STR = os.getenv('PARSER_TARGET_DATE')

# Путь для записи XCom файла (стандартный для DockerOperator)
XCOM_PATH = "/airflow/xcom/return.json"

# --- Валидация обязательных переменных ---
# Валидируем переменные, необходимые для этого скрипта в контексте Airflow
REQUIRED_VARS_AIRFLOW = {
    'TELEGRAM_API_ID': TELEGRAM_API_ID,
    'TELEGRAM_API_HASH': TELEGRAM_API_HASH,
    # TELEGRAM_PHONE нужен только если сессия не существует
    'PARSER_CHANNEL_IDENTIFIER': PARSER_CHANNEL_IDENTIFIER,
    'PARSER_CHANNEL_ID': PARSER_CHANNEL_ID,
    'PARSER_TARGET_DATE_STR': PARSER_TARGET_DATE_STR,
    'DB_HOST': DB_HOST,
    'DB_USER': DB_USER,
    'DB_PASSWORD': DB_PASSWORD,
    'DB_NAME': DB_NAME,
}

missing_vars = [name for name, value in REQUIRED_VARS_AIRFLOW.items() if not value]
if missing_vars:
    logging.error(f"Ошибка: Отсутствуют обязательные переменные окружения для задачи парсинга: {', '.join(missing_vars)}")
    sys.exit(1)

# --- Преобразование и проверка типов ---
try:
    # Преобразуем PARSER_CHANNEL_ID в int, так как он будет использоваться как числовой ID
    # Если он уже передается как числовой ID в переменной окружения, это будет работать.
    # Если PARSER_CHANNEL_IDENTIFIER - это числовой ID, можно было бы использовать его,
    # но явная передача PARSER_CHANNEL_ID надежнее для записи в БД.
    channel_id_for_db = int(PARSER_CHANNEL_ID)
except (ValueError, TypeError):
    logging.error(f"Ошибка: PARSER_CHANNEL_ID ('{PARSER_CHANNEL_ID}') должен быть числовым идентификатором канала.")
    sys.exit(1)

try:
    # Преобразуем строку даты в объект date
    target_date = datetime.datetime.strptime(PARSER_TARGET_DATE_STR, '%Y-%m-%d').date()
except ValueError:
    logging.error(f"Ошибка: Неверный формат PARSER_TARGET_DATE_STR ('{PARSER_TARGET_DATE_STR}'). Ожидается 'YYYY-MM-DD'.")
    sys.exit(1)


# Глобальная переменная для клиента Telegram (упрощает управление в async функциях)
telegram_client: TelegramClient | None = None

# --- Функции ---

def connect_db(retry_count=5, delay=5):
    """Пытается подключиться к БД PostgreSQL несколько раз."""
    conn = None
    for attempt in range(1, retry_count + 1):
        try:
            logging.info(f"Попытка подключения к БД ({attempt}/{retry_count}): host={DB_HOST} dbname={DB_NAME} user={DB_USER}")
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                dbname=DB_NAME,
                connect_timeout=10
            )
            conn.autocommit = False # Управляем транзакциями вручную
            logging.info("Успешное подключение к БД!")
            return conn
        except OperationalError as e:
            logging.warning(f"Ошибка подключения к БД: {e}")
            if attempt < retry_count:
                logging.info(f"Повторная попытка через {delay} секунд...")
                time.sleep(delay)
            else:
                logging.error("Превышено количество попыток подключения к БД.")
                return None
        except Exception as e:
            logging.error(f"Неожиданная ошибка при подключении к БД: {e}")
            # Закрываем соединение, если оно было создано перед другой ошибкой
            if conn:
                try:
                    conn.close()
                except Exception as close_err:
                    logging.error(f"Ошибка при закрытии соединения после ошибки подключения: {close_err}")
            return None # Явно возвращаем None
    return None # Если цикл завершился без успешного подключения

def setup_database_schema(conn):
    """Создает или проверяет таблицу telegram_messages и добавляет нужные колонки/индексы."""
    if not conn:
        logging.error("Невозможно настроить схему: нет соединения с БД.")
        return False
    try:
        with conn.cursor() as cur:
            logging.info("Проверка/создание таблицы 'telegram_messages'...")
            # Основная структура таблицы
            cur.execute("""
                CREATE TABLE IF NOT EXISTS telegram_messages (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT NOT NULL,
                    channel_id BIGINT NOT NULL, -- Изменен на NOT NULL, т.к. мы всегда знаем ID канала
                    message_date TIMESTAMPTZ NOT NULL,
                    text TEXT,
                    sender_id BIGINT,
                    views INTEGER,
                    forwards INTEGER,
                    is_reply BOOLEAN,
                    reply_to_msg_id BIGINT,
                    has_media BOOLEAN,
                    raw_data JSONB,
                    parsed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
                );
            """)
            # Добавляем ограничение уникальности (если его нет)
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'telegram_messages_uniq_constraint'
                          AND conrelid = 'telegram_messages'::regclass
                    ) THEN
                        ALTER TABLE telegram_messages
                        ADD CONSTRAINT telegram_messages_uniq_constraint UNIQUE (channel_id, message_id);
                        RAISE NOTICE 'Ограничение telegram_messages_uniq_constraint добавлено.';
                    END IF;
                END;
                $$;
            """)
            # Создаем индексы (если их нет)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_date ON telegram_messages (message_date DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel_date ON telegram_messages (channel_id, message_date DESC);")
            # Индекс по channel_id_ref (если используем его для фильтрации)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel_ref_date ON telegram_messages (channel_id_ref, message_date DESC);")

            conn.commit()
            logging.info("Схема БД 'telegram_messages' проверена/обновлена.")
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Ошибка при настройке схемы БД 'telegram_messages': {error}")
        try:
            conn.rollback()
        except Exception as rb_error:
            logging.error(f"Ошибка при откате транзакции: {rb_error}")
        return False


async def initialize_telegram_client():
    """Инициализирует и подключает Telegram клиент."""
    global telegram_client # Используем глобальную переменную
    if telegram_client and telegram_client.is_connected():
        logging.info("Telegram клиент уже подключен.")
        return True

    logging.info("Инициализация Telegram клиента.")
    # Создаем папку для сессии, если ее нет (важно при запуске в Docker)
    os.makedirs(TELEGRAM_SESSION_FOLDER, exist_ok=True)

    try:
        # Проверяем наличие файла сессии
        session_file_exists = os.path.exists(f"{TELEGRAM_SESSION_PATH}.session")
        logging.info(f"Файл сессии '{TELEGRAM_SESSION_PATH}.session' существует: {session_file_exists}")

        if not session_file_exists and not TELEGRAM_PHONE:
             logging.error("Ошибка: Файл сессии не найден и TELEGRAM_PHONE не указан для создания новой сессии.")
             return False

        telegram_client = TelegramClient(TELEGRAM_SESSION_PATH, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
        logging.info("Подключение к Telegram...")
        await telegram_client.connect()

        if not await telegram_client.is_user_authorized():
            logging.warning("Требуется авторизация пользователя.")
            if not TELEGRAM_PHONE:
                 logging.error("Ошибка: Требуется авторизация, но TELEGRAM_PHONE не указан.")
                 await telegram_client.disconnect()
                 return False
            # ВАЖНО: Интерактивная авторизация здесь невозможна при запуске в Airflow/Docker.
            # Сессия ДОЛЖНА быть создана заранее и файл .session должен быть доступен.
            # Если сессии нет, просто выходим с ошибкой.
            logging.error("Ошибка: Авторизация не пройдена. Файл сессии не найден или недействителен.")
            logging.error("Пожалуйста, создайте файл сессии локально и предоставьте его контейнеру через volume.")
            await telegram_client.disconnect()
            return False
        else:
            me = await telegram_client.get_me()
            if me:
                 logging.info(f"Успешно подключен к Telegram как {me.first_name} (ID: {me.id}).")
                 return True
            else:
                 # Случай, если сессия есть, но get_me() не сработал (маловероятно)
                 logging.error("Не удалось получить информацию о пользователе, хотя сессия существует.")
                 await telegram_client.disconnect()
                 return False

    except AuthKeyError:
         logging.error("Ошибка ключа авторизации (AuthKeyError). Возможно, сессия устарела или повреждена.")
         logging.error("Попробуйте удалить файл сессии и создать его заново локально.")
         if telegram_client and telegram_client.is_connected(): await telegram_client.disconnect()
         return False
    except UserDeactivatedBanError:
         logging.error("Ошибка: Аккаунт пользователя деактивирован или забанен.")
         if telegram_client and telegram_client.is_connected(): await telegram_client.disconnect()
         return False
    except ConnectionError as e:
         logging.error(f"Ошибка соединения с Telegram: {e}")
         if telegram_client and telegram_client.is_connected(): await telegram_client.disconnect()
         return False
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при инициализации Telegram клиента: {e}", exc_info=True)
        if telegram_client and telegram_client.is_connected():
            try:
                await telegram_client.disconnect()
            except Exception as disc_err:
                 logging.error(f"Ошибка при отключении клиента после ошибки инициализации: {disc_err}")
        return False


async def parse_channel_for_day(channel_identifier: str, target_date: date):
    """Парсит сообщения из указанного канала за конкретную дату."""
    global telegram_client
    if not telegram_client or not telegram_client.is_connected():
        logging.error("Невозможно парсить: Telegram клиент не подключен.")
        return None, None # Возвращаем None для сообщений и ID канала

    logging.info(f"Начало парсинга канала '{channel_identifier}' за дату {target_date.isoformat()}")

    # Определяем начало и конец дня в UTC
    day_start_utc = datetime.datetime.combine(target_date, time.min, tzinfo=timezone.utc)
    day_end_utc = day_start_utc + timedelta(days=1) # До начала следующего дня

    messages_data = []
    numeric_channel_id = None
    processed_count = 0

    try:
        # Получаем сущность канала
        logging.info(f"Получение информации о канале '{channel_identifier}'...")
        entity = await telegram_client.get_entity(channel_identifier)
        if not isinstance(entity, Channel):
             # Мы ожидаем именно канал, а не пользователя или чат
             logging.error(f"Сущность '{channel_identifier}' не является каналом (тип: {type(entity)}).")
             return None, None
        numeric_channel_id = entity.id # Используем реальный ID канала
        logging.info(f"Канал найден: '{entity.title}' (ID: {numeric_channel_id})")

        # Итерируемся по сообщениям за указанный день
        # offset_date - дата, начиная с которой идем НАЗАД во времени
        # Поэтому используем начало СЛЕДУЮЩЕГО дня как offset_date
        logging.info(f"Итерация сообщений с offset_date={day_end_utc.isoformat()}...")
        async for message in telegram_client.iter_messages(entity, offset_date=day_end_utc, reverse=False):
            # Время сообщения в UTC
            message_date_utc = message.date.replace(tzinfo=timezone.utc) # Убедимся, что оно aware UTC

            # Прекращаем итерацию, если сообщение вышло за рамки нужного дня
            if message_date_utc < day_start_utc:
                logging.info(f"Достигнуто начало дня {target_date.isoformat()}. Завершение итерации.")
                break

            # Собираем данные сообщения
            message_info = {
                'message_id': message.id,
                'channel_id': numeric_channel_id, # Используем фактический ID канала
                'message_date': message_date_utc,
                'text': message.text or "", # Гарантируем строку
                'sender_id': message.sender_id,
                'views': message.views if hasattr(message, 'views') else None,
                'forwards': message.forwards if hasattr(message, 'forwards') else None,
                'is_reply': message.is_reply,
                'reply_to_msg_id': message.reply_to_msg_id if message.is_reply else None,
                'has_media': message.media is not None,
                # Копируем сырые данные как JSONB (преобразуем дату в строку ISO)
                'raw_data': {
                    'id': message.id,
                    'date': message.date.isoformat(), # Используем ISO формат для JSON
                    'message': message.message, # Синоним text?
                    'out': message.out,
                    'mentioned': message.mentioned,
                    'media_unread': message.media_unread,
                    'silent': message.silent,
                    'post': message.post,
                    'from_scheduled': message.from_scheduled,
                    'legacy': message.legacy,
                    'edit_hide': message.edit_hide,
                    'pinned': message.pinned,
                    # ... и другие поля, которые могут быть интересны ...
                }
            }
            messages_data.append(message_info)
            processed_count += 1
            if processed_count % 100 == 0:
                logging.info(f"Собрано {processed_count} сообщений за {target_date.isoformat()}...")

        logging.info(f"Парсинг за {target_date.isoformat()} завершен. Получено сообщений: {len(messages_data)}")
        # Сообщения уже идут от новых к старым из-за reverse=False и offset_date
        # Если нужно от старых к новым, развернем список
        messages_data.reverse()
        logging.info("Список сообщений развернут (от старых к новым).")
        return messages_data, numeric_channel_id

    except (UsernameNotOccupiedError, ChannelPrivateError, ValueError) as e:
         logging.error(f"Ошибка доступа к каналу '{channel_identifier}': {e}")
    except FloodWaitError as e:
         # В Airflow лучше упасть и попробовать позже, чем спать
         logging.error(f"Ошибка FloodWait: {e.seconds} секунд. Задача завершится с ошибкой.")
         # Можно было бы `time.sleep(e.seconds)`, но это плохая практика в Airflow
         # Лучше настроить retries в DAG
         raise e # Перевыбрасываем ошибку, чтобы Airflow сделал retry
    except AuthKeyError:
         logging.error("Ошибка ключа авторизации во время парсинга. Сессия невалидна.")
         # Нет смысла продолжать
         raise # Перевыбрасываем
    except TypeError as e:
         logging.error(f"Ошибка TypeError при iter_messages: {e}")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка парсинга канала '{channel_identifier}': {e}", exc_info=True)

    # Возвращаем None, None в случае любой ошибки внутри try-except
    return None, None

async def save_messages_to_db(conn, messages_list: list, channel_id_ref: str):
    """Сохраняет список сообщений в базу данных, добавляя channel_id_ref."""
    if not conn:
        logging.error("Невозможно сохранить сообщения: нет соединения с БД.")
        return 0, 0 # saved_count, skipped_count
    if not messages_list:
        logging.info("Нет сообщений для сохранения в БД.")
        return 0, 0

    saved_count = 0
    skipped_count = 0
    # Добавляем channel_id_ref в запрос
    insert_query = """
        INSERT INTO telegram_messages (
            message_id, channel_id, message_date, text, sender_id,
            views, forwards, is_reply, reply_to_msg_id, has_media, raw_data,
            channel_id_ref -- <--- Новая колонка
        ) VALUES (
            %(message_id)s, %(channel_id)s, %(message_date)s, %(text)s, %(sender_id)s,
            %(views)s, %(forwards)s, %(is_reply)s, %(reply_to_msg_id)s, %(has_media)s,
            %(raw_data)s::jsonb,
            %(channel_id_ref)s -- <--- Значение для новой колонки
        )
        ON CONFLICT ON CONSTRAINT telegram_messages_uniq_constraint DO NOTHING;
    """
    logging.info(f"Начало сохранения {len(messages_list)} сообщений в БД для channel_id_ref='{channel_id_ref}'...")
    try:
        with conn.cursor() as cur:
            # Используем execute_batch для потенциального ускорения, но простой цикл надежнее для ON CONFLICT
            for msg_data in messages_list:
                # Добавляем channel_id_ref к данным сообщения
                msg_data['channel_id_ref'] = channel_id_ref
                # Преобразуем raw_data в JSON строку перед передачей в psycopg2
                msg_data['raw_data'] = json.dumps(msg_data.get('raw_data', {}))

                try:
                    cur.execute(insert_query, msg_data)
                    if cur.rowcount > 0:
                        saved_count += 1
                    else:
                        skipped_count += 1
                # Ловим конкретные ошибки БД, если нужно
                except psycopg2.DatabaseError as db_err:
                    logging.error(f"Ошибка БД при вставке сообщения ID={msg_data.get('message_id')} для канала {channel_id_ref}: {db_err}")
                    conn.rollback() # Откат только этой неудачной вставки
                    # Можно добавить логику пропуска или повторной попытки
                    # Пока просто пропускаем и идем дальше
                    continue
                except Exception as e:
                     logging.error(f"Неожиданная ошибка при вставке сообщения ID={msg_data.get('message_id')}: {e}")
                     conn.rollback()
                     continue # Пропускаем сообщение

            # Коммитим всю транзакцию после цикла
            conn.commit()
            logging.info(f"Сохранение в БД завершено. Новых записей: {saved_count}, пропущено (дубликатов): {skipped_count}.")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Критическая ошибка при работе с БД во время сохранения: {error}", exc_info=True)
        if conn:
            try:
                conn.rollback()
            except Exception as rb_error:
                logging.error(f"Ошибка при откате транзакции: {rb_error}")
        # Возвращаем текущие счетчики, даже если произошла ошибка в середине
        return saved_count, skipped_count

    return saved_count, skipped_count

def write_xcom_data(data: dict):
    """Записывает данные в файл XCom для передачи следующей задаче."""
    logging.info(f"Запись данных XCom в файл: {XCOM_PATH}")
    try:
        # Убедимся, что директория существует (Airflow/DockerOperator должен ее создать)
        os.makedirs(os.path.dirname(XCOM_PATH), exist_ok=True)
        with open(XCOM_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2) # Используем indent для читаемости
        logging.info(f"Данные XCom успешно записаны: {data}")
        return True
    except IOError as e:
        logging.error(f"Ошибка записи в файл XCom '{XCOM_PATH}': {e}")
    except TypeError as e:
         logging.error(f"Ошибка сериализации данных XCom в JSON: {e}")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при записи XCom: {e}", exc_info=True)
    return False


async def main():
    """Основная асинхронная функция скрипта парсера для Airflow."""
    global telegram_client # Чтобы можно было использовать в finally
    db_connection = None
    exit_code = 0 # Успех по умолчанию
    parsed_messages_count = 0
    actual_channel_id = None # Фактический числовой ID канала

    try:
        # 1. Подключение к БД
        db_connection = connect_db()
        if not db_connection:
            raise Exception("Не удалось подключиться к базе данных.")

        # 2. Проверка/Настройка схемы БД
        if not setup_database_schema(db_connection):
            raise Exception("Не удалось настроить схему БД.")

        # 3. Инициализация и подключение Telegram клиента
        if not await initialize_telegram_client():
            raise Exception("Не удалось инициализировать или подключиться к Telegram.")

        # 4. Запуск парсинга канала за указанную дату
        # PARSER_CHANNEL_IDENTIFIER - то, что понимает Telethon (ссылка, @username, id)
        # target_date - объект date
        parsed_messages, actual_channel_id = await parse_channel_for_day(PARSER_CHANNEL_IDENTIFIER, target_date)

        if parsed_messages is None:
             # Ошибка произошла внутри parse_channel_for_day и уже залогирована
             raise Exception(f"Произошла ошибка во время парсинга канала '{PARSER_CHANNEL_IDENTIFIER}'.")

        if actual_channel_id is None:
             # Не смогли определить ID канала
             raise Exception(f"Не удалось определить числовой ID для канала '{PARSER_CHANNEL_IDENTIFIER}'.")

        parsed_messages_count = len(parsed_messages)
        logging.info(f"Успешно получено {parsed_messages_count} сообщений из Telegram.")

        # 5. Сохранение результатов в БД
        # Используем PARSER_CHANNEL_ID (который должен быть числовым и передан из DAG) как channel_id_ref
        saved_count, skipped_count = await save_messages_to_db(db_connection, parsed_messages, str(channel_id_for_db)) # Преобразуем ID в строку для VARCHAR колонки
        logging.info(f"Результаты сохранения: {saved_count} новых, {skipped_count} пропущено.")

        # 6. Запись XCom файла для следующей задачи
        xcom_output = {
            "channel_id": channel_id_for_db, # Передаем числовой ID канала
            "target_date_str": PARSER_TARGET_DATE_STR, # Передаем дату
            "parsed_count": parsed_messages_count, # Кол-во сообщений, полученных из телеграма
            "saved_count": saved_count, # Кол-во реально сохраненных в БД
            "status": "success"
        }
        if not write_xcom_data(xcom_output):
             # Если не удалось записать XCom, считаем это ошибкой
             raise Exception("Не удалось записать данные XCom.")

    except SystemExit as e:
        # Ловим sys.exit(), который могли вызвать ранее
        exit_code = e.code if isinstance(e.code, int) else 1
        logging.warning(f"Скрипт завершается через sys.exit() с кодом: {exit_code}")
    except Exception as e:
        # Ловим все остальные ошибки
        logging.error(f"Критическая ошибка в main: {e}", exc_info=True)
        exit_code = 1 # Устанавливаем код ошибки
        # Попытка записать XCom с ошибкой
        error_xcom_output = {
            "channel_id": channel_id_for_db if actual_channel_id else PARSER_CHANNEL_ID,
            "target_date_str": PARSER_TARGET_DATE_STR,
            "status": "failure",
            "error": str(e)
        }
        write_xcom_data(error_xcom_output)
    finally:
        # --- Корректное отключение ---
        # Отключаемся от Telegram
        if telegram_client and telegram_client.is_connected():
            logging.info("Отключение от Telegram...")
            try:
                await telegram_client.disconnect()
                logging.info("Соединение с Telegram закрыто.")
            except Exception as disc_err:
                logging.error(f"Ошибка при отключении от Telegram: {disc_err}")

        # Закрываем соединение с БД
        if db_connection:
            logging.info("Закрытие соединения с БД...")
            try:
                db_connection.close()
                logging.info("Соединение с БД закрыто.")
            except Exception as close_err:
                 logging.error(f"Ошибка при закрытии соединения с БД: {close_err}")

        logging.info(f"--- Скрипт парсера завершен с кодом выхода: {exit_code} ---")
        # Завершаем скрипт с соответствующим кодом
        sys.exit(exit_code)

# --- Точка входа ---
if __name__ == "__main__":
    try:
        logging.info("Запуск основного асинхронного процесса парсера...")
        # Используем asyncio.run() для запуска асинхронной функции main
        asyncio.run(main())
    except KeyboardInterrupt:
         logging.warning("Программа прервана пользователем (Ctrl+C).")
         sys.exit(130) # Стандартный код выхода для KeyboardInterrupt
    except RuntimeError as e:
         # Ловим ошибку, если run вызывается для уже работающего цикла (маловероятно здесь)
         if "cannot schedule new futures after shutdown" in str(e):
             logging.warning("Цикл событий asyncio был остановлен.")
         else:
             logging.error(f"Ошибка выполнения Runtime: {e}", exc_info=True)
         sys.exit(1)
    except SystemExit as e:
         # main() может вызвать sys.exit(), ловим здесь, чтобы ничего больше не делать
         logging.info(f"Завершение работы по SystemExit с кодом {e.code}.")
         # Не нужно делать sys.exit() еще раз
    except Exception as e:
         # Ловим любые другие ошибки на самом верхнем уровне
         logging.critical(f"Неперехваченная ошибка на верхнем уровне: {e}", exc_info=True)
         sys.exit(1)
    # finally:
        # Этот finally не нужен, т.к. sys.exit() завершит процесс
        # logging.info("Скрипт парсера завершил свою работу на верхнем уровне.")