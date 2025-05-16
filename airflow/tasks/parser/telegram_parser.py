import asyncio
import datetime
from datetime import date, time, timedelta, timezone # Убедимся, что все компоненты datetime импортированы
import os
import sys
import json
import time as sync_time # Переименуем, чтобы не конфликтовать с datetime.time
import logging
# urllib.parse не используется напрямую в этом скрипте, можно удалить, если не нужен для get_channel_username
# from urllib.parse import urlparse
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError # Убраны неиспользуемые errorcodes, errors
from psycopg2.extras import Json # Не используется напрямую, psycopg2 сам справляется с JSONB

# typing.Union и Optional нужны для тайп-хинтов
from typing import Union, Optional, List, Tuple, Dict, Any

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError, AuthKeyError, UserDeactivatedBanError, UsernameNotOccupiedError, ChannelPrivateError
from telethon.tl.types import Channel

import base64 # Для преобразования bytes в строку, если нужно сохранить содержимое

# --- Настройка Логгирования ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [Parser] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__) # Используем именованный логгер

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
load_dotenv()

# --- ЧТЕНИЕ НАСТРОЕК ИЗ ОКРУЖЕНИЯ ---

# Telegram API Credentials
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
TELEGRAM_PHONE = os.getenv('TELEGRAM_PHONE')

# Telegram Session
TELEGRAM_SESSION_FOLDER = os.getenv('TELEGRAM_SESSION_FOLDER_IN_CONTAINER', '/app/session')
TELEGRAM_SESSION_NAME = os.getenv('TELEGRAM_SESSION_NAME', 'my_telegram_session')
TELEGRAM_SESSION_PATH = os.path.join(TELEGRAM_SESSION_FOLDER, TELEGRAM_SESSION_NAME)

# PostgreSQL Database Settings
DB_HOST = os.getenv('DB_HOST', 'postgres_results_db_service')
DB_PORT = os.getenv('DB_PORT', '5432') # Внутренний порт PostgreSQL
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Параметры, передаваемые Airflow DockerOperator
PARSER_CHANNEL_IDENTIFIER = os.getenv('PARSER_CHANNEL_IDENTIFIER') # Для Telethon (ссылка, @username, id)
PARSER_CHANNEL_ID_AS_STR = os.getenv('PARSER_CHANNEL_ID') # Ожидаем строку с числовым ID канала
PARSER_TARGET_DATE_STR = os.getenv('PARSER_TARGET_DATE') # 'YYYY-MM-DD'

# Путь для записи XCom файла
XCOM_PATH = "/airflow/xcom/return.json"

# --- Валидация обязательных переменных ---
REQUIRED_VARS_AIRFLOW = {
    'TELEGRAM_API_ID': TELEGRAM_API_ID,
    'TELEGRAM_API_HASH': TELEGRAM_API_HASH,
    'PARSER_CHANNEL_IDENTIFIER': PARSER_CHANNEL_IDENTIFIER,
    'PARSER_CHANNEL_ID': PARSER_CHANNEL_ID_AS_STR, # Проверяем строковое представление
    'PARSER_TARGET_DATE_STR': PARSER_TARGET_DATE_STR,
    'DB_HOST': DB_HOST,
    'DB_USER': DB_USER,
    'DB_PASSWORD': DB_PASSWORD,
    'DB_NAME': DB_NAME,
}
# TELEGRAM_PHONE проверяется позже, если сессия не найдена

missing_vars = [name for name, value in REQUIRED_VARS_AIRFLOW.items() if not value]
if missing_vars:
    logger.error(f"Ошибка: Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
    sys.exit(1)

# --- Преобразование и проверка типов входных параметров ---
try:
    # Это числовой ID канала, который мы будем использовать для записи в БД и XCom
    # Он передается из DAG-генератора на основе поля "id" из channels.json
    channel_id_for_db_and_xcom = int(PARSER_CHANNEL_ID_AS_STR)
except (ValueError, TypeError):
    logger.error(f"Ошибка: PARSER_CHANNEL_ID ('{PARSER_CHANNEL_ID_AS_STR}') должен быть числовым идентификатором канала.")
    sys.exit(1)

try:
    target_date_obj = datetime.datetime.strptime(PARSER_TARGET_DATE_STR, '%Y-%m-%d').date()
except ValueError:
    logger.error(f"Ошибка: Неверный формат PARSER_TARGET_DATE_STR ('{PARSER_TARGET_DATE_STR}'). Ожидается 'YYYY-MM-DD'.")
    sys.exit(1)

# Глобальная переменная для клиента Telegram
telegram_client: Optional[TelegramClient] = None # Используем Optional из typing

# --- Функции ---

def connect_db(retry_count: int = 5, delay: int = 5) -> Optional[psycopg2.extensions.connection]:
    """Пытается подключиться к БД PostgreSQL несколько раз."""
    conn: Optional[psycopg2.extensions.connection] = None
    for attempt in range(1, retry_count + 1):
        try:
            logger.info(f"Попытка подключения к БД ({attempt}/{retry_count}): host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER}")
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                dbname=DB_NAME,
                connect_timeout=10
            )
            conn.autocommit = False # Управляем транзакциями вручную
            logger.info("Успешное подключение к БД!")
            return conn
        except OperationalError as e:
            logger.warning(f"Ошибка подключения к БД: {e}")
            if attempt < retry_count:
                logger.info(f"Повторная попытка через {delay} секунд...")
                sync_time.sleep(delay) # Используем sync_time для синхронной задержки
            else:
                logger.error("Превышено количество попыток подключения к БД.")
                return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при подключении к БД: {e}", exc_info=True)
            if conn:
                try:
                    conn.close()
                except Exception as close_err:
                    logger.error(f"Ошибка при закрытии соединения после ошибки подключения: {close_err}")
            return None
    return None

def setup_database_schema(conn: psycopg2.extensions.connection) -> bool:
    """Создает или проверяет таблицу telegram_messages."""
    if not conn:
        logger.error("Невозможно настроить схему: нет соединения с БД.")
        return False
    try:
        with conn.cursor() as cur:
            logger.info("Проверка/создание таблицы 'telegram_messages'...")
            # Основная структура таблицы. Колонка channel_id BIGINT NOT NULL уже есть и будет содержать числовой ID.
            # channel_id_ref больше не нужна.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS telegram_messages (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT NOT NULL,
                    channel_id BIGINT NOT NULL,
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
            # Ограничение уникальности по (channel_id, message_id)
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
                        RAISE NOTICE 'Ограничение telegram_messages_uniq_constraint добавлено в telegram_messages.';
                    END IF;
                END;
                $$;
            """)
            # Индексы
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_date ON telegram_messages (message_date DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel_date ON telegram_messages (channel_id, message_date DESC);")
            conn.commit()
            logger.info("Схема БД 'telegram_messages' проверена/обновлена.")
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Ошибка при настройке схемы БД 'telegram_messages': {error}", exc_info=True)
        try:
            conn.rollback()
        except Exception as rb_error:
            logger.error(f"Ошибка при откате транзакции: {rb_error}")
        return False

async def initialize_telegram_client() -> bool:
    """Инициализирует и подключает Telegram клиент."""
    global telegram_client
    if telegram_client and telegram_client.is_connected():
        logger.info("Telegram клиент уже подключен.")
        return True

    logger.info(f"Инициализация Telegram клиента с сессией: {TELEGRAM_SESSION_PATH}")
    os.makedirs(TELEGRAM_SESSION_FOLDER, exist_ok=True)

    try:
        session_file_full_path = f"{TELEGRAM_SESSION_PATH}.session"
        session_file_exists = os.path.exists(session_file_full_path)
        logger.info(f"Файл сессии '{session_file_full_path}' существует: {session_file_exists}")

        if not session_file_exists and not TELEGRAM_PHONE:
             logger.error("Ошибка: Файл сессии не найден и TELEGRAM_PHONE не указан для создания новой сессии.")
             return False

        telegram_client = TelegramClient(TELEGRAM_SESSION_PATH, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
        logger.info("Подключение к Telegram...")
        await telegram_client.connect()

        if not await telegram_client.is_user_authorized():
            logger.warning("Требуется авторизация пользователя.")
            if not TELEGRAM_PHONE:
                 logger.error("Ошибка: Требуется авторизация, но TELEGRAM_PHONE не указан.")
                 if telegram_client.is_connected(): await telegram_client.disconnect()
                 return False

            logging.error("Ошибка: Авторизация не пройдена. Файл сессии не найден или недействителен.")
            logging.error("Пожалуйста, создайте файл сессии локально и предоставьте его контейнеру через volume.")
            if telegram_client.is_connected(): await telegram_client.disconnect()
            return False
        else:
            me = await telegram_client.get_me()
            if me:
                 logger.info(f"Успешно подключен к Telegram как {me.first_name} (ID: {me.id}).")
                 return True
            else:
                 logger.error("Не удалось получить информацию о пользователе, хотя сессия существует.")
                 if telegram_client.is_connected(): await telegram_client.disconnect()
                 return False
    except AuthKeyError:
         logger.error("Ошибка ключа авторизации (AuthKeyError). Возможно, сессия устарела или повреждена.")
         if telegram_client and telegram_client.is_connected(): await telegram_client.disconnect()
         return False
    except UserDeactivatedBanError:
         logger.error("Ошибка: Аккаунт пользователя деактивирован или забанен.")
         if telegram_client and telegram_client.is_connected(): await telegram_client.disconnect()
         return False
    except ConnectionError as e: # Более общий тип ошибки соединения
         logger.error(f"Ошибка соединения с Telegram: {e}")
         if telegram_client and telegram_client.is_connected(): await telegram_client.disconnect()
         return False
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при инициализации Telegram клиента: {e}", exc_info=True)
        if telegram_client and telegram_client.is_connected():
            try:
                await telegram_client.disconnect()
            except Exception as disc_err:
                 logger.error(f"Ошибка при отключении клиента после ошибки инициализации: {disc_err}")
        return False

async def parse_channel_for_day(channel_identifier_for_telethon: str, target_date_to_parse: date) -> Tuple[Optional[List[Dict[str, Any]]], Optional[int]]:
    """Парсит сообщения из указанного канала за конкретную дату.
    Возвращает список словарей с данными сообщений и числовой ID канала.
    """
    global telegram_client
    if not telegram_client or not telegram_client.is_connected():
        logger.error("Невозможно парсить: Telegram клиент не подключен.")
        return None, None

    logger.info(f"Начало парсинга канала '{channel_identifier_for_telethon}' за дату {target_date_to_parse.isoformat()}")

    day_start_utc = datetime.datetime.combine(target_date_to_parse, time.min, tzinfo=timezone.utc)
    day_end_utc = day_start_utc + timedelta(days=1) # до начала следующего дня (не включительно)

    messages_data: List[Dict[str, Any]] = []
    actual_numeric_channel_id: Optional[int] = None
    processed_count = 0

    try:
        logger.info(f"Получение информации о канале '{channel_identifier_for_telethon}'...")
        entity = await telegram_client.get_entity(channel_identifier_for_telethon)

        if not isinstance(entity, Channel):
             logger.error(f"Сущность '{channel_identifier_for_telethon}' не является каналом (тип: {type(entity)}).")
             return None, None
        actual_numeric_channel_id = entity.id
        logger.info(f"Канал найден: '{getattr(entity, 'title', 'N/A')}' (ID: {actual_numeric_channel_id})")

        logger.info(f"Итерация сообщений с offset_date={day_end_utc.isoformat()} (поиск сообщений ДО этой даты)...")
        async for message in telegram_client.iter_messages(entity, offset_date=day_end_utc, reverse=False):
            message_date_utc = message.date.replace(tzinfo=timezone.utc)

            if message_date_utc < day_start_utc:
                logger.info(f"Достигнуто начало дня {target_date_to_parse.isoformat()} (сообщение от {message_date_utc}). Завершение итерации.")
                break
            
            # Проверка, что сообщение попадает в нужный день (на случай граничных условий Telethon)
            if not (day_start_utc <= message_date_utc < day_end_utc):
                continue

            message_info = {
                'message_id': message.id,
                'channel_id': actual_numeric_channel_id, # Используем фактический ID канала
                'message_date': message_date_utc,
                'text': message.text or "",
                'sender_id': message.sender_id,
                'views': message.views,
                'forwards': message.forwards,
                'is_reply': message.is_reply,
                'reply_to_msg_id': message.reply_to_msg_id,
                'has_media': message.media is not None,
                'raw_data': message.to_dict() # Сохраняем весь объект сообщения как dict
            }
            messages_data.append(message_info)
            processed_count += 1
            if processed_count % 50 == 0: # Логируем реже
                logger.info(f"Собрано {processed_count} сообщений за {target_date_to_parse.isoformat()}...")

        logger.info(f"Парсинг за {target_date_to_parse.isoformat()} завершен. Получено сообщений: {len(messages_data)}")
        messages_data.reverse() # Разворачиваем, чтобы были от старых к новым для сохранения
        logger.info("Список сообщений развернут (от старых к новым).")
        return messages_data, actual_numeric_channel_id

    except (UsernameNotOccupiedError, ChannelPrivateError) as e:
         logger.error(f"Ошибка доступа к каналу '{channel_identifier_for_telethon}': {e}")
    except ValueError as e: # Может возникнуть, если channel_identifier некорректен для get_entity
         logger.error(f"Ошибка при получении сущности канала '{channel_identifier_for_telethon}': {e}")
    except FloodWaitError as e:
         logger.error(f"Ошибка FloodWait: {e.seconds} секунд. Задача завершится с ошибкой и должна быть перезапущена Airflow.")
         raise # Перевыбрасываем, чтобы Airflow знал об ошибке
    except AuthKeyError:
         logger.error("Критическая ошибка: Ключ авторизации невалиден во время парсинга. Сессия повреждена.")
         raise
    except Exception as e:
        logger.error(f"Непредвиденная ошибка парсинга канала '{channel_identifier_for_telethon}': {e}", exc_info=True)

    return None, actual_numeric_channel_id # Возвращаем ID, если он был получен, даже при ошибке парсинга


async def save_messages_to_db(conn: psycopg2.extensions.connection, messages_list: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Сохраняет список сообщений в базу данных. Использует channel_id из данных сообщения."""
    if not conn:
        logger.error("Невозможно сохранить сообщения: нет соединения с БД.")
        return 0, 0
    if not messages_list:
        logger.info("Нет сообщений для сохранения в БД.")
        return 0, 0

    saved_count = 0
    skipped_count = 0
    # channel_id_ref больше не используется
    insert_query = """
        INSERT INTO telegram_messages (
            message_id, channel_id, message_date, text, sender_id,
            views, forwards, is_reply, reply_to_msg_id, has_media, raw_data
        ) VALUES (
            %(message_id)s, %(channel_id)s, %(message_date)s, %(text)s, %(sender_id)s,
            %(views)s, %(forwards)s, %(is_reply)s, %(reply_to_msg_id)s, %(has_media)s,
            %(raw_data_json)s -- Используем преобразованный в JSON объект
        )
        ON CONFLICT ON CONSTRAINT telegram_messages_uniq_constraint DO NOTHING;
    """
    logger.info(f"Начало сохранения {len(messages_list)} сообщений в БД...")
    try:
        with conn.cursor() as cur:
            for msg_data in messages_list:
                db_insert_data = msg_data.copy()
                raw_data_dict = db_insert_data.get('raw_data', {})

                def convert_to_json_serializable(item):
                    """Рекурсивно преобразует несериализуемые типы в словарях/списках."""
                    if isinstance(item, dict):
                        return {k: convert_to_json_serializable(v) for k, v in item.items()}
                    elif isinstance(item, list):
                        return [convert_to_json_serializable(elem) for elem in item]
                    elif isinstance(item, (datetime.datetime, datetime.date)):
                        return item.isoformat()
                    elif isinstance(item, bytes):
                        # Вариант 1: Декодировать в строку, если это текст (например, UTF-8)
                        # Это предпочтительно, если ты ожидаешь, что это текстовые данные.
                        try:
                            return item.decode('utf-8')
                        except UnicodeDecodeError:
                            # Вариант 2: Если это не текст, а бинарные данные,
                            # можно представить их как base64 строку, чтобы не потерять.
                            # Или просто заменить на плейсхолдер.
                            logging.warning(f"Не удалось декодировать bytes как UTF-8, будет использовано base64 или плейсхолдер.")
                            # return base64.b64encode(item).decode('ascii') # Base64 вариант
                            return f"<bytes_data_len:{len(item)}>" # Простой плейсхолдер
                    return item

                serializable_raw_data = convert_to_json_serializable(raw_data_dict) # Используем новую функцию
                db_insert_data['raw_data_json'] = json.dumps(serializable_raw_data)

                try:
                    cur.execute(insert_query, db_insert_data) # insert_query должен ожидать строку для raw_data
                    if cur.rowcount > 0:
                        saved_count += 1
                    else:
                        skipped_count += 1
                except psycopg2.DatabaseError as db_err:
                    logger.error(f"Ошибка БД при вставке сообщения ID={msg_data.get('message_id')}: {db_err}")
                    # Не откатываем всю транзакцию, чтобы другие сообщения могли сохраниться
                    # conn.rollback() # Откат всей транзакции здесь не нужен
                    # Если ошибка на уровне конкретного сообщения, логируем и пропускаем
                    skipped_count += 1 # Считаем его пропущенным
                    continue # к следующему сообщению
                except Exception as e:
                     logger.error(f"Неожиданная ошибка при подготовке/вставке сообщения ID={msg_data.get('message_id')}: {e}", exc_info=True)
                     skipped_count += 1
                     continue

            conn.commit() # Коммитим все успешные вставки
            logger.info(f"Сохранение в БД завершено. Новых записей: {saved_count}, пропущено (ошибки/дубликаты): {skipped_count}.")
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Критическая ошибка во время пакетного сохранения в БД: {error}", exc_info=True)
        if conn:
            try:
                conn.rollback() # Откатываем всю транзакцию в случае общей ошибки
            except Exception as rb_error:
                logger.error(f"Ошибка при откате транзакции: {rb_error}")
        # Возвращаем 0, так как транзакция была отменена
        return 0, len(messages_list) # Все сообщения считаются пропущенными

    return saved_count, skipped_count

def write_xcom_data(data: Dict[str, Any]) -> bool:
    """Записывает данные в файл XCom для передачи следующей задаче."""
    logger.info(f"Запись данных XCom в файл: {XCOM_PATH}")
    try:
        xcom_dir = os.path.dirname(XCOM_PATH)
        if not os.path.exists(xcom_dir): # Проверка и создание директории, если ее нет
            os.makedirs(xcom_dir, exist_ok=True)
            logger.info(f"Создана директория для XCom: {xcom_dir}")

        with open(XCOM_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Данные XCom успешно записаны: {data}")
        return True
    except IOError as e:
        logger.error(f"Ошибка записи в файл XCom '{XCOM_PATH}': {e}")
    except TypeError as e:
         logger.error(f"Ошибка сериализации данных XCom в JSON: {e}")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при записи XCom: {e}", exc_info=True)
    return False

async def main():
    """Основная асинхронная функция скрипта парсера для Airflow."""
    global telegram_client
    db_connection: Optional[psycopg2.extensions.connection] = None
    exit_code = 0
    parsed_messages_count = 0
    # Используем channel_id_for_db_and_xcom, который был преобразован из PARSER_CHANNEL_ID в начале скрипта
    # actual_channel_id_from_telethon будет числовым ID, полученным от Telethon
    actual_channel_id_from_telethon: Optional[int] = None

    try:
        logger.info("--- Запуск скрипта парсера (Airflow Task) ---")
        logger.info(f"Целевой канал (идентификатор для Telethon): {PARSER_CHANNEL_IDENTIFIER}")
        logger.info(f"Числовой ID канала (из конфига DAG): {channel_id_for_db_and_xcom}")
        logger.info(f"Целевая дата парсинга: {PARSER_TARGET_DATE_STR}")

        db_connection = connect_db()
        if not db_connection:
            raise Exception("Не удалось подключиться к базе данных.")

        if not setup_database_schema(db_connection):
            raise Exception("Не удалось настроить схему БД 'telegram_messages'.")

        if not await initialize_telegram_client():
            raise Exception("Не удалось инициализировать или подключиться к Telegram.")

        parsed_messages, actual_channel_id_from_telethon = await parse_channel_for_day(
            PARSER_CHANNEL_IDENTIFIER, target_date_obj
        )

        if parsed_messages is None:
             raise Exception(f"Произошла ошибка во время парсинга канала '{PARSER_CHANNEL_IDENTIFIER}'. Подробности в логах выше.")

        if actual_channel_id_from_telethon is None:
             # Эта проверка важна, так как actual_channel_id_from_telethon используется для записи в БД
             raise Exception(f"Не удалось определить фактический числовой ID для канала '{PARSER_CHANNEL_IDENTIFIER}'.")
        
        # Проверка соответствия ID (опционально, но полезно для отладки)
        if actual_channel_id_from_telethon != channel_id_for_db_and_xcom:
            logger.warning(f"Внимание! ID канала из конфига ({channel_id_for_db_and_xcom}) "
                           f"не совпадает с ID, полученным от Telegram ({actual_channel_id_from_telethon}) "
                           f"для идентификатора '{PARSER_CHANNEL_IDENTIFIER}'. "
                           f"Будет использован ID от Telegram: {actual_channel_id_from_telethon}.")
            # Решаем, какой ID использовать. Лучше тот, что от Telegram, так как он точный.
            # Но для консистентности с config/channels.json и связью с summaries,
            # нужно чтобы ID из конфига был правильным числовым ID.
            # Пока оставим использование channel_id_for_db_and_xcom для XCom,
            # а actual_channel_id_from_telethon для записи в telegram_messages.channel_id.
            # В идеале, channel_id_for_db_and_xcom ДОЛЖЕН быть == actual_channel_id_from_telethon.

        parsed_messages_count = len(parsed_messages)
        logger.info(f"Успешно получено {parsed_messages_count} сообщений из Telegram.")

        saved_count, skipped_count = await save_messages_to_db(db_connection, parsed_messages)
        logger.info(f"Результаты сохранения в БД: {saved_count} новых, {skipped_count} пропущено.")

        if parsed_messages_count > 0 and saved_count == 0 and skipped_count == parsed_messages_count:
            # Эта ситуация означает, что ВСЕ сообщения не удалось сохранить
            logger.error("Ни одно из полученных сообщений не было сохранено в БД из-за ошибок.")
            # Если это критично, то нужно упасть
            raise Exception("Критическая ошибка: не удалось сохранить сообщения в БД.")
        elif parsed_messages_count > 0 and saved_count == 0 and skipped_count < parsed_messages_count:
            # Некоторые сообщения не были сохранены, но и не все были пропущены из-за ошибок - странная ситуация
            logger.warning("Сообщения были получены, но ни одно не сохранено, и не все были пропущены из-за ошибок. Проверьте логи.")
            # Можно решить, считать ли это ошибкой

        # Формирование XCom
        current_status = "success"
        if exit_code != 0: # Если ранее уже была ошибка
            current_status = "failure"
        elif parsed_messages_count > 0 and saved_count == 0: # Если не сохранили ничего из того, что получили
            current_status = "partial_failure" # или "warning"

        xcom_output = {
            "channel_id": channel_id_for_db_and_xcom,
            "target_date_str": PARSER_TARGET_DATE_STR,
            "parsed_count": parsed_messages_count,
            "saved_count": saved_count,
            "status": current_status # Обновленный статус
        }
        if not write_xcom_data(xcom_output):
             raise Exception("Не удалось записать данные XCom.")

    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else 1
        logger.warning(f"Скрипт завершается через sys.exit() с кодом: {exit_code}")
    except Exception as e:
        logger.error(f"Критическая ошибка в main: {e}", exc_info=True)
        exit_code = 1
        error_xcom_output = {
            "channel_id": channel_id_for_db_and_xcom, # ID из конфига
            "target_date_str": PARSER_TARGET_DATE_STR,
            "status": "failure",
            "error_type": type(e).__name__,
            "error_message": str(e)
        }
        write_xcom_data(error_xcom_output) # Попытка записать XCom с ошибкой
    finally:
        if telegram_client and telegram_client.is_connected():
            logger.info("Отключение от Telegram...")
            try:
                await telegram_client.disconnect()
                logger.info("Соединение с Telegram закрыто.")
            except Exception as disc_err:
                logger.error(f"Ошибка при отключении от Telegram: {disc_err}")

        if db_connection:
            logger.info("Закрытие соединения с БД...")
            try:
                db_connection.close()
                logger.info("Соединение с БД закрыто.")
            except Exception as close_err:
                 logger.error(f"Ошибка при закрытии соединения с БД: {close_err}")

        logger.info(f"--- Скрипт парсера завершен с кодом выхода: {exit_code} ---")
        sys.exit(exit_code)

if __name__ == "__main__":
    # Этот блок предназначен для локального запуска, но в Airflow он не выполняется напрямую.
    # Airflow DockerOperator запускает скрипт, и main() вызывается из-за sys.exit() в конце.
    final_exit_code = 0
    try:
        logger.info("Запуск основного асинхронного процесса парсера (локальный или прямой вызов)...")
        asyncio.run(main())
        # asyncio.run(main()) вызовет sys.exit() внутри main->finally,
        # поэтому код после него обычно не выполняется, если main() завершается через sys.exit().
        # Чтобы это корректно обработать, нужно ловить SystemExit.
    except KeyboardInterrupt:
         logger.warning("Программа прервана пользователем (Ctrl+C).")
         final_exit_code = 130
    except SystemExit as e:
         logger.info(f"Перехвачен SystemExit с кодом {e.code}. Завершение.")
         final_exit_code = e.code if isinstance(e.code, int) else 1
    except Exception as e:
         logger.critical(f"Неперехваченная ошибка на самом верхнем уровне: {e}", exc_info=True)
         final_exit_code = 1
    finally:
        # Этот лог может не всегда появляться, если sys.exit() в main() уже завершил процесс.
        logger.info(f"Скрипт парсера окончательно завершается с кодом {final_exit_code} (из __main__ блока).")
        # Убедимся, что процесс завершается с правильным кодом, если main не сделал этого
        if final_exit_code != 0 and sys.exc_info()[0] is None : # Если нет активного исключения
            sys.exit(final_exit_code)