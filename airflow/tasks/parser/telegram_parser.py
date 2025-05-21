import asyncio
import datetime
from datetime import date, time, timedelta, timezone
import os
import sys
import json
import time as sync_time
import logging
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import Json
from typing import Optional, List, Tuple, Dict, Any

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError, AuthKeyError, UserDeactivatedBanError, UsernameNotOccupiedError, ChannelPrivateError
from telethon.tl.types import Channel

import base64 # Для преобразования bytes в строку, когда нужно сохранить содержимое

# Настройка Логгирования
logging.basicConfig(
    level=logging.INFO, 
    format='[%(asctime)s] [%(levelname)s] [Parser] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Загрузка и чление переменных окружения
load_dotenv()

TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
TELEGRAM_PHONE = os.getenv('TELEGRAM_PHONE')
TELEGRAM_SESSION_FOLDER = os.getenv('TELEGRAM_SESSION_FOLDER_IN_CONTAINER', '/app/session')
TELEGRAM_SESSION_NAME = os.getenv('TELEGRAM_SESSION_NAME', 'my_telegram_session')
TELEGRAM_SESSION_PATH = os.path.join(TELEGRAM_SESSION_FOLDER, TELEGRAM_SESSION_NAME)
DB_HOST = os.getenv('DB_HOST', 'postgres_results_db_service')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
PARSER_CHANNEL_IDENTIFIER = os.getenv('PARSER_CHANNEL_IDENTIFIER')
PARSER_CHANNEL_ID_AS_STR = os.getenv('PARSER_CHANNEL_ID')
PARSER_TARGET_DATE_STR = os.getenv('PARSER_TARGET_DATE')
XCOM_PATH = "/airflow/xcom/return.json"

# Валидация обязательных переменных
REQUIRED_VARS_AIRFLOW = {
    'TELEGRAM_API_ID': TELEGRAM_API_ID,
    'TELEGRAM_API_HASH': TELEGRAM_API_HASH,
    'PARSER_CHANNEL_IDENTIFIER': PARSER_CHANNEL_IDENTIFIER,
    'PARSER_CHANNEL_ID': PARSER_CHANNEL_ID_AS_STR,
    'PARSER_TARGET_DATE_STR': PARSER_TARGET_DATE_STR,
    'DB_HOST': DB_HOST,
    'DB_USER': DB_USER,
    'DB_PASSWORD': DB_PASSWORD,
    'DB_NAME': DB_NAME,
}
missing_vars = [name for name, value in REQUIRED_VARS_AIRFLOW.items() if not value]
if missing_vars:
    logger.error(f"Ошибка: Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
    sys.exit(1)

# Преобразование и проверка типов входных параметров
try:
    channel_id_for_db_and_xcom = int(PARSER_CHANNEL_ID_AS_STR)
except (ValueError, TypeError):
    logger.error(f"Ошибка: PARSER_CHANNEL_ID ('{PARSER_CHANNEL_ID_AS_STR}') должен быть числовым идентификатором канала.")
    sys.exit(1)
try:
    target_date_obj = datetime.datetime.strptime(PARSER_TARGET_DATE_STR, '%Y-%m-%d').date()
except ValueError:
    logger.error(f"Ошибка: Неверный формат PARSER_TARGET_DATE_STR ('{PARSER_TARGET_DATE_STR}'). Ожидается 'YYYY-MM-DD'.")
    sys.exit(1)

telegram_client: Optional[TelegramClient] = None

# Функции
def connect_db(retry_count: int = 5, delay: int = 5) -> Optional[psycopg2.extensions.connection]:
    conn: Optional[psycopg2.extensions.connection] = None
    for attempt in range(1, retry_count + 1):
        try:
            logger.info(f"Попытка подключения к БД ({attempt}/{retry_count}): host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER}")
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, connect_timeout=10
            )
            conn.autocommit = False
            logger.info("Успешное подключение к БД!")
            return conn
        except OperationalError as e:
            logger.warning(f"Ошибка подключения к БД: {e}")
            if attempt < retry_count:
                logger.info(f"Повторная попытка через {delay} секунд...")
                sync_time.sleep(delay)
            else:
                logger.error("Превышено количество попыток подключения к БД.")
                return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при подключении к БД: {e}", exc_info=True)
            if conn:
                try: conn.close()
                except Exception as close_err: logger.error(f"Ошибка при закрытии соединения: {close_err}")
            return None
    return None

def setup_database_schema(conn: psycopg2.extensions.connection) -> bool:
    if not conn:
        logger.error("Невозможно настроить схему: нет соединения с БД.")
        return False
    try:
        with conn.cursor() as cur:
            logger.info("Проверка/создание таблицы 'telegram_messages'...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS telegram_messages (
                    id SERIAL PRIMARY KEY, message_id BIGINT NOT NULL, channel_id BIGINT NOT NULL,
                    message_date TIMESTAMPTZ NOT NULL, text TEXT, sender_id BIGINT,
                    views INTEGER, forwards INTEGER, is_reply BOOLEAN, reply_to_msg_id BIGINT,
                    has_media BOOLEAN, raw_data JSONB, parsed_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
                );""")
            cur.execute("""
                DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'telegram_messages_uniq_constraint'
                AND conrelid = 'telegram_messages'::regclass) THEN ALTER TABLE telegram_messages
                ADD CONSTRAINT telegram_messages_uniq_constraint UNIQUE (channel_id, message_id);
                RAISE NOTICE 'Ограничение telegram_messages_uniq_constraint добавлено.'; END IF; END $$;""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_date ON telegram_messages (message_date DESC);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_channel_date ON telegram_messages (channel_id, message_date DESC);")
            conn.commit()
            logger.info("Схема БД 'telegram_messages' проверена/обновлена.")
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Ошибка при настройке схемы БД 'telegram_messages': {error}", exc_info=True)
        try: conn.rollback()
        except Exception as rb_error: logger.error(f"Ошибка при откате транзакции: {rb_error}")
        return False

async def initialize_telegram_client() -> bool:
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
             logger.error("Ошибка: Файл сессии не найден и TELEGRAM_PHONE не указан для новой сессии.")
             return False
        telegram_client = TelegramClient(TELEGRAM_SESSION_PATH, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
        logger.info("Подключение к Telegram...")
        await telegram_client.connect()
        if not await telegram_client.is_user_authorized():
            logger.error("Ошибка: Авторизация не пройдена. Файл сессии недействителен или отсутствует. Создайте его заранее.")
            if telegram_client.is_connected(): await telegram_client.disconnect()
            return False
        me = await telegram_client.get_me()
        if me:
             logger.info(f"Успешно подключен к Telegram как {me.first_name} (ID: {me.id}).")
             return True
        else:
             logger.error("Не удалось получить информацию о пользователе после авторизации.")
             if telegram_client.is_connected(): await telegram_client.disconnect()
             return False
    except (AuthKeyError, UserDeactivatedBanError, ConnectionError) as e: # Объединяем известные ошибки
         logger.error(f"Ошибка Telegram при инициализации: {type(e).__name__} - {e}")
         if telegram_client and telegram_client.is_connected(): await telegram_client.disconnect()
         return False
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при инициализации Telegram клиента: {e}", exc_info=True)
        if telegram_client and telegram_client.is_connected():
            try: await telegram_client.disconnect()
            except Exception as disc_err: logger.error(f"Ошибка при отключении клиента: {disc_err}")
        return False

def convert_to_json_serializable(item: Any) -> Any:
    """Рекурсивно преобразует несериализуемые типы (datetime, date, bytes) в словарях/списках."""
    if isinstance(item, dict):
        return {k: convert_to_json_serializable(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [convert_to_json_serializable(elem) for elem in item]
    elif isinstance(item, (datetime.datetime, datetime.date)):
        return item.isoformat()
    elif isinstance(item, bytes):
        # Пытаемся декодировать как UTF-8, если не удается - используем base64
        try:
            return item.decode('utf-8')
        except UnicodeDecodeError:
            logger.debug(f"Не удалось декодировать bytes (длина: {len(item)}) как UTF-8, используется base64.")
            return base64.b64encode(item).decode('ascii')
    return item

async def parse_channel_for_day(channel_identifier_for_telethon: str, target_date_to_parse: date) -> Tuple[Optional[List[Dict[str, Any]]], Optional[int]]:
    global telegram_client
    if not telegram_client or not telegram_client.is_connected():
        logger.error("Парсинг невозможен: Telegram клиент не подключен.")
        return None, None
    logger.info(f"Начало парсинга канала '{channel_identifier_for_telethon}' за дату {target_date_to_parse.isoformat()}")
    day_start_utc = datetime.datetime.combine(target_date_to_parse, time.min, tzinfo=timezone.utc)
    day_end_utc = day_start_utc + timedelta(days=1)
    messages_data: List[Dict[str, Any]] = []
    actual_numeric_channel_id: Optional[int] = None
    try:
        logger.info(f"Получение информации о канале '{channel_identifier_for_telethon}'...")
        entity = await telegram_client.get_entity(channel_identifier_for_telethon)
        if not isinstance(entity, Channel):
             logger.error(f"Сущность '{channel_identifier_for_telethon}' не является каналом (тип: {type(entity)}).")
             return None, None
        actual_numeric_channel_id = entity.id
        logger.info(f"Канал найден: '{getattr(entity, 'title', 'N/A')}' (ID: {actual_numeric_channel_id})")
        logger.info(f"Итерация сообщений с offset_date={day_end_utc.isoformat()}...")
        async for message in telegram_client.iter_messages(entity, offset_date=day_end_utc, reverse=False):
            message_date_utc = message.date.replace(tzinfo=timezone.utc)
            if message_date_utc < day_start_utc:
                logger.info(f"Достигнуто начало дня {target_date_to_parse.isoformat()}. Завершение итерации.")
                break
            if not (day_start_utc <= message_date_utc < day_end_utc):
                continue
            message_info = {
                'message_id': message.id, 'channel_id': actual_numeric_channel_id,
                'message_date': message_date_utc, 'text': message.text or "",
                'sender_id': message.sender_id, 'views': message.views,
                'forwards': message.forwards, 'is_reply': message.is_reply,
                'reply_to_msg_id': message.reply_to_msg_id, 'has_media': message.media is not None,
                'raw_data': message.to_dict()
            }
            messages_data.append(message_info)
            if len(messages_data) % 50 == 0:
                logger.info(f"Собрано {len(messages_data)} сообщений...")
        logger.info(f"Парсинг завершен. Получено сообщений: {len(messages_data)}")
        messages_data.reverse()
        logger.info("Список сообщений развернут.")
        return messages_data, actual_numeric_channel_id
    except (UsernameNotOccupiedError, ChannelPrivateError, ValueError) as e:
         logger.error(f"Ошибка доступа/получения канала '{channel_identifier_for_telethon}': {e}")
    except FloodWaitError as e:
         logger.error(f"Ошибка FloodWait: {e.seconds} секунд. Задача должна быть перезапущена.")
         raise
    except AuthKeyError:
         logger.error("Критическая ошибка: Ключ авторизации невалиден.")
         raise
    except Exception as e:
        logger.error(f"Непредвиденная ошибка парсинга канала '{channel_identifier_for_telethon}': {e}", exc_info=True)
    return None, actual_numeric_channel_id

async def save_messages_to_db(conn: psycopg2.extensions.connection, messages_list: List[Dict[str, Any]]) -> Tuple[int, int]:
    if not conn or not messages_list:
        logger.info("Нет данных для сохранения в БД.")
        return 0, 0
    saved_count = 0; skipped_count = 0
    insert_query = """
        INSERT INTO telegram_messages (
            message_id, channel_id, message_date, text, sender_id, views, forwards,
            is_reply, reply_to_msg_id, has_media, raw_data
        ) VALUES (
            %(message_id)s, %(channel_id)s, %(message_date)s, %(text)s, %(sender_id)s,
            %(views)s, %(forwards)s, %(is_reply)s, %(reply_to_msg_id)s, %(has_media)s,
            %(raw_data)s -- Передаем обработанный словарь, psycopg2.extras.Json позаботится
        ) ON CONFLICT ON CONSTRAINT telegram_messages_uniq_constraint DO NOTHING;"""
    logger.info(f"Начало сохранения {len(messages_list)} сообщений в БД...")
    try:
        with conn.cursor() as cur:
            for msg_data in messages_list:
                db_insert_data = msg_data.copy()
                # Преобразуем raw_data в JSON-совместимый словарь и затем в psycopg2.extras.Json
                serializable_raw_data = convert_to_json_serializable(db_insert_data.get('raw_data', {}))
                db_insert_data['raw_data'] = Json(serializable_raw_data) # Используем psycopg2.extras.Json

                # Логирование данных перед вставкой
                log_data = {k: v for k,v in db_insert_data.items() if k != 'raw_data'}
                log_data['raw_data_type'] = type(db_insert_data['raw_data']).__name__
                logger.debug(f"Данные для вставки (ID: {db_insert_data.get('message_id')}): {log_data}")
                try:
                    cur.execute(insert_query, db_insert_data)
                    if cur.rowcount > 0: saved_count += 1
                    else: skipped_count += 1
                except psycopg2.DatabaseError as db_err:
                    logger.error(f"Ошибка БД при вставке сообщения ID={msg_data.get('message_id')}: {db_err}", exc_info=True)
                    skipped_count += 1
                    raise # Перевыбрасываем, чтобы откатить всю транзакцию
                except Exception as e:
                     logger.error(f"Неожиданная ошибка при подготовке/вставке сообщения ID={msg_data.get('message_id')}: {e}", exc_info=True)
                     skipped_count += 1
                     raise # Перевыбрасываем
            conn.commit()
            logger.info(f"Сохранение в БД завершено. Новых записей: {saved_count}, пропущено (ошибки/дубликаты): {skipped_count}.")
    except (Exception, psycopg2.DatabaseError) as error: # Ловим и psycopg2.DatabaseError здесь тоже
        logger.error(f"Критическая ошибка во время пакетного сохранения в БД: {error}", exc_info=True)
        if conn:
            try: conn.rollback()
            except Exception as rb_error: logger.error(f"Ошибка при откате транзакции: {rb_error}")
        return 0, len(messages_list) # Все считаются пропущенными, если транзакция отменена
    return saved_count, skipped_count

def write_xcom_data(data: Dict[str, Any]) -> bool:
    logger.info(f"Запись данных XCom в файл: {XCOM_PATH}")
    try:
        xcom_dir = os.path.dirname(XCOM_PATH)
        if xcom_dir and not os.path.exists(xcom_dir): # Проверка, что xcom_dir не пустой
            os.makedirs(xcom_dir, exist_ok=True)
            logger.info(f"Создана директория для XCom: {xcom_dir}")
        with open(XCOM_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Данные XCom успешно записаны: {data}")
        return True
    except Exception as e: # Ловим более общие ошибки
        logger.error(f"Ошибка при записи XCom: {e}", exc_info=True)
    return False

async def main():
    global telegram_client
    db_connection: Optional[psycopg2.extensions.connection] = None
    exit_code = 0 # Успех по умолчанию
    parsed_messages_count = 0
    saved_count = 0
    skipped_count = 0
    actual_channel_id_from_telethon: Optional[int] = None
    xcom_status = "failure" # Статус по умолчанию для XCom, если что-то пойдет не так до его явной установки
    error_message_for_xcom = "Неизвестная ошибка"
    exception_for_xcom: Optional[Exception] = None


    try:
        logger.info("--- Запуск скрипта парсера (Airflow Task) ---")
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

        if parsed_messages is None or actual_channel_id_from_telethon is None:
             raise Exception(f"Ошибка парсинга канала '{PARSER_CHANNEL_IDENTIFIER}'.")

        # Проверка ID
        if actual_channel_id_from_telethon != channel_id_for_db_and_xcom:
            logger.warning(f"ID канала из конфига DAG ({channel_id_for_db_and_xcom}) "
                           f"не совпадает с ID от Telegram ({actual_channel_id_from_telethon}). "
                           f"В БД будет записан ID от Telegram: {actual_channel_id_from_telethon}. "
                           f"Для XCom будет использован ID из конфига DAG: {channel_id_for_db_and_xcom}.")

        parsed_messages_count = len(parsed_messages)
        logger.info(f"Успешно получено {parsed_messages_count} сообщений из Telegram.")

        if parsed_messages_count > 0:
            saved_count, skipped_count = await save_messages_to_db(db_connection, parsed_messages)
            logger.info(f"Результаты сохранения в БД: {saved_count} новых, {skipped_count} пропущено (дубликаты/ошибки).")

            # Проверяем, были ли ошибки именно при вставке (если save_messages_to_db перебрасывает исключение при ошибке)
            # Если saved_count == 0 и skipped_count == parsed_messages_count, И при этом save_messages_to_db
            # не выбросила исключение, то это означает, что все были дубликаты.
            # Если же save_messages_to_db выбросит исключение, мы его поймаем в общем блоке except.
            if saved_count == 0 and skipped_count == parsed_messages_count and parsed_messages_count > 0:
                logger.info("Все полученные сообщения уже существуют в БД (или были пропущены без ошибок вставки).")
                xcom_status = "success_duplicates_found" # Новый статус для XCom
            elif saved_count > 0 or (parsed_messages_count > 0 and skipped_count < parsed_messages_count) :
                xcom_status = "success" # Были сохранены новые или часть была дубликатами, но не все


        else: # parsed_messages_count == 0
            logger.info("Новых сообщений для сохранения не найдено.")
            xcom_status = "success_no_new_messages"

        # Если дошли сюда без исключений, значит, основные операции прошли
        # exit_code остается 0

    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else 1
        exception_for_xcom = e
        logger.warning(f"Скрипт завершается через sys.exit() с кодом: {exit_code}")
    except FloodWaitError as e_flood: # Ловим FloodWaitError отдельно, чтобы передать его в XCom
        logger.error(f"Критическая ошибка FloodWait в main: {e_flood}", exc_info=True)
        exit_code = 1
        exception_for_xcom = e_flood
        xcom_status = "failure_flood_wait"
    except AuthKeyError as e_auth:
        logger.error(f"Критическая ошибка AuthKeyError в main: {e_auth}", exc_info=True)
        exit_code = 1
        exception_for_xcom = e_auth
        xcom_status = "failure_auth_key"
    except Exception as e_main: # Ловим все остальные ошибки
        logger.error(f"Критическая ошибка в main: {e_main}", exc_info=True)
        exit_code = 1
        exception_for_xcom = e_main
        xcom_status = "failure" # Общий failure, если не был установлен специфичный
    finally:
        # Формируем XCom на основе финального статуса и ошибки (если была)
        if exception_for_xcom:
            error_message_for_xcom = str(exception_for_xcom)
            error_type_for_xcom = type(exception_for_xcom).__name__
        else:
            error_message_for_xcom = None
            error_type_for_xcom = None
        
        # Если exit_code !=0, но xcom_status не был установлен на failure, установим его
        if exit_code != 0 and xcom_status not in ["failure", "failure_flood_wait", "failure_auth_key"]:
            xcom_status = "failure"
            if not error_message_for_xcom: # Если ошибка не была явно поймана и сохранена
                error_message_for_xcom = "Неизвестная ошибка привела к ненулевому коду выхода."
                error_type_for_xcom = "UnknownException"


        xcom_output = {
            "channel_id": channel_id_for_db_and_xcom,
            "target_date_str": PARSER_TARGET_DATE_STR,
            "parsed_count": parsed_messages_count,
            "saved_count": saved_count,
            "skipped_because_duplicate_or_no_error": skipped_count if saved_count == 0 and parsed_messages_count > 0 and exit_code == 0 else 0, # Сколько было пропущено без ошибок (по сути, дубликаты)
            "status": xcom_status,
            "error_type": error_type_for_xcom,
            "error_message": error_message_for_xcom
        }
        actual_xcom_written = write_xcom_data(xcom_output) # Записываем XCom

        # Закрываем соединения после записи XCom
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

        # Логируем перед выходом
        logger.info(f"--- Скрипт парсера завершен с кодом выхода: {exit_code} (XCom записан: {actual_xcom_written}) ---")
        sys.exit(exit_code)

if __name__ == "__main__":
    final_exit_code = 0
    try:
        logger.info("Запуск основного асинхронного процесса парсера (локальный или прямой вызов)...")
        asyncio.run(main())
    except KeyboardInterrupt:
         logger.warning("Программа прервана пользователем (Ctrl+C).")
         final_exit_code = 130
         sys.exit(final_exit_code)
    except SystemExit as e:
         logger.info(f"Перехвачен SystemExit с кодом {e.code}. Завершение (из __main__).")
         final_exit_code = e.code if isinstance(e.code, int) else 1
    except Exception as e:
         logger.critical(f"Неперехваченная ошибка на самом верхнем уровне: {e}", exc_info=True)
         final_exit_code = 1
         sys.exit(final_exit_code)
