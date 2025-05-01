import asyncio
import datetime
import os
import re
import json
import time # Для синхронной задержки в connect_db
from urllib.parse import urlparse
from dotenv import load_dotenv, find_dotenv
import psycopg2 # Для PostgreSQL
from psycopg2 import OperationalError, errorcodes, errors # Для обработки ошибок БД

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telethon.errors.rpcerrorlist import UsernameNotOccupiedError, ChannelPrivateError

# Попытка импорта zoneinfo (Python 3.9+)
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    _HAVE_ZONEINFO = True
except ImportError:
    _HAVE_ZONEINFO = False

# Попытка импорта tzlocal для автоопределения пояса
try:
    from tzlocal import get_localzone
    _HAVE_TZLOCAL = True
except ImportError:
    _HAVE_TZLOCAL = False

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
load_dotenv(find_dotenv(), override=True) # override=True, чтобы .env перезаписывал переменные терминала

# --- ЧТЕНИЕ НАСТРОЕК ИЗ ОКРУЖЕНИЯ ---

# Telegram API Credentials (Пользовательский аккаунт)
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
TELEGRAM_PHONE = os.getenv('TELEGRAM_PHONE') # Телефон для входа/авторизации

# Telegram Session & Target
TARGET_CHANNEL_LINK = os.getenv('TARGET_CHANNEL_LINK')
# Путь к ПАПКЕ сессии внутри контейнера (или локально)
TELEGRAM_SESSION_FOLDER = os.getenv('TELEGRAM_SESSION_FOLDER_IN_CONTAINER', '.')
# Имя файла сессии (без .session)
TELEGRAM_SESSION_NAME = os.getenv('TELEGRAM_SESSION_NAME', 'my_telegram_session')
# Полный путь к файлу сессии
TELEGRAM_SESSION_PATH = os.path.join(TELEGRAM_SESSION_FOLDER, TELEGRAM_SESSION_NAME)

# PostgreSQL Database Settings
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Parsing Parameters
def str_to_bool(val):
    if not val: return False
    return val.lower() in ('true', '1', 't', 'yes', 'y')

PARSE_BY_DATE = str_to_bool(os.getenv('PARSE_BY_DATE', 'False'))
START_DATE_STR = os.getenv('START_DATE_STR', None)
DATE_FORMAT = os.getenv('DATE_FORMAT', "%Y-%m-%d %H:%M:%S")
AUTO_DETECT_TIMEZONE = str_to_bool(os.getenv('AUTO_DETECT_TIMEZONE', 'True'))
TIME_ZONE_STR = os.getenv('TIME_ZONE_STR', 'Europe/Moscow')

def str_to_int_or_none(val):
    if val is None or val == '': return None
    try: return int(val)
    except (ValueError, TypeError): return None
MESSAGE_LIMIT = str_to_int_or_none(os.getenv('MESSAGE_LIMIT', '100'))

# --- Валидация обязательных переменных ---
REQUIRED_VARS = {
    'TELEGRAM_API_ID': TELEGRAM_API_ID,
    'TELEGRAM_API_HASH': TELEGRAM_API_HASH,
    'TELEGRAM_PHONE': TELEGRAM_PHONE, # Телефон теперь обязателен
    'TARGET_CHANNEL_LINK': TARGET_CHANNEL_LINK,
    'DB_HOST': DB_HOST,
    'DB_USER': DB_USER,
    'DB_PASSWORD': DB_PASSWORD,
    'DB_NAME': DB_NAME,
}

missing_vars = [name for name, value in REQUIRED_VARS.items() if not value]
if missing_vars:
    print(f"[!] Ошибка: Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
    exit(1)

if PARSE_BY_DATE and not START_DATE_STR:
    print(f"[!] Ошибка: PARSE_BY_DATE=True, но START_DATE_STR не установлена.")
    exit(1)

# Глобальная переменная для клиента Telegram
client = None

# --- Функции ---

def get_channel_username(link):
    if not link: return None
    parsed = urlparse(link)
    path = parsed.path.strip('/')
    if path.startswith('joinchat/'): return link
    else: parts = path.split('/'); return parts[-1] if parts else None

def make_datetime_aware(date_str, auto_detect=False, manual_tz_str=None):
    try:
        dt_naive = datetime.datetime.strptime(date_str, DATE_FORMAT)
        tz_repr_str = None
        if auto_detect:
            print("[*] Режим таймзоны: Автоопределение.")
            if _HAVE_TZLOCAL:
                try:
                    local_tz = get_localzone(); dt_aware = dt_naive.replace(tzinfo=local_tz)
                    tz_repr_str = f"{str(local_tz)}_local"; print(f"[*] Лог: Локальная зона '{tz_repr_str}'.")
                    return dt_aware, tz_repr_str
                except Exception as e: print(f"[!] Ошибка автоопределения зоны: {e}")
            else: print("[!] Предупреждение: 'tzlocal' недоступна. Автоопределение невозможно.")
            tz_repr_str = "UTC"; dt_aware = dt_naive.replace(tzinfo=datetime.timezone.utc); print(f"[*] Лог: Используется UTC.")
            return dt_aware, tz_repr_str
        else:
            effective_tz_str = manual_tz_str.strip() if manual_tz_str else ""
            print(f"[*] Режим таймзоны: Ручное указание ('{effective_tz_str}' или UTC).")
            if effective_tz_str:
                if _HAVE_ZONEINFO:
                    try:
                        tz = ZoneInfo(effective_tz_str); dt_aware = dt_naive.replace(tzinfo=tz)
                        tz_repr_str = effective_tz_str; print(f"[*] Лог: Используется зона: {tz_repr_str}")
                        return dt_aware, tz_repr_str
                    except ZoneInfoNotFoundError: print(f"[!] Ошибка: Зона '{effective_tz_str}' не найдена.") ; return None, None
                else: print("[!] Ошибка: 'zoneinfo' недоступен (нужен Python 3.9+)."); return None, None
            else:
                tz_repr_str = "UTC"; dt_aware = dt_naive.replace(tzinfo=datetime.timezone.utc)
                print(f"[*] Лог: Зона не указана, используется UTC."); return dt_aware, tz_repr_str
    except ValueError: print(f"[!] Ошибка: Неверный формат START_DATE_STR ('{date_str}'). Ожидается: '{DATE_FORMAT}'"); return None, None
    except Exception as e: print(f"[!] Ошибка обработки даты: {e}"); return None, None

def connect_db(retry_count=5, delay=5):
    """Пытается подключиться к БД несколько раз."""
    conn = None; attempt = 0
    while attempt < retry_count:
        attempt += 1
        try:
            print(f"[*] Попытка подключения к БД ({attempt}/{retry_count}): host={DB_HOST} dbname={DB_NAME} user={DB_USER}")
            conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)
            conn.autocommit = False # Управляем транзакциями вручную
            print("[*] Успешное подключение к БД!")
            return conn
        except OperationalError as e:
            print(f"[!] Ошибка подключения к БД: {e}")
            if attempt < retry_count: print(f"[*] Повторная попытка через {delay} секунд..."); time.sleep(delay)
            else: print("[!] Превышено количество попыток подключения к БД."); return None
    return None

def setup_database_schema(conn):
    """Создает таблицу для сообщений, если она не существует."""
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS telegram_messages (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT NOT NULL,
                    channel_id BIGINT,
                    message_date TIMESTAMPTZ NOT NULL,
                    text TEXT,
                    sender_id BIGINT,
                    views INTEGER,
                    forwards INTEGER,
                    is_reply BOOLEAN,
                    reply_to_msg_id BIGINT,
                    has_media BOOLEAN,
                    raw_data JSONB,
                    parsed_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'telegram_messages_uniq_constraint'
                    ) THEN
                        ALTER TABLE telegram_messages
                        ADD CONSTRAINT telegram_messages_uniq_constraint UNIQUE (channel_id, message_id);
                    END IF;
                END;
                $$;
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_telegram_messages_date ON telegram_messages (message_date);")
            conn.commit()
            print("[*] Схема БД проверена/создана.")
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"[!] Ошибка при настройке схемы БД: {error}")
        conn.rollback(); return False

async def parse_channel(current_client, channel_entity, limit=None, start_date=None):
    """Парсит сообщения из указанного канала."""
    print(f"\n[*] Начинаю парсинг канала: {channel_entity}")
    use_offset_date = None; use_reverse = False
    if start_date:
        if start_date.tzinfo is None or start_date.tzinfo.utcoffset(start_date) is None:
             start_date = start_date.replace(tzinfo=datetime.timezone.utc)
        print(f"[*] Цель: {limit or 'все'} сообщений ПОСЛЕ {start_date.isoformat()}")
        use_offset_date = start_date; use_reverse = True
    else:
        print(f"[*] Цель: ПОСЛЕДНИЕ {limit or 'все доступные'} сообщений")
        use_reverse = False

    messages_data = []
    try:
        entity = await current_client.get_entity(channel_entity)
        print(f"[*] Канал найден: {entity.title} (ID: {entity.id})")
        numeric_channel_id = entity.id

        processed_count = 0
        async for message in current_client.iter_messages(entity, limit=limit, offset_date=use_offset_date, reverse=use_reverse):
            message_date_utc = message.date.astimezone(datetime.timezone.utc)
            message_info = {
                'id': message.id,
                'channel_id': numeric_channel_id,
                'date': message_date_utc,
                'text': message.text or "",
                'sender_id': message.sender_id,
                'views': message.views if hasattr(message, 'views') else None,
                'forwards': message.forwards if hasattr(message, 'forwards') else None,
                'is_reply': message.is_reply,
                'reply_to_msg_id': message.reply_to_msg_id if message.is_reply else None,
                'has_media': message.media is not None,
            }
            messages_data.append(message_info)
            processed_count += 1
            if processed_count % 100 == 0: print(f"[*] Собрано {processed_count} сообщений...")

        print(f"[*] Парсинг завершен. Получено сообщений: {len(messages_data)}")
        if not use_reverse and not use_offset_date and messages_data:
             messages_data.reverse(); print("[*] Лог: Результат развернут (от старых к новым).")
        return messages_data

    except (UsernameNotOccupiedError, ChannelPrivateError, ValueError) as e:
         print(f"[!] Ошибка доступа к каналу '{channel_entity}': {e}")
    except FloodWaitError as e: print(f"[!] Ошибка FloodWait: подождите {e.seconds} секунд.")
    except TypeError as e: print(f"[!] Ошибка TypeError при iter_messages: {e}")
    except Exception as e: import traceback; print(f"[!] Непредвиденная ошибка парсинга: {e}"); traceback.print_exc()
    return None

async def save_messages_to_db(conn, messages_list):
    """Сохраняет список сообщений в базу данных."""
    if not conn or not messages_list:
        print("[!] Нет соединения с БД или нет сообщений для сохранения.")
        return 0

    saved_count = 0; skipped_count = 0
    insert_query = """
        INSERT INTO telegram_messages (
            message_id, channel_id, message_date, text, sender_id,
            views, forwards, is_reply, reply_to_msg_id, has_media, raw_data
        ) VALUES (
            %(id)s, %(channel_id)s, %(date)s, %(text)s, %(sender_id)s,
            %(views)s, %(forwards)s, %(is_reply)s, %(reply_to_msg_id)s, %(has_media)s,
            %(raw_data)s::jsonb
        )
        ON CONFLICT ON CONSTRAINT telegram_messages_uniq_constraint DO NOTHING;
    """
    try:
        with conn.cursor() as cur:
            for msg_data in messages_list:
                raw_copy = msg_data.copy()
                raw_copy['date'] = raw_copy['date'].isoformat()
                msg_data['raw_data'] = json.dumps(raw_copy)

                try:
                    cur.execute(insert_query, msg_data)
                    if cur.rowcount > 0: saved_count += 1
                    else: skipped_count += 1
                except (Exception, psycopg2.DatabaseError) as error:
                    print(f"[!] Ошибка вставки сообщения ID={msg_data.get('id')}: {error}")
                    conn.rollback(); continue

            conn.commit()
            print(f"[*] Сохранение в БД завершено. Новых записей: {saved_count}, пропущено (дубликатов): {skipped_count}.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"[!] Критическая ошибка при работе с БД: {error}")
        if conn: conn.rollback()
    return saved_count


async def main():
    """Основная функция скрипта."""
    global client
    db_connection = None

    try:
        # --- Подключение к БД ---
        db_connection = connect_db()
        if not db_connection: raise Exception("Не удалось подключиться к базе данных.")

        # --- Инициализация схемы БД ---
        if not setup_database_schema(db_connection):
            raise Exception("Не удалось настроить схему БД.")

        # --- Инициализация клиента Telegram ---
        print("[*] Инициализация Telegram клиента для пользователя.")
        os.makedirs(TELEGRAM_SESSION_FOLDER, exist_ok=True)
        client = TelegramClient(TELEGRAM_SESSION_PATH, int(TELEGRAM_API_ID), TELEGRAM_API_HASH, system_version="4.16.30-vxCUSTOM")

        # --- Подключение и авторизация Telegram (Пользователь) ---
        print("[*] Подключаюсь к Telegram...")
        await client.connect()
        if not await client.is_user_authorized():
            print("[*] Требуется авторизация пользователя.")
            await client.send_code_request(TELEGRAM_PHONE)
            # ВНИМАНИЕ: Этот блок потребует интерактивного ввода, если сессия не существует.
            # При запуске в Docker/Airflow убедитесь, что файл сессии (.session)
            # уже создан локально и помещен в папку, которая монтируется как volume.
            try:
                code = input('[INTERACTIVE] Введите код подтверждения из Telegram: ')
                await client.sign_in(TELEGRAM_PHONE, code)
            except SessionPasswordNeededError:
                password = input('[INTERACTIVE] Введите пароль 2FA: ')
                await client.sign_in(password=password)
            except EOFError:
                # Эта ошибка возникнет, если input() вызывается в неинтерактивной среде (например, Docker без -it)
                print("\n[!!!] Ошибка: Попытка интерактивного ввода в неинтерактивной среде.")
                print("[!!!] Пожалуйста, создайте файл сессии Telegram локально перед запуском контейнера.")
                raise # Перевыбрасываем ошибку, чтобы остановить выполнение
            print("[*] Авторизация пользователя прошла успешно!")
        else:
            me = await client.get_me()
            print(f"[*] Успешно подключен как {me.first_name} (сессия: '{TELEGRAM_SESSION_PATH}.session').")

        # --- Основная логика парсинга ---
        print(f"\n[*] Целевой канал для парсинга: {TARGET_CHANNEL_LINK}")
        channel_entity = get_channel_username(TARGET_CHANNEL_LINK)
        if not channel_entity: raise ValueError("Некорректная ссылка TARGET_CHANNEL_LINK.")

        start_date_obj = None; tz_repr_for_filename = None
        if PARSE_BY_DATE:
            print(f"[*] Режим парсинга: ПОСЛЕ даты {START_DATE_STR}")
            start_date_obj, tz_repr_for_filename = make_datetime_aware(START_DATE_STR, AUTO_DETECT_TIMEZONE, TIME_ZONE_STR)
            if not start_date_obj: raise ValueError("Не удалось обработать дату начала.")
            print(f"[*] Дата начала для запроса: {start_date_obj.isoformat()}")
        else:
            print(f"[*] Режим парсинга: ПОСЛЕДНИЕ {MESSAGE_LIMIT or 'N/A'} сообщений")

        # --- Запуск функции парсинга ---
        parsed_messages = await parse_channel(client, channel_entity, limit=MESSAGE_LIMIT, start_date=start_date_obj)

        # --- Обработка и сохранение результатов в БД ---
        if parsed_messages is not None:
            print(f"\n--- Успешно получено {len(parsed_messages)} сообщений из Telegram ---")
            if parsed_messages:
                print("--- Пример первых сообщений (до 3) ---")
                for i, msg in enumerate(parsed_messages[:3]): print(f"\nСообщение #{i+1} (ID: {msg['id']}) Date: {msg['date']} Text: {msg['text'][:100] if msg['text'] else '[Нет текста]'}...")
                print("-" * 30)
                await save_messages_to_db(db_connection, parsed_messages)
            else:
                 print("[*] Новых сообщений для сохранения нет.")
        else:
             print("[*] Парсинг не был успешно завершен или не вернул сообщений.")

    except KeyboardInterrupt:
        print("\n[*] Выход по требованию пользователя (Ctrl+C)")
    except Exception as e:
        import traceback
        print(f"[!!!] Произошла критическая ошибка: {e}")
        traceback.print_exc()
    finally:
        # --- Корректное отключение ---
        if client and client.is_connected():
            print("\n[*] Отключаюсь от Telegram...")
            await client.disconnect()
            print("[*] Соединение с Telegram закрыто.")
        if db_connection:
            print("[*] Закрываю соединение с БД...")
            db_connection.close()
            print("[*] Соединение с БД закрыто.")
        print("[*] Завершение работы скрипта.")

# --- Точка входа ---
if __name__ == "__main__":
    try:
        print("[*] Запуск основного асинхронного процесса...")
        asyncio.run(main())
    except KeyboardInterrupt:
         print("\n[*] Программа прервана (Ctrl+C).")
    except RuntimeError as e:
         if "cannot schedule new futures after shutdown" in str(e):
             print("\n[*] Цикл событий был остановлен.")
         else:
             print(f"[!] Ошибка выполнения Runtime: {e}")
    finally:
        print("[*] Скрипт завершил свою работу на верхнем уровне.")