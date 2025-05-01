import asyncio
import datetime
import os
import re
import json
from urllib.parse import urlparse
from dotenv import load_dotenv, find_dotenv # <<< Добавлено для .env

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
# Ищет .env файл и загружает переменные из него в окружение ОС
# Это удобно для локального запуска. В Docker переменные будут переданы иначе.
load_dotenv(find_dotenv(), override=True)

# --- ЧТЕНИЕ НАСТРОЕК ИЗ ОКРУЖЕНИЯ ---

# Telegram API Credentials (Обязательные)
TELEGRAM_API_ID = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
TELEGRAM_PHONE = os.getenv('TELEGRAM_PHONE') # Используется для входа/авторизации

# Telegram Session & Target (Обязательные)
TELEGRAM_SESSION_NAME = os.getenv('TELEGRAM_SESSION_NAME', 'my_telegram_session') # Имя файла сессии
TARGET_CHANNEL_LINK = os.getenv('TARGET_CHANNEL_LINK') # Ссылка на целевой канал

# Parsing Parameters
# Функция для безопасного преобразования строки в bool
def str_to_bool(val):
    if not val: return False
    return val.lower() in ('true', '1', 't', 'yes', 'y')

PARSE_BY_DATE = str_to_bool(os.getenv('PARSE_BY_DATE', 'False')) # По умолчанию парсим последние N

# Date/Time Settings (если PARSE_BY_DATE = True)
START_DATE_STR = os.getenv('START_DATE_STR', None) # Пример: "2025-04-17 00:00:00"
DATE_FORMAT = os.getenv('DATE_FORMAT', "%Y-%m-%d %H:%M:%S") # Формат даты

AUTO_DETECT_TIMEZONE = str_to_bool(os.getenv('AUTO_DETECT_TIMEZONE', 'True'))
TIME_ZONE_STR = os.getenv('TIME_ZONE_STR', 'Europe/Moscow') # Используется, если AUTO_DETECT_TIMEZONE = False

# Message Limit
# Функция для безопасного преобразования строки в int или None
def str_to_int_or_none(val):
    if val is None or val == '': return None
    try:
        return int(val)
    except (ValueError, TypeError):
        print(f"[!] Предупреждение: Не удалось преобразовать '{val}' в число. Используется None.")
        return None

MESSAGE_LIMIT = str_to_int_or_none(os.getenv('MESSAGE_LIMIT', '50')) # Лимит сообщений (по умолчанию 50)

# Output Settings
OUTPUT_DIRECTORY = os.getenv('OUTPUT_DIRECTORY', "parser_results") # Папка для сохранения результатов
SAVE_TO_JSON = str_to_bool(os.getenv('SAVE_TO_JSON', 'True')) # Сохранять ли в JSON

# --- Валидация обязательных переменных ---
REQUIRED_VARS = {
    'TELEGRAM_API_ID': TELEGRAM_API_ID,
    'TELEGRAM_API_HASH': TELEGRAM_API_HASH,
    'TELEGRAM_PHONE': TELEGRAM_PHONE,
    'TARGET_CHANNEL_LINK': TARGET_CHANNEL_LINK
}

missing_vars = [name for name, value in REQUIRED_VARS.items() if not value]
if missing_vars:
    print(f"[!] Ошибка: Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
    print("[*] Пожалуйста, определите их в вашем .env файле или в окружении системы.")
    exit(1) # Выход с ошибкой

# Проверка даты, если парсим по дате
if PARSE_BY_DATE and not START_DATE_STR:
    print(f"[!] Ошибка: PARSE_BY_DATE=True, но переменная окружения START_DATE_STR не установлена.")
    exit(1)


# --- Функции (без изменений, т.к. используют переменные, прочитанные выше) ---

def get_channel_username(link):
    """Извлекает username или joinchat хэш из ссылки"""
    if not link: return None # Добавлена проверка на пустую ссылку
    parsed = urlparse(link)
    path = parsed.path.strip('/')
    if path.startswith('joinchat/'):
        return link # Возвращаем всю ссылку для joinchat
    else:
        # Предполагаем, что последняя часть пути - это username
        parts = path.split('/')
        return parts[-1] if parts else None

def make_datetime_aware(date_str, auto_detect=False, manual_tz_str=None):
    """
    Преобразует наивную строку даты/времени в timezone-aware datetime.
    Использует глобальную переменную DATE_FORMAT.
    """
    try:
        dt_naive = datetime.datetime.strptime(date_str, DATE_FORMAT)
        tz_repr_str = None

        if auto_detect:
            print("[*] Режим таймзоны: Автоматическое определение локального пояса.")
            if _HAVE_TZLOCAL:
                try:
                    local_tz = get_localzone()
                    dt_aware = dt_naive.replace(tzinfo=local_tz)
                    detected_tz_name = str(local_tz)
                    tz_repr_str = f"{detected_tz_name}_local"
                    print(f"[*] Лог: Обнаружена локальная таймзона: {detected_tz_name}. Используется как '{tz_repr_str}'.")
                    return dt_aware, tz_repr_str
                except Exception as e:
                    print(f"[!] Ошибка при определении локальной таймзоны: {e}")
                    tz_repr_str = "UTC"
                    dt_aware = dt_naive.replace(tzinfo=datetime.timezone.utc)
                    print(f"[*] Лог: Используется UTC как запасной вариант.")
                    return dt_aware, tz_repr_str
            else:
                print("[!] Предупреждение: Библиотека 'tzlocal' недоступна (`pip install tzlocal`). Автоопределение невозможно.")
                tz_repr_str = "UTC"
                dt_aware = dt_naive.replace(tzinfo=datetime.timezone.utc)
                print(f"[*] Лог: Используется UTC.")
                return dt_aware, tz_repr_str
        else:
            effective_tz_str = manual_tz_str.strip() if manual_tz_str else ""
            print(f"[*] Режим таймзоны: Ручное указание.")

            if effective_tz_str:
                print(f"[*] Попытка использовать указанную таймзону: '{effective_tz_str}'")
                if _HAVE_ZONEINFO:
                    try:
                        tz = ZoneInfo(effective_tz_str)
                        dt_aware = dt_naive.replace(tzinfo=tz)
                        tz_repr_str = effective_tz_str
                        print(f"[*] Лог: Используется указанная таймзона: {tz_repr_str}")
                        return dt_aware, tz_repr_str
                    except ZoneInfoNotFoundError:
                        print(f"[!] Ошибка: Временная зона '{effective_tz_str}' не найдена с помощью 'zoneinfo'.")
                        return None, None
                else:
                    print("[!] Ошибка: Модуль 'zoneinfo' недоступен (нужен Python 3.9+ для указания таймзоны по имени).")
                    print(f"    Невозможно использовать '{effective_tz_str}'.")
                    return None, None
            else:
                tz_repr_str = "UTC"
                dt_aware = dt_naive.replace(tzinfo=datetime.timezone.utc)
                print(f"[*] Лог: Временная зона не указана вручную, используется UTC.")
                return dt_aware, tz_repr_str

    except ValueError:
        print(f"[!] Ошибка: Неверный формат даты/времени в START_DATE_STR ('{date_str}'). Ожидается: '{DATE_FORMAT}'")
        return None, None
    except Exception as e:
        print(f"[!] Неожиданная ошибка при обработке даты/времени: {e}")
        return None, None

async def parse_channel(client, channel_entity, limit=None, start_date=None):
    """
    Парсит сообщения из указанного канала.
    """
    print(f"\n[*] Начинаю парсинг канала: {channel_entity}")
    action_description = ""
    use_offset_date = None
    use_reverse = False

    if start_date:
        if start_date.tzinfo is None or start_date.tzinfo.utcoffset(start_date) is None:
             print("[!] Внутренняя ошибка: start_date должна быть timezone-aware перед передачей в parse_channel.")
             start_date = start_date.replace(tzinfo=datetime.timezone.utc) # Запасной вариант

        action_description = f"сообщений после {start_date.isoformat()}"
        print(f"[*] Цель: получить {limit or 'все'} {action_description}")
        use_offset_date = start_date
        use_reverse = True
    else:
        action_description = f"{limit or 'всех доступных последних'} сообщений"
        print(f"[*] Цель: получить {action_description}")
        # limit уже установлен из переменной окружения
        use_reverse = False # Получаем последние сообщения (не реверс)

    messages_data = []
    try:
        entity = await client.get_entity(channel_entity)
        print(f"[*] Канал найден: {entity.title}")

        processed_count = 0
        async for message in client.iter_messages(
            entity,
            limit=limit,
            offset_date=use_offset_date,
            reverse=use_reverse
        ):
            message_info = {
                'id': message.id,
                'date': message.date.isoformat(),
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

            if processed_count % 100 == 0 and processed_count > 0:
                 print(f"[*] Собрано {processed_count} сообщений...")

        print(f"[*] Парсинг завершен. Получено сообщений: {len(messages_data)}")
        # Если парсили НЕ по дате, нужно развернуть список, т.к. iter_messages возвращает от новых к старым
        if not use_reverse and not use_offset_date:
             messages_data.reverse()
             print("[*] Лог: Результат развернут для хронологического порядка (от старых к новым).")
        return messages_data

    except UsernameNotOccupiedError:
        print(f"[!] Ошибка: Канал/пользователь с username '{channel_entity}' не найден.")
    except ChannelPrivateError:
        print(f"[!] Ошибка: Канал '{channel_entity}' является приватным или у вас нет к нему доступа.")
    except ValueError as e:
         print(f"[!] Ошибка: Не удалось распознать ссылку или username '{channel_entity}'. Убедитесь, что они верны. {e}")
    except FloodWaitError as e:
        print(f"[!] Ошибка FloodWait: Telegram попросил подождать {e.seconds} секунд.")
        print(f"[*] Пожалуйста, попробуйте запустить скрипт позже или уменьшите лимит.")
    except TypeError as e:
        print(f"[!] Ошибка TypeError при вызове iter_messages: {e}. Возможно, проблема с параметром start_date.")
    except Exception as e:
        import traceback
        print(f"[!] Произошла непредвиденная ошибка при парсинге: {e}")
        traceback.print_exc()
    return None

async def main():
    # --- Чтение конфигурации УЖЕ ВЫПОЛНЕНО В НАЧАЛЕ СКРИПТА ---
    # Переменные TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE уже доступны

    # --- Инициализация клиента ---
    # Используем TELEGRAM_SESSION_NAME из окружения
    client = TelegramClient(TELEGRAM_SESSION_NAME, int(TELEGRAM_API_ID), TELEGRAM_API_HASH, system_version="4.16.30-vxCUSTOM")

    # --- Подключение и авторизация ---
    print("[*] Подключаюсь к Telegram...")
    try:
        await client.connect()
        if not await client.is_user_authorized():
            print("[*] Требуется авторизация.")
            # Используем TELEGRAM_PHONE из окружения
            await client.send_code_request(TELEGRAM_PHONE)
            code = input('[*] Пожалуйста, введите код подтверждения из Telegram: ')
            try:
                 signed_in_user = await client.sign_in(TELEGRAM_PHONE, code)
                 print(f"[*] Вход выполнен как: {signed_in_user.first_name}")
            except SessionPasswordNeededError:
                 password = input('[*] Требуется пароль двухфакторной аутентификации: ')
                 signed_in_user = await client.sign_in(password=password)
                 print(f"[*] Вход выполнен как: {signed_in_user.first_name}")
            print("[*] Авторизация прошла успешно!")
        else:
            me = await client.get_me()
            print(f"[*] Успешно подключен как {me.first_name} (используется существующая сессия '{TELEGRAM_SESSION_NAME}.session').")

    except FloodWaitError as e:
         print(f"[!] Ошибка FloodWait при подключении/авторизации: подождите {e.seconds} секунд.")
         return
    except Exception as e:
        print(f"[!] Ошибка подключения или авторизации: {e}")
        if client.is_connected(): await client.disconnect()
        return

    # --- Основная логика парсинга ---
    try:
        # Используем TARGET_CHANNEL_LINK из окружения
        print(f"\n[*] Целевой канал для парсинга: {TARGET_CHANNEL_LINK}")
        channel_entity = get_channel_username(TARGET_CHANNEL_LINK)
        if not channel_entity:
            print("[!] Некорректная ссылка в TARGET_CHANNEL_LINK. Завершение.")
            return

        # --- Установка параметров парсинга (уже прочитаны из окружения) ---
        start_date_obj = None
        tz_repr_for_filename = None
        limit = MESSAGE_LIMIT # Используем значение из окружения

        if PARSE_BY_DATE:
            print(f"[*] Режим парсинга: ПОСЛЕ даты {START_DATE_STR}")
            start_date_obj, tz_repr_for_filename = make_datetime_aware(
                START_DATE_STR,
                auto_detect=AUTO_DETECT_TIMEZONE,
                manual_tz_str=TIME_ZONE_STR
            )
            if not start_date_obj:
                print("[!] Не удалось обработать дату/время начала. Завершение.")
                return
            print(f"[*] Дата начала для запроса: {start_date_obj.isoformat()} (Пояс для интерпретации: '{tz_repr_for_filename}')")
        else:
            print(f"[*] Режим парсинга: ПОСЛЕДНИЕ {limit or 'N/A'} сообщений")
            if limit is None:
                 print("[!] Предупреждение: PARSE_BY_DATE=False, MESSAGE_LIMIT не установлен (или равен 0/None). Парсинг может быть долгим.")

        # --- Запуск функции парсинга ---
        parsed_messages = await parse_channel(client, channel_entity, limit=limit, start_date=start_date_obj)

        # --- Обработка и сохранение результатов ---
        if parsed_messages is not None:
            print(f"\n--- Успешно получено {len(parsed_messages)} сообщений ---")

            if len(parsed_messages) > 0:
                print("--- Пример первых сообщений (до 3) ---")
                for i, msg in enumerate(parsed_messages[:3]):
                    print(f"\nСообщение #{i+1} (ID: {msg['id']})")
                    print(f"  Дата (UTC): {msg['date']}")
                    print(f"  Текст: {msg['text'][:150] if msg['text'] else '[Нет текста]'}...")
                print("-" * 30)

            # --- Сохранение в JSON (если включено) ---
            if SAVE_TO_JSON:
                print("[*] Сохранение результатов в JSON файл...")
                # Используем OUTPUT_DIRECTORY из окружения
                output_dir_path = OUTPUT_DIRECTORY
                try:
                    os.makedirs(output_dir_path, exist_ok=True)
                    print(f"[*] Директория для результатов: '{output_dir_path}'")
                except OSError as e:
                    print(f"[!] Ошибка при создании директории '{output_dir_path}': {e}")
                    output_dir_path = "."

                safe_channel_name = "".join(c if c.isalnum() or c in ('_', '-') else '_' for c in str(channel_entity))
                filename = ""
                if PARSE_BY_DATE and start_date_obj and tz_repr_for_filename:
                     time_str = start_date_obj.strftime("%Y%m%d_%H%M%S")
                     safe_tz_repr = re.sub(r'[^\w\-\.]+', '_', tz_repr_for_filename)
                     filename = f"{safe_channel_name}_after_{time_str}_{safe_tz_repr}.json"
                else:
                     filename = f"{safe_channel_name}_last_{limit if limit is not None else 'all'}.json" # Исправлено None на 'all'

                full_filepath = os.path.join(output_dir_path, filename)

                try:
                    with open(full_filepath, 'w', encoding='utf-8') as f:
                        json.dump(parsed_messages, f, ensure_ascii=False, indent=4)
                    print(f"[*] Данные успешно сохранены в файл: {full_filepath}")
                except IOError as e:
                    print(f"[!] Ошибка ввода-вывода при сохранении файла '{full_filepath}': {e}")
                except Exception as e:
                    print(f"[!] Неожиданная ошибка при сохранении в JSON: {e}")
            else:
                print("[*] Сохранение в JSON отключено (SAVE_TO_JSON = False).")
        else:
             print("[*] Парсинг не был успешно завершен или не вернул сообщений.")

    except KeyboardInterrupt:
        print("\n[*] Выход по требованию пользователя (Ctrl+C)")
    except Exception as e:
        import traceback
        print(f"[!] Произошла непредвиденная ошибка в основной части программы: {e}")
        traceback.print_exc()
    finally:
        if client.is_connected():
            print("\n[*] Отключаюсь от Telegram...")
            await client.disconnect()
            print("[*] Соединение закрыто.")
        print("[*] Завершение работы скрипта.")

# --- Точка входа в программу ---
if __name__ == "__main__":
    # Оборачиваем запуск main для корректной обработки в разных средах
    # и перехвата KeyboardInterrupt на верхнем уровне.
    loop = None
    try:
        # Пытаемся получить существующий цикл (важно для Jupyter/IPython)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None # Нет запущенного цикла

        if loop and loop.is_running():
            print("[*] Обнаружен запущенный цикл событий. Используем await.")
            # В Jupyter/IPython просто вызовите 'await main()' в ячейке.
            # Этот блок может быть полезен для других сценариев.
            # Создаем задачу и ждем ее завершения (примерный подход)
            tsk = loop.create_task(main())
            # В реальном асинхронном приложении здесь может быть другая логика
            # Для скрипта, если мы сюда попали, это может быть сложно обработать правильно
            # поэтому просто выводим сообщение. Для скрипта обычно используется else блок.

        else:
            # Стандартный запуск для обычного .py скрипта
            print("[*] Запуск через asyncio.run()")
            asyncio.run(main())

    except KeyboardInterrupt:
         print("\n[*] Программа прервана (Ctrl+C).")
    except RuntimeError as e:
         if "cannot schedule new futures after shutdown" in str(e):
             # Эта ошибка может возникнуть при прерывании во время работы asyncio.run()
             print("\n[*] Цикл событий был остановлен во время выполнения.")
         elif "cannot be called from a running event loop" in str(e):
             print("\n[!] Ошибка: Попытка вложенного вызова asyncio.run(). Проверьте структуру кода.")
         else:
             print(f"[!] Произошла ошибка выполнения Runtime: {e}")
    finally:
        print("[*] Скрипт завершил работу.")
