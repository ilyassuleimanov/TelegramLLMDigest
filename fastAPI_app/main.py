# fastAPI_app/main.py

import os
import json
import logging # Импортируем библиотеку логирования
from typing import List, Optional
from datetime import date, datetime, timedelta, timezone # Добавил timedelta и timezone

import asyncpg
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from dotenv import load_dotenv

# --- Настройка Логирования ---
# Получаем уровень логирования из переменной окружения, по умолчанию INFO
LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR, logging.INFO)

# Создаем и настраиваем логгер
logger = logging.getLogger("fastapi_app")
logger.setLevel(LOG_LEVEL)

# Создаем обработчик для вывода логов в stdout (консоль)
handler = logging.StreamHandler()
handler.setLevel(LOG_LEVEL)

# Создаем форматтер и добавляем его к обработчику
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Добавляем обработчик к логгеру
# Проверяем, чтобы не добавлять обработчик многократно при перезагрузках uvicorn --reload
if not logger.handlers:
    logger.addHandler(handler)

# Загрузка переменных окружения
load_dotenv()
logger.info("Переменные окружения загружены (если .env файл существует).")

# Конфигурация
DB_HOST = os.getenv("DB_HOST")
DB_PORT_STR = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

CHANNELS_FILE_PATH_INTERNAL = os.getenv("CHANNELS_FILE_PATH", "/app/config/channels.json")

logger.info(f"DB_HOST: {DB_HOST}, DB_PORT: {DB_PORT_STR}, DB_USER: {'SET' if DB_USER else 'NOT SET'}, DB_NAME: {DB_NAME}")
logger.info(f"CHANNELS_FILE_PATH_INTERNAL: {CHANNELS_FILE_PATH_INTERNAL}")


# Pydantic Модели
class Channel(BaseModel):
    id: int
    telegram_identifier: str
    display_name: str
    description: Optional[str] = None

class Summary(BaseModel):
    summary_id: int
    channel_id: int
    summary_date: date
    summary_text: str
    llm_model_used: Optional[str] = None
    generated_at: datetime

class TelegramMessage(BaseModel):
    id: int
    message_id: int
    channel_id: int
    message_date: datetime
    text: Optional[str] = None
    sender_id: Optional[int] = None
    views: Optional[int] = None
    forwards: Optional[int] = None
    is_reply: Optional[bool] = None
    reply_to_msg_id: Optional[int] = None
    has_media: Optional[bool] = None
    parsed_at: datetime

# База данных
async def get_db_pool():
    if not hasattr(get_db_pool, "pool"):
        if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT_STR]):
            missing_vars = [
                var_name for var_name, var_val in {
                    "DB_HOST": DB_HOST, "DB_PORT": DB_PORT_STR, "DB_USER": DB_USER,
                    "DB_PASSWORD": DB_PASSWORD, "DB_NAME": DB_NAME
                }.items() if not var_val
            ]
            error_msg = f"One or more database connection environment variables are not set: {', '.join(missing_vars)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        try:
            db_port = int(DB_PORT_STR)
            logger.info(f"Попытка создания пула соединений к БД: host={DB_HOST}, port={db_port}, user={DB_USER}, dbname={DB_NAME}")
            get_db_pool.pool = await asyncpg.create_pool(
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME,
                host=DB_HOST,
                port=db_port,
                min_size=1, # Минимальное количество соединений в пуле
                max_size=10 # Максимальное количество соединений в пуле
            )
            logger.info("Пул соединений к БД успешно создан.")
        except ValueError:
            error_msg = f"Неверное значение DB_PORT: '{DB_PORT_STR}'. Должно быть целым числом."
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        except Exception as e:
            error_msg = f"Ошибка подключения к БД: {e}"
            logger.error(error_msg, exc_info=True) # exc_info=True добавит traceback
            raise RuntimeError(error_msg)
    return get_db_pool.pool

# FastAPI Приложение
app = FastAPI(
    title="Telegram Channel Summaries API",
    description="API for accessing parsed Telegram posts and their summaries.",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    logger.info("FastAPI приложение запускается...")
    try:
        await get_db_pool() # Инициализируем пул при старте
    except RuntimeError as e:
        logger.critical(f"Критическая ошибка при создании пула БД на старте: {e}. Приложение может не работать корректно.")
    logger.info("FastAPI приложение успешно запущено.")


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("FastAPI приложение останавливается...")
    if hasattr(get_db_pool, "pool") and get_db_pool.pool:
        await get_db_pool.pool.close()
        logger.info("Пул соединений к БД закрыт.")
    logger.info("FastAPI приложение успешно остановлено.")


# Вспомогательные функции
def load_channels_from_file() -> List[Channel]:
    logger.info(f"Попытка загрузки каналов из файла: {CHANNELS_FILE_PATH_INTERNAL}")
    try:
        with open(CHANNELS_FILE_PATH_INTERNAL, 'r', encoding='utf-8') as f:
            channels_data = json.load(f)
        logger.info(f"Каналы успешно загружены из {CHANNELS_FILE_PATH_INTERNAL}. Найдено: {len(channels_data)} каналов.")
        return [Channel(**channel_data) for channel_data in channels_data]
    except FileNotFoundError:
        error_msg = f"Файл каналов не найден по пути: {CHANNELS_FILE_PATH_INTERNAL}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
    except json.JSONDecodeError:
        error_msg = f"Ошибка декодирования JSON в файле каналов: {CHANNELS_FILE_PATH_INTERNAL}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)
    except Exception as e:
        error_msg = f"Неожиданная ошибка при загрузке каналов: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise HTTPException(status_code=500, detail=error_msg)

# Эндпоинты
@app.get("/channels", response_model=List[Channel], tags=["Channels"])
async def get_channels_endpoint():
    logger.info("Запрос на эндпоинт /channels")
    return load_channels_from_file()

@app.get("/summaries/{channel_id}", response_model=List[Summary], tags=["Summaries"])
async def get_summaries_for_channel(channel_id: int, pool: asyncpg.Pool = Depends(get_db_pool)):
    logger.info(f"Запрос на эндпоинт /summaries/{channel_id}")
    query = """
        SELECT summary_id, channel_id, summary_date, summary_text, llm_model_used, generated_at
        FROM summaries
        WHERE channel_id = $1
        ORDER BY summary_date DESC;
    """
    try:
        logger.info(f"Выполнение SQL запроса для summaries: {query} с channel_id={channel_id}")
        rows = await pool.fetch(query, channel_id)
        logger.info(f"Получено {len(rows)} саммари для канала {channel_id}")
        if rows:
            logger.info(f"Первая строка данных для summaries: {dict(rows[0])}")
        else:
            logger.info(f"Саммари для канала {channel_id} не найдены.")
        return [Summary(**row) for row in rows]
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка БД при получении саммари для канала {channel_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {str(e)}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении саммари для канала {channel_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Неожиданная ошибка: {str(e)}")


@app.get("/posts/{channel_id}/{summary_date_str}", response_model=List[TelegramMessage], tags=["Posts"])
async def get_posts_for_channel_and_date(
    channel_id: int,
    summary_date_str: str,
    pool: asyncpg.Pool = Depends(get_db_pool)
):
    logger.info(f"Запрос на эндпоинт /posts/{channel_id}/{summary_date_str}")
    logger.info(f"Получены параметры: channel_id={channel_id}, summary_date_str={summary_date_str}")
    try:
        target_date = date.fromisoformat(summary_date_str)
        logger.info(f"Преобразованная дата: {target_date}")
    except ValueError:
        logger.warning(f"Неверный формат даты '{summary_date_str}' в запросе /posts.", exc_info=True)
        raise HTTPException(status_code=400, detail="Неверный формат даты. Используйте YYYY-MM-DD.")

    start_of_target_day_utc = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)
    start_of_next_day_utc = start_of_target_day_utc + timedelta(days=1)
    logger.info(f"Диапазон дат для запроса постов: с {start_of_target_day_utc} по {start_of_next_day_utc}")

    query = """
        SELECT id, message_id, channel_id, message_date, text, sender_id,
               views, forwards, is_reply, reply_to_msg_id, has_media, parsed_at
        FROM telegram_messages
        WHERE channel_id = $1 AND message_date >= $2 AND message_date < $3
        ORDER BY message_date ASC;
    """
    try:
        logger.info(f"Выполнение SQL запроса для posts: {query} с параметрами: {channel_id}, {start_of_target_day_utc}, {start_of_next_day_utc}")
        rows = await pool.fetch(query, channel_id, start_of_target_day_utc, start_of_next_day_utc)
        logger.info(f"Получено {len(rows)} постов для канала {channel_id} за дату {summary_date_str}")
        if rows:
            logger.info(f"Первая строка данных для posts: {dict(rows[0])}")
        return [TelegramMessage(**row) for row in rows]
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка БД при получении постов для канала {channel_id} за дату {summary_date_str}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {str(e)}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении постов для канала {channel_id} за дату {summary_date_str}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Неожиданная ошибка: {str(e)}")

# Для локального запуска
if __name__ == "__main__":
    import uvicorn
    logger.info("Запуск FastAPI приложения локально через uvicorn...")
    # Проверки переменных окружения и файла каналов уже происходят при инициализации и в get_db_pool
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level=LOG_LEVEL_STR.lower())