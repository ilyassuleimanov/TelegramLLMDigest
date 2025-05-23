from __future__ import annotations

import json
import os
from datetime import timedelta
import logging

import pendulum

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Настройка логгирования для DAG генератора
# Это логирование будет видно в логах Airflow Scheduler при парсинге DAG-файлов
LOG_FORMAT = '[%(asctime)s] [%(levelname)s] [DAGGenerator] - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Глобальные конфигурационные переменные
PARSER_DOCKER_IMAGE = os.getenv('PARSER_DOCKER_IMAGE_TAG', "telegram_parser_task:latest")
SUMMARIZER_DOCKER_IMAGE = os.getenv('SUMMARIZER_DOCKER_IMAGE_TAG', "telegram_summarizer_task:latest")

# Путь к конфигурационному файлу каналов внутри контейнеров Airflow (scheduler/webserver)
CHANNELS_CONFIG_FILE_PATH_IN_CONTAINER = os.getenv('CHANNELS_FILE_PATH_IN_CONTAINER', '/opt/airflow/config/channels.json')

# Имя сети Docker Compose
DOCKER_NETWORK_NAME = os.getenv('DOCKER_NETWORK_NAME', 'mlops_project_app_net') # Запомнили дефолт

# Путь к ПАПКЕ с файлами сессий Telegram на хосте (где работает Docker Engine)
# Пример: /home/username/project_name/session
HOST_PATH_TO_TG_SESSIONS_FOLDER = os.getenv('HOST_PATH_TO_TG_SESSIONS_FOLDER')

# Путь к папке сессий внутри контейнера задачи парсера
# Этот путь должен совпадать с тем, что ожидает скрипт telegram_parser.py
TASK_CONTAINER_SESSIONS_PATH = os.getenv('TELEGRAM_SESSION_FOLDER_IN_TASK_CONTAINER', '/app/session')

# Загрузка конфигурации каналов
channels_config_list = []
try:
    logger.info(f"Попытка загрузки конфигурации каналов из: {CHANNELS_CONFIG_FILE_PATH_IN_CONTAINER}")
    with open(CHANNELS_CONFIG_FILE_PATH_IN_CONTAINER, 'r', encoding='utf-8') as f:
        loaded_data = json.load(f)
    if isinstance(loaded_data, list):
        channels_config_list = loaded_data
        logger.info(f"Загружена конфигурация для {len(channels_config_list)} каналов.")
    else:
        logger.error("Ошибка: Конфигурация каналов должна быть JSON-списком.")
except FileNotFoundError:
    logger.error(f"Ошибка: Файл конфигурации каналов не найден по пути: {CHANNELS_CONFIG_FILE_PATH_IN_CONTAINER}")
    logger.error(f"    Убедитесь, что путь корректен и файл смонтирован в контейнеры Airflow.")
except (json.JSONDecodeError, TypeError) as e:
    logger.error(f"Ошибка при чтении или парсинге файла конфигурации каналов {CHANNELS_CONFIG_FILE_PATH_IN_CONTAINER}: {e}")
except Exception as e:
    logger.error(f"Неизвестная ошибка при загрузке конфигурации каналов: {e}", exc_info=True)

if not channels_config_list:
    logger.warning("Конфигурация каналов пуста или не загружена. DAG-и не будут сгенерированы.")

# Общие переменные окружения для передачи в DockerOperator
# Эти значения будут браться из окружения, в котором запущен Airflow Scheduler.

# БД результатов
DB_HOST_RESULTS = os.getenv('DB_HOST')
DB_NAME_RESULTS = os.getenv('DB_NAME')
DB_USER_RESULTS = os.getenv('DB_USER')
DB_PASSWORD_RESULTS = os.getenv('DB_PASSWORD')
DB_PORT_RESULTS = os.getenv('DB_PORT')

# Telegram API
TELEGRAM_API_ID_ENV = os.getenv('TELEGRAM_API_ID')
TELEGRAM_API_HASH_ENV = os.getenv('TELEGRAM_API_HASH')
TELEGRAM_PHONE_ENV = os.getenv('TELEGRAM_PHONE') # Для пользовательской сессии
TELEGRAM_SESSION_NAME_ENV = os.getenv('TELEGRAM_SESSION_NAME', 'my_telegram_session') # Имя файла сессии (без .session)

# LLM API
GIGACHAT_API_KEY_ENV = os.getenv('GIGACHAT_API_KEY')


# Генерация DAG-ов
for channel_config in channels_config_list:
    # Числовой ID канала из конфига
    channel_id_numeric = channel_config.get("id")
    # Идентификатор для парсера
    telegram_identifier_for_parsing = channel_config.get("telegram_identifier")
    # Отображаемое имя для UI и тегов
    channel_display_name = channel_config.get("display_name", str(channel_id_numeric))

    if not all([isinstance(channel_id_numeric, int), telegram_identifier_for_parsing]):
        logger.warning(f"Пропуск конфигурации канала: отсутствует 'id' (должен быть int) или 'telegram_identifier'. Конфиг: {channel_config}")
        continue

    dag_id = f"telegram_summary_channel_{channel_id_numeric}"

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }

    logger.info(f"Генерация DAG: {dag_id} для канала '{channel_display_name}' ({telegram_identifier_for_parsing})")

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'Парсинг и суммризация Telegram канала: {channel_display_name}',
        schedule_interval='*/30 * * * *', # Каждые 30 минут
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), 
        catchup=False,
        tags=['telegram_summary', channel_display_name.lower().replace(" ", "_")],
        doc_md=channel_config.get("description", f"Автоматический сбор и суммризация постов из канала {channel_display_name}.")
    ) as dag:

        # Переменные окружения для задачи парсинга
        parser_environment = {
            'PARSER_CHANNEL_IDENTIFIER': telegram_identifier_for_parsing,
            'PARSER_CHANNEL_ID': str(channel_id_numeric),
            'PARSER_TARGET_DATE': '{{ ds }}',
            'TELEGRAM_API_ID': TELEGRAM_API_ID_ENV,
            'TELEGRAM_API_HASH': TELEGRAM_API_HASH_ENV,
            'TELEGRAM_PHONE': TELEGRAM_PHONE_ENV,
            'TELEGRAM_SESSION_NAME': TELEGRAM_SESSION_NAME_ENV,
            'TELEGRAM_SESSION_FOLDER_IN_CONTAINER': TASK_CONTAINER_SESSIONS_PATH,
            'DB_HOST': DB_HOST_RESULTS,
            'DB_NAME': DB_NAME_RESULTS,
            'DB_USER': DB_USER_RESULTS,
            'DB_PASSWORD': DB_PASSWORD_RESULTS,
            'DB_PORT': DB_PORT_RESULTS
        }
        # Очищаем от None значений
        parser_environment = {k: v for k, v in parser_environment.items() if v is not None}

        # Получаем XCom от предыдущей задачи.
        # ti (task instance) - это объект, доступный в Jinja контексте.
        # xcom_pull получает значение, которое было "запушено" задачей parse_channel_posts.
        xcom_data_from_parser_json_str = "{{ ti.xcom_pull(task_ids='parse_channel_posts', key='return_value') }}"

        # Переменные окружения для задачи суммризации
        summarizer_environment = {
            'GIGACHAT_API_KEY': GIGACHAT_API_KEY_ENV,
            'DB_HOST': DB_HOST_RESULTS,
            'DB_NAME': DB_NAME_RESULTS,
            'DB_USER': DB_USER_RESULTS,
            'DB_PASSWORD': DB_PASSWORD_RESULTS,
            'DB_PORT': DB_PORT_RESULTS,
            'XCOM_DATA_JSON': xcom_data_from_parser_json_str
        }
        summarizer_environment = {k: v for k, v in summarizer_environment.items() if v is not None}

        # Настройка монтирования тома для сессии Telegram
        parser_mounts = []
        if HOST_PATH_TO_TG_SESSIONS_FOLDER:
            try:
                session_mount = Mount(
                    target=TASK_CONTAINER_SESSIONS_PATH,
                    source=HOST_PATH_TO_TG_SESSIONS_FOLDER,
                    type='bind',
                    read_only=False
                )
                parser_mounts.append(session_mount)
                logger.info(f"Для DAG {dag_id} будет использован Mount: source='{HOST_PATH_TO_TG_SESSIONS_FOLDER}', target='{TASK_CONTAINER_SESSIONS_PATH}'")
            except Exception as e:
                logger.error(f"Ошибка при создании объекта Mount для сессий: {e}", exc_info=True)
        else:
            logger.warning(f"Для DAG {dag_id}: HOST_PATH_TO_TG_SESSIONS_FOLDER не задан. Монтирование сессий не будет выполнено.")

        # Задача 1: Парсинг постов
        parse_channel_task = DockerOperator(
            task_id="parse_channel_posts",
            image=PARSER_DOCKER_IMAGE,
            # Используем bash -c для выполнения нескольких команд:
            # 1. Запускаем парсер, сохраняем его код выхода.
            # 2. Если файл XCom создан парсером, выводим его содержимое в stdout.
            # 3. Завершаем скрипт с оригинальным кодом выхода парсера.
            command=[
                "bash", "-c",
                """
                python telegram_parser.py;
                EXIT_CODE=$?;
                echo "Python script exited with code $EXIT_CODE";
                if [ -f /airflow/xcom/return.json ]; then
                    echo "Found XCom file, outputting its content for XCom push:";
                    cat /airflow/xcom/return.json;
                else
                    echo "XCom file /airflow/xcom/return.json not found by bash script.";
                fi;
                exit $EXIT_CODE
                """
            ],
            environment=parser_environment,
            mounts=parser_mounts, 
            docker_url="unix://var/run/docker.sock",
            network_mode=DOCKER_NETWORK_NAME,
            auto_remove=True,
            do_xcom_push=True,
            mount_tmp_dir=False,
        )

        # Задача 2: Суммризация постов
        summarize_posts_task = DockerOperator(
            task_id="summarize_daily_posts",
            image=SUMMARIZER_DOCKER_IMAGE,
            command=["python", "summarizer.py"], # Скрипт читает XCom из /airflow/xcom/return.json
            environment=summarizer_environment,
            docker_url="unix://var/run/docker.sock",
            network_mode=DOCKER_NETWORK_NAME,
            auto_remove=True,
            mount_tmp_dir=False,
        )

        # Определение порядка выполнения задач
        parse_channel_task >> summarize_posts_task

    # Регистрация DAG в глобальном пространстве имен Airflow
    globals()[dag_id] = dag # Это делает DAG видимым для Airflow
    logger.info(f"Успешно сгенерирован и зарегистрирован DAG: {dag_id}")

if not channels_config_list:
     logger.warning("Ни одного DAG не было сгенерировано, так как конфигурация каналов пуста или не была загружена.")

logger.info(f"Завершение работы DAG генератора. Всего зарегистрировано DAG-ов из этого файла: {len(channels_config_list)}")