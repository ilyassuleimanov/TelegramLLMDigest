import streamlit as st
import requests
import os
from datetime import datetime
# import pandas as pd # Пока не используется, можно закомментировать или удалить

# --- Начальная Настройка ---

# Получаем базовый URL FastAPI сервиса из переменной окружения
FASTAPI_BASE_URL = os.getenv("FASTAPI_BASE_URL", "http://localhost:8000") # Дефолт для локального теста

# URL эндпоинтов
CHANNELS_URL = f"{FASTAPI_BASE_URL}/channels"
SUMMARIES_URL_TEMPLATE = f"{FASTAPI_BASE_URL}/summaries/{{channel_id}}"
POSTS_URL_TEMPLATE = f"{FASTAPI_BASE_URL}/posts/{{channel_id}}/{{date_str}}"

# Конфигурация страницы Streamlit
st.set_page_config(
    page_title="Анализатор Telegram Каналов",
    page_icon="📰",
    layout="wide"
)

st.title("📰 Анализатор Telegram Каналов")
st.markdown("Просматривайте саммари и посты из отслеживаемых Telegram каналов.")

# --- Функции для Взаимодействия с API (с кэшированием) ---

@st.cache_data(ttl=3600) # Кэшируем на 1 час
def get_channels():
    """Получает список каналов с API."""
    try:
        response = requests.get(CHANNELS_URL)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка при загрузке списка каналов: {e}")
        return []
    except Exception as e: # Общий обработчик для других ошибок, например, JSONDecodeError
        st.error(f"Неожиданная ошибка при получении каналов: {e}")
        return []

@st.cache_data(ttl=600) # Кэшируем на 10 минут
def get_summaries(channel_id: int):
    """Получает список саммари для указанного канала."""
    if not channel_id:
        return []
    try:
        url = SUMMARIES_URL_TEMPLATE.format(channel_id=channel_id)
        response = requests.get(url)
        response.raise_for_status()
        summaries = response.json()
        for summary in summaries:
            if isinstance(summary.get("summary_date"), str):
                summary["summary_date"] = datetime.fromisoformat(summary["summary_date"].split("T")[0]).date() # Убираем время, если есть, и 'Z'
        summaries.sort(key=lambda x: x["summary_date"], reverse=True)
        return summaries
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка при загрузке саммари для канала {channel_id}: {e}")
        return []
    except Exception as e:
        st.error(f"Произошла неожиданная ошибка при обработке саммари: {e}")
        return []


@st.cache_data(ttl=600) # Кэшируем на 10 минут
def get_posts(channel_id: int, date_str: str):
    """Получает список постов для указанного канала и даты."""
    if not channel_id or not date_str:
        return []
    try:
        url = POSTS_URL_TEMPLATE.format(channel_id=channel_id, date_str=date_str)
        response = requests.get(url)
        response.raise_for_status()
        posts = response.json()
        for post in posts:
            if isinstance(post.get("message_date"), str):
                # Проверяем, есть ли 'Z' и корректно обрабатываем
                date_val = post["message_date"]
                if date_val.endswith("Z"):
                    post["message_date"] = datetime.fromisoformat(date_val.replace("Z", "+00:00"))
                else:
                    post["message_date"] = datetime.fromisoformat(date_val)
        return posts
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка при загрузке постов для канала {channel_id} за {date_str}: {e}")
        return []
    except Exception as e:
        st.error(f"Произошла неожиданная ошибка при обработке постов: {e}")
        return []

# --- Инициализация Session State ---
if 'selected_channel_id' not in st.session_state:
    st.session_state.selected_channel_id = None
if 'selected_summary_date_str' not in st.session_state: # Для управления раскрытием expander'а
    st.session_state.selected_summary_date_str = None
if 'posts_to_display_info' not in st.session_state: # Словарь {'channel_id': id, 'date_str': date} или None
    st.session_state.posts_to_display_info = None


# --- Основная Логика Приложения ---

# Шаг 1: Загрузка и Отображение Каналов
channels_data = get_channels()
channel_map_name_to_id = {}
channel_map_id_to_name = {}

if channels_data:
    for channel in channels_data:
        channel_map_name_to_id[channel['display_name']] = channel['id']
        channel_map_id_to_name[channel['id']] = channel['display_name']
    
    options = ["Выберите канал..."] + list(channel_map_name_to_id.keys())
    
    default_index = 0 # Индекс для "Выберите канал..."
    if st.session_state.selected_channel_id and st.session_state.selected_channel_id in channel_map_id_to_name:
        try:
            default_index = options.index(channel_map_id_to_name[st.session_state.selected_channel_id])
        except ValueError:
            pass # Если имя не найдено, останется "Выберите канал..."
    
    selected_channel_name = st.sidebar.selectbox(
        "Выберите канал:",
        options,
        index=default_index
    )

    if selected_channel_name != "Выберите канал...":
        new_selected_channel_id = channel_map_name_to_id[selected_channel_name]
        # Если канал изменился, сбрасываем детали (саммари и посты)
        if st.session_state.selected_channel_id != new_selected_channel_id:
            st.session_state.posts_to_display_info = None
            st.session_state.selected_summary_date_str = None
            # st.rerun() # Можно вызвать, если нужно немедленное обновление,
                       # но обычно Streamlit сам перерисовывает при изменении виджета
        st.session_state.selected_channel_id = new_selected_channel_id
    else:
        if st.session_state.selected_channel_id is not None: # Если был выбран канал, а теперь "Выберите..."
            st.session_state.selected_channel_id = None
            st.session_state.posts_to_display_info = None
            st.session_state.selected_summary_date_str = None
            st.rerun() # Перезапускаем, чтобы очистить отображение
        st.info("Пожалуйста, выберите канал из списка слева для просмотра информации.")
else:
    st.sidebar.warning("Не удалось загрузить список каналов.")
    if st.session_state.selected_channel_id is not None: # Если был выбран канал, а теперь ошибка загрузки
        st.session_state.selected_channel_id = None
        st.session_state.posts_to_display_info = None
        st.session_state.selected_summary_date_str = None
        st.rerun()

st.divider()

# Шаг 2: Отображение Списка Саммари
if st.session_state.selected_channel_id:
    current_channel_name = channel_map_id_to_name.get(st.session_state.selected_channel_id, "Неизвестный канал")
    st.subheader(f"Саммари для канала: «{current_channel_name}»")

    with st.spinner(f"Загрузка саммари для канала «{current_channel_name}»..."):
        summaries = get_summaries(st.session_state.selected_channel_id)

    if summaries:
        for summary in summaries:
            summary_date_obj = summary["summary_date"] # Должен быть уже datetime.date
            date_str_iso = summary_date_obj.isoformat()
            date_str_formatted = summary_date_obj.strftime("%d %B %Y")
            
            # Раскрываем expander, если его дата совпадает с selected_summary_date_str
            # и если сейчас не отображаются посты (или если отображаются посты для ДРУГОЙ даты)
            is_expanded = (st.session_state.selected_summary_date_str == date_str_iso) and \
                          (not st.session_state.posts_to_display_info or \
                           st.session_state.posts_to_display_info.get("date_str") != date_str_iso)


            with st.expander(f"Саммари за {date_str_formatted}", expanded=is_expanded):
                st.markdown(f"**Дата:** {date_str_formatted}")
                st.markdown(summary["summary_text"])
                
                button_key = f"show_posts_{st.session_state.selected_channel_id}_{date_str_iso}"
                if st.button("Показать посты за этот день", key=button_key):
                    st.session_state.posts_to_display_info = {
                        "channel_id": st.session_state.selected_channel_id,
                        "date_str": date_str_iso
                    }
                    st.session_state.selected_summary_date_str = date_str_iso
                    st.rerun()
    elif current_channel_name != "Неизвестный канал":
        st.info(f"Саммари для канала «{current_channel_name}» пока отсутствуют.")

# Контейнер для постов
posts_placeholder = st.empty()

# Шаг 3: Отображение Постов
if st.session_state.posts_to_display_info:
    info = st.session_state.posts_to_display_info
    channel_id_for_posts = info["channel_id"]
    date_str_for_posts = info["date_str"]

    # Показываем посты только если они для текущего выбранного канала
    if channel_id_for_posts == st.session_state.selected_channel_id:
        with posts_placeholder.container(): # Используем .container() на placeholder
            current_channel_name_for_posts = channel_map_id_to_name.get(channel_id_for_posts, "Неизвестный канал")
            formatted_date_for_header = datetime.fromisoformat(date_str_for_posts).strftime('%d %B %Y')
            
            st.subheader(f"Посты за {formatted_date_for_header} для канала «{current_channel_name_for_posts}»")

            with st.spinner(f"Загрузка постов..."):
                posts = get_posts(channel_id_for_posts, date_str_for_posts)

            if posts:
                for post in posts:
                    post_date_obj = post["message_date"] # Должен быть уже datetime.datetime
                    st.markdown(f"---")
                    st.caption(f"Опубликовано: {post_date_obj.strftime('%d.%m.%Y %H:%M:%S')}")
                    if post.get("text"):
                        st.markdown(post["text"])
                    else:
                        st.markdown("_Сообщение без текста (возможно, только медиа)_")
                st.markdown(f"---")
            elif current_channel_name_for_posts != "Неизвестный канал":
                st.info(f"Посты для канала «{current_channel_name_for_posts}» за {formatted_date_for_header} не найдены.")
            
            if st.button("Скрыть посты / Назад к саммари", key="hide_posts_button_v3"):
                st.session_state.posts_to_display_info = None
                # selected_summary_date_str остается, чтобы expander был открыт
                st.rerun()
    else:
        # Если выбранный канал изменился, а posts_to_display_info еще от старого, очищаем
        st.session_state.posts_to_display_info = None
        posts_placeholder.empty() # Очищаем контейнер
        # st.rerun() # Можно вызвать, чтобы немедленно убрать, но следующая отрисовка это сделает

elif not st.session_state.selected_channel_id and not channels_data: # Если каналы не загружены
    pass # Сообщение об ошибке загрузки каналов уже есть
elif not st.session_state.selected_channel_id and channels_data: # Если каналы загружены, но ни один не выбран
    pass # Сообщение "Выберите канал..." уже есть