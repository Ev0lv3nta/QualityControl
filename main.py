import asyncio
import logging
import os
import uuid
import secrets
import contextlib
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
import json
from html import escape

import asyncpg
from PIL import Image
from pyzbar import pyzbar
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.filters.callback_data import CallbackData
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, BotCommand, ReplyKeyboardRemove
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

# =====================================================
# КОНФИГУРАЦИЯ
# =====================================================

# НОВОЕ: Версия структуры FSM. Меняйте, если вносите несовместимые изменения в PROCESS_CHAINS
STATE_VERSION = 1

MAX_DB_RETRIES = 3
DB_RETRY_DELAY = 2
TOKEN_TTL_SECONDS = 3600  # 1 час

# =====================================================
# ИНИЦИАЛИЗАЦИЯ
# =====================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# Загрузка переменных окружения из .env и (опционально) secrets.env
load_dotenv()
load_dotenv("secrets.env")

def require_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        logger.critical(f"Отсутствует обязательная переменная окружения: {name}")
        raise SystemExit(1)
    return value

TELEGRAM_TOKEN = require_env("TELEGRAM_TOKEN")
DB_USER = require_env("DB_USER")
DB_PASS = require_env("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = require_env("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
QR_IMAGE_DIR = os.getenv("QR_IMAGE_DIR", "qr_images_storage")
CONTROL_PHOTO_DIR = os.getenv("CONTROL_PHOTO_DIR", "control_photos")

STAGE_TITLES: Dict[str, str] = {
    "forming": "Формовка",
    "accumulation": "Зона накопления ГП",
    "packaging": "Упаковка",
    "cgp": "ЦГП",
}

PARAM_TITLES: Dict[str, Dict[str, str]] = {
    "forming": {
        "shell_diameter": "Диаметр оболочки (мм)",
        "weight_sample_grams": "Вес образца (г)",
        "stuffing_diameter": "Диаметр после набивки (мм)",
        "stuffing_length_visual": "Длина после набивки (мм)",
        "mince_contamination_visual": "Загрязнение фаршем",
        "hanging_quality_visual": "Качество навески",
    },
    "accumulation": {
        "temperature": "Температура в ГП перед упаковкой (°C)",
        "contamination_visual": "Загрязнения",
        "wrinkling_visual": "Морщинистость",
        "smoking_color_calorimeter": "Цвет копчения (колориметр)",
        "structure_visual": "Разработка (структура)",
        "porosity_visual": "Пористость",
        "slips_visual": "Слипы",
        "print_defects_visual": "Соответствие печати",
        "shell_adhesion_physical": "Адгезия оболочки",
        "organoleptics": "Органолептика"
    },
    "packaging": {
        "gas_mixture_ratio": "Соотношение газовой смеси",
        "package_integrity": "Целостность упаковки",
        "weight_compliance_operator": "Вес, оператор (г)",
        "weight_compliance_technologist": "Вес, технолог (г)",
    },
    "cgp": {
        "cgp_inserts_visual": "Контроль вложений",
    },
}

bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
db_pool: Optional[asyncpg.Pool] = None
TOKEN_CLEANUP_TASK: Optional[asyncio.Task] = None

# =====================================================
# НОВОЕ: Фабрики CallbackData
# =====================================================

class StageCallback(CallbackData, prefix="stage"):
    name: str

class ProcessNavCallback(CallbackData, prefix="proc_nav"):
    action: str  # back, cancel

class ChoiceCallback(CallbackData, prefix="choice"):
    value: str

class ParamMenuCallback(CallbackData, prefix="param"):
    action: str
    process_name: Optional[str] = None
    param_key: Optional[str] = None

class FormingCallback(CallbackData, prefix="forming"):
    action: str  # new, continue, add_another, finish
    session_id: Optional[int] = None

class AccumulationCallback(CallbackData, prefix="accum"):
    action: str  # new, continue
    token: Optional[str] = None

class CgpCallback(CallbackData, prefix="cgp"):
    action: str  # new, continue
    token: Optional[str] = None

class RegistrationCallback(CallbackData, prefix="reg"):
    position: str

# =====================================================
# СОСТОЯНИЯ FSM
# =====================================================

class Registration(StatesGroup):
    waiting_for_name = State()
    waiting_for_position = State()

class Process(StatesGroup):
    in_progress = State()
    param_menu = State()
    waiting_for_qr = State()
    forming_confirm_next = State()
    waiting_for_param_photo = State()
    waiting_for_param_comment = State()

# =====================================================
# ПРОЦЕССЫ
# =====================================================

PROCESS_CHAINS = {
    "forming": [
        {'key': 'shell_diameter', 'prompt': "<b>Образец №{sample_number}.</b> Введите 'Диаметр оболочки' (мм):", 'type': 'float', 'validation': {'min': 1, 'max': 500}},
        {'key': 'weight_sample_grams', 'prompt': "<b>Образец №{sample_number}.</b> Введите 'Вес образца' (г):", 'type': 'float', 'validation': {'min': 1, 'max': 100000}},
        {'key': 'stuffing_diameter', 'prompt': "<b>Образец №{sample_number}.</b> Введите 'Диаметр после набивки' (мм):", 'type': 'float', 'validation': {'min': 1, 'max': 500}},
        {'key': 'stuffing_length_visual', 'prompt': "<b>Образец №{sample_number}.</b> Введите 'Длину после набивки' (мм):", 'type': 'float', 'validation': {'min': 1, 'max': 10000}},
        {'key': 'mince_contamination_visual', 'prompt': "<b>Образец №{sample_number}.</b> Оцените 'Загрязнение фаршем':", 'type': 'choice', 'choices': {"✅ Норма": "norm", "❌ Дефект": "defect"}, 'photo_on_defect': True},
        {'key': 'hanging_quality_visual', 'prompt': "<b>Образец №{sample_number}.</b> Оцените 'Качество навески':", 'type': 'choice', 'choices': {"✅ Норма": "norm", "❌ Дефект": "defect"}, 'photo_on_defect': True},
    ],
    "accumulation": [
        {'key': 'temperature', 'prompt': "<b>Этап 2: Зона накопления ГП</b>\nВведите 'Температуру в ГП перед упаковкой' (°C):", 'type': 'float', 'validation': {'min': -50, 'max': 200}},
        {'key': 'contamination_visual', 'prompt': "Оцените 'Загрязнения':", 'type': 'choice', 'choices': {"✅ Норма": "norm", "❌ Дефект": "defect"}, 'photo_on_defect': True},
        {'key': 'wrinkling_visual', 'prompt': "Оцените 'Морщинистость':", 'type': 'choice', 'choices': {"Отсутствует": "absent", "Незначительная": "minor", "Сильная": "major"}, 'require_photo_always': True},
        {'key': 'smoking_color_calorimeter', 'prompt': "Оцените 'Цвет копчения':", 'type': 'choice', 'choices': {"✅ Норма": "norm", "❌ Дефект": "defect"}, 'photo_on_defect': True},
        {'key': 'structure_visual', 'prompt': "Оцените 'Разработку (структуру)':", 'type': 'choice', 'choices': {"✅ Норма": "norm", "❌ Дефект": "defect"}, 'photo_on_defect': True},
        {'key': 'porosity_visual', 'prompt': "Оцените 'Пористость':", 'type': 'choice', 'choices': {"✅ Норма": "norm", "❌ Дефект": "defect"}, 'require_photo_always': True},
        {'key': 'slips_visual', 'prompt': "Оцените 'Слипы':", 'type': 'choice', 'choices': {"✅ Норма": "norm", "❌ Дефект": "defect"}, 'photo_on_defect': True},
        {'key': 'print_defects_visual', 'prompt': "Оцените 'Соответствие печати на оболочке продукту':", 'type': 'choice', 'choices': {"✅ Соответствует": "absent", "❌ Не соответствует": "present"}, 'photo_on_defect': True},
        {'key': 'shell_adhesion_physical', 'prompt': "Оцените 'Адгезию оболочки':", 'type': 'choice', 'choices': {"✅ Норма": "norm", "❌ Дефект": "defect"}, 'photo_on_defect': True},
        {'key': 'organoleptics', 'prompt': "Оцените 'Органолептику (соответствие набору специй)':", 'type': 'choice', 'choices': {"✅ Норма": "norm", "❌ Дефект": "defect"}, 'comment_on_defect': True, 'comment_prompt': "Опишите дефект органолептики:"}
    ],
    "packaging": [
        {'key': 'gas_mixture_ratio', 'prompt': "Оцените 'Соотношение газовой смеси':", 'type': 'choice', 'choices': {"✅ Норма": "norm", "❌ Дефект": "defect"}},
        {'key': 'package_integrity', 'prompt': "Оцените 'Нарушение целостности упаковки':", 'type': 'choice', 'choices': {"✅ Нет нарушений": "no", "❌ Есть нарушения": "yes"}, 'photo_on_defect': True},
        {'key': 'weight_compliance_operator', 'prompt': "Введите вес, измеренный оператором (г):", 'type': 'float', 'validation': {'min': 1, 'max': 100000}},
        {'key': 'weight_compliance_technologist', 'prompt': "Введите вес, измеренный контролёром-технологом (г):", 'type': 'float', 'validation': {'min': 1, 'max': 100000}},
    ],
    "cgp": [
        {'key': 'cgp_inserts_visual', 'prompt': "Оцените 'Контроль вложений':", 'type': 'choice', 'choices': {"✅ Соответствует": "ok", "❌ Не соответствует": "not_ok"}, 'photo_on_defect': True},
    ]
}

# =====================================================
# ВСПОМОГАТЕЛЬНЫЕ
# =====================================================

def get_user_info(target: Message | CallbackQuery) -> str:
    user = target.from_user
    return f"User(id={user.id}, name='{user.full_name}')"

async def download_telegram_file_by_file_id(file_id: str, destination_path: str) -> bool:
    try:
        file = await bot.get_file(file_id)
        # ИЗМЕНЕНО: aiogram 3.x предпочитает download_file, fallback уже не так актуален, но оставим для надёжности.
        await bot.download_file(file.file_path, destination_path)
        return True
    except Exception as e:
        logger.error(f"Ошибка загрузки файла Telegram по file_id={file_id}: {e}")
        return False

def build_control_photo_path(base_dir_name: str, process_name: str, param_key: str, original_file_path: Optional[str] = None) -> str:
    subdir = os.path.join(CONTROL_PHOTO_DIR, base_dir_name, process_name, param_key)
    os.makedirs(subdir, exist_ok=True)
    ext = 'jpg'
    if original_file_path and '.' in original_file_path:
        ext = original_file_path.split('.')[-1]
    filename = f"{uuid.uuid4().hex}.{ext}"
    return os.path.join(subdir, filename)

def is_choice_defect(step_key: str, value: str) -> bool:
    defect_values_map = {
        'mince_contamination_visual': {'defect'},
        'hanging_quality_visual': {'defect'},
        'contamination_visual': {'defect'},
        'slips_visual': {'defect'},
        'print_defects_visual': {'present'},
        'shell_adhesion_physical': {'defect'},
        'smoking_color_calorimeter': {'defect'},
        'porosity_visual': {'defect'},
        'organoleptics': {'defect'},
        'structure_visual': {'defect'},
        'package_integrity': {'yes'},
        'gas_mixture_ratio': {'defect'},
        'cgp_inserts_visual': {'not_ok'},
        'wrinkling_visual': set(), # Здесь нет дефектных значений
    }
    return value in defect_values_map.get(step_key, set())

async def validate_input(value: Any, step_config: Dict) -> tuple[bool, str]:
    v_type, validation = step_config.get('type'), step_config.get('validation', {})
    if v_type == 'float':
        min_val, max_val = validation.get('min'), validation.get('max')
        if min_val is not None and value < min_val: return False, f"Значение должно быть не менее {min_val}"
        if max_val is not None and value > max_val: return False, f"Значение должно быть не более {max_val}"
    elif v_type == 'text':
        max_length = validation.get('max_length')
        if max_length and len(str(value)) > max_length: return False, f"Текст слишком длинный (максимум {max_length} символов)"
    return True, ""

def _sync_decode_multi_qr(image_path: str) -> List[Dict[str, Any]]:
    try:
        with Image.open(image_path) as im:
            objs = [o for o in pyzbar.decode(im) if getattr(o, 'type', '') == 'QRCODE']
            items = []
            for o in objs:
                r = o.rect
                items.append({'text': o.data.decode('utf-8'), 'x': r.left, 'y': r.top, 'w': r.width, 'h': r.height, 'area': r.width * r.height})
            seen, uniq = set(), []
            for it in items:
                if it['text'] not in seen:
                    seen.add(it['text']); uniq.append(it)
            if not uniq:
                return []
            uniq.sort(key=lambda i: i['area'], reverse=True)
            top2 = uniq[:2]
            top2.sort(key=lambda i: i['x'])
            return top2
    except Exception as e:
        logger.error(f"Ошибка мульти-декодирования QR {image_path}: {e}")
        return []

async def decode_multi_qr_from_image_async(image_path: str) -> List[Dict[str, Any]]:
    return await asyncio.to_thread(_sync_decode_multi_qr, image_path)

# =====================================================
# БАЗА ДАННЫХ
# =====================================================

async def create_db_pool() -> bool:
    global db_pool
    for attempt in range(MAX_DB_RETRIES):
        try:
            db_pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=DB_HOST, port=DB_PORT, min_size=2, max_size=10, command_timeout=10)
            logger.info("✅ Пул соединений с PostgreSQL успешно создан")
            return True
        except Exception as e:
            logger.error(f"❌ Попытка {attempt + 1}/{MAX_DB_RETRIES} подключения к БД неудачна: {e}")
            if attempt < MAX_DB_RETRIES - 1: await asyncio.sleep(DB_RETRY_DELAY)
    logger.critical("❌ Не удалось создать пул соединений с PostgreSQL после всех попыток")
    return False

async def db_execute(query: str, *args) -> bool:
    if not db_pool: return False
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(query, *args)
            return True
    except Exception as e:
        logger.error(f"Ошибка выполнения запроса: {e}\nQuery: {query[:200]}...")
        return False

async def db_fetchval(query: str, *args) -> Any:
    if not db_pool: return None
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetchval(query, *args)
    except Exception as e:
        logger.error(f"Ошибка выполнения запроса: {e}\nQuery: {query[:200]}...")
        return None

async def db_fetchall(query: str, *args) -> List[asyncpg.Record]:
    if not db_pool: return []
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetch(query, *args)
    except Exception as e:
        logger.error(f"Ошибка выполнения запроса: {e}\nQuery: {query[:200]}...")
        return []

# =====================================================
# НОВОЕ: Работа с токенами в БД
# =====================================================

async def create_action_token(user_id: int, action_type: str, data: Dict[str, Any]) -> Optional[str]:
    token = secrets.token_urlsafe(16)
    expires_at = datetime.utcnow() + timedelta(seconds=TOKEN_TTL_SECONDS)
    success = await db_execute(
        "INSERT INTO action_tokens (token, user_id, action_type, token_data, expires_at) VALUES ($1, $2, $3, $4, $5)",
        token, user_id, action_type, json.dumps(data), expires_at
    )
    if not success:
        logger.error("Не удалось создать токен действия для пользователя %s", user_id)
        return None
    return token

async def get_action_token(token: str) -> Optional[Dict[str, Any]]:
    record = await db_fetchall(
        "SELECT user_id, action_type, token_data FROM action_tokens WHERE token = $1 AND expires_at > NOW()",
        token
    )
    if not record:
        return None
    data = record[0]['token_data']
    return {
        'user_id': record[0]['user_id'],
        'action_type': record[0]['action_type'],
        'data': json.loads(data) if isinstance(data, str) else data
    }

async def delete_action_token(token: str):
    await db_execute("DELETE FROM action_tokens WHERE token = $1", token)

async def cleanup_expired_tokens_db():
    deleted_count = await db_fetchval(
        "WITH deleted AS (DELETE FROM action_tokens WHERE expires_at < NOW() RETURNING 1) SELECT count(*) FROM deleted"
    )
    if deleted_count is None:
        return
    if deleted_count > 0:
        logger.info(f"Удалено просроченных токенов из БД: {deleted_count}")

async def _token_cleanup_scheduler():
    while True:
        await asyncio.sleep(TOKEN_TTL_SECONDS)
        if not db_pool:
            continue
        logger.info("Запуск очистки просроченных токенов...")
        await cleanup_expired_tokens_db()

# =====================================================
# FSM ЧЕРНОВИКИ
# =====================================================

async def save_state_to_db(user_id: int, state: FSMContext):
    current_fsm_state = await state.get_state()
    if not current_fsm_state or not current_fsm_state.startswith('Process'):
        return
    data = await state.get_data()
    data['fsm_state'] = current_fsm_state
    # НОВОЕ: Добавляем версию состояния
    data['state_version'] = STATE_VERSION
    process_name = data.get('process_name')
    if not process_name:
        logger.warning(f"User(id={user_id}) | Попытка сохранить состояние без process_name.")
        return
    await db_execute(
        """
        INSERT INTO state_storage (user_id, process_name, state_data, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (user_id, process_name) DO UPDATE SET
            state_data = EXCLUDED.state_data,
            updated_at = NOW();
        """,
        user_id, process_name, json.dumps(data, ensure_ascii=False)
    )

async def load_state_from_db(user_id: int, process_name: str, state: FSMContext) -> bool:
    record = await db_fetchall(
        "SELECT state_data FROM state_storage WHERE user_id = $1 AND process_name = $2",
        user_id, process_name
    )
    if not record or record[0]['state_data'] is None:
        return False

    try:
        raw = record[0]['state_data']
        data = raw if isinstance(raw, dict) else json.loads(raw)

        # НОВОЕ: Проверяем версию состояния
        if data.get('state_version') != STATE_VERSION:
            logger.warning(f"User(id={user_id}) | Версия черновика устарела. Черновик будет удален.")
            await clear_state_for_process(user_id, process_name)
            return False # Возвращаем False, чтобы вызывающая функция знала, что сессия не восстановлена

        await state.set_data(data)
        fsm_state_str = data.get('fsm_state')
        await state.set_state(fsm_state_str or Process.param_menu)
        return True
    except Exception as e:
        logger.error(f"Ошибка загрузки черновика: {e}")
        await clear_state_for_process(user_id, process_name)
        return False

async def clear_state_for_process(user_id: int, process_name: str):
    await db_execute("DELETE FROM state_storage WHERE user_id = $1 AND process_name = $2", user_id, process_name)

# =====================================================
# КЛАВИАТУРЫ
# =====================================================

def main_menu_kb() -> types.InlineKeyboardMarkup:
    # ИЗМЕНЕНО: Используем CallbackData
    builder = InlineKeyboardBuilder()
    builder.button(text="🔧 Этап 1: Формовка", callback_data=StageCallback(name="forming"))
    builder.button(text="📦 Этап 2: Зона накопления ГП", callback_data=StageCallback(name="accumulation"))
    builder.button(text="📋 Этап 3: Упаковка", callback_data=StageCallback(name="packaging"))
    builder.button(text="📑 Этап 4: ЦГП", callback_data=StageCallback(name="cgp"))
    builder.adjust(1)
    return builder.as_markup()

def cancel_kb() -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🏠 Главное меню", callback_data=ProcessNavCallback(action="cancel"))
    return builder.as_markup()

def full_nav_kb() -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data=ProcessNavCallback(action="back"))
    builder.button(text="🏠 Главное меню", callback_data=ProcessNavCallback(action="cancel"))
    builder.adjust(1)
    return builder.as_markup()

def choice_kb(choices: Dict[str, str], nav_kb: types.InlineKeyboardMarkup) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for text, val in choices.items():
        builder.button(text=text, callback_data=ChoiceCallback(value=val))
    builder.adjust(1)
    # Добавляем навигационные кнопки
    for row in nav_kb.inline_keyboard:
        builder.row(*row)
    return builder.as_markup()

def build_param_menu(process_name: str, filled_keys: set[str]) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    chain = PROCESS_CHAINS.get(process_name, [])
    for step in chain:
        short = PARAM_TITLES.get(process_name, {}).get(step['key'], step['key'])
        label = f"✅ {short}" if step['key'] in filled_keys else short
        builder.button(text=label[:64], callback_data=ParamMenuCallback(action="open", process_name=process_name, param_key=step['key']))
    builder.adjust(1)
    builder.row(types.InlineKeyboardButton(text="🏁 Сохранить и завершить", callback_data=ParamMenuCallback(action="done").pack()))
    builder.row(types.InlineKeyboardButton(text="🏠 Главное меню", callback_data=ProcessNavCallback(action="cancel").pack()))
    return builder.as_markup()

async def show_param_menu(message: Message, state: FSMContext, edit_message: bool = True):
    data = await state.get_data()
    process_name = data.get('process_name')
    values = data.get('values', {})
    kb = build_param_menu(process_name, set(values.keys()))
    stage_title = STAGE_TITLES.get(process_name, process_name)
    text = f"Выберите параметр контроля ({stage_title}):"

    sent_message = None
    try:
        if edit_message:
            sent_message = await message.edit_text(text, reply_markup=kb)
        else:
            sent_message = await message.answer(text, reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            sent_message = message
        else:
            logger.warning(f"Не удалось отредактировать сообщение, отправляю новое: {e}")
            sent_message = await message.answer(text, reply_markup=kb)

    if sent_message:
        await state.update_data(last_bot_message_id=sent_message.message_id, chat_id=sent_message.chat.id)
        await save_state_to_db(data.get('user_id') or message.from_user.id, state)


# =====================================================
# FSM И ПРОЦЕССЫ
# =====================================================
# Логика функций start_process, ask_current_question, finish_process остается почти без изменений

async def ask_current_question(message: Message, state: FSMContext, edit_message: bool = False):
    data = await state.get_data()
    process_name, step_index = data.get('process_name'), data.get('step_index', 0)
    chain = PROCESS_CHAINS.get(process_name)
    if not chain:
        await message.answer("❌ Произошла ошибка. Начните заново.")
        await state.clear()
        if process_name: await clear_state_for_process(message.from_user.id, process_name)
        return

    current_step = chain[step_index]
    prompt = current_step['prompt'].format(**data)
    nav_kb = full_nav_kb()
    reply_markup = choice_kb(current_step['choices'], nav_kb) if current_step['type'] == 'choice' else nav_kb

    sent_message = None
    try:
        if edit_message:
            sent_message = await message.edit_text(text=prompt, reply_markup=reply_markup)
        else:
            sent_message = await message.answer(prompt, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            sent_message = message
        else:
            sent_message = await bot.send_message(message.chat.id, text=prompt, reply_markup=reply_markup)

    if sent_message:
        await state.update_data(last_bot_message_id=sent_message.message_id, chat_id=sent_message.chat.id)
        await save_state_to_db(data.get('user_id') or message.from_user.id, state)


async def finish_process(message: Message, state: FSMContext):
    data = await state.get_data()
    process_name, values, user_id = data.get('process_name'), data.get('values', {}), data.get('user_id')

    if data.get('pending_photo_required'):
        await message.answer("Сначала пришлите обязательное фото/QR."); return
    if data.get('pending_comment_required'):
        await message.answer("Сначала введите обязательный комментарий."); return
    if not values:
        await message.answer("Заполните хотя бы один параметр."); return
    if not user_id:
        await message.answer("❌ Критическая ошибка: не найден ID пользователя.")
        if process_name: await clear_state_for_process(data.get('user_id'), process_name)
        await state.clear()
        return

    # Проверки на обязательные фото и комментарии
    for key, value in values.items():
        step_config = next((step for step in PROCESS_CHAINS.get(process_name, []) if step['key'] == key), None)
        if not step_config: continue
        param_title = PARAM_TITLES.get(process_name, {}).get(key, key)
        if step_config.get('require_photo_always') and key not in data.get('photos', {}):
            await message.answer(f"❌ Для «{param_title}» обязательно фото."); return
        if step_config.get('photo_on_defect') and is_choice_defect(key, value) and key not in data.get('photos', {}):
            await message.answer(f"❌ Для дефекта «{param_title}» обязательно фото."); return
        if step_config.get('comment_on_defect') and is_choice_defect(key, value) and not values.get(f"{key}_comment"):
            await message.answer(f"❌ Для дефекта «{param_title}» обязателен комментарий."); return

    # Добавление QR-данных в итоговый JSON
    qr_prefixes = {
        'forming': 'frame',
        'accumulation': 'accumulation',
        'packaging': 'packaging',
        'cgp': 'cgp'
    }
    prefix = qr_prefixes.get(process_name)
    if prefix:
        tare_key, goods_key, text_key = f'{prefix}_qr_tare', f'{prefix}_qr_goods', f'{prefix}_qr_text'
        tare = data.get(tare_key)
        goods = data.get(goods_key) or data.get(text_key)
        if tare: values[tare_key] = tare
        if goods:
            values[goods_key] = goods
            values[text_key] = goods # Для совместимости

    if process_name == 'forming':
        values['sample_number'] = data.get('sample_number', 1)

    if photos_map := data.get('photos'):
        values['photos'] = photos_map

    value_numeric = next((values[s['key']] for s in PROCESS_CHAINS.get(process_name, []) if s['type'] == 'float' and s['key'] in values), None)
    forming_session_id = data.get('forming_session_id')
    last_bot_message_id = data.get('last_bot_message_id')
    chat_id = data.get('chat_id')

    success = await db_execute(
        """INSERT INTO control_data (user_id, stage_name, forming_session_id, value_numeric, data)
           VALUES ($1, $2, $3, $4, $5)""",
        user_id, process_name, forming_session_id, value_numeric, json.dumps(values, ensure_ascii=False)
    )

    if not success:
        if chat_id and last_bot_message_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=last_bot_message_id,
                    text="❌ Произошла ошибка при сохранении данных.", reply_markup=None,
                )
            except Exception: pass
        await message.answer("Попробуйте снова.", reply_markup=main_menu_kb())
        await state.clear()
        return

    await clear_state_for_process(user_id, process_name)
    
    success_text = f"✅ Данные для этапа <b>'{STAGE_TITLES.get(process_name, process_name)}'</b> успешно сохранены."
    if process_name == "forming":
        success_text = f"✅ Данные для <b>Образца №{data['sample_number']}</b> сохранены."
        if chat_id and last_bot_message_id:
            try:
                await bot.edit_message_text(text=success_text, chat_id=chat_id, message_id=last_bot_message_id, reply_markup=None)
            except Exception: pass
        
        await state.set_state(Process.forming_confirm_next)
        builder = InlineKeyboardBuilder()
        builder.button(text="➕ Добавить еще образец", callback_data=FormingCallback(action="add_another"))
        builder.button(text="🏁 Завершить контроль рамы", callback_data=FormingCallback(action="finish"))
        sent_msg = await message.answer("Что делаем дальше?", reply_markup=builder.as_markup())
        await state.update_data(last_bot_message_id=sent_msg.message_id, chat_id=sent_msg.chat.id)
        await save_state_to_db(user_id, state)
    else:
        await state.clear()
        if chat_id and last_bot_message_id:
            try:
                await bot.edit_message_text(text=success_text, chat_id=chat_id, message_id=last_bot_message_id, reply_markup=None)
            except Exception: pass
        await message.answer("Выберите этап контроля:", reply_markup=main_menu_kb())


# =====================================================
# КОМАНДЫ И ХЭНДЛЕРЫ
# =====================================================

async def ensure_user_registered(user_id: int, full_name: str) -> bool:
    if await db_fetchval("SELECT 1 FROM users WHERE user_id = $1", user_id): return True
    return await db_execute("INSERT INTO users (user_id, full_name) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING", user_id, full_name)

async def cmd_start(message: Message, state: FSMContext):
    if await state.get_state():
        await save_state_to_db(message.from_user.id, state)
        await message.answer("Открыто меню. Текущий прогресс сохранён.", reply_markup=ReplyKeyboardRemove())
    
    if await db_fetchval("SELECT 1 FROM users WHERE user_id = $1", message.from_user.id):
        full_name_db = await db_fetchval("SELECT full_name FROM users WHERE user_id = $1", message.from_user.id)
        display_name = full_name_db or message.from_user.full_name
        await message.answer(f"Добро пожаловать, {escape(display_name)}! Выберите этап контроля:", reply_markup=main_menu_kb())
    else:
        await state.set_state(Registration.waiting_for_name)
        await message.answer("Здравствуйте! Для начала работы, введите ваши <b>Фамилию и Имя</b>.")

async def process_registration(message: Message, state: FSMContext):
    if not message.text or len(message.text.strip().split()) < 2:
        await message.answer("⚠️ Пожалуйста, введите и фамилию, и имя."); return
    full_name = message.text.strip()[:255]
    
    if await db_execute("INSERT INTO users (user_id, full_name) VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET full_name = $2", message.from_user.id, full_name):
        await state.set_state(Registration.waiting_for_position)
        builder = InlineKeyboardBuilder()
        positions = ["Оператор/технолог", "Контролёр - технолог", "Оператор", "Оператор - наладчик"]
        for pos in positions:
            builder.button(text=pos, callback_data=RegistrationCallback(position=pos))
        builder.adjust(1)
        await message.answer("Укажите, пожалуйста, вашу <b>должность</b>:", reply_markup=builder.as_markup())
    else:
        await message.answer("❌ Произошла ошибка при регистрации. Попробуйте снова.")

async def process_registration_position_cb(callback: CallbackQuery, state: FSMContext, callback_data: RegistrationCallback):
    position = callback_data.position
    if await db_execute("UPDATE users SET position = $1 WHERE user_id = $2", position, callback.from_user.id):
        await state.clear()
        try:
            await callback.message.edit_text("✅ Спасибо! Данные регистрации сохранены.", reply_markup=None)
        except Exception: pass
        await callback.message.answer("Теперь вы можете выбрать этап контроля:", reply_markup=main_menu_kb())
    else:
        await callback.message.answer("❌ Не удалось сохранить должность. Попробуйте ещё раз.")
    await callback.answer()

async def process_cancel_callback(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    current_fsm_state = await state.get_state()
    # Логика отката значения, если юзер отменил ввод фото/комментария
    if current_fsm_state in {Process.waiting_for_param_photo.state, Process.waiting_for_param_comment.state}:
        key_to_revert = data.get('pending_photo_param_key') or data.get('pending_comment_param_key')
        if key_to_revert:
            values = data.get('values', {}); values.pop(key_to_revert, None)
            photos = data.get('photos', {}); photos.pop(key_to_revert, None)
            await state.update_data(
                values=values, photos=photos,
                pending_photo_required=False, pending_photo_param_key=None,
                pending_comment_required=False, pending_comment_param_key=None
            )
    
    await save_state_to_db(callback.from_user.id, state)
    try:
        await callback.message.edit_text("🏠 Вы в главном меню.", reply_markup=None)
    except Exception: pass
    await callback.message.answer("Выберите этап контроля:", reply_markup=main_menu_kb())
    await callback.answer()

async def process_stage_selection(callback: CallbackQuery, state: FSMContext, callback_data: StageCallback):
    user = callback.from_user
    stage_name = callback_data.name
    if not await ensure_user_registered(user.id, user.full_name):
        await callback.answer("❌ Ошибка базы данных.", show_alert=True); return

    is_loaded = await load_state_from_db(user.id, stage_name, state)
    if is_loaded:
        data = await state.get_data()
        if last_message_id := data.get('last_bot_message_id'):
            try: await bot.delete_message(data.get('chat_id'), last_message_id)
            except Exception: pass
        
        fsm_state = await state.get_state()
        if fsm_state == Process.waiting_for_param_photo.state:
            sent = await callback.message.answer("📷 Фото обязательно. Пришлите фото или вернитесь в меню.", reply_markup=cancel_kb())
            await state.update_data(last_bot_message_id=sent.message_id, chat_id=sent.chat.id)
        elif fsm_state == Process.waiting_for_param_comment.state:
            sent = await callback.message.answer("📝 Требуется комментарий. Введите текст или вернитесь в меню.", reply_markup=cancel_kb())
            await state.update_data(last_bot_message_id=sent.message_id, chat_id=sent.chat.id)
        else:
            await state.set_state(Process.param_menu)
            await show_param_menu(callback.message, state)
        await callback.answer("↩️ Ваш прошлый сеанс восстановлен."); return
    
    # ИЗМЕНЕНО: Если черновик не загружен (в т.ч. из-за старой версии), сообщаем об этом
    if not is_loaded and await db_fetchval("SELECT 1 FROM state_storage WHERE user_id = $1 AND process_name = $2", user.id, stage_name):
        await callback.answer("Ваша прошлая сессия устарела из-за обновления и была сброшена. Начните заново.", show_alert=True)
    
    # Дальнейшая логика по этапам
    builder = InlineKeyboardBuilder()
    if stage_name == "forming":
        active_forming = await db_fetchall("SELECT session_id, frame_qr_text AS code FROM forming_sessions WHERE user_id = $1 AND completed_at IS NULL ORDER BY created_at DESC LIMIT 1", user.id)
        if active_forming:
            s = active_forming[0]
            builder.button(text=f"↩️ Продолжить раму {s['code'][:40]}", callback_data=FormingCallback(action="continue", session_id=s['session_id']))
            builder.button(text="➕ Добавить новую раму", callback_data=FormingCallback(action="new"))
            builder.button(text="🏠 Главное меню", callback_data=ProcessNavCallback(action="cancel"))
            builder.adjust(1)
            await callback.message.edit_text("<b>Этап 1: Формовка</b>\nВыберите действие:", reply_markup=builder.as_markup())
        else:
            await state.set_state(Process.waiting_for_qr)
            await state.update_data(process_name_after_qr="forming")
            await callback.message.edit_text("<b>Этап 1: Формовка</b>\nОтправьте фото с двумя QR-кодами (слева тара, справа товар).", reply_markup=cancel_kb())

    elif stage_name in ("accumulation", "cgp"):
        last_data_record = await db_fetchall("SELECT data FROM control_data WHERE user_id = $1 AND stage_name = $2 ORDER BY created_at DESC LIMIT 1", user.id, stage_name)
        
        goods, tare = None, None
        if last_data_record:
            d = last_data_record[0]['data']
            if isinstance(d, dict):
                tare = d.get(f'{stage_name}_qr_tare')
                goods = d.get(f'{stage_name}_qr_goods') or d.get(f'{stage_name}_qr_text')
        
        if goods:
            token_data = {'goods': str(goods), 'tare': tare}
            token = await create_action_token(user.id, stage_name, token_data)

            if token:
                if stage_name == "accumulation":
                    builder.button(
                        text=f"↩️ Продолжить раму {str(goods)[:40]}",
                        callback_data=AccumulationCallback(action="continue", token=token)
                    )
                else:  # cgp
                    builder.button(
                        text=f"↩️ Продолжить паллет {str(goods)[:40]}",
                        callback_data=CgpCallback(action="continue", token=token)
                    )

            if stage_name == "accumulation":
                builder.button(text="➕ Добавить новую раму", callback_data=AccumulationCallback(action="new"))
            else:  # cgp
                builder.button(text="➕ Сканировать новый паллет", callback_data=CgpCallback(action="new"))

            builder.button(text="🏠 Главное меню", callback_data=ProcessNavCallback(action="cancel"))
            builder.adjust(1)
            await callback.message.edit_text(
                f"<b>{STAGE_TITLES[stage_name]}</b>\nВыберите действие:",
                reply_markup=builder.as_markup()
            )
        else:
            await state.set_state(Process.waiting_for_qr)
            await state.update_data(process_name_after_qr=stage_name)
            await callback.message.edit_text(
                f"<b>{STAGE_TITLES[stage_name]}</b>\nОтправьте фото с QR-кодами (слева тара, справа товар).",
                reply_markup=cancel_kb()
            )
            
    elif stage_name == "packaging":
        await state.set_state(Process.waiting_for_qr)
        await state.update_data(process_name_after_qr="packaging")
        await callback.message.edit_text("<b>Этап 3: Упаковка</b>\nОтправьте фото с QR-кодами (слева тара, справа товар).", reply_markup=cancel_kb())

    await callback.answer()

async def param_menu_done_handler(callback: CallbackQuery, state: FSMContext, callback_data: ParamMenuCallback):
    data = await state.get_data()
    if data.get('pending_photo_required'):
        await callback.answer("Сначала пришлите обязательное фото/QR", show_alert=True); return
    if data.get('pending_comment_required'):
        await callback.answer("Сначала введите обязательный комментарий", show_alert=True); return
    if not data.get('values'):
        await callback.answer("Заполните хотя бы один параметр", show_alert=True); return
    
    await finish_process(callback.message, state)
    await callback.answer()

async def new_scan_handler(callback: CallbackQuery, state: FSMContext):
    """Общий хендлер для кнопок 'Добавить новую раму', 'Сканировать новый паллет' и т.д."""
    # Определяем этап по типу callback_data
    stage_map = {
        FormingCallback: "forming",
        AccumulationCallback: "accumulation",
        CgpCallback: "cgp"
    }
    process_name = next((stage for cb_type, stage in stage_map.items() if isinstance(callback.data, str) and callback.data.startswith(cb_type.prefix)), None)
    
    if not process_name: # fallback для старых кнопок или ошибок
        if "accum" in callback.data: process_name = "accumulation"
        elif "forming" in callback.data: process_name = "forming"
        elif "cgp" in callback.data: process_name = "cgp"

    await state.set_state(Process.waiting_for_qr)
    await state.update_data(process_name_after_qr=process_name)
    await callback.message.edit_text(f"<b>{STAGE_TITLES[process_name]}</b>\nОтправьте фото с QR-кодами (слева тара, справа товар).", reply_markup=cancel_kb())
    await callback.answer()

async def continue_session_handler(callback: CallbackQuery, state: FSMContext):
    """Общий хендлер для кнопок 'Продолжить раму/паллет'."""
    cb_data = None
    if callback.data.startswith(AccumulationCallback.prefix):
        cb_data = AccumulationCallback.unpack(callback.data)
    elif callback.data.startswith(CgpCallback.prefix):
        cb_data = CgpCallback.unpack(callback.data)

    token_info = await get_action_token(cb_data.token)
    if not token_info or token_info.get('user_id') != callback.from_user.id:
        await callback.answer("Сессия не найдена или устарела. Начните заново.", show_alert=True); return
    
    process_name = token_info['action_type']
    tok_data = token_info['data']

    await state.update_data(
        user_id=callback.from_user.id, process_name=process_name,
        **{f"{process_name}_qr_goods": tok_data.get('goods')},
        **{f"{process_name}_qr_tare": tok_data.get('tare')},
        **{f"{process_name}_qr_text": tok_data.get('goods')}, # Совместимость
        values={}, photos={}, step_index=0,
        pending_photo_required=False, pending_comment_required=False,
    )
    await clear_state_for_process(callback.from_user.id, process_name)
    await state.set_state(Process.param_menu)
    await show_param_menu(callback.message, state)
    await delete_action_token(cb_data.token) # Удаляем токен после использования
    await callback.answer()

async def forming_continue_handler(callback: CallbackQuery, state: FSMContext, callback_data: FormingCallback):
    session_id = callback_data.session_id
    session_data = await db_fetchall("SELECT frame_qr_text, frame_qr_tare FROM forming_sessions WHERE session_id = $1", session_id)
    if not session_data:
        await callback.answer("Сессия формовки не найдена.", show_alert=True); return
    
    goods = session_data[0]['frame_qr_text']
    tare = session_data[0]['frame_qr_tare']

    await state.update_data(
        user_id=callback.from_user.id, process_name="forming", forming_session_id=session_id,
        frame_qr_goods=goods, frame_qr_text=goods, frame_qr_tare=tare,
        values={}, photos={}, step_index=0, sample_number=1, # При продолжении начинаем с 1 образца
        pending_photo_required=False,
    )
    await clear_state_for_process(callback.from_user.id, "forming")
    await state.set_state(Process.param_menu)
    await show_param_menu(callback.message, state)
    await callback.answer()

async def param_open_handler(callback: CallbackQuery, state: FSMContext, callback_data: ParamMenuCallback):
    data = await state.get_data()
    chain = PROCESS_CHAINS.get(callback_data.process_name, [])
    try:
        idx = next(i for i, s in enumerate(chain) if s['key'] == callback_data.param_key)
    except StopIteration:
        await callback.answer("Ошибка: параметр не найден.", show_alert=True); return

    if callback_data.process_name == "forming" and 'sample_number' not in data:
        await state.update_data(sample_number=1)

    await state.update_data(step_index=idx)
    await state.set_state(Process.in_progress)
    await ask_current_question(callback.message, state, edit_message=True)
    await callback.answer()

async def process_qr_code(message: Message, state: FSMContext):
    user = message.from_user
    if not await ensure_user_registered(user.id, user.full_name):
        await message.answer("❌ Ошибка базы данных."); return

    file = await bot.get_file(message.photo[-1].file_id)
    ext = file.file_path.split('.')[-1] if file.file_path else 'jpg'
    local_file_path = os.path.join(QR_IMAGE_DIR, f"{uuid.uuid4().hex}.{ext}")
    
    if not await download_telegram_file_by_file_id(message.photo[-1].file_id, local_file_path):
        await message.answer("⚠️ Не удалось загрузить фото. Попробуйте ещё раз."); return

    results = await decode_multi_qr_from_image_async(local_file_path)
    if not results or len(results) != 2:
        await message.answer("⚠️ Не удалось распознать два QR-кода. Фото должно содержать QR тары (слева) и QR товара (справа)."); return

    tare_text, goods_text = results[0]['text'], results[1]['text']
    await message.answer(f"✅ Распознано:\nТара: <code>{escape(tare_text)}</code>\nТовар: <code>{escape(goods_text)}</code>")

    data = await state.get_data()
    process_name = data.get('process_name_after_qr')

    if process_name == "forming":
        session_id = await db_fetchval(
            """
            WITH existing AS (
                SELECT session_id FROM forming_sessions
                WHERE frame_qr_text = $2 AND completed_at IS NULL
                LIMIT 1
            ), ins AS (
                INSERT INTO forming_sessions (user_id, frame_qr_text, frame_qr_tare, frame_qr_tg_file_id, frame_qr_image_path)
                SELECT $1, $2, $3, $4, $5
                WHERE NOT EXISTS (SELECT 1 FROM existing)
                RETURNING session_id
            )
            SELECT session_id FROM existing UNION ALL SELECT session_id FROM ins;
            """, user.id, goods_text, tare_text, file.file_id, local_file_path
        )
        
        if session_id:
            await state.update_data(
                user_id=user.id, process_name="forming", forming_session_id=session_id,
                frame_qr_tare=tare_text, frame_qr_goods=goods_text, frame_qr_text=goods_text,
                step_index=0, sample_number=1, values={}, photos={},
            )
            await state.set_state(Process.param_menu)
            await show_param_menu(message, state, edit_message=False)
        else:
            owner_info = await db_fetchall("SELECT u.full_name FROM forming_sessions fs JOIN users u ON u.user_id = fs.user_id WHERE fs.frame_qr_text = $1 AND fs.completed_at IS NULL LIMIT 1", goods_text)
            owner_name = owner_info[0]['full_name'] if owner_info else "другим сотрудником"
            await message.answer(f"⚠️ Эта рама уже в работе у: <b>{escape(owner_name)}</b>.")
            await state.clear()
    else: # accumulation, packaging, cgp
        await state.set_state(Process.param_menu)
        await state.update_data(
            user_id=user.id, process_name=process_name,
            **{f"{process_name}_qr_tare": tare_text},
            **{f"{process_name}_qr_goods": goods_text},
            **{f"{process_name}_qr_text": goods_text},
            control_dir=datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
            values={}, photos={},
        )
        await clear_state_for_process(user.id, process_name)
        await show_param_menu(message, state, edit_message=False)

    await state.update_data(process_name_after_qr=None)
    await save_state_to_db(user.id, state)


async def handle_param_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    param_key = data.get('pending_photo_param_key')
    process_name = data.get('process_name')
    if not param_key or not process_name:
        await message.answer("❌ Ошибка. Начните заново через главное меню."); return

    control_dir = data.get('control_dir') or datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file = await bot.get_file(message.photo[-1].file_id)
    dest_path = build_control_photo_path(control_dir, process_name, param_key, getattr(file, 'file_path', None))
    
    if not await download_telegram_file_by_file_id(message.photo[-1].file_id, dest_path):
        await message.answer("❌ Не удалось сохранить фото. Отправьте его ещё раз."); return

    photos = data.get('photos', {})
    photos.setdefault(param_key, []).append(dest_path)
    await state.update_data(photos=photos, pending_photo_required=False, pending_photo_param_key=None)
    await state.set_state(Process.param_menu)
    await show_param_menu(message, state, edit_message=False)


async def handle_param_comment(message: Message, state: FSMContext):
    if not message.text:
        await message.reply("⚠️ Пожалуйста, введите текстовый комментарий."); return
    data = await state.get_data()
    param_key = data.get('pending_comment_param_key')
    if not param_key:
        await message.answer("❌ Ошибка. Начните заново через главное меню."); return

    values = data.get('values', {})
    values[f"{param_key}_comment"] = message.text.strip()
    await state.update_data(values=values, pending_comment_required=False, pending_comment_param_key=None)
    await state.set_state(Process.param_menu)
    await show_param_menu(message, state, edit_message=False)

async def process_step_answer(message: Message, state: FSMContext):
    if not message.text:
        await message.reply("⚠️ Пожалуйста, введите текстовое значение."); return
    data = await state.get_data()
    process_name, step_index = data.get('process_name'), data.get('step_index')
    chain = PROCESS_CHAINS.get(process_name)

    if not all([process_name, isinstance(step_index, int), chain, 0 <= step_index < len(chain)]):
        await message.answer("❌ Произошла ошибка.", reply_markup=main_menu_kb())
        if process_name: await clear_state_for_process(message.from_user.id, process_name)
        await state.clear(); return

    current_step = chain[step_index]
    if current_step['type'] == 'choice':
        await message.reply("Пожалуйста, используйте кнопки ниже."); return

    value_to_save = message.text.strip()
    if current_step['type'] == 'float':
        try:
            value_to_save = float(value_to_save.replace(',', '.'))
        except ValueError:
            await message.reply("⚠️ Неверный формат. Введите число."); return

    is_valid, error_msg = await validate_input(value_to_save, current_step)
    if not is_valid:
        await message.reply(f"⚠️ {error_msg}"); return

    values = data.get('values', {})
    values[current_step['key']] = value_to_save
    await state.update_data(values=values)
    
    if current_step.get('require_photo_always'):
        await state.update_data(pending_photo_param_key=current_step['key'], pending_photo_required=True)
        await state.set_state(Process.waiting_for_param_photo)
        await message.answer("📷 Фото обязательно. Пришлите фото или вернитесь в меню.", reply_markup=cancel_kb())
    else:
        await state.set_state(Process.param_menu)
        await show_param_menu(message, state, edit_message=False)


async def process_choice_answer(callback: CallbackQuery, state: FSMContext, callback_data: ChoiceCallback):
    data = await state.get_data()
    process_name, step_index = data.get('process_name'), data.get('step_index')
    chain = PROCESS_CHAINS.get(process_name)
    if not all([process_name, isinstance(step_index, int), chain, 0 <= step_index < len(chain)]):
        await callback.answer(); return

    current_step = chain[step_index]
    step_key = current_step['key']
    value_to_save = callback_data.value
    
    values = data.get('values', {})
    values[step_key] = value_to_save
    await state.update_data(values=values)
    
    need_photo = current_step.get('require_photo_always') or \
                 (current_step.get('photo_on_defect') and is_choice_defect(step_key, value_to_save))
    
    need_comment = current_step.get('comment_on_defect') and is_choice_defect(step_key, value_to_save)

    if need_photo:
        await state.update_data(pending_photo_param_key=step_key, pending_photo_required=True)
        await state.set_state(Process.waiting_for_param_photo)
        await callback.message.edit_text("📷 Фото обязательно. Пришлите фото или вернитесь в меню.", reply_markup=cancel_kb())
    elif need_comment:
        await state.update_data(pending_comment_param_key=step_key, pending_comment_required=True)
        await state.set_state(Process.waiting_for_param_comment)
        prompt = current_step.get('comment_prompt', "Пожалуйста, оставьте комментарий:")
        await callback.message.edit_text(prompt, reply_markup=cancel_kb())
    else:
        await state.set_state(Process.param_menu)
        await show_param_menu(callback.message, state)
    
    await callback.answer()

async def process_navigation_back(callback: CallbackQuery, state: FSMContext, callback_data: ProcessNavCallback):
    await state.set_state(Process.param_menu)
    await show_param_menu(callback.message, state)
    await callback.answer()

async def forming_confirm_handler(callback: CallbackQuery, state: FSMContext, callback_data: FormingCallback):
    data = await state.get_data()
    if callback_data.action == "add_another":
        new_sample_number = data.get('sample_number', 1) + 1
        await state.update_data(step_index=0, sample_number=new_sample_number, values={}, photos={})
        await state.set_state(Process.param_menu)
        await show_param_menu(callback.message, state)
    elif callback_data.action == "finish":
        if session_id := data.get('forming_session_id'):
            await db_execute("UPDATE forming_sessions SET completed_at = NOW() WHERE session_id = $1", session_id)
        await state.clear()
        if data.get("process_name"):
            await clear_state_for_process(callback.from_user.id, data["process_name"])
        await callback.message.edit_text("✅ Контроль рамы завершен.", reply_markup=None)
        await callback.message.answer("Выберите этап контроля:", reply_markup=main_menu_kb())
    await callback.answer()

async def handle_unexpected_message(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Используйте команду /start для начала работы.")
    else:
        # Не отвечаем на сообщения, когда не ожидаем их, чтобы избежать спама
        logger.info(f"Неожиданное сообщение от {message.from_user.id} в состоянии {current_state}")

# =====================================================
# ЗАПУСК
# =====================================================

async def on_startup(bot: Bot):
    os.makedirs(QR_IMAGE_DIR, exist_ok=True)
    os.makedirs(CONTROL_PHOTO_DIR, exist_ok=True)
    if not await create_db_pool():
        raise SystemExit(1)
    await bot.set_my_commands([BotCommand(command="/start", description="🏠 Главное меню")])
    global TOKEN_CLEANUP_TASK
    TOKEN_CLEANUP_TASK = asyncio.create_task(_token_cleanup_scheduler())

async def on_shutdown(bot: Bot):
    if TOKEN_CLEANUP_TASK:
        TOKEN_CLEANUP_TASK.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await TOKEN_CLEANUP_TASK
    if db_pool:
        await db_pool.close()
    logger.info("Бот остановлен.")

def register_handlers(dp: Dispatcher):
    # Регистрация
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(process_registration, Registration.waiting_for_name)
    dp.callback_query.register(process_registration_position_cb, Registration.waiting_for_position, RegistrationCallback.filter())

    # Главное меню и выбор этапа
    dp.callback_query.register(process_cancel_callback, ProcessNavCallback.filter(F.action == "cancel"))
    dp.callback_query.register(process_stage_selection, StageCallback.filter())

    # Меню параметров
    dp.callback_query.register(param_menu_done_handler, Process.param_menu, ParamMenuCallback.filter(F.action == "done"))
    dp.callback_query.register(param_open_handler, Process.param_menu, ParamMenuCallback.filter(F.action == "open"))

    # Кнопки "Новый" / "Продолжить"
    dp.callback_query.register(new_scan_handler, FormingCallback.filter(F.action == "new"))
    dp.callback_query.register(new_scan_handler, AccumulationCallback.filter(F.action == "new"))
    dp.callback_query.register(new_scan_handler, CgpCallback.filter(F.action == "new"))
    dp.callback_query.register(forming_continue_handler, FormingCallback.filter(F.action == "continue"))
    dp.callback_query.register(continue_session_handler, AccumulationCallback.filter(F.action == "continue"))
    dp.callback_query.register(continue_session_handler, CgpCallback.filter(F.action == "continue"))

    # Подтверждение после формовки
    dp.callback_query.register(forming_confirm_handler, Process.forming_confirm_next, FormingCallback.filter())

    # Ввод данных в процессе
    dp.callback_query.register(process_choice_answer, Process.in_progress, ChoiceCallback.filter())
    dp.callback_query.register(process_navigation_back, Process.in_progress, ProcessNavCallback.filter(F.action == "back"))
    dp.message.register(process_step_answer, Process.in_progress)

    # Ожидание QR, фото, комментариев
    dp.message.register(process_qr_code, Process.waiting_for_qr, F.photo)
    dp.message.register(handle_param_photo, Process.waiting_for_param_photo, F.photo)
    dp.message.register(handle_param_comment, Process.waiting_for_param_comment, F.text)
    
    # Обработка неожиданных сообщений
    dp.message.register(handle_unexpected_message)

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    register_handlers(dp)
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.critical(f"Критическая ошибка в главном цикле: {e}", exc_info=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот выключается...")
