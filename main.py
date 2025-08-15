import asyncio
import logging
import os
import uuid
import secrets
from typing import Optional, Dict, List, Any

import asyncpg
from PIL import Image
from pyzbar import pyzbar
from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, BotCommand, ReplyKeyboardRemove
from aiogram.exceptions import TelegramBadRequest
from html import escape
from dotenv import load_dotenv
from datetime import datetime
import json

# =====================================================
# КОНФИГУРАЦИЯ
# =====================================================

MAX_DB_RETRIES = 3
DB_RETRY_DELAY = 2
TOKEN_TTL_SECONDS = 3600  # 1 час

# =====================================================
# ИНИЦИАЛИЗАЦИЯ
# =====================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
load_dotenv(".env.local")
load_dotenv("secrets.env")
load_dotenv(".env.example")

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

# token -> { 'user_id': int, 'goods': str, 'tare': Optional[str], 'created_at': datetime }
ACCUM_CONTINUE_TOKENS: Dict[str, Dict[str, Any]] = {}

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
        try:
            await bot.download_file(file.file_path, destination_path)
            return True
        except Exception:
            await bot.download(file, destination=destination_path)
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
        'wrinkling_visual': set(),
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

# --------- QR-декодирование: два кода с одного фото ---------

def _sync_decode_multi_qr(image_path: str) -> List[Dict[str, Any]]:
    try:
        with Image.open(image_path) as im:
            objs = [o for o in pyzbar.decode(im) if getattr(o, 'type', '') == 'QRCODE']
            items = []
            for o in objs:
                r = o.rect
                items.append({'text': o.data.decode('utf-8'), 'x': r.left, 'y': r.top, 'w': r.width, 'h': r.height, 'area': r.width * r.height})
            # уник по тексту
            seen, uniq = set(), []
            for it in items:
                if it['text'] not in seen:
                    seen.add(it['text']); uniq.append(it)
            if not uniq:
                return []
            # берем 2 самых крупных, слева-направо
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
# FSM ЧЕРНОВИКИ
# =====================================================

async def save_state_to_db(user_id: int, state: FSMContext):
    current_fsm_state = await state.get_state()
    if not current_fsm_state or not current_fsm_state.startswith('Process'):
        return
    data = await state.get_data()
    data['fsm_state'] = current_fsm_state
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
    if record and record[0]['state_data'] is not None:
        try:
            raw = record[0]['state_data']
            data = raw if isinstance(raw, dict) else json.loads(raw)
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
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🔧 Этап 1: Формовка", callback_data="stage_forming")],
        [types.InlineKeyboardButton(text="📦 Этап 2: Зона накопления ГП", callback_data="stage_accumulation")],
        [types.InlineKeyboardButton(text="📋 Этап 3: Упаковка", callback_data="stage_packaging")],
        [types.InlineKeyboardButton(text="📑 Этап 4: ЦГП", callback_data="stage_cgp")]
    ])

def cancel_kb() -> types.InlineKeyboardMarkup:
    return types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")]])

def full_nav_kb(is_first_step: bool = False) -> types.InlineKeyboardMarkup:
    rows = []
    rows.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data="process_back")])
    rows.append([types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)

def choice_kb(prefix: str, choices: Dict[str, str], nav_kb: types.InlineKeyboardMarkup) -> types.InlineKeyboardMarkup:
    buttons = [types.InlineKeyboardButton(text=text, callback_data=f"{prefix}:{val}") for text, val in choices.items()]
    keyboard_layout = [[button] for button in buttons]
    for row in nav_kb.inline_keyboard:
        keyboard_layout.append(row)
    return types.InlineKeyboardMarkup(inline_keyboard=keyboard_layout)

def build_param_menu(process_name: str, filled_keys: set[str]) -> types.InlineKeyboardMarkup:
    chain = PROCESS_CHAINS.get(process_name, [])
    buttons: List[types.InlineKeyboardButton] = []
    for step in chain:
        short = PARAM_TITLES.get(process_name, {}).get(step['key'])
        if not short:
            prompt = step['prompt']
            short = prompt.split('\n')[-1].replace('<b>', '').replace('</b>', '')
        label = f"✅ {short}" if step['key'] in filled_keys else short
        buttons.append(types.InlineKeyboardButton(text=label[:64], callback_data=f"param_open:{process_name}:{step['key']}"))
    rows = [[button] for button in buttons]
    rows.append([types.InlineKeyboardButton(text="🏁 Сохранить и завершить", callback_data="param_done")])
    rows.append([types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)

async def show_param_menu(message: Message, state: FSMContext):
    data = await state.get_data()
    process_name = data.get('process_name')
    values = data.get('values', {})
    kb = build_param_menu(process_name, set(values.keys()))
    try:
        stage_title = STAGE_TITLES.get(process_name, process_name)
        sent_message = await message.edit_text(f"Выберите параметр контроля ({stage_title}):", reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            sent_message = message
        else:
            stage_title = STAGE_TITLES.get(process_name, process_name)
            sent_message = await message.answer(f"Выберите параметр контроля ({stage_title}):", reply_markup=kb)
    await state.update_data(last_bot_message_id=sent_message.message_id, chat_id=sent_message.chat.id)

# =====================================================
# FSM И ПРОЦЕССЫ
# =====================================================

async def start_process(user_id: int, user_name: str, message_to_reply: Message, state: FSMContext, process_name: str, session_id: Optional[int] = None, session_type: Optional[str] = None):
    if process_name not in PROCESS_CHAINS:
        await message_to_reply.answer("❌ Произошла ошибка. Попробуйте снова.")
        return
    control_dir = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    data_to_set = {
        "user_id": user_id,
        "process_name": process_name,
        "step_index": 0,
        "sample_number": 1,
        "values": {},
        "photos": {},
        "control_dir": control_dir,
    }
    if session_type and session_id:
        data_to_set[session_type] = session_id
    await state.set_data(data_to_set)
    await state.set_state(Process.param_menu)
    await show_param_menu(message_to_reply, state)

async def ask_current_question(message: Message, state: FSMContext, edit_message: bool = False):
    data = await state.get_data()
    process_name, step_index = data.get('process_name'), data.get('step_index', 0)
    chain = PROCESS_CHAINS.get(process_name)
    if not chain:
        await message.answer("❌ Произошла ошибка. Начните заново.")
        await state.clear()
        if process_name: await clear_state_for_process(message.from_user.id, process_name)
        return
    if step_index >= len(chain):
        await finish_process(message, state)
        return
    current_step = chain[step_index]
    prompt = current_step['prompt'].format(**data)
    nav_kb = full_nav_kb(is_first_step=(step_index == 0))
    reply_markup = choice_kb(f"{process_name}_{step_index}", current_step['choices'], nav_kb) if current_step['type'] == 'choice' else nav_kb
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
    await state.update_data(last_bot_message_id=sent_message.message_id, chat_id=sent_message.chat.id)

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
        await state.clear()
        if process_name: await clear_state_for_process(data.get('user_id'), process_name)
        return

    for key, value in values.items():
        step_config = next((step for step in PROCESS_CHAINS.get(process_name, []) if step['key'] == key), None)
        if not step_config: continue
        if step_config.get('require_photo_always') and key not in data.get('photos', {}):
            param_title = PARAM_TITLES.get(process_name, {}).get(key, key)
            await message.answer(f"❌ Для «{param_title}» обязательно фото."); return
        if step_config.get('photo_on_defect') and is_choice_defect(key, value) and key not in data.get('photos', {}):
            param_title = PARAM_TITLES.get(process_name, {}).get(key, key)
            await message.answer(f"❌ Для дефекта «{param_title}» обязательно фото."); return
        if step_config.get('comment_on_defect') and is_choice_defect(key, value):
            comment_key = f"{key}_comment"
            if not values.get(comment_key):
                param_title = PARAM_TITLES.get(process_name, {}).get(key, key)
                await message.answer(f"❌ Для дефекта «{param_title}» обязателен комментарий."); return

    value_numeric = None
    for step in PROCESS_CHAINS.get(process_name, []):
        if step['type'] == 'float' and step['key'] in values:
            value_numeric = values[step['key']]
            break

    if process_name == 'forming':
        values['sample_number'] = data.get('sample_number', 1)

    photos_map = data.get('photos')
    if photos_map:
        values['photos'] = photos_map

    if process_name == 'accumulation':
        tare = data.get('accumulation_qr_tare')
        goods = data.get('accumulation_qr_goods') or data.get('accumulation_qr_text')
        if tare:  values['accumulation_qr_tare'] = tare
        if goods: values['accumulation_qr_goods'] = goods
        if goods: values['accumulation_qr_text'] = goods  # совместимость

    if process_name == 'cgp':
        tare = data.get('cgp_qr_tare')
        goods = data.get('cgp_qr_goods') or data.get('cgp_qr_text')
        if tare:  values['cgp_qr_tare'] = tare
        if goods: values['cgp_qr_goods'] = goods
        if goods: values['cgp_qr_text'] = goods  # совместимость

    forming_session_id = data.get('forming_session_id')
    last_bot_message_id = data.get('last_bot_message_id')
    chat_id = data.get('chat_id')

    success = await db_execute(
        """INSERT INTO control_data (user_id, stage_name, forming_session_id, value_numeric, data)
           VALUES ($1, $2, $3, $4, $5)""",
        user_id, process_name, forming_session_id, value_numeric, json.dumps(values, ensure_ascii=False)
    )
    if not success:
        await state.clear()
        if chat_id and last_bot_message_id:
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=last_bot_message_id, text="❌ Произошла ошибка при сохранении данных.", reply_markup=None)
            except Exception:
                pass
        await message.answer("Попробуйте снова.", reply_markup=main_menu_kb())
        return

    await clear_state_for_process(user_id, process_name)

    if process_name == "forming":
        success_text = f"✅ Данные для <b>Образца №{data['sample_number']}</b> сохранены."
        if chat_id and last_bot_message_id:
            try: await bot.edit_message_text(text=success_text, chat_id=chat_id, message_id=last_bot_message_id, reply_markup=None)
            except Exception: pass
        await state.set_state(Process.forming_confirm_next)
        sent_msg = await message.answer(
            "Что делаем дальше?",
            reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="➕ Добавить еще образец", callback_data="forming_add_another")],
                [types.InlineKeyboardButton(text="🏁 Завершить контроль рамы", callback_data="forming_finish")]
            ])
        )
        await state.update_data(last_bot_message_id=sent_msg.message_id, chat_id=sent_msg.chat.id)
        await save_state_to_db(user_id, state)
    else:
        stage_title = STAGE_TITLES.get(process_name, process_name)
        success_text = f"✅ Данные для этапа <b>'{stage_title}'</b> успешно сохранены."
        await state.clear()
        if chat_id and last_bot_message_id:
            try: await bot.edit_message_text(text=success_text, chat_id=chat_id, message_id=last_bot_message_id, reply_markup=None)
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
        await state.update_data(fsm_state=await state.get_state())
        await save_state_to_db(message.from_user.id, state)
        await message.answer("Открыто меню. Текущий прогресс сохранён.", reply_markup=ReplyKeyboardRemove())
    if await db_fetchval("SELECT 1 FROM users WHERE user_id = $1", message.from_user.id):
        full_name_db = await db_fetchval("SELECT full_name FROM users WHERE user_id = $1", message.from_user.id)
        display_name = full_name_db or message.from_user.full_name or message.from_user.first_name or "Пользователь"
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
        await message.answer("Укажите, пожалуйста, вашу <b>должность</b>:", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="Оператор/технолог", callback_data="position:Оператор/технолог")],
            [types.InlineKeyboardButton(text="Контролёр - технолог", callback_data="position:Контролёр - технолог")],
            [types.InlineKeyboardButton(text="Оператор", callback_data="position:Оператор")],
            [types.InlineKeyboardButton(text="Оператор - наладчик", callback_data="position:Оператор - наладчик")],
        ]))
    else:
        await message.answer("❌ Произошла ошибка при регистрации. Попробуйте снова.")

async def process_registration_position(message: Message, state: FSMContext):
    await message.answer("⚠️ Выберите должность кнопкой ниже.")

async def process_registration_position_cb(callback: CallbackQuery, state: FSMContext):
    position = callback.data.split(":", 1)[1][:255]
    user_id = callback.from_user.id
    ok = await db_execute("UPDATE users SET position = $1 WHERE user_id = $2", position, user_id)
    if ok:
        await state.clear()
        try: await bot.edit_message_text(text="✅ Спасибо! Данные регистрации сохранены.", chat_id=callback.message.chat.id, message_id=callback.message.message_id, reply_markup=None)
        except Exception: pass
        await callback.message.answer("Теперь вы можете выбрать этап контроля:", reply_markup=main_menu_kb())
    else:
        await callback.message.answer("❌ Не удалось сохранить должность. Попробуйте ещё раз.")
    await callback.answer()

def _cleanup_expired_tokens():
    now = datetime.now()
    for k, v in list(ACCUM_CONTINUE_TOKENS.items()):
        if (now - v.get('created_at', now)).total_seconds() > TOKEN_TTL_SECONDS:
            ACCUM_CONTINUE_TOKENS.pop(k, None)

async def process_cancel_callback(callback: CallbackQuery, state: FSMContext):
    current_fsm_state = await state.get_state()
    data = await state.get_data()
    if current_fsm_state == Process.waiting_for_param_photo.state and data.get('pending_photo_required'):
        param_key_to_revert = data.get('pending_photo_param_key')
        if param_key_to_revert:
            values = data.get('values', {})
            photos = data.get('photos', {})
            values.pop(param_key_to_revert, None)
            photos.pop(param_key_to_revert, None)
            await state.set_state(Process.param_menu)
            await state.update_data(values=values, photos=photos, pending_photo_required=False, pending_photo_param_key=None, fsm_state=Process.param_menu.state)
    if current_fsm_state == Process.waiting_for_param_comment.state and data.get('pending_comment_required'):
        param_key_to_revert = data.get('pending_comment_param_key')
        if param_key_to_revert:
            values = data.get('values', {})
            values.pop(param_key_to_revert, None)
            await state.set_state(Process.param_menu)
            await state.update_data(values=values, pending_comment_required=False, pending_comment_param_key=None, fsm_state=Process.param_menu.state)

    await state.update_data(fsm_state=await state.get_state())
    await save_state_to_db(callback.from_user.id, state)
    try: await bot.edit_message_text(text="🏠 Вы в главном меню.", chat_id=callback.message.chat.id, message_id=callback.message.message_id, reply_markup=None)
    except Exception: pass
    await callback.message.answer("Выберите этап контроля:", reply_markup=main_menu_kb())
    await callback.answer()

async def process_stage_selection(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    if not await ensure_user_registered(user.id, user.full_name):
        await callback.answer("❌ Ошибка базы данных.", show_alert=True); return
    stage_name = callback.data.split("_")[1]

    if await load_state_from_db(user.id, stage_name, state):
        data = await state.get_data()
        last_message_id = data.get('last_bot_message_id'); chat_id = data.get('chat_id')
        if last_message_id and chat_id:
            try: await bot.delete_message(chat_id, last_message_id)
            except: pass
        fsm_state = await state.get_state()
        if fsm_state == Process.waiting_for_param_photo.state:
            sent = await callback.message.answer("📷 Фото обязательно. Пришлите фото или нажмите '🏠 Главное меню'.", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")]]))
            await state.update_data(last_bot_message_id=sent.message_id, chat_id=sent.chat.id)
            await callback.answer("Возобновили незавершённый шаг (нужно фото)."); return
        if fsm_state == Process.waiting_for_param_comment.state:
            sent = await callback.message.answer("📝 Требуется комментарий. Введите текст или нажмите '🏠 Главное меню'.", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")]]))
            await state.update_data(last_bot_message_id=sent.message_id, chat_id=sent.chat.id)
            await callback.answer("Возобновили незавершённый шаг (нужен комментарий)."); return
        await state.set_state(Process.param_menu)
        await show_param_menu(callback.message, state)
        await callback.answer("↩️ Ваш прошлый сеанс восстановлен."); return

    _cleanup_expired_tokens()

    if stage_name == "forming":
        active_forming = await db_fetchall(
            """
            SELECT session_id, COALESCE(frame_qr_goods, frame_qr_text) AS code
            FROM forming_sessions
            WHERE user_id = $1 AND completed_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user.id
        )
        if active_forming:
            s = active_forming[0]
            keyboard_rows = [
                [types.InlineKeyboardButton(text=f"↩️ Продолжить раму {s['code'][:40]}", callback_data=f"forming_continue:{s['session_id']}")],
                [types.InlineKeyboardButton(text="➕ Добавить новую раму", callback_data="forming_new")],
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")]
            ]
            await callback.message.edit_text("<b>Этап 1: Формовка</b>\nВыберите действие:", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard_rows))
            await state.update_data(last_bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)
        else:
            await state.set_state(Process.waiting_for_qr)
            await state.update_data(process_name_after_qr="forming")
            await callback.message.edit_text("<b>Этап 1: Формовка</b>\nОтправьте фото с двумя QR-кодами (слева тара, справа товар).", reply_markup=cancel_kb())

    elif stage_name == "accumulation":
        last_row = await db_fetchall(
            """
            SELECT data
            FROM control_data
            WHERE user_id = $1 AND stage_name = 'accumulation'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user.id
        )
        if last_row:
            d = last_row[0]['data']
            goods = (d.get('accumulation_qr_goods') if isinstance(d, dict) else None) or await db_fetchval(
                """
                SELECT data->>'accumulation_qr_text'
                FROM control_data
                WHERE user_id = $1 AND stage_name='accumulation'
                ORDER BY created_at DESC LIMIT 1
                """, user.id)
            token = secrets.token_urlsafe(12)
            ACCUM_CONTINUE_TOKENS[token] = {'user_id': user.id, 'goods': str(goods) if goods else None, 'tare': d.get('accumulation_qr_tare') if isinstance(d, dict) else None, 'created_at': datetime.now()}
            keyboard_rows = [
                [types.InlineKeyboardButton(text=f"↩️ Продолжить раму {str(goods)[:40] if goods else ''}", callback_data=f"accum_continue:{token}")],
                [types.InlineKeyboardButton(text="➕ Добавить новую раму", callback_data="accum_new")],
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")]
            ]
            await callback.message.edit_text("<b>Этап 2: Зона накопления ГП</b>\nВыберите действие:", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard_rows))
            await state.update_data(last_bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)
        else:
            await state.set_state(Process.waiting_for_qr)
            await state.update_data(process_name_after_qr="accumulation")
            await callback.message.edit_text("<b>Этап 2: Зона накопления ГП</b>\nОтправьте фото с двумя QR-кодами (слева тара, справа товар).", reply_markup=cancel_kb())

    elif stage_name == "cgp":
        last_row = await db_fetchall(
            """
            SELECT data
            FROM control_data
            WHERE user_id = $1 AND stage_name = 'cgp'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user.id
        )
        goods = None; tare = None
        if last_row:
            d = last_row[0]['data']
            if isinstance(d, dict):
                tare = d.get('cgp_qr_tare')
                goods = d.get('cgp_qr_goods') or d.get('cgp_qr_text')
        if goods:
            token = secrets.token_urlsafe(12)
            ACCUM_CONTINUE_TOKENS[token] = {'user_id': user.id, 'goods': str(goods), 'tare': tare, 'created_at': datetime.now()}
            keyboard_rows = [
                [types.InlineKeyboardButton(text=f"↩️ Продолжить паллет {str(goods)[:40]}", callback_data=f"cgp_continue:{token}")],
                [types.InlineKeyboardButton(text="➕ Сканировать новый паллет", callback_data="cgp_new")],
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")]
            ]
            await callback.message.edit_text("<b>Этап 4: ЦГП</b>\nВыберите действие:", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keyboard_rows))
            await state.update_data(last_bot_message_id=callback.message.message_id, chat_id=callback.message.chat.id)
        else:
            await state.set_state(Process.waiting_for_qr)
            await state.update_data(process_name_after_qr="cgp")
            await callback.message.edit_text("<b>Этап 4: ЦГП</b>\nОтправьте фото ярлыка с двумя QR-кодами (слева тара, справа товар).", reply_markup=cancel_kb())

    else:
        await start_process(user.id, user.full_name, callback.message, state, stage_name)
    await callback.answer()

async def param_menu_done(callback: CallbackQuery, state: FSMContext):
    data_state = await state.get_data()
    last_id = data_state.get('last_bot_message_id')
    if last_id and callback.message.message_id != last_id: await callback.answer(); return
    if data_state.get('pending_photo_required'): await callback.answer("Сначала пришлите обязательное фото/QR", show_alert=True); return
    if data_state.get('pending_comment_required'): await callback.answer("Сначала введите обязательный комментарий", show_alert=True); return
    values = data_state.get('values') or {}
    if not values: await callback.answer("Заполните хотя бы один параметр", show_alert=True); return
    await finish_process(callback.message, state)
    await callback.answer()

async def accumulation_new_handler(callback: CallbackQuery, state: FSMContext):
    data_state = await state.get_data()
    last_id = data_state.get('last_bot_message_id')
    if last_id and callback.message.message_id != last_id: await callback.answer(); return
    await state.set_state(Process.waiting_for_qr)
    await state.update_data(process_name_after_qr="accumulation")
    await callback.message.edit_text("<b>Этап 2: Зона накопления ГП</b>\nОтправьте фото с двумя QR-кодами (слева тара, справа товар).", reply_markup=cancel_kb())
    await callback.answer()

async def accumulation_continue_handler(callback: CallbackQuery, state: FSMContext):
    data_state = await state.get_data()
    last_id = data_state.get('last_bot_message_id')
    if last_id and callback.message.message_id != last_id: await callback.answer(); return
    try:
        token = callback.data.split(":", 1)[1]
    except Exception:
        await callback.answer(); return
    tok_data = ACCUM_CONTINUE_TOKENS.get(token)
    if not tok_data or tok_data.get('user_id') != callback.from_user.id:
        await callback.answer("Не удалось найти раму.", show_alert=True); return
    if (datetime.now() - tok_data.get('created_at', datetime.now())).total_seconds() > TOKEN_TTL_SECONDS:
        ACCUM_CONTINUE_TOKENS.pop(token, None)
        await callback.answer("Ссылка устарела. Создайте новую через меню.", show_alert=True); return

    await state.update_data(
        user_id=callback.from_user.id, process_name="accumulation",
        accumulation_qr_text=tok_data.get('goods'),
        accumulation_qr_goods=tok_data.get('goods'),
        accumulation_qr_tare=tok_data.get('tare'),
        values={}, photos={}, step_index=0,
        pending_photo_required=False, pending_photo_param_key=None
    )
    await clear_state_for_process(callback.from_user.id, "accumulation")
    await state.set_state(Process.param_menu)
    await show_param_menu(callback.message, state)
    try: del ACCUM_CONTINUE_TOKENS[token]
    except Exception: pass
    await callback.answer()

async def forming_new_handler(callback: CallbackQuery, state: FSMContext):
    data_state = await state.get_data()
    last_id = data_state.get('last_bot_message_id')
    if last_id and callback.message.message_id != last_id: await callback.answer(); return
    await state.set_state(Process.waiting_for_qr)
    await state.update_data(process_name_after_qr="forming")
    await callback.message.edit_text("<b>Этап 1: Формовка</b>\nОтправьте фото с двумя QR-кодами (слева тара, справа товар).", reply_markup=cancel_kb())
    await callback.answer()

async def forming_continue_handler(callback: CallbackQuery, state: FSMContext):
    data_state = await state.get_data()
    last_id = data_state.get('last_bot_message_id')
    if last_id and callback.message.message_id != last_id: await callback.answer(); return
    try:
        parts = callback.data.split(":", 1)
        session_id = int(parts[1]) if len(parts) > 1 else None
    except Exception:
        session_id = None
    await state.update_data(
        user_id=callback.from_user.id, process_name="forming", forming_session_id=session_id,
        values={}, photos={}, step_index=0,
        pending_photo_required=False, pending_photo_param_key=None
    )
    await clear_state_for_process(callback.from_user.id, "forming")
    await state.set_state(Process.param_menu)
    await show_param_menu(callback.message, state)
    await callback.answer()

async def cgp_new_handler(callback: CallbackQuery, state: FSMContext):
    data_state = await state.get_data()
    last_id = data_state.get('last_bot_message_id')
    if last_id and callback.message.message_id != last_id: await callback.answer(); return
    await state.set_state(Process.waiting_for_qr)
    await state.update_data(process_name_after_qr="cgp")
    await callback.message.edit_text("<b>Этап 4: ЦГП</b>\nОтправьте фото ярлыка с двумя QR-кодами (слева тара, справа товар).", reply_markup=cancel_kb())
    await callback.answer()

async def cgp_continue_handler(callback: CallbackQuery, state: FSMContext):
    data_state = await state.get_data()
    last_id = data_state.get('last_bot_message_id')
    if last_id and callback.message.message_id != last_id: await callback.answer(); return
    try:
        token = callback.data.split(":", 1)[1]
    except Exception:
        await callback.answer(); return
    tok_data = ACCUM_CONTINUE_TOKENS.get(token)
    if not tok_data or tok_data.get('user_id') != callback.from_user.id:
        await callback.answer("Не удалось найти паллет.", show_alert=True); return
    if (datetime.now() - tok_data.get('created_at', datetime.now())).total_seconds() > TOKEN_TTL_SECONDS:
        ACCUM_CONTINUE_TOKENS.pop(token, None)
        await callback.answer("Ссылка устарела. Создайте новую через меню.", show_alert=True); return

    await state.update_data(
        user_id=callback.from_user.id, process_name="cgp",
        cgp_qr_text=tok_data.get('goods'),
        cgp_qr_goods=tok_data.get('goods'),
        cgp_qr_tare=tok_data.get('tare'),
        values={}, photos={}, step_index=0,
        pending_photo_required=False, pending_photo_param_key=None
    )
    await clear_state_for_process(callback.from_user.id, "cgp")
    await state.set_state(Process.param_menu)
    await show_param_menu(callback.message, state)
    try: del ACCUM_CONTINUE_TOKENS[token]
    except Exception: pass
    await callback.answer()

async def param_open_handler(callback: CallbackQuery, state: FSMContext):
    data_state = await state.get_data()
    last_id = data_state.get('last_bot_message_id')
    if last_id and callback.message.message_id != last_id: await callback.answer(); return
    try:
        _, process_name, key = callback.data.split(":", 2)
    except Exception:
        await callback.answer(); return
    data = await state.get_data()
    chain = PROCESS_CHAINS.get(process_name, [])
    try:
        idx = next(i for i, s in enumerate(chain) if s['key'] == key)
    except StopIteration:
        await callback.answer(); return
    if process_name == "forming":
        await state.update_data(sample_number=data.get('sample_number', 1))
    current_fsm_state = await state.get_state()
    await state.update_data(step_index=idx, fsm_state=current_fsm_state)
    await state.set_state(Process.in_progress)
    await save_state_to_db(callback.from_user.id, state)
    await ask_current_question(callback.message, state, edit_message=True)
    await callback.answer()

async def process_qr_code(message: Message, state: FSMContext):
    user = message.from_user
    if not await ensure_user_registered(user.id, user.full_name):
        await message.answer("❌ Ошибка базы данных."); return

    file = await bot.get_file(message.photo[-1].file_id)
    ext = file.file_path.split('.')[-1] if getattr(file, 'file_path', None) else 'jpg'
    local_file_path = os.path.join(QR_IMAGE_DIR, f"{uuid.uuid4().hex}.{ext}")
    download_ok = await download_telegram_file_by_file_id(message.photo[-1].file_id, local_file_path)
    if not download_ok:
        await message.answer("⚠️ Не удалось загрузить фото. Попробуйте ещё раз."); return

    results = await decode_multi_qr_from_image_async(local_file_path)
    if not results:
        await message.answer("⚠️ QR-коды не распознаны. Пришлите фото так, чтобы оба кода были видны."); return

    # Левый = тара, правый = товар. Если один — считаем его «товар».
    if len(results) == 1:
        tare_text = None
        goods_text = results[0]['text']
    else:
        tare_text = results[0]['text']
        goods_text = results[1]['text']

    lines = []
    if tare_text: lines.append(f"Тара: <code>{escape(tare_text)}</code>")
    lines.append(f"Товар: <code>{escape(goods_text)}</code>")
    await message.answer("✅ Распознано:\n" + "\n".join(lines))

    data = await state.get_data()
    process_name_after_qr = data.get('process_name_after_qr')

    if process_name_after_qr == "forming":
        # goods_text остаётся ключом рамы (frame_qr_text)
        session_id = await db_fetchval(
            """
            WITH ins AS (
                INSERT INTO forming_sessions (user_id, frame_qr_text, frame_qr_tg_file_id, frame_qr_image_path, frame_qr_tare, frame_qr_goods)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT DO NOTHING
                RETURNING session_id
            )
            SELECT session_id FROM ins
            UNION ALL
            SELECT session_id FROM forming_sessions
            WHERE frame_qr_text = $2 AND user_id = $1 AND completed_at IS NULL
            LIMIT 1;
            """,
            user.id, goods_text, file.file_id, local_file_path, tare_text, goods_text
        )
        if session_id:
            await state.update_data(
                user_id=user.id, process_name="forming", forming_session_id=session_id,
                step_index=0, sample_number=1, values={}, photos={},
                pending_photo_required=False, pending_photo_param_key=None
            )
            await state.set_state(Process.param_menu)
            await show_param_menu(message, state)
        else:
            owner_info = await db_fetchall(
                """
                SELECT fs.user_id, u.full_name
                FROM forming_sessions fs
                LEFT JOIN users u ON u.user_id = fs.user_id
                WHERE fs.frame_qr_text = $1 AND fs.completed_at IS NULL
                ORDER BY fs.created_at DESC
                LIMIT 1
                """,
                goods_text
            )
            if owner_info and owner_info[0]['user_id'] != user.id:
                owner_name = owner_info[0]['full_name'] or str(owner_info[0]['user_id'])
                await message.answer(f"⚠️ Эта рама уже в работе у: <b>{escape(owner_name)}</b>.")
            else:
                await message.answer("❌ Ошибка создания сессии.")
            await state.clear()
            await clear_state_for_process(user.id, "forming")

    elif process_name_after_qr == "accumulation":
        await state.set_state(Process.param_menu)
        await state.update_data(
            user_id=user.id, process_name="accumulation",
            accumulation_qr_tare=tare_text,
            accumulation_qr_goods=goods_text,
            accumulation_qr_text=goods_text,  # совместимость
            accumulation_qr_tg_file_id=file.file_id, accumulation_qr_image_path=local_file_path,
            values={}, photos={}, pending_photo_required=False, pending_photo_param_key=None
        )
        await clear_state_for_process(user.id, "accumulation")
        await show_param_menu(message, state)

    elif process_name_after_qr == "packaging":
        await start_process(user.id, user.full_name, message, state, "packaging")

    elif process_name_after_qr == "cgp":
        await state.set_state(Process.param_menu)
        await state.update_data(
            user_id=user.id, process_name="cgp",
            cgp_qr_tare=tare_text,
            cgp_qr_goods=goods_text,
            cgp_qr_text=goods_text,  # совместимость
            cgp_qr_tg_file_id=file.file_id, cgp_qr_image_path=local_file_path,
            values={}, photos={}, pending_photo_required=False, pending_photo_param_key=None
        )
        await clear_state_for_process(user.id, "cgp")
        await show_param_menu(message, state)

    await state.update_data(process_name_after_qr=None)

async def process_qr_invalid(message: Message):
    await message.answer("📷 Пожалуйста, отправьте фото с QR-кодами.")

async def handle_param_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    param_key = data.get('pending_photo_param_key')
    process_name = data.get('pending_photo_process_name')
    control_dir = data.get('control_dir') or datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file = await bot.get_file(message.photo[-1].file_id)
    dest_path = build_control_photo_path(control_dir, process_name, param_key, getattr(file, 'file_path', None))
    ok = await download_telegram_file_by_file_id(message.photo[-1].file_id, dest_path)
    if not ok:
        await message.answer("❌ Не удалось сохранить фото. Отправьте его ещё раз или нажмите '🏠 Главное меню'."); return
    photos = data.get('photos', {})
    photos.setdefault(param_key, []).append(dest_path)
    await state.update_data(photos=photos, pending_photo_required=False)
    await state.set_state(Process.param_menu)
    await save_state_to_db(message.from_user.id, state)
    await show_param_menu(message, state)

async def handle_param_photo_invalid(message: Message, state: FSMContext):
    data = await state.get_data()
    if data.get('pending_photo_required'):
        await message.answer("📷 Фото обязательно. Пришлите фото или нажмите '🏠 Главное меню'.")
    else:
        await message.answer("📷 Пожалуйста, отправьте фото или нажмите '🏠 Главное меню'.")

async def handle_param_comment(message: Message, state: FSMContext):
    if not message.text:
        await message.reply("⚠️ Пожалуйста, введите текстовый комментарий."); return
    data = await state.get_data()
    param_key = data.get('pending_comment_param_key')
    comment_text = message.text.strip()
    comment_key = f"{param_key}_comment"
    values = data.get('values', {})
    values[comment_key] = comment_text
    await state.update_data(values=values, pending_comment_required=False)
    await state.set_state(Process.param_menu)
    await save_state_to_db(message.from_user.id, state)
    await show_param_menu(message, state)

async def handle_param_comment_invalid(message: Message, state: FSMContext):
    await message.answer("Пожалуйста, введите текстовый комментарий или нажмите '🏠 Главное меню'.")

async def process_step_answer(message: Message, state: FSMContext):
    if not message.text:
        await message.reply("⚠️ Пожалуйста, введите текстовое значение."); return
    data = await state.get_data()
    process_name = data.get('process_name')
    if not process_name:
        await message.answer("❌ Произошла ошибка.", reply_markup=main_menu_kb())
        await state.clear()
        if process_name: await clear_state_for_process(message.from_user.id, process_name)
        return
    chain, step_index = PROCESS_CHAINS.get(process_name), data.get('step_index', 0)
    if not chain or not (0 <= step_index < len(chain)):
        await message.answer("❌ Произошла ошибка.", reply_markup=main_menu_kb())
        await state.clear()
        if process_name: await clear_state_for_process(message.from_user.id, process_name)
        return
    current_step, user_input, value_to_save = chain[step_index], message.text.strip(), message.text.strip()
    if current_step['type'] == 'choice':
        await message.reply("Пожалуйста, используйте кнопки ниже для выбора значения."); return
    if current_step['type'] == 'float':
        try: value_to_save = float(user_input.replace(',', '.'))
        except Exception:
            await message.reply("⚠️ Неверный формат. Введите число."); return
    is_valid, error_msg = await validate_input(value_to_save, current_step)
    if not is_valid:
        await message.reply(f"⚠️ {error_msg}"); return
    current_fsm_state = await state.get_state()
    current_values = data.get('values', {})
    current_values[current_step['key']] = value_to_save
    if current_step.get('require_photo_always'):
        try:
            last_id = data.get('last_bot_message_id'); chat_id = data.get('chat_id')
            if last_id and chat_id:
                await bot.edit_message_reply_markup(chat_id=chat_id, message_id=last_id, reply_markup=None)
        except Exception: pass
        await state.update_data(values=current_values, fsm_state=Process.waiting_for_param_photo.state, pending_photo_param_key=current_step['key'], pending_photo_process_name=process_name, pending_photo_required=True)
        await state.set_state(Process.waiting_for_param_photo)
        await save_state_to_db(message.from_user.id, state)
        sent = await message.answer("📷 Фото обязательно. Пришлите фото или нажмите '🏠 Главное меню'.", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")]]))
        await state.update_data(last_bot_message_id=sent.message_id, chat_id=sent.chat.id)
        return
    await state.update_data(values=current_values, fsm_state=current_fsm_state)
    await save_state_to_db(message.from_user.id, state)
    await state.set_state(Process.param_menu)
    await show_param_menu(message, state)

async def process_choice_answer(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    last_id = data.get('last_bot_message_id')
    if last_id and callback.message.message_id != last_id: await callback.answer(); return
    process_name, step_index = data.get('process_name'), data.get('step_index', 0)
    chain = PROCESS_CHAINS.get(process_name)
    if not chain or step_index >= len(chain) or chain[step_index]['type'] != 'choice':
        await callback.answer(); return
    current_fsm_state = await state.get_state()
    value_to_save = callback.data.split(':')[-1]
    step_key = chain[step_index]['key']
    current_values = data.get('values', {})
    current_values[step_key] = value_to_save
    need_photo = (chain[step_index].get('require_photo_always') or (chain[step_index].get('photo_on_defect') and is_choice_defect(step_key, value_to_save)))
    if need_photo:
        try:
            last_id = data.get('last_bot_message_id'); chat_id = data.get('chat_id')
            if last_id and chat_id:
                await bot.edit_message_reply_markup(chat_id=chat_id, message_id=last_id, reply_markup=None)
        except Exception: pass
        await state.update_data(values=current_values, fsm_state=Process.waiting_for_param_photo.state, pending_photo_param_key=step_key, pending_photo_process_name=process_name, pending_photo_required=True)
        await state.set_state(Process.waiting_for_param_photo)
        await save_state_to_db(callback.from_user.id, state)
        sent = await callback.message.answer("📷 Фото обязательно. Пришлите фото или нажмите '🏠 Главное меню'.", reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")]]))
        await state.update_data(last_bot_message_id=sent.message_id, chat_id=sent.chat.id)
        await callback.answer(); return
    need_comment = chain[step_index].get('comment_on_defect') and is_choice_defect(step_key, value_to_save)
    if need_comment:
        try:
            last_id = data.get('last_bot_message_id'); chat_id = data.get('chat_id')
            if last_id and chat_id:
                await bot.edit_message_reply_markup(chat_id=chat_id, message_id=last_id, reply_markup=None)
        except Exception: pass
        await state.update_data(values=current_values, fsm_state=Process.waiting_for_param_comment.state, pending_comment_param_key=step_key, pending_comment_process_name=process_name, pending_comment_required=True)
        await state.set_state(Process.waiting_for_param_comment)
        await save_state_to_db(callback.from_user.id, state)
        comment_prompt = chain[step_index].get('comment_prompt', "Пожалуйста, оставьте комментарий:")
        sent = await callback.message.answer(comment_prompt, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="🏠 Главное меню", callback_data="cancel_action")]]))
        await state.update_data(last_bot_message_id=sent.message_id, chat_id=sent.chat.id)
        await callback.answer(); return
    await state.update_data(values=current_values, pending_photo_required=False, fsm_state=current_fsm_state)
    await save_state_to_db(callback.from_user.id, state)
    await state.set_state(Process.param_menu)
    await show_param_menu(callback.message, state)
    await callback.answer()

async def process_navigation(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    step_index = data.get('step_index', 0)
    current_state = await state.get_state()
    last_id = data.get('last_bot_message_id')
    if last_id and callback.message.message_id != last_id: await callback.answer(); return
    if current_state == Process.waiting_for_param_photo.state and data.get('pending_photo_required'):
        await callback.answer("Сначала пришлите обязательное фото/QR", show_alert=True); return
    if current_state == Process.waiting_for_param_comment.state and data.get('pending_comment_required'):
        await callback.answer("Сначала введите обязательный комментарий", show_alert=True); return
    if callback.data == "process_back":
        process_name = data.get('process_name')
        await state.set_state(Process.param_menu)
        await save_state_to_db(callback.from_user.id, state)
        await show_param_menu(callback.message, state)
    elif callback.data == "process_skip":
        process_name = data.get('process_name')
        chain = PROCESS_CHAINS.get(process_name)
        if not chain or not (0 <= step_index < len(chain)): await callback.answer(); return
        await save_state_to_db(callback.from_user.id, state)
        await state.set_state(Process.param_menu)
        await show_param_menu(callback.message, state)
    await callback.answer()

async def forming_confirm_handler(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if callback.data == "forming_add_another":
        new_sample_number = data.get('sample_number', 1) + 1
        await state.update_data(step_index=0, sample_number=new_sample_number, values={}, photos={})
        await state.set_state(Process.param_menu)
        await show_param_menu(callback.message, state)
    elif callback.data == "forming_finish":
        session_id = data.get('forming_session_id')
        if session_id:
            await db_execute("UPDATE forming_sessions SET completed_at = NOW() WHERE session_id = $1", session_id)
        await state.clear()
        if data.get("process_name"):
            await clear_state_for_process(callback.from_user.id, data["process_name"])
        await bot.edit_message_text(text="✅ Контроль рамы завершен.", chat_id=callback.message.chat.id, message_id=callback.message.message_id, reply_markup=None)
        await callback.message.answer("Выберите этап контроля:", reply_markup=main_menu_kb())
    await callback.answer()

async def handle_unexpected_message(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("Используйте команду /start для начала работы.")
    else:
        await message.answer("⚠️ Пожалуйста, используйте кнопки или введите корректные данные.")

# =====================================================
# УСТАРЕВШИЕ КНОПКИ
# =====================================================

async def handle_expired_button(callback: CallbackQuery):
    try: await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logger.warning(f"Не удалось убрать клавиатуру: {e}")
    await callback.answer("Эта кнопка или сессия больше не активны.\n\nНачните заново из главного меню.", show_alert=True)

async def expired_param_open(callback: CallbackQuery, state: FSMContext): await handle_expired_button(callback)
async def expired_param_done(callback: CallbackQuery, state: FSMContext): await handle_expired_button(callback)
async def expired_choice_answer(callback: CallbackQuery, state: FSMContext): await handle_expired_button(callback)
async def expired_process_navigation(callback: CallbackQuery, state: FSMContext): await handle_expired_button(callback)

# =====================================================
# ЗАПУСК
# =====================================================

async def on_startup(bot: Bot):
    os.makedirs(QR_IMAGE_DIR, exist_ok=True)
    os.makedirs(CONTROL_PHOTO_DIR, exist_ok=True)
    if not await create_db_pool():
        await bot.session.close()
        raise SystemExit(1)
    await bot.set_my_commands([BotCommand(command="/start", description="🏠 Главное меню")])

async def on_shutdown(bot: Bot):
    if db_pool: await db_pool.close()
    await bot.session.close()

def register_handlers(dp: Dispatcher):
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(process_registration, Registration.waiting_for_name)
    dp.message.register(process_registration_position, Registration.waiting_for_position)
    dp.callback_query.register(process_registration_position_cb, Registration.waiting_for_position, F.data.startswith("position:"))

    dp.callback_query.register(process_cancel_callback, F.data == "cancel_action")
    dp.callback_query.register(process_stage_selection, F.data.startswith("stage_"))

    dp.callback_query.register(param_menu_done, Process.param_menu, F.data == "param_done")
    dp.callback_query.register(param_open_handler, Process.param_menu, F.data.startswith("param_open:"))

    dp.callback_query.register(accumulation_new_handler, F.data == "accum_new")
    dp.callback_query.register(accumulation_continue_handler, F.data.startswith("accum_continue:"))
    dp.callback_query.register(forming_new_handler, F.data == "forming_new")
    dp.callback_query.register(forming_continue_handler, F.data.startswith("forming_continue"))
    dp.callback_query.register(cgp_new_handler, F.data == "cgp_new")
    dp.callback_query.register(cgp_continue_handler, F.data.startswith("cgp_continue"))
    dp.callback_query.register(forming_confirm_handler, Process.forming_confirm_next)

    dp.callback_query.register(process_choice_answer, Process.in_progress, F.data.regexp(r'^(forming|accumulation|packaging|cgp)_\d+:.+'))
    dp.callback_query.register(process_navigation, Process.in_progress, F.data.in_({"process_back", "process_skip"}))
    dp.message.register(process_step_answer, Process.in_progress)

    dp.message.register(process_qr_code, Process.waiting_for_qr, F.photo)
    dp.message.register(process_qr_invalid, Process.waiting_for_qr)
    dp.message.register(handle_param_photo, Process.waiting_for_param_photo, F.photo)
    dp.message.register(handle_param_photo_invalid, Process.waiting_for_param_photo)
    dp.message.register(handle_param_comment, Process.waiting_for_param_comment, F.text)
    dp.message.register(handle_param_comment_invalid, Process.waiting_for_param_comment)

    dp.callback_query.register(expired_param_open, F.data.startswith("param_open:"))
    dp.callback_query.register(expired_param_done, F.data == "param_done")
    dp.callback_query.register(expired_choice_answer, F.data.regexp(r'^(forming|accumulation|packaging|cgp)_\d+:.+'))
    dp.callback_query.register(expired_process_navigation, F.data.in_({"process_back", "process_skip"}))

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
        pass
