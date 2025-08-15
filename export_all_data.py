import os
import json
import csv
import asyncio
from typing import Any, Dict, List, Set

from dotenv import load_dotenv
import asyncpg

# Загружаем переменные окружения из secrets.env и проверяем успех
if not load_dotenv("secrets.env"):
    raise FileNotFoundError("Файл secrets.env не найден")

required_vars = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "DB_NAME")
for var in required_vars:
    if var not in os.environ:
        raise EnvironmentError(f"Environment variable {var} is not set")

DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ["DB_PORT"])
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_NAME = os.environ["DB_NAME"]

OUTPUT_FILE = "all_data.csv"

QR_COLUMNS = [
    "frame_qr_goods",
    "frame_qr_tare",
    "accumulation_qr_goods",
    "accumulation_qr_tare",
    "packaging_qr_goods",
    "packaging_qr_tare",
    "cgp_qr_goods",
    "cgp_qr_tare",
]

# Русские названия этапов и параметров (скопировано из основного бота)
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
        "organoleptics": "Органолептика",
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

# Переводы значений параметров
PARAM_VALUE_TITLES: Dict[str, Dict[str, str]] = {
    # Формовка
    "mince_contamination_visual": {"norm": "Норма", "defect": "Дефект"},
    "hanging_quality_visual": {"norm": "Норма", "defect": "Дефект"},
    # Зона накопления ГП
    "contamination_visual": {"norm": "Норма", "defect": "Дефект"},
    "wrinkling_visual": {
        "absent": "Отсутствует",
        "minor": "Незначительная",
        "major": "Сильная",
    },
    "smoking_color_calorimeter": {"norm": "Норма", "defect": "Дефект"},
    "structure_visual": {"norm": "Норма", "defect": "Дефект"},
    "porosity_visual": {"norm": "Норма", "defect": "Дефект"},
    "slips_visual": {"norm": "Норма", "defect": "Дефект"},
    "print_defects_visual": {"absent": "Соответствует", "present": "Не соответствует"},
    "shell_adhesion_physical": {"norm": "Норма", "defect": "Дефект"},
    "organoleptics": {"norm": "Норма", "defect": "Дефект"},
    # Упаковка
    "gas_mixture_ratio": {"norm": "Норма", "defect": "Дефект"},
    "package_integrity": {"no": "Нет нарушений", "yes": "Есть нарушения"},
    # ЦГП
    "cgp_inserts_visual": {"ok": "Соответствует", "not_ok": "Не соответствует"},
}

COLUMN_TITLES_RU: Dict[str, str] = {
    "created_at": "Дата создания",
    "user_id": "ID пользователя",
    "full_name": "ФИО",
    "position": "Должность",
    "stage_name": "Этап",
    "forming_session_id": "ID сессии формовки",
    "value_numeric": "Числовое значение",
    "frame_qr_goods": "QR рамы (товар)",
    "frame_qr_tare": "QR рамы (тара)",
    "accumulation_qr_goods": "QR зоны накопления (товар)",
    "accumulation_qr_tare": "QR зоны накопления (тара)",
    "packaging_qr_goods": "QR упаковки (товар)",
    "packaging_qr_tare": "QR упаковки (тара)",
    "cgp_qr_goods": "QR ЦГП (товар)",
    "cgp_qr_tare": "QR ЦГП (тара)",
}

# Добавляем переводы этапов и параметров
COLUMN_TITLES_RU.update(STAGE_TITLES)
for params in PARAM_TITLES.values():
    COLUMN_TITLES_RU.update(params)

async def fetch_records(pool: asyncpg.Pool) -> List[asyncpg.Record]:
    query = """
        SELECT
            cd.user_id,
            u.full_name,
            u.position,
            cd.stage_name,
            cd.forming_session_id,
            cd.value_numeric,
            cd.data,
            cd.created_at
        FROM control_data cd
        JOIN users u ON u.user_id = cd.user_id
        ORDER BY cd.created_at
    """
    async with pool.acquire() as conn:
        return await conn.fetch(query)

async def export_all_data(filename: str = OUTPUT_FILE) -> None:
    try:
        pool = await asyncpg.create_pool(
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            host=DB_HOST,
            port=DB_PORT,
        )
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return

    try:
        records = await fetch_records(pool)
    finally:
        await pool.close()

    data_keys: Set[str] = set()
    rows: List[Dict[str, Any]] = []
    for rec in records:
        data = rec["data"]
        if not isinstance(data, dict):
            data = json.loads(data)
        data_keys.update(data.keys())
        row = {
            "created_at": rec["created_at"],
            "user_id": rec["user_id"],
            "full_name": rec["full_name"],
            "position": rec["position"],
            "stage_name": STAGE_TITLES.get(rec["stage_name"], rec["stage_name"]),
            "forming_session_id": rec["forming_session_id"],
            "value_numeric": rec["value_numeric"],
        }
        row.update(
            {

                k: (
                    PARAM_VALUE_TITLES.get(k, {}).get(v, v)
                    if not isinstance(v, (dict, list))
                    else json.dumps(v, ensure_ascii=False)
                )
=======
                k: PARAM_VALUE_TITLES.get(k, {}).get(v, v)

                for k, v in data.items()
            }
        )
        rows.append(row)

    base_columns = [
        "created_at",
        "user_id",
        "full_name",
        "position",
        "stage_name",
        "forming_session_id",
        "value_numeric",
    ]

    extra_columns = QR_COLUMNS + sorted(k for k in data_keys if k not in QR_COLUMNS)
    fieldnames = base_columns + [c for c in extra_columns if c not in base_columns]

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        # Записываем русские заголовки колонок
        writer.writerow({k: COLUMN_TITLES_RU.get(k, k) for k in fieldnames})
        for row in rows:
            writer.writerow(row)

if __name__ == "__main__":
    asyncio.run(export_all_data())
