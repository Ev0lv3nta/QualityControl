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
            "stage_name": rec["stage_name"],
            "forming_session_id": rec["forming_session_id"],
            "value_numeric": rec["value_numeric"],
        }
        row.update(data)
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
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

if __name__ == "__main__":
    asyncio.run(export_all_data())
