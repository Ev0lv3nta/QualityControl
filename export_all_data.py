import os
import json
import csv
import asyncio
from typing import Any, Dict, List, Set

import asyncpg

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "qualitycontrol")

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
            u.name,
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
    pool = await asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT,
    )
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
            "name": rec["name"],
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
        "name",
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
