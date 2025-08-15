import asyncio
import csv
import json
import os
import asyncpg

DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

async def export_csv(path: str = "control_data_export.csv") -> None:
    pool = await asyncpg.create_pool(user=DB_USER, password=DB_PASS, database=DB_NAME, host=DB_HOST, port=DB_PORT)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT cd.created_at,
                       fs.frame_qr_text,
                       fs.frame_qr_tare,
                       cd.data
                FROM control_data cd
                LEFT JOIN forming_sessions fs ON fs.session_id = cd.forming_session_id
                ORDER BY cd.created_at
                """,
            )
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow(["created_at", "QR рамы (товар)", "QR рамы (тара)", "data"])
            for r in rows:
                writer.writerow([
                    r["created_at"],
                    r["frame_qr_text"],
                    r["frame_qr_tare"],
                    json.dumps(r["data"], ensure_ascii=False),
                ])
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(export_csv())
