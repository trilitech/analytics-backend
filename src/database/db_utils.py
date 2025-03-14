import os
import asyncpg

db_pool = None

async def init_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        min_size=1,
        max_size=10
    )

async def close_db_pool():
    global db_pool
    if db_pool:
        await db_pool.close()
        db_pool = None

async def get_db_conn():
    async with db_pool.acquire() as conn:
        yield conn
