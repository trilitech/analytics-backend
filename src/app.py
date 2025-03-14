from fastapi import FastAPI
from database import init_db_pool
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from routers import stats

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db_pool()
    yield
    pass

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://analytics.tzpro.io"],
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(stats.stats_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)