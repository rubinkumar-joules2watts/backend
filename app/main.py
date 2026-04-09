from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .config import UPLOADS_DIR, load_settings
from .db import close_database, ensure_indexes, get_database
from .router import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    database = get_database()
    ensure_indexes(database)
    app.state.database = database
    yield
    close_database()


settings = load_settings()

UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Delivery Tracker API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.cors_origins),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router, prefix="/api")
app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")


def run() -> None:
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=settings.api_port, reload=True)