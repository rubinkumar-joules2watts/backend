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
    # Load settings at runtime when environment variables are available
    settings = load_settings()
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    database = get_database()
    ensure_indexes(database)
    app.state.database = database
    app.state.settings = settings
    yield
    close_database()


# Create app without loading settings at import time
app = FastAPI(title="Delivery Tracker API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for now, will be updated with proper settings
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router, prefix="/api")
app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")


def run() -> None:
    import uvicorn

    settings = load_settings()
    uvicorn.run("app.main:app", host="0.0.0.0", port=settings.api_port, reload=True)