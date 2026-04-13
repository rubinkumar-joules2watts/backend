from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
UPLOADS_DIR = BASE_DIR / "uploads"
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

COLLECTIONS = (
    "clients",
    "projects",
    "milestones",
    "team_members",
    "project_assignments",
    "audit_log",
    "project_updates",
    "project_documents",
    "team_members_engagement",
)


@dataclass(frozen=True)
class Settings:
    mongo_uri: str
    mongo_db_name: str
    api_port: int
    cors_origins: tuple[str, ...]


def load_settings() -> Settings:
    load_dotenv(BASE_DIR / ".env")

    mongo_uri = (
        os.getenv("MONGODB_URI")
        or os.getenv("MONGO_URI")
        or os.getenv("MongoDB_URI")
        or ""
    ).strip()
    # For Vercel deployment, provide a fallback if MONGODB_URI is not set
    if not mongo_uri:
        # This will be overridden by environment variables in production
        mongo_uri = "mongodb://localhost:27017"  # Fallback for local development

    db_name = os.getenv("MONGODB_DB_NAME", "delivery_tracker").strip() or "delivery_tracker"
    port = int(os.getenv("API_PORT") or os.getenv("PORT") or "8000")

    origins_raw = os.getenv(
        "CORS_ORIGINS",
        "http://localhost:8080,http://127.0.0.1:8080,http://localhost:5173,http://127.0.0.1:5173, https://j2w-flow-insight-frontend.vercel.app/",
    )
    origins = tuple(origin.strip() for origin in origins_raw.split(",") if origin.strip()) or ("*",)

    return Settings(
        mongo_uri=mongo_uri,
        mongo_db_name=db_name,
        api_port=port,
        cors_origins=origins,
    )