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
    gemini_api_key: str
    gemini_model: str
    gpt4omini_api_key: str
    gpt4omini_endpoint: str
    gpt4omini_api_version: str
    gpt4omini_deployment_name: str
    azure_storage_account_name: str
    azure_storage_account_key: str
    azure_storage_container_name: str


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

    gemini_api_key = (
        os.getenv("GEMINI_API_KEY")
        or os.getenv("GOOGLE_API_KEY")
        or os.getenv("GEMINI_API_TOKEN")
        or ""
    ).strip()

    gemini_model = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip() or "gemini-2.5-flash"

    gpt4omini_api_key = (os.getenv("GPT4OMINI_API_KEY") or "").strip()
    gpt4omini_endpoint = (os.getenv("GPT4OMINI_ENDPOINT") or "").strip()
    gpt4omini_api_version = (os.getenv("GPT4OMINI_API_VERSION") or "2024-12-01-preview").strip() or "2024-12-01-preview"
    gpt4omini_deployment_name = (os.getenv("GPT4OMINI_DEPLOYMENT_NAME") or "gpt-4o-mini").strip() or "gpt-4o-mini"

    azure_storage_account_name = (os.getenv("AZURE_STORAGE_ACCOUNT_NAME") or "").strip()
    azure_storage_account_key = (os.getenv("AZURE_STORAGE_ACCOUNT_KEY") or "").strip()
    azure_storage_container_name = (os.getenv("AZURE_STORAGE_CONTAINER_NAME") or "").strip()

    return Settings(
        mongo_uri=mongo_uri,
        mongo_db_name=db_name,
        api_port=port,
        cors_origins=origins,
        gemini_api_key=gemini_api_key,
        gemini_model=gemini_model,
        gpt4omini_api_key=gpt4omini_api_key,
        gpt4omini_endpoint=gpt4omini_endpoint,
        gpt4omini_api_version=gpt4omini_api_version,
        gpt4omini_deployment_name=gpt4omini_deployment_name,
        azure_storage_account_name=azure_storage_account_name,
        azure_storage_account_key=azure_storage_account_key,
        azure_storage_container_name=azure_storage_container_name,
    )