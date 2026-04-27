from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import dotenv_values, load_dotenv


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
    "project_summaries",
)


@dataclass(frozen=True)
class Settings:
    mongo_uri: str
    mongo_db_name: str
    api_port: int
    cors_origins: tuple[str, ...]
    gemini_api_key: str
    gemini_model: str
    groq_api_key: str
    groq_model: str
    gpt4omini_api_key: str
    gpt4omini_endpoint: str
    gpt4omini_api_version: str
    gpt4omini_deployment_name: str
    azure_storage_account_name: str
    azure_storage_account_key: str
    azure_storage_container_name: str


def load_settings() -> Settings:
    env_path = BASE_DIR / ".env"
    # Load environment variables from .env without overriding non-empty OS env vars.
    # If a variable exists but is set to an empty string in the OS env, we still want
    # to fall back to the value from .env (common in local shells).
    load_dotenv(env_path)
    dotenv_map = {k: (v or "").strip() for k, v in dotenv_values(env_path).items() if k}

    def env_value(*names: str) -> str:
        for name in names:
            val = (os.getenv(name) or "").strip()
            if val:
                return val
        for name in names:
            val = (dotenv_map.get(name) or "").strip()
            if val:
                return val
        return ""

    mongo_uri = env_value("MONGODB_URI", "MONGO_URI", "MongoDB_URI")
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

    gemini_api_key = env_value("GEMINI_API_KEY", "GOOGLE_API_KEY", "GEMINI_API_TOKEN")

    gemini_model = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash").strip() or "gemini-2.5-flash"

    # Some setups may refer to Groq as "Grok" in env naming; accept both.
    groq_api_key = env_value("GROQ_API_KEY", "GROK_API_KEY")
    groq_model = (
        env_value("GROQ_MODEL", "GROK_MODEL") or "llama-3.1-8b-instant"
    ).strip() or "llama-3.1-8b-instant"

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
        groq_api_key=groq_api_key,
        groq_model=groq_model,
        gpt4omini_api_key=gpt4omini_api_key,
        gpt4omini_endpoint=gpt4omini_endpoint,
        gpt4omini_api_version=gpt4omini_api_version,
        gpt4omini_deployment_name=gpt4omini_deployment_name,
        azure_storage_account_name=azure_storage_account_name,
        azure_storage_account_key=azure_storage_account_key,
        azure_storage_container_name=azure_storage_container_name,
    )
