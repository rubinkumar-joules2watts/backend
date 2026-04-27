from __future__ import annotations

from functools import lru_cache

from fastapi import Request

from .azure_openai_service import AzureOpenAIService
from .gemini_service import GeminiService
from .groq_service import GroqService


@lru_cache(maxsize=1)
def _service_singleton(api_key: str, model: str) -> GeminiService:
    # The key/model are part of the cache key; changing env vars requires process restart.
    return GeminiService(api_key=api_key, model=model)


def get_gemini_service(request: Request) -> GeminiService:
    """FastAPI dependency helper.

    Usage:
        from fastapi import Depends
        from app.ai.dependencies import get_gemini_service

        @router.post("/...")
        async def endpoint(gemini: GeminiService = Depends(get_gemini_service)):
            ...
    """

    settings = request.app.state.settings
    return _service_singleton(settings.gemini_api_key, settings.gemini_model)


@lru_cache(maxsize=1)
def _groq_singleton(api_key: str, model: str) -> GroqService:
    return GroqService(api_key=api_key, model=model)


def get_groq_service(request: Request) -> GroqService:
    """FastAPI dependency helper for Groq."""
    settings = request.app.state.settings
    return _groq_singleton(settings.groq_api_key, settings.groq_model)


@lru_cache(maxsize=1)
def _gpt4omini_singleton(
    api_key: str,
    endpoint: str,
    api_version: str,
    deployment_name: str,
) -> AzureOpenAIService:
    return AzureOpenAIService(
        api_key=api_key,
        endpoint=endpoint,
        api_version=api_version,
        deployment_name=deployment_name,
    )


def get_gpt4omini_service(request: Request) -> AzureOpenAIService:
    """FastAPI dependency helper for Azure OpenAI GPT-4o-mini deployment."""

    settings = request.app.state.settings
    return _gpt4omini_singleton(
        settings.gpt4omini_api_key,
        settings.gpt4omini_endpoint,
        settings.gpt4omini_api_version,
        settings.gpt4omini_deployment_name,
    )


def get_ai_service() -> GeminiService:
    """Helper to get the AI service outside of a FastAPI request context.

    Priority:
    1) Groq (when `GROQ_API_KEY` is set)
    2) Gemini
    """
    from ..config import load_settings
    settings = load_settings()
    if settings.groq_api_key:
        return _groq_singleton(settings.groq_api_key, settings.groq_model)
    return _service_singleton(settings.gemini_api_key, settings.gemini_model)
