from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from .errors import AIConfigurationError, AIServiceError


@dataclass(frozen=True)
class GroqGenerateConfig:
    temperature: float | None = 0.2
    max_output_tokens: int | None = 1024


class GroqService:
    """Groq text generation via OpenAI-compatible Chat Completions API.

    Docs: https://console.groq.com/docs
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "llama-3.1-8b-instant",
        api_base: str = "https://api.groq.com/openai/v1",
        timeout_s: float = 60.0,
    ) -> None:
        api_key = (api_key or "").strip()
        if not api_key:
            raise AIConfigurationError(
                "Groq API key is missing. Set GROQ_API_KEY in backend/.env or environment."
            )
        self._api_key = api_key
        self._model = (model or "").strip() or "llama-3.1-8b-instant"
        self._api_base = api_base.rstrip("/")
        self._timeout = httpx.Timeout(timeout_s)

    @property
    def model(self) -> str:
        return self._model

    def _url(self) -> str:
        return f"{self._api_base}/chat/completions"

    async def generate_text(
        self,
        *,
        prompt: str,
        system_instruction: str | None = None,
        config: Any | None = None,
    ) -> str:
        prompt = (prompt or "").strip()
        if not prompt:
            raise ValueError("prompt is required")

        # Accept GeminiGenerateConfig-style objects too (used elsewhere in the codebase).
        temperature = getattr(config, "temperature", None)
        max_tokens = getattr(config, "max_output_tokens", None)

        payload: dict[str, Any] = {
            "model": self._model,
            "messages": [],
        }
        if system_instruction:
            payload["messages"].append({"role": "system", "content": system_instruction})
        payload["messages"].append({"role": "user", "content": prompt})

        if temperature is not None:
            payload["temperature"] = temperature
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens

        headers = {"Authorization": f"Bearer {self._api_key}"}

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                response = await client.post(self._url(), headers=headers, json=payload)
            except httpx.HTTPError as exc:
                raise AIServiceError(f"Groq request failed: {exc}") from exc

        if response.status_code >= 400:
            raise AIServiceError(f"Groq error {response.status_code}: {response.text[:500]}")

        data = response.json()
        try:
            choices = data.get("choices") or []
            if not choices:
                return ""
            message = choices[0].get("message") or {}
            content = message.get("content") or ""
            return str(content).strip()
        except Exception as exc:  # noqa: BLE001
            raise AIServiceError(f"Unexpected Groq response format: {data}") from exc

