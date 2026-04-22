from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from .errors import AIConfigurationError, AIServiceError


@dataclass(frozen=True)
class GeminiGenerateConfig:
    temperature: float | None = 0.2
    top_p: float | None = None
    top_k: int | None = None
    max_output_tokens: int | None = 1024
    response_mime_type: str | None = None


class GeminiService:
    """Gemini text generation via Generative Language API (REST).

    Notes:
    - Reads the API key from config passed to the constructor.
    - Default model is expected to be something like: "gemini-2.5-flash".
    - Uses the v1beta generateContent endpoint.
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "gemini-2.5-flash",
        api_base: str = "https://generativelanguage.googleapis.com",
        timeout_s: float = 60.0,
    ) -> None:
        api_key = (api_key or "").strip()
        if not api_key:
            raise AIConfigurationError(
                "Gemini API key is missing. Set GEMINI_API_KEY or GOOGLE_API_KEY in backend/.env or environment."
            )

        self._api_key = api_key
        self._model = (model or "").strip() or "gemini-2.5-flash"
        self._api_base = api_base.rstrip("/")
        self._timeout = httpx.Timeout(timeout_s)

    @property
    def model(self) -> str:
        return self._model

    def _url(self) -> str:
        # Endpoint expects `models/{model}:generateContent`
        return f"{self._api_base}/v1beta/models/{self._model}:generateContent"

    def _build_payload(
        self,
        *,
        prompt: str,
        system_instruction: str | None,
        config: GeminiGenerateConfig,
    ) -> dict[str, Any]:
        contents: list[dict[str, Any]] = [
            {"role": "user", "parts": [{"text": prompt}]},
        ]

        payload: dict[str, Any] = {"contents": contents}

        if system_instruction:
            payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}

        generation_config: dict[str, Any] = {}
        if config.temperature is not None:
            generation_config["temperature"] = config.temperature
        if config.top_p is not None:
            generation_config["topP"] = config.top_p
        if config.top_k is not None:
            generation_config["topK"] = config.top_k
        if config.max_output_tokens is not None:
            generation_config["maxOutputTokens"] = config.max_output_tokens
        if config.response_mime_type is not None:
            generation_config["responseMimeType"] = config.response_mime_type

        if generation_config:
            payload["generationConfig"] = generation_config

        return payload

    async def generate_text(
        self,
        *,
        prompt: str,
        system_instruction: str | None = None,
        config: GeminiGenerateConfig | None = None,
    ) -> str:
        """Generate plain text from a prompt."""

        prompt = (prompt or "").strip()
        if not prompt:
            raise ValueError("prompt is required")

        cfg = config or GeminiGenerateConfig(response_mime_type="text/plain")

        params = {"key": self._api_key}
        payload = self._build_payload(
            prompt=prompt,
            system_instruction=system_instruction,
            config=cfg,
        )

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                response = await client.post(self._url(), params=params, json=payload)
            except httpx.HTTPError as exc:
                raise AIServiceError(f"Gemini request failed: {exc}") from exc

        if response.status_code >= 400:
            # Avoid leaking secrets; Gemini errors do not include the key anyway.
            raise AIServiceError(
                f"Gemini error {response.status_code}: {response.text[:500]}"
            )

        data = response.json()

        # Typical response shape:
        # {"candidates":[{"content":{"parts":[{"text":"..."}]}}]}
        try:
            candidates = data.get("candidates") or []
            if not candidates:
                return ""
            parts = (candidates[0].get("content") or {}).get("parts") or []
            texts = [p.get("text") for p in parts if isinstance(p, dict) and p.get("text")]
            return "".join(texts).strip()
        except Exception as exc:  # noqa: BLE001
            raise AIServiceError(f"Unexpected Gemini response format: {data}") from exc

    async def generate_json(
        self,
        *,
        prompt: str,
        system_instruction: str | None = None,
        config: GeminiGenerateConfig | None = None,
    ) -> Any:
        """Generate JSON output.

        Uses `responseMimeType=application/json` and then parses the returned text as JSON.
        """

        import json

        cfg = config or GeminiGenerateConfig(response_mime_type="application/json")
        text = await self.generate_text(
            prompt=prompt,
            system_instruction=system_instruction,
            config=cfg,
        )
        if not text:
            return None

        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise AIServiceError(f"Gemini returned non-JSON content: {text[:500]}") from exc
