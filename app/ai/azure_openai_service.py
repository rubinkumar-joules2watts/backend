from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import httpx

from .errors import AIConfigurationError, AIServiceError


@dataclass(frozen=True)
class AzureChatConfig:
    temperature: float | None = 0.2
    max_tokens: int | None = 1024
    top_p: float | None = None
    presence_penalty: float | None = None
    frequency_penalty: float | None = None


class AzureOpenAIService:
    """Azure OpenAI (Chat Completions) service.

    Designed for deployments like GPT-4o-mini via:
    POST {endpoint}/openai/deployments/{deployment}/chat/completions?api-version=...

    Auth uses header: api-key: <key>
    """

    def __init__(
        self,
        *,
        api_key: str,
        endpoint: str,
        api_version: str,
        deployment_name: str,
        timeout_s: float = 60.0,
    ) -> None:
        api_key = (api_key or "").strip()
        endpoint = (endpoint or "").strip()
        api_version = (api_version or "").strip()
        deployment_name = (deployment_name or "").strip()

        if not api_key:
            raise AIConfigurationError(
                "Azure OpenAI API key is missing. Set GPT4OMINI_API_KEY in backend/.env or environment."
            )
        if not endpoint:
            raise AIConfigurationError(
                "Azure OpenAI endpoint is missing. Set GPT4OMINI_ENDPOINT in backend/.env or environment."
            )
        if not api_version:
            raise AIConfigurationError(
                "Azure OpenAI api-version is missing. Set GPT4OMINI_API_VERSION in backend/.env or environment."
            )
        if not deployment_name:
            raise AIConfigurationError(
                "Azure OpenAI deployment name is missing. Set GPT4OMINI_DEPLOYMENT_NAME in backend/.env or environment."
            )

        self._api_key = api_key
        self._endpoint = endpoint.rstrip("/")
        self._api_version = api_version
        self._deployment_name = deployment_name
        self._timeout = httpx.Timeout(timeout_s)

    @property
    def deployment_name(self) -> str:
        return self._deployment_name

    def _url(self) -> str:
        return f"{self._endpoint}/openai/deployments/{self._deployment_name}/chat/completions"

    def _headers(self) -> dict[str, str]:
        return {"api-key": self._api_key, "Content-Type": "application/json"}

    async def chat(
        self,
        *,
        user_prompt: str,
        system_prompt: str | None = None,
        config: AzureChatConfig | None = None,
        response_format: Literal["text", "json"] = "text",
    ) -> str:
        """Chat completion returning the assistant message content as string."""

        user_prompt = (user_prompt or "").strip()
        if not user_prompt:
            raise ValueError("user_prompt is required")

        messages: list[dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})

        cfg = config or AzureChatConfig()

        payload: dict[str, Any] = {
            "messages": messages,
        }
        if cfg.temperature is not None:
            payload["temperature"] = cfg.temperature
        if cfg.max_tokens is not None:
            payload["max_tokens"] = cfg.max_tokens
        if cfg.top_p is not None:
            payload["top_p"] = cfg.top_p
        if cfg.presence_penalty is not None:
            payload["presence_penalty"] = cfg.presence_penalty
        if cfg.frequency_penalty is not None:
            payload["frequency_penalty"] = cfg.frequency_penalty

        if response_format == "json":
            # Supported by newer Azure OpenAI API versions for GPT-4o family.
            payload["response_format"] = {"type": "json_object"}

        params = {"api-version": self._api_version}

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                response = await client.post(
                    self._url(),
                    headers=self._headers(),
                    params=params,
                    json=payload,
                )
            except httpx.HTTPError as exc:
                raise AIServiceError(f"Azure OpenAI request failed: {exc}") from exc

        if response.status_code >= 400:
            raise AIServiceError(
                f"Azure OpenAI error {response.status_code}: {response.text[:500]}"
            )

        data = response.json()
        try:
            choices = data.get("choices") or []
            if not choices:
                return ""
            message = (choices[0].get("message") or {})
            content = message.get("content")
            return (content or "").strip()
        except Exception as exc:  # noqa: BLE001
            raise AIServiceError(f"Unexpected Azure OpenAI response format: {data}") from exc

    async def generate_text(
        self,
        *,
        prompt: str,
        system_prompt: str | None = None,
        config: AzureChatConfig | None = None,
    ) -> str:
        return await self.chat(
            user_prompt=prompt,
            system_prompt=system_prompt,
            config=config,
            response_format="text",
        )

    async def generate_json(
        self,
        *,
        prompt: str,
        system_prompt: str | None = None,
        config: AzureChatConfig | None = None,
    ) -> Any:
        import json

        text = await self.chat(
            user_prompt=prompt,
            system_prompt=system_prompt,
            config=config,
            response_format="json",
        )
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise AIServiceError(f"Azure OpenAI returned non-JSON content: {text[:500]}") from exc
