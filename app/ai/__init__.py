"""AI service layer.

Currently supports Gemini via the Generative Language API.
"""

from .azure_openai_service import AzureOpenAIService
from .gemini_service import GeminiService

__all__ = ["AzureOpenAIService", "GeminiService"]
