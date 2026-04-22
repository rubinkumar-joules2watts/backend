from __future__ import annotations


class AIServiceError(RuntimeError):
    """Raised when an upstream AI provider returns an error."""


class AIConfigurationError(AIServiceError):
    """Raised when required AI configuration is missing or invalid."""
