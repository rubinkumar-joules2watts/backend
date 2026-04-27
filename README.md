## Backend

FastAPI + MongoDB backend for the delivery tracker.

### Run locally

```bash
uv sync
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Environment

- `MONGODB_URI` is read from `backend/.env`
- `MONGODB_DB_NAME` defaults to `delivery_tracker`
- `API_PORT` defaults to `8000`
- Debug DB logging (for `/api/reports/generate` reads): set `J2W_LOG_DB=1` (optional: `J2W_LOG_DB_LIMIT=200`, `J2W_LOG_DB_PRETTY=1`, `J2W_LOG_DB_STR_LIMIT=500`)

### Gemini (Flash 2.5)

Set these in `backend/.env` (see `backend/.env.example`) or your deployment environment:

- `GEMINI_API_KEY` (or `GOOGLE_API_KEY`)
- `GEMINI_MODEL` (default: `gemini-2.5-flash`)

### Groq (Llama 3.1)

If `GROQ_API_KEY` is set, the insight report generation uses Groq (instead of Gemini).

- `GROQ_API_KEY`
- `GROQ_MODEL` (default: `llama-3.1-8b-instant`)

Minimal usage (inside an async FastAPI endpoint):

```python
from fastapi import APIRouter, Depends

from app.ai import GeminiService
from app.ai.dependencies import get_gemini_service

router = APIRouter()


@router.post("/ai/demo")
async def ai_demo(gemini: GeminiService = Depends(get_gemini_service)):
	text = await gemini.generate_text(prompt="Write a one-line status update.")
	return {"text": text}
```

### Azure OpenAI (GPT-4o-mini)

Set these in `backend/.env` (see `backend/.env.example`) or your deployment environment:

- `GPT4OMINI_API_KEY`
- `GPT4OMINI_ENDPOINT` (example: `https://<resource>.cognitiveservices.azure.com/`)
- `GPT4OMINI_API_VERSION` (default: `2024-12-01-preview`)
- `GPT4OMINI_DEPLOYMENT_NAME` (default: `gpt-4o-mini`)

Minimal usage (inside an async FastAPI endpoint):

```python
from fastapi import APIRouter, Depends

from app.ai import AzureOpenAIService
from app.ai.dependencies import get_gpt4omini_service

router = APIRouter()


@router.post("/ai/demo-azure")
async def ai_demo_azure(azure: AzureOpenAIService = Depends(get_gpt4omini_service)):
	text = await azure.generate_text(prompt="Write a one-line status update.")
	return {"text": text}
```
