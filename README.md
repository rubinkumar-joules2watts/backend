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
