from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from .config import UPLOADS_DIR
from .db import get_database
from .service import (
    create_records,
    delete_record,
    get_dashboard_counters,
    get_milestone_health,
    get_record,
    list_records,
    patch_record,
    replace_record,
    save_upload,
)


router = APIRouter()


@router.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@router.get("/dashboard/counters")
def dashboard_counters() -> dict[str, int]:
    return get_dashboard_counters(get_database())


@router.get("/projects/{project_id}/milestone-health")
def get_milestone_health_endpoint(project_id: str) -> dict[str, Any]:
    """
    Get milestone health tracker for a project.
    Shows all milestones (practice, signoff, invoice) with ETA vs actual dates.
    Returns status (On Track/At Risk/Blocked/Completed) and colors for each milestone across weeks.
    """
    return get_milestone_health(get_database(), project_id)


@router.post("/upload")
def upload_file(
    file: UploadFile = File(...),
    project_id: str = Form(...),
    update_id: str | None = Form(None),
    category: str | None = Form(None),
) -> dict[str, Any]:
    return save_upload(get_database(), file, project_id, update_id, category)


@router.get("/files/{file_path:path}")
def download_file(file_path: str, download: str | None = None) -> FileResponse:
    resolved = (UPLOADS_DIR.parent / file_path).resolve()
    uploads_root = UPLOADS_DIR.resolve()

    if not resolved.is_relative_to(uploads_root):
        raise HTTPException(status_code=404, detail="File not found")

    if not resolved.exists():
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(path=str(resolved), filename=download or Path(file_path).name)


@router.patch("/projects/{project_id}")
def update_project(project_id: str, payload: Any = Body(...)) -> dict[str, Any] | None:
    """
    Update a project by ID.
    
    Args:
        project_id: The ID of the project to update
        payload: The project data to update
        
    Returns:
        The updated project record
    """
    return patch_record(get_database(), "projects", project_id, payload)


@router.delete("/projects_update/{update_id}")
def delete_project_update(update_id: str) -> dict[str, Any]:
    """
    Delete a project update by ID.
    
    Args:
        update_id: The ID of the project update to delete

    Returns:
        A deletion result object
    """
    return delete_record(get_database(), "project_updates", update_id)


@router.get("/{table}")
def read_table(request: Request, table: str) -> list[dict[str, Any]]:
    return list_records(get_database(), table, dict(request.query_params))


@router.get("/{table}/{record_id}")
def read_record(table: str, record_id: str) -> dict[str, Any]:
    return get_record(get_database(), table, record_id)


@router.post("/{table}")
def create_table_record(table: str, payload: Any = Body(...)) -> Any:
    return create_records(get_database(), table, payload)


@router.patch("/{table}/{record_id}")
def update_table_record(table: str, record_id: str, payload: Any = Body(...)) -> dict[str, Any] | None:
    return patch_record(get_database(), table, record_id, payload)


@router.put("/{table}/{record_id}")
def replace_table_record(table: str, record_id: str, payload: Any = Body(...)) -> dict[str, Any]:
    return replace_record(get_database(), table, record_id, payload)


@router.delete("/{table}/{record_id}")
def remove_table_record(table: str, record_id: str) -> dict[str, Any]:
    return delete_record(get_database(), table, record_id)