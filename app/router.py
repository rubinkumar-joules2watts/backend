from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Body, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from .config import UPLOADS_DIR
from .db import get_database
from .document_parser import extract_proposal_from_document
from .service import (
    create_records,
    delete_record,
    get_dashboard_counters,
    get_milestone_health,
    get_record,
    get_team_members_with_engagement,
    list_records,
    patch_record,
    replace_record,
    save_upload,
    update_milestone_health,
    update_week_status,
)


router = APIRouter()
logger = logging.getLogger("delivery_tracker.api")

if not logger.handlers:
    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        logs_dir / "api_extract.log",
        maxBytes=2 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False


@router.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@router.get("/dashboard/counters")
def dashboard_counters() -> dict[str, int]:
    return get_dashboard_counters(get_database())


@router.get("/team_members")
def get_team_members_endpoint() -> list[dict[str, Any]]:
    """
    Get all team members with their engagement metrics.

    Aggregates engagement data from team_members_engagement collection and includes:
    - engagement_pct: Average engagement percentage across all projects
    - total_engagement_hours: Sum of hours across all projects
    - total_tasks_completed: Sum of completed tasks across all projects
    - total_tasks_pending: Sum of pending tasks across all projects
    - projects_assigned: Number of projects the member is assigned to
    - engagements: List of all engagement records for the member

    Returns:
        List of team members with aggregated engagement data
    """
    return get_team_members_with_engagement(get_database())


@router.get("/projects/first")
def get_first_project() -> dict[str, Any] | None:
    """
    Get the first project (by creation date, oldest first).
    Returns None if no projects exist.
    """
    projects = list_records(get_database(), "projects", {"orderBy": "created_at", "ascending": "true", "limit": "1"})
    return projects[0] if projects else None


@router.get("/projects/{project_id}/milestone-health")
def get_milestone_health_endpoint(project_id: str) -> dict[str, Any]:
    """
    Get milestone health tracker for a project.
    Shows all milestones (practice, signoff, invoice) with ETA vs actual dates.
    Returns status (On Track/At Risk/Blocked/Completed) and colors for each milestone across weeks.
    """
    return get_milestone_health(get_database(), project_id)


@router.patch("/milestones/{milestone_id}/health/{milestone_type}")
def update_milestone_health_endpoint(
    milestone_id: str,
    milestone_type: str,
    payload: Any = Body(...),
) -> dict[str, Any] | None:
    """
    Update a specific milestone health type (practice, signoff, or invoice).

    Args:
        milestone_id: The ID of the milestone
        milestone_type: The type of milestone - "practice", "signoff", or "invoice"
        payload: Update payload

    Payload Examples:
        # For practice status
        {
            "status": "At Risk"  // "On Track", "At Risk", "Blocked", "Completed"
        }

        # For signoff
        {
            "status": "Done",  // "Done" or "Pending"
            "date": "2026-02-07"  // optional, only for "Done"
        }

        # For invoice
        {
            "status": "Done",  // "Done" or "Pending"
            "date": "2026-02-07"  // optional, only for "Done"
        }

    Returns:
        Updated milestone with regenerated week data
    """
    status = payload.get("status")
    date = payload.get("date")

    if not status:
        raise HTTPException(status_code=400, detail="status field is required")

    return update_milestone_health(get_database(), milestone_id, milestone_type, status, date)


@router.patch("/milestones/{milestone_id}/health/{milestone_type}/week/{week_number}")
def update_week_status_endpoint(
    milestone_id: str,
    milestone_type: str,
    week_number: int,
    payload: Any = Body(...),
) -> dict[str, Any] | None:
    """
    Update status for a specific week in a milestone.

    Args:
        milestone_id: The ID of the milestone
        milestone_type: The type of milestone - "practice", "signoff", or "invoice"
        week_number: The week number to update
        payload: Week update payload

    Payload Example:
        {
            "week_status": "At Risk",           // New status from user
            "week_label": "Feb 16-22, 2026",
            "color": "orange",
            "date": "2026-02-20"
        }

    Returns:
        Updated milestone with the week status changed
    """
    return update_week_status(get_database(), milestone_id, milestone_type, week_number, payload)


@router.post("/upload")
def upload_file(
    file: UploadFile = File(...),
    project_id: str = Form(...),
    update_id: str | None = Form(None),
    category: str | None = Form(None),
) -> dict[str, Any]:
    return save_upload(get_database(), file, project_id, update_id, category)


@router.post("/documents/extract")
async def extract_document(file: UploadFile = File(...)) -> dict[str, Any]:
    request_id = str(uuid4())[:8]
    started_at = perf_counter()
    logger.info("extract_api_start request_id=%s filename=%s", request_id, file.filename)

    file_bytes = await file.read()
    try:
        result = extract_proposal_from_document(file.filename or "document", file_bytes)
    except Exception:
        elapsed_ms = int((perf_counter() - started_at) * 1000)
        logger.exception(
            "extract_api_error request_id=%s filename=%s bytes=%s ms=%s",
            request_id,
            file.filename,
            len(file_bytes),
            elapsed_ms,
        )
        raise

    elapsed_ms = int((perf_counter() - started_at) * 1000)
    proposal = result.get("proposal", {})
    logger.info(
        "extract_api_success request_id=%s filename=%s bytes=%s ms=%s milestones=%s resources=%s warnings=%s",
        request_id,
        file.filename,
        len(file_bytes),
        elapsed_ms,
        len(proposal.get("milestones", [])),
        len(proposal.get("resources", [])),
        len(proposal.get("warnings", [])),
    )
    return result


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


@router.get("/team_members_engagement/member/{member_id}/project/{project_id}")
def get_team_member_project_engagement(member_id: str, project_id: str) -> list[dict[str, Any]]:
    """
    Get engagement record for a specific team member on a specific project.

    Args:
        member_id: The ID of the team member
        project_id: The ID of the project

    Returns:
        Engagement record(s) for that member on that project
    """
    return list_records(
        get_database(),
        "team_members_engagement",
        {"team_member_id": member_id, "project_id": project_id}
    )


@router.get("/team_members_engagement/member/{member_id}")
def get_team_member_engagements(member_id: str) -> list[dict[str, Any]]:
    """
    Get all engagement records for a specific team member across all projects.

    Args:
        member_id: The ID of the team member

    Returns:
        List of engagement records for that team member
    """
    return list_records(get_database(), "team_members_engagement", {"team_member_id": member_id})


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