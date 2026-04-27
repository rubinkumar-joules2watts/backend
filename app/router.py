from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from .config import UPLOADS_DIR
from .db import get_database
from .document_parser import extract_proposal_from_document
from .ai.dependencies import get_gpt4omini_service
from .ai.azure_openai_service import AzureOpenAIService
from .service import (
    auto_allocate_resources,
    create_records,
    delete_record,
    export_status_report,
    get_dashboard_counters,
    get_milestone_health,
    get_record,
    get_skills_for_designation,
    get_team_members_with_engagement,
    list_records,
    patch_record,
    replace_record,
    save_upload,
    search_resources,
    delete_week_status,
    update_milestone_health,
    update_week_status,
    upload_to_cloud,
    generate_project_insight_report,
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


@router.delete("/milestones/{milestone_id}/health/{milestone_type}/week/{week_number}")
def delete_week_status_endpoint(
    milestone_id: str,
    milestone_type: str,
    week_number: int,
) -> dict[str, Any] | None:
    """Delete a specific week entry from a milestone's weeks array."""
    return delete_week_status(get_database(), milestone_id, milestone_type, week_number)


@router.post("/upload_cloud")
async def upload_file_cloud(
    request: Request,
    file: UploadFile = File(...),
    project_id: str = Form(...),
    update_id: str | None = Form(None),
    category: str | None = Form(None),
) -> dict[str, Any]:
    """
    Upload any file to Azure Blob Storage and record it in project_documents.

    Form fields:
        file        — the file to upload (required)
        project_id  — project to associate the document with (required)
        update_id   — project update to link the file to (optional)
        category    — document category (optional)

    Stores under: centralized_delivery_tracker_records/{timestamp}-{filename}
    """
    return await upload_to_cloud(
        get_database(),
        file,
        request.app.state.settings,
        project_id=project_id,
        update_id=update_id,
        category=category,
    )


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

    media_type = None
    if resolved.suffix.lower() == ".pdf":
        media_type = "application/pdf"
    elif resolved.suffix.lower() == ".docx":
        media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

    # Treat query param as boolean flag (e.g., download=true) instead of filename.
    as_attachment = str(download or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    if as_attachment:
        return FileResponse(
            path=str(resolved),
            filename=Path(file_path).name,
            media_type=media_type,
        )

    return FileResponse(
        path=str(resolved),
        media_type=media_type,
    )


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


@router.post("/resources/search")
def search_resources_endpoint(payload: Any = Body(...)) -> dict[str, Any]:
    """
    Search team members matching required skills and availability.

    Body:
        {
            "skills": ["Python", "Docker", ...],
            "bandwidth_needed": 100,
            "resource_type": "Internal",   // "Internal" | "External" | omit for all
            "project_start": "2026-04-13",
            "project_end": "2026-07-06"
        }

    resource_type rules:
        - "Internal" → members with resource_type="Internal" + Consultants
        - "External" → members with resource_type="External" only
        - omitted    → all active members

    Returns members sorted by composite score (70% skill + 30% availability).
    """
    return search_resources(
        get_database(),
        required_skills=payload.get("skills", []),
        bandwidth_needed=int(payload.get("bandwidth_needed", 0)),
    )


@router.post("/resources/auto-allocate")
def auto_allocate_endpoint(payload: Any = Body(...)) -> list[dict[str, Any]]:
    """
    Auto-allocate team members to resource slots.

    Body:
        {
            "resources": [
                {"id": "r1", "role": "Project Manager", "skills": [...], "bandwidth": 100},
                ...
            ],
            "project_start": "2026-04-13",
            "project_end": "2026-07-06"
        }

    Each member is assigned at most once; minimum skill score 50, minimum availability 20%.
    """
    return auto_allocate_resources(
        get_database(),
        resources_input=payload.get("resources", []),
        project_start=payload.get("project_start"),
        project_end=payload.get("project_end"),
    )


@router.post("/designations/skills")
async def get_designation_skills(
    payload: Any = Body(...),
    azure: AzureOpenAIService = Depends(get_gpt4omini_service),
) -> dict[str, Any]:
    """
    Return the skill set expected for a given designation, powered by Azure OpenAI (GPT-4o-mini).

    Body:
        {"designation": "Senior Software Engineer"}

    Response shape:
        {
            "designation": "Senior Software Engineer",
            "level": "Senior",
            "technical_skills": [...],
            "tools_and_technologies": [...],
            "soft_skills": [...],
            "domain_knowledge": [...],
            "certifications": [...]
        }
    """
    designation = (payload.get("designation") or "").strip() if isinstance(payload, dict) else ""
    if not designation:
        raise HTTPException(status_code=400, detail="designation is required in request body")
    return await get_skills_for_designation(designation, azure)


@router.post("/reports/generate")
async def generate_report_endpoint(payload: Any = Body(...)) -> list[dict[str, Any]]:
    """
    Generate an AI-driven insight report for selected projects and date range.
    
    Body:
        {
            "project_ids": ["pid1", "pid2"],
            "start_date": "2026-04-01",
            "end_date": "2026-04-30"
        }
    """
    project_ids = payload.get("project_ids", [])
    if not project_ids:
        raise HTTPException(status_code=400, detail="project_ids list is required")
        
    return await generate_project_insight_report(
        get_database(),
        project_ids=project_ids,
        start_date_str=payload.get("start_date"),
        end_date_str=payload.get("end_date")
    )


@router.post("/export-status-report")
def export_status_report_endpoint(payload: Any = Body(...)) -> dict[str, Any]:
    """
    Export edited insight reports into DOCX or PDF.

    Body:
        {
            "reports": [...],              # required
            "batch_mode": "single",        # "single" | "per_project"
            "format": "docx",              # "docx" | "pdf"
            "file_name": "optional-name"   # used for single mode
        }
    """
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")
    return export_status_report(payload)


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
