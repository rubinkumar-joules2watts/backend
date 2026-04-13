from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


TableName = Literal[
    "clients",
    "projects",
    "milestones",
    "team_members",
    "project_assignments",
    "audit_log",
    "project_updates",
    "project_documents",
    "team_members_engagement",
]


class HealthResponse(BaseModel):
    ok: bool = True


class ErrorResponse(BaseModel):
    error: str


class DeleteResponse(BaseModel):
    ok: bool = True
    deleted: dict[str, Any] | None = None


class UploadResponse(BaseModel):
    id: str
    name: str
    size: int
    type: str
    path: str
    project_id: str
    category: str | None = None
    created_at: str | None = None


class ProjectUpdateUpload(BaseModel):
    project_id: str = Field(..., min_length=1)
    update_id: str | None = None
    category: str | None = None


class WeekMarker(BaseModel):
    """Represents a milestone marker on a specific week."""
    week_number: int
    week_label: str  # e.g., "Jan 1", "Feb 2"
    status: str  # "On Track", "At Risk", "Blocked", "Completed", "Done", "Pending"
    color: str  # "green", "orange", "red", "blue", "gray"
    date: str  # ISO format date


class WeekData(BaseModel):
    """Week-wise milestone data stored in database."""
    week_number: int
    week_label: str  # e.g., "Feb 2-8, 2025"
    status: str  # Status for that specific week
    color: str  # Color code for that week
    date: str  # ISO format date


class MilestoneHealth(BaseModel):
    """Health status for a single milestone type."""
    milestone_type: str  # "practice", "signoff", "invoice"
    eta_date: str | None
    actual_date: str | None
    eta_weeks: list[WeekMarker] = []
    actual_weeks: list[WeekMarker] = []


class MilestoneHealthResponse(BaseModel):
    """Response containing all milestone health data for a project."""
    project_id: str
    project_name: str
    milestones: list[MilestoneHealth]
    weeks_range: dict[str, str]  # start_week and end_week labels