from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
from typing import Any
from uuid import uuid4

from fastapi import HTTPException, UploadFile
from pymongo.database import Database

from .config import COLLECTIONS, UPLOADS_DIR


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_table(table: str) -> None:
    if table not in COLLECTIONS:
        raise HTTPException(status_code=404, detail=f"Unknown table: {table}")


def to_plain_document(document: dict[str, Any] | None) -> dict[str, Any] | None:
    if document is None:
        return None

    result = dict(document)
    result.pop("_id", None)
    result["id"] = result.get("id") or str(document.get("_id"))
    return result


def normalize_on_insert(table: str, document: dict[str, Any]) -> dict[str, Any]:
    next_document = deepcopy(document)
    now = utc_now_iso()
    next_document["id"] = str(next_document.get("id") or uuid4())

    if table == "clients":
        next_document["created_at"] = next_document.get("created_at") or now

    if table in {"projects", "milestones", "team_members"}:
        next_document["created_at"] = next_document.get("created_at") or now
        next_document["updated_at"] = next_document.get("updated_at") or now

    if table in {"project_assignments", "project_updates", "project_documents"}:
        next_document["created_at"] = next_document.get("created_at") or now

    if table == "audit_log":
        next_document["created_at"] = next_document.get("created_at") or now
        next_document["changed_by"] = next_document.get("changed_by") or "system"

    return next_document


def build_filter(query_params: dict[str, str]) -> dict[str, str]:
    filter_doc: dict[str, str] = {}
    for key, raw_value in query_params.items():
        if key in {"orderBy", "ascending", "limit", "offset"}:
            continue
        if raw_value in {"", None}:
            continue
        filter_doc[key] = str(raw_value)
    return filter_doc


def parse_date(date_str: str | None) -> datetime | None:
    """Parse date string to datetime object. Handles multiple formats: YYYY-MM-DD, ISO format."""
    if not date_str:
        return None
    try:
        if isinstance(date_str, str):
            # Handle ISO format with Z
            if "T" in date_str:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            # Handle simple date format YYYY-MM-DD
            elif "-" in date_str:
                dt = datetime.fromisoformat(date_str)
            else:
                return None

            # Ensure timezone-aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        return date_str
    except (ValueError, AttributeError, TypeError):
        return None


def get_week_number_and_label(date: datetime, start_date: datetime) -> tuple[int, str]:
    """Get week number and label based on the actual date provided."""
    days_diff = (date.date() - start_date.date()).days
    week_number = days_diff // 7

    # Label the week by the date provided, not the week start
    # This ensures Feb 2 is labeled as "Feb 2, 2025" not "Jan 27, 2025"
    week_label = date.strftime("%b %d, %Y").lstrip("0").replace(" 0", " ")

    return week_number, week_label


def calculate_status(date: datetime, eta_date: datetime | None, actual_date: datetime | None) -> tuple[str, str]:
    """
    Calculate status and color based on date comparison.
    Returns (status, color).
    """
    # Make date timezone-aware if it's naive
    if date and date.tzinfo is None:
        date = date.replace(tzinfo=timezone.utc)

    if actual_date:
        # Make actual_date timezone-aware if it's naive
        if actual_date.tzinfo is None:
            actual_date = actual_date.replace(tzinfo=timezone.utc)

        if eta_date:
            # Make eta_date timezone-aware if it's naive
            if eta_date.tzinfo is None:
                eta_date = eta_date.replace(tzinfo=timezone.utc)

            if actual_date.date() <= eta_date.date():
                return "On Track", "green"
            else:
                days_diff = (actual_date.date() - eta_date.date()).days
                if days_diff <= 14:  # Within 2 weeks of ETA
                    return "At Risk", "orange"
                else:
                    return "At Risk", "orange"
        return "On Track", "green"
    else:
        now = datetime.now(timezone.utc)
        if date.date() <= now.date():
            return "Blocked", "red"
        days_until = (date.date() - now.date()).days
        if days_until <= 14:  # Within 2 weeks
            return "At Risk", "orange"
        else:
            return "On Track", "green"


def get_status_and_color(status_value: str | None, is_pending: bool = False) -> tuple[str, str]:
    """
    Determine status and color based on status value and pending flag.
    - Pending: Amber
    - Completed/Done: Blue
    - On Hold: Amber
    - On Track: Green
    - At Risk: Orange
    - Blocked: Red
    """
    if is_pending or status_value == "Pending":
        return "Pending", "amber"

    status_lower = (status_value or "").lower()

    if status_lower in {"completed", "done"}:
        return "Completed", "blue"
    elif status_lower == "on hold":
        return "On Hold", "amber"
    elif status_lower == "blocked":
        return "Blocked", "red"
    elif status_lower == "on track":
        return "On Track", "green"
    elif status_lower == "at risk":
        return "At Risk", "orange"

    return "On Track", "green"


def get_milestone_health(database: Database, project_id: str) -> dict[str, Any]:
    """
    Get milestone health tracker data for a project.
    - Practice: range from actual_start to actual_end_eta with colors based on status field
    - Signoff: point at actual_end_eta week with client_signoff_status (Done=green, Pending=orange)
    - Invoice: point at actual_end_eta week with invoice_status (Done=green, Pending=orange)
    """
    # Fetch project
    project = database["projects"].find_one({"id": str(project_id)})
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    # Fetch all milestones for this project
    milestones = list(database["milestones"].find({"project_id": str(project_id)}))
    if not milestones:
        raise HTTPException(status_code=404, detail="No milestones found for this project")

    # Parse all dates to find date range
    all_dates = []
    for milestone in milestones:
        # Practice dates
        actual_start = parse_date(milestone.get("actual_start"))
        actual_end = parse_date(milestone.get("actual_end_eta"))

        if actual_start:
            all_dates.append(actual_start)
        if actual_end:
            all_dates.append(actual_end)

    if not all_dates:
        raise HTTPException(status_code=400, detail="No valid dates found in milestones")

    start_date = min(all_dates)
    end_date = max(all_dates)

    # Extend range to include full weeks
    start_date = start_date - timedelta(days=start_date.weekday())  # Start from Monday
    end_date = end_date + timedelta(days=(6 - end_date.weekday()))  # End on Sunday

    # Generate all weeks
    weeks_data: dict[int, dict[str, Any]] = {}
    current = start_date
    week_num = 0
    while current <= end_date:
        week_label = current.strftime("%b %d, %Y").lstrip("0").replace(" 0", " ")
        weeks_data[week_num] = {"label": week_label, "start": current.isoformat()}
        current += timedelta(days=7)
        week_num += 1

    # Process all milestones
    milestone_types_data = {"practice": [], "signoff": [], "invoice": []}

    for milestone in milestones:
        milestone_code = milestone.get("milestone_code", "")
        description = milestone.get("description", "")

        # === PRACTICE MILESTONE ===
        actual_start = parse_date(milestone.get("actual_start"))
        actual_end = parse_date(milestone.get("actual_end_eta"))
        milestone_status = milestone.get("status", "").strip()

        practice_weeks = []
        practice_color = "gray"

        if actual_start and actual_end:
            # Determine final week color based on milestone status
            status_lower = milestone_status.lower() if milestone_status else ""
            if status_lower == "completed":
                final_color = "blue"
                final_status = "Completed"
            elif status_lower == "at risk":
                final_color = "orange"
                final_status = "At Risk"
            elif status_lower == "blocked":
                final_color = "red"
                final_status = "Blocked"
            else:  # "on track" or default
                final_color = "green"
                final_status = "On Track"

            practice_color = final_color

            # Calculate week number of actual_end
            end_week_num = (actual_end.date() - start_date.date()).days // 7

            current = actual_start
            while current.date() <= actual_end.date():
                week_num, week_label = get_week_number_and_label(current, start_date)

                # Check if this is the final week (same week as actual_end_eta)
                is_final_week = week_num == end_week_num

                if is_final_week:
                    # Final week: use milestone status
                    week_status = final_status
                    week_color = final_color
                else:
                    # Earlier weeks: always "On Track" (Green)
                    week_status = "On Track"
                    week_color = "green"

                practice_weeks.append({
                    "week_number": week_num,
                    "week_label": week_label,
                    "status": week_status,
                    "color": week_color,
                    "date": current.isoformat()
                })
                current += timedelta(days=7)

        milestone_types_data["practice"].append({
            "milestone_code": milestone_code,
            "description": description,
            "milestone_type": "practice",
            "start_date": actual_start.isoformat() if actual_start else None,
            "end_date": actual_end.isoformat() if actual_end else None,
            "weeks": practice_weeks,
            "completion_pct": milestone.get("completion_pct", 0),
            "status": milestone_status,
            "color": practice_color,
            "days_variance": milestone.get("days_variance", 0)
        })

        # === SIGNOFF MILESTONE ===
        actual_end_eta = parse_date(milestone.get("actual_end_eta"))
        signoff_weeks = []
        signoff_status_raw = milestone.get("client_signoff_status", "").strip().lower()

        if actual_end_eta:
            week_num, week_label = get_week_number_and_label(actual_end_eta, start_date)

            # Map client_signoff_status to status and color
            if signoff_status_raw == "done":
                signoff_status = "Done"
                signoff_color = "green"
            elif signoff_status_raw == "pending":
                signoff_status = "Pending"
                signoff_color = "orange"
            else:
                signoff_status = signoff_status_raw.capitalize() if signoff_status_raw else "Pending"
                signoff_color = "orange" if signoff_status_raw == "pending" else "green"

            signoff_weeks.append({
                "week_number": week_num,
                "week_label": week_label,
                "status": signoff_status,
                "color": signoff_color,
                "date": actual_end_eta.isoformat()
            })

        milestone_types_data["signoff"].append({
            "milestone_code": milestone_code,
            "description": description,
            "milestone_type": "signoff",
            "date": actual_end_eta.isoformat() if actual_end_eta else None,
            "weeks": signoff_weeks,
            "signoff_status": milestone.get("client_signoff_status", ""),
            "status": milestone.get("status", "")
        })

        # === INVOICE MILESTONE ===
        actual_end_eta = parse_date(milestone.get("actual_end_eta"))
        invoice_weeks = []
        invoice_status_raw = milestone.get("invoice_status", "").strip().lower()

        if actual_end_eta:
            week_num, week_label = get_week_number_and_label(actual_end_eta, start_date)

            # Map invoice_status to status and color
            if invoice_status_raw == "done":
                invoice_status = "Done"
                invoice_color = "green"
            elif invoice_status_raw == "pending":
                invoice_status = "Pending"
                invoice_color = "orange"
            else:
                invoice_status = invoice_status_raw.capitalize() if invoice_status_raw else "Pending"
                invoice_color = "orange" if invoice_status_raw == "pending" else "green"

            invoice_weeks.append({
                "week_number": week_num,
                "week_label": week_label,
                "status": invoice_status,
                "color": invoice_color,
                "date": actual_end_eta.isoformat()
            })

        milestone_types_data["invoice"].append({
            "milestone_code": milestone_code,
            "description": description,
            "milestone_type": "invoice",
            "date": actual_end_eta.isoformat() if actual_end_eta else None,
            "weeks": invoice_weeks,
            "invoice_status": milestone.get("invoice_status", ""),
            "status": milestone.get("status", "")
        })

    return {
        "project_id": project_id,
        "project_name": project.get("name", ""),
        "practice": milestone_types_data["practice"],
        "signoff": milestone_types_data["signoff"],
        "invoice": milestone_types_data["invoice"],
        "weeks_range": {
            "start_week": weeks_data[0]["label"] if weeks_data else "",
            "end_week": weeks_data[max(weeks_data.keys())]["label"] if weeks_data else "",
            "total_weeks": len(weeks_data)
        },
        "all_weeks": weeks_data
    }


def get_dashboard_counters(database: Database) -> dict[str, int]:
    """Return dashboard counts for project status counters."""
    projects = database["projects"]

    active_filter = {
        "$and": [
            {"status": {"$exists": True}},
            {"status": {"$not": {"$regex": "^(Blocked|Completed)$", "$options": "i"}}},
        ]
    }
    on_track_filter = {"status": {"$regex": "^On Track$", "$options": "i"}}
    at_risk_filter = {"status": {"$regex": "^At Risk$", "$options": "i"}}
    blocked_filter = {"status": {"$regex": "^Blocked$", "$options": "i"}}

    return {
        "active_projects": projects.count_documents(active_filter),
        "on_track_projects": projects.count_documents(on_track_filter),
        "at_risk_projects": projects.count_documents(at_risk_filter),
        "blocked_projects": projects.count_documents(blocked_filter),
    }


def list_records(database: Database, table: str, query_params: dict[str, str]) -> list[dict[str, Any]]:
    ensure_table(table)
    cursor = database[table].find(build_filter(query_params))

    order_by = query_params.get("orderBy")
    if order_by:
        descending = query_params.get("ascending") == "false"
        cursor = cursor.sort(order_by, -1 if descending else 1)

    limit = query_params.get("limit")
    if limit is not None:
        try:
            limit_value = int(limit)
        except ValueError:
            limit_value = 0
        if limit_value > 0:
            cursor = cursor.limit(limit_value)

    offset = query_params.get("offset")
    if offset is not None:
        try:
            offset_value = int(offset)
        except ValueError:
            offset_value = 0
        if offset_value > 0:
            cursor = cursor.skip(offset_value)

    return [to_plain_document(doc) or {} for doc in cursor]


def get_record(database: Database, table: str, record_id: str) -> dict[str, Any]:
    ensure_table(table)
    document = database[table].find_one({"id": str(record_id)})
    if document is None:
        raise HTTPException(status_code=404, detail=f"{table} record not found")
    return to_plain_document(document) or {}


def create_records(database: Database, table: str, payload: Any) -> list[dict[str, Any]] | dict[str, Any]:
    ensure_table(table)

    if not isinstance(payload, (dict, list)):
        raise HTTPException(status_code=400, detail="Invalid body")

    if isinstance(payload, list):
        documents = [normalize_on_insert(table, item) for item in payload]
        if not documents:
            return []
        database[table].insert_many(documents)
        return [to_plain_document(document) or {} for document in documents]

    document = normalize_on_insert(table, payload)
    database[table].insert_one(document)
    return to_plain_document(document) or {}


def patch_record(database: Database, table: str, record_id: str, changes: Any) -> dict[str, Any] | None:
    ensure_table(table)

    if not isinstance(changes, dict):
        raise HTTPException(status_code=400, detail="Invalid body")

    next_changes = deepcopy(changes)
    if table in {"projects", "milestones", "team_members"}:
        next_changes["updated_at"] = utc_now_iso()

    database[table].update_one({"id": str(record_id)}, {"$set": next_changes})
    updated = database[table].find_one({"id": str(record_id)})
    return to_plain_document(updated)


def replace_record(database: Database, table: str, record_id: str, replacement: Any) -> dict[str, Any]:
    ensure_table(table)

    if not isinstance(replacement, dict):
        raise HTTPException(status_code=400, detail="Invalid body")

    existing = database[table].find_one({"id": str(record_id)})
    if existing is None:
        raise HTTPException(status_code=404, detail=f"{table} record not found")

    next_document = {**existing, **replacement, "id": str(record_id)}
    if table in {"projects", "milestones", "team_members"}:
        next_document["updated_at"] = utc_now_iso()

    database[table].replace_one({"id": str(record_id)}, next_document)
    updated = database[table].find_one({"id": str(record_id)})
    return to_plain_document(updated) or {}


def delete_record(database: Database, table: str, record_id: str) -> dict[str, Any]:
    ensure_table(table)

    existing = database[table].find_one({"id": str(record_id)})

    if table == "project_documents" and existing and existing.get("path"):
        full_path = Path(UPLOADS_DIR.parent, existing["path"])
        if full_path.exists():
            full_path.unlink()

    database[table].delete_one({"id": str(record_id)})
    return {"ok": True, "deleted": to_plain_document(existing)}


def save_upload(
    database: Database,
    file: UploadFile,
    project_id: str,
    update_id: str | None = None,
    category: str | None = None,
) -> dict[str, Any]:
    if not project_id:
        raise HTTPException(status_code=400, detail="Project ID is required")

    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

    safe_name = Path(file.filename or "upload.bin").name
    unique_name = f"{int(datetime.now(timezone.utc).timestamp() * 1000)}-{safe_name}"
    disk_path = UPLOADS_DIR / unique_name

    with disk_path.open("wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    file_details: dict[str, Any] = {
        "name": safe_name,
        "size": disk_path.stat().st_size,
        "type": file.content_type or "application/octet-stream",
        "path": f"uploads/{unique_name}",
        "project_id": project_id,
    }
    if category:
        file_details["category"] = category

    document = normalize_on_insert("project_documents", file_details)
    database["project_documents"].insert_one(document)

    if update_id:
        database["project_updates"].update_one(
            {"id": update_id},
            {"$set": {"file_path": file_details["path"], "file_name": file_details["name"]}},
        )

    return to_plain_document(document) or {}