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

    # Initialize week data arrays for milestones (will be populated after insert)
    if table == "milestones":
        next_document["practice_weeks"] = next_document.get("practice_weeks") or []
        next_document["signoff_weeks"] = next_document.get("signoff_weeks") or []
        next_document["invoice_weeks"] = next_document.get("invoice_weeks") or []

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


def generate_practice_weeks(milestone: dict[str, Any], start_date: datetime, weeks_data: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Generate practice week data based on actual_start, actual_end_eta, and status.
    Returns list of week markers with status and color for each week.

    Status Logic:
    - If status="Completed" AND actual_end_eta date has passed → Show as Completed (blue)
    - Else use the milestone status field as-is
    """
    practice_weeks = []

    actual_start = parse_date(milestone.get("actual_start"))
    actual_end = parse_date(milestone.get("actual_end_eta"))
    milestone_status = (milestone.get("status") or "").strip()

    if not actual_start or not actual_end:
        return practice_weeks

    # ============ DETERMINE FINAL WEEK STATUS AND COLOR ============
    # This is where we check the "Completed" status based on the milestone status field
    # The final week gets the current milestone status, regardless of the actual_end_eta date

    status_lower = milestone_status.lower() if milestone_status else ""

    # Map status to color and display status
    # - If status="Completed" → Show as blue/Completed
    # - If status="At Risk" → Show as orange/At Risk
    # - If status="Blocked" → Show as red/Blocked
    # - Otherwise → Show as green/On Track

    if status_lower == "completed":
        # ✅ MILESTONE IS MARKED AS COMPLETED
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

    # Find the Monday of the week containing actual_start
    week_start_of_start = actual_start - timedelta(days=actual_start.weekday())

    # If actual_start is on Fri/Sat/Sun (last 3 days of week), skip partial week
    if actual_start.weekday() >= 4:  # Friday=4, Saturday=5, Sunday=6
        week_start_of_start = week_start_of_start + timedelta(days=7)

    milestone_start_week_num = (week_start_of_start.date() - start_date.date()).days // 7

    # Find the Monday of the week containing actual_end
    week_start_of_end = actual_end - timedelta(days=actual_end.weekday())
    end_week_num = (week_start_of_end.date() - start_date.date()).days // 7

    # If milestone_start > end, include at least the end week
    if milestone_start_week_num > end_week_num:
        milestone_start_week_num = end_week_num

    # Include all weeks where the milestone is running
    for week_num_iter in range(milestone_start_week_num, end_week_num + 1):
        if week_num_iter in weeks_data:
            week_label = weeks_data[week_num_iter]["label"]

            is_final_week = week_num_iter == end_week_num
            is_first_week = week_num_iter == milestone_start_week_num

            if is_final_week:
                week_status = final_status
                week_color = final_color
            else:
                week_status = "On Track"
                week_color = "green"

            # Determine the date for this week
            if is_first_week and is_final_week:
                week_date = actual_end.isoformat()
            elif is_first_week:
                week_date = actual_start.isoformat()
            elif is_final_week:
                week_date = actual_end.isoformat()
            else:
                week_date = weeks_data[week_num_iter]["start"]

            practice_weeks.append({
                "week_number": week_num_iter,
                "week_label": week_label,
                "status": week_status,
                "color": week_color,
                "date": week_date
            })

    return practice_weeks


def generate_signoff_weeks(milestone: dict[str, Any], start_date: datetime, weeks_data: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Generate signoff week data based on client_signoff_status.
    Returns list with single week marker showing signoff status.

    Logic:
    - If status="Done" → Use signedoff_date or actual_end_eta
    - If status="Pending" → Use actual_end_eta
    """
    signoff_weeks = []

    signoff_status_raw = (milestone.get("client_signoff_status") or "").strip().lower()
    actual_end_eta = parse_date(milestone.get("actual_end_eta"))

    # Determine which date to use based on status
    if signoff_status_raw == "done":
        signoff_date = parse_date(milestone.get("signedoff_date"))
        signoff_display_date = signoff_date or actual_end_eta
        signoff_status = "Done"
        signoff_color = "green"
    else:
        signoff_display_date = actual_end_eta
        signoff_status = "Pending"
        signoff_color = "orange"

    if signoff_display_date:
        week_num, _ = get_week_number_and_label(signoff_display_date, start_date)
        week_label = weeks_data[week_num]["label"] if week_num in weeks_data else ""

        signoff_weeks.append({
            "week_number": week_num,
            "week_label": week_label,
            "status": signoff_status,
            "color": signoff_color,
            "date": signoff_display_date.isoformat()
        })

    return signoff_weeks


def generate_invoice_weeks(milestone: dict[str, Any], start_date: datetime, weeks_data: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Generate invoice week data based on invoice_status.
    Returns list with single week marker showing invoice status.

    Logic:
    - If status="Done" → Use invoice_raised_date or actual_end_eta
    - If status="Pending" → Use actual_end_eta
    """
    invoice_weeks = []

    invoice_status_raw = (milestone.get("invoice_status") or "").strip().lower()
    actual_end_eta = parse_date(milestone.get("actual_end_eta"))

    # Determine which date to use based on status
    if invoice_status_raw == "done":
        invoice_date = parse_date(milestone.get("invoice_raised_date"))
        invoice_display_date = invoice_date or actual_end_eta
        invoice_status = "Done"
        invoice_color = "green"
    else:
        invoice_display_date = actual_end_eta
        invoice_status = "Pending"
        invoice_color = "orange"

    if invoice_display_date:
        week_num, _ = get_week_number_and_label(invoice_display_date, start_date)
        week_label = weeks_data[week_num]["label"] if week_num in weeks_data else ""

        invoice_weeks.append({
            "week_number": week_num,
            "week_label": week_label,
            "status": invoice_status,
            "color": invoice_color,
            "date": invoice_display_date.isoformat()
        })

    return invoice_weeks


def generate_all_milestone_weeks(database: Database, milestone: dict[str, Any]) -> dict[str, Any]:
    """
    Generate and store all week data (practice, signoff, invoice) for a milestone.
    Called when a milestone is created or updated.
    """
    project_id = milestone.get("project_id")

    # Get all milestones to calculate date range
    all_milestones = list(database["milestones"].find({"project_id": str(project_id)}))

    all_dates = []
    for m in all_milestones:
        actual_start = parse_date(m.get("actual_start"))
        actual_end = parse_date(m.get("actual_end_eta"))
        signoff_date = parse_date(m.get("signedoff_date"))
        signoff_eta = parse_date(m.get("actual_end_eta"))
        invoice_date = parse_date(m.get("invoice_raised_date"))
        invoice_eta = parse_date(m.get("actual_end_eta"))

        for date in [actual_start, actual_end, signoff_date, signoff_eta, invoice_date, invoice_eta]:
            if date:
                all_dates.append(date)

    if not all_dates:
        return {
            "practice_weeks": [],
            "signoff_weeks": [],
            "invoice_weeks": []
        }

    start_date = min(all_dates)
    end_date = max(all_dates)

    # Extend range to include full weeks
    start_date = start_date - timedelta(days=start_date.weekday())
    end_date = end_date + timedelta(days=(6 - end_date.weekday()))

    # Generate all weeks
    weeks_data: dict[int, dict[str, Any]] = {}
    current = start_date
    week_num = 0
    while current <= end_date:
        week_end = current + timedelta(days=6)
        if current.month == week_end.month:
            week_label = f"{current.strftime('%b %d')}-{week_end.strftime('%d, %Y')}".lstrip("0").replace(" 0", " ")
        else:
            week_label = f"{current.strftime('%b %d')}-{week_end.strftime('%b %d, %Y')}".lstrip("0").replace(" 0", " ")
        weeks_data[week_num] = {"label": week_label, "start": current.isoformat()}
        current += timedelta(days=7)
        week_num += 1

    # Generate week data for each type
    practice_weeks = generate_practice_weeks(milestone, start_date, weeks_data)
    signoff_weeks = generate_signoff_weeks(milestone, start_date, weeks_data)
    invoice_weeks = generate_invoice_weeks(milestone, start_date, weeks_data)

    return {
        "practice_weeks": practice_weeks,
        "signoff_weeks": signoff_weeks,
        "invoice_weeks": invoice_weeks
    }


def regenerate_project_milestone_weeks(database: Database, project_id: str) -> None:
    """
    Regenerate week data for all milestones in a project.
    Called after creating or updating any milestone.
    """
    milestones = list(database["milestones"].find({"project_id": str(project_id)}))

    for milestone in milestones:
        week_data = generate_all_milestone_weeks(database, milestone)
        database["milestones"].update_one(
            {"id": milestone.get("id")},
            {"$set": week_data}
        )


def get_milestone_health(database: Database, project_id: str) -> dict[str, Any]:
    """
    Get milestone health tracker data for a project.
    Uses pre-calculated week data stored in milestones collection.

    Filtering Logic:
    - Practice weeks: Show if actual_end_eta exists
    - Signoff: Show if actual_end_eta exists (calculate status based on client_signoff_status)
    - Invoice: Show if actual_end_eta exists (calculate status based on invoice_status)
    """
    # Fetch project
    project = database["projects"].find_one({"id": str(project_id)})
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    # Fetch all milestones for this project
    milestones = list(database["milestones"].find({"project_id": str(project_id)}))
    if not milestones:
        raise HTTPException(status_code=404, detail="No milestones found for this project")

    # Collect all week data from milestones
    practice_data = []
    signoff_data = []
    invoice_data = []
    all_weeks_collected: dict[int, dict[str, Any]] = {}

    for milestone in milestones:
        milestone_code = milestone.get("milestone_code", "")
        description = milestone.get("description", "")
        milestone_id = milestone.get("id")
        actual_end_eta = milestone.get("actual_end_eta")

        # Get practice weeks (from stored data)
        # Show practice weeks only if actual_end_eta exists
        practice_weeks = milestone.get("practice_weeks", [])
        if practice_weeks and actual_end_eta:
            # Determine overall color based on final week status
            final_week = practice_weeks[-1]
            practice_color = final_week.get("color", "gray")

            # Collect weeks for all_weeks map
            for week in practice_weeks:
                week_num = week.get("week_number", 0)
                if week_num not in all_weeks_collected:
                    all_weeks_collected[week_num] = {
                        "label": week.get("week_label", ""),
                        "start": week.get("date", "")
                    }

            actual_start = parse_date(milestone.get("actual_start"))
            actual_end = parse_date(milestone.get("actual_end_eta"))

            practice_data.append({
                "id": milestone_id,
                "milestone_code": milestone_code,
                "description": description,
                "milestone_type": "practice",
                "start_date": actual_start.isoformat() if actual_start else None,
                "end_date": actual_end.isoformat() if actual_end else None,
                "weeks": practice_weeks,
                "completion_pct": milestone.get("completion_pct", 0),
                "status": milestone.get("status", ""),
                "color": practice_color,
                "days_variance": milestone.get("days_variance", 0)
            })

        # Get signoff data
        # Show signoff if actual_end_eta exists, regardless of whether signoff_weeks exists
        if actual_end_eta:
            signoff_weeks = milestone.get("signoff_weeks", [])
            
            # Collect weeks for all_weeks map if they exist
            for week in signoff_weeks:
                week_num = week.get("week_number", 0)
                if week_num not in all_weeks_collected:
                    all_weeks_collected[week_num] = {
                        "label": week.get("week_label", ""),
                        "start": week.get("date", "")
                    }

            signoff_display_date = parse_date(milestone.get("signedoff_date")) or parse_date(milestone.get("actual_end_eta"))
            
            # Calculate signoff status based on client_signoff_status
            client_signoff_status = milestone.get("client_signoff_status", "")
            if client_signoff_status == "Done":
                calculated_signoff_status = "Done"
            elif client_signoff_status == "Partial":
                calculated_signoff_status = "Partial"
            else:
                calculated_signoff_status = "Pending"

            signoff_data.append({
                "id": milestone_id,
                "milestone_code": milestone_code,
                "description": description,
                "milestone_type": "signoff",
                "date": signoff_display_date.isoformat() if signoff_display_date else None,
                "signoff_status": client_signoff_status,
                "status": calculated_signoff_status
            })

        # Get invoice data
        # Show invoice if actual_end_eta exists, regardless of whether invoice_weeks exists
        if actual_end_eta:
            invoice_weeks = milestone.get("invoice_weeks", [])
            
            # Collect weeks for all_weeks map if they exist
            for week in invoice_weeks:
                week_num = week.get("week_number", 0)
                if week_num not in all_weeks_collected:
                    all_weeks_collected[week_num] = {
                        "label": week.get("week_label", ""),
                        "start": week.get("date", "")
                    }

            invoice_display_date = parse_date(milestone.get("invoice_raised_date")) or parse_date(milestone.get("actual_end_eta"))
            
            # Calculate invoice status based on invoice_status
            invoice_status = milestone.get("invoice_status", "")
            if invoice_status == "Done":
                calculated_invoice_status = "Done"
            elif invoice_status == "Partial":
                calculated_invoice_status = "Partial"
            else:
                calculated_invoice_status = "Pending"

            invoice_data.append({
                "id": milestone_id,
                "milestone_code": milestone_code,
                "description": description,
                "milestone_type": "invoice",
                "date": invoice_display_date.isoformat() if invoice_display_date else None,
                "invoice_status": invoice_status,
                "status": calculated_invoice_status
            })

    # If no weeks were collected, return empty
    if not all_weeks_collected:
        return {
            "project_id": project_id,
            "project_name": project.get("name", ""),
            "practice": practice_data,
            "signoff": signoff_data,
            "invoice": invoice_data,
            "weeks_range": {
                "start_week": "",
                "end_week": "",
                "total_weeks": 0
            },
            "all_weeks": {}
        }

    # Sort weeks by number
    sorted_week_nums = sorted(all_weeks_collected.keys())
    weeks_range = {
        "start_week": all_weeks_collected[sorted_week_nums[0]]["label"] if sorted_week_nums else "",
        "end_week": all_weeks_collected[sorted_week_nums[-1]]["label"] if sorted_week_nums else "",
        "total_weeks": len(all_weeks_collected)
    }

    return {
        "project_id": project_id,
        "project_name": project.get("name", ""),
        "practice": practice_data,
        "signoff": signoff_data,
        "invoice": invoice_data,
        "weeks_range": weeks_range,
        "all_weeks": all_weeks_collected
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
    completed_filter = {"status": {"$regex": "^Completed$", "$options": "i"}}

    return {
        "total_projects": projects.count_documents({}),
        "active_projects": projects.count_documents(active_filter),
        "on_track_projects": projects.count_documents(on_track_filter),
        "at_risk_projects": projects.count_documents(at_risk_filter),
        "blocked_projects": projects.count_documents(blocked_filter),
        "completed_projects": projects.count_documents(completed_filter),
    }


def get_team_members_with_engagement(database: Database) -> list[dict[str, Any]]:
    """
    Get all team members with their engagement metrics aggregated from team_members_engagement collection.
    
    Returns:
        List of team members with engagement_pct and other engagement data
    """
    team_members_collection = database["team_members"]
    engagement_collection = database["team_members_engagement"]
    
    # Get all team members
    team_members = [to_plain_document(doc) for doc in team_members_collection.find({})]
    
    # Enhance each team member with engagement data
    result = []
    for member in team_members:
        member_id = member.get("id")
        
        # Get all engagement records for this member
        engagements = list(engagement_collection.find({"team_member_id": member_id}))
        
        if engagements:
            # Calculate average engagement percentage from engagement_level or engagement_percentage
            engagement_levels = []
            for eng in engagements:
                # Try engagement_percentage first, then engagement_level
                level = eng.get("engagement_percentage")
                if level is None:
                    level = eng.get("engagement_level")
                
                if level is not None:
                    # Convert to float if it's a string
                    try:
                        level = float(level)
                        engagement_levels.append(level)
                    except (ValueError, TypeError):
                        pass
            
            avg_engagement_pct = sum(engagement_levels) / len(engagement_levels) if engagement_levels else 0
            
            # Calculate total hours and tasks
            total_hours = sum(eng.get("engagement_hours", 0) for eng in engagements)
            total_tasks_completed = sum(eng.get("task_completed", 0) for eng in engagements)
            total_tasks_pending = sum(eng.get("task_pending", 0) for eng in engagements)
            project_count = len(engagements)
            
            # Add engagement data to member
            member["engagement_pct"] = round(avg_engagement_pct, 2)
            member["total_engagement_hours"] = total_hours
            member["total_tasks_completed"] = total_tasks_completed
            member["total_tasks_pending"] = total_tasks_pending
            member["projects_assigned"] = project_count
            member["engagements"] = [to_plain_document(eng) for eng in engagements]
        else:
            # No engagements found
            member["engagement_pct"] = 0
            member["total_engagement_hours"] = 0
            member["total_tasks_completed"] = 0
            member["total_tasks_pending"] = 0
            member["projects_assigned"] = 0
            member["engagements"] = []
        
        result.append(member)
    
    return result


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

        # Regenerate week data for milestones
        if table == "milestones":
            for doc in documents:
                project_id = doc.get("project_id")
                if project_id:
                    regenerate_project_milestone_weeks(database, project_id)

        return [to_plain_document(document) or {} for document in documents]

    document = normalize_on_insert(table, payload)
    database[table].insert_one(document)

    # Regenerate week data for milestones
    if table == "milestones":
        project_id = document.get("project_id")
        if project_id:
            regenerate_project_milestone_weeks(database, project_id)

    return to_plain_document(document) or {}


def update_milestone_health(database: Database, milestone_id: str, milestone_type: str, status: str, date: str | None = None) -> dict[str, Any] | None:
    """
    Update a specific milestone health type (practice, signoff, or invoice).

    Args:
        database: MongoDB database connection
        milestone_id: ID of the milestone to update
        milestone_type: Type of update - "practice", "signoff", or "invoice"
        status: New status value
        date: Optional date for signoff/invoice (when marked as "Done")

    Returns:
        Updated milestone document

    Example:
        # Update practice status
        update_milestone_health(db, "m123", "practice", "At Risk")

        # Mark signoff as done with date
        update_milestone_health(db, "m123", "signoff", "Done", "2026-02-07")
    """
    if milestone_type not in {"practice", "signoff", "invoice"}:
        raise HTTPException(status_code=400, detail=f"Invalid milestone type: {milestone_type}")

    # Validate status values
    if milestone_type == "practice":
        valid_statuses = {"On Track", "At Risk", "Blocked", "Completed"}
        if status not in valid_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid practice status: {status}")
        update_dict = {"status": status}

    elif milestone_type == "signoff":
        valid_statuses = {"Done", "Pending"}
        if status not in valid_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid signoff status: {status}")
        update_dict = {"client_signoff_status": status}
        if status == "Done" and date:
            update_dict["signedoff_date"] = date

    elif milestone_type == "invoice":
        valid_statuses = {"Done", "Pending"}
        if status not in valid_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid invoice status: {status}")
        update_dict = {"invoice_status": status}
        if status == "Done" and date:
            update_dict["invoice_raised_date"] = date

    # Perform the update
    return patch_record(database, "milestones", milestone_id, update_dict)



def update_week_status(
    database: Database,
    milestone_id: str,
    milestone_type: str,
    week_number: int,
    payload: dict[str, Any]
) -> dict[str, Any] | None:
    """
    Update status for a specific week in a milestone.

    If the week exists, it will be updated.
    If the week doesn't exist, it will be created and added to the array.
    All other weeks remain unchanged.

    Args:
        database: MongoDB database connection
        milestone_id: ID of the milestone to update
        milestone_type: Type of update - "practice", "signoff", or "invoice"
        week_number: The week number to update or create
        payload: Week update payload containing:
            {
                "week_status": "At Risk",        // New status (required)
                "week_label": "Feb 16-22, 2026", // Display label (required)
                "color": "orange",               // Color indicator (required)
                "date": "2026-02-20"             // Week date (required)
            }

    Returns:
        Updated milestone document with the week added/updated

    Example:
        # Update existing week 2
        update_week_status(
            db, "m1", "practice", 2,
            {
                "week_status": "At Risk",
                "week_label": "Feb 16-22, 2026",
                "color": "orange",
                "date": "2026-02-20"
            }
        )

        # Create new week 5 if it doesn't exist
        update_week_status(
            db, "m1", "practice", 5,
            {
                "week_status": "On Track",
                "week_label": "Mar 9-15, 2026",
                "color": "green",
                "date": "2026-03-09"
            }
        )
    """
    # Validate milestone type
    if milestone_type not in {"practice", "signoff", "invoice"}:
        raise HTTPException(status_code=400, detail="Invalid milestone type")

    # Get the milestone
    milestone = database["milestones"].find_one({"id": milestone_id})
    if not milestone:
        raise HTTPException(status_code=404, detail="Milestone not found")

    # Get the appropriate weeks array key
    if milestone_type == "practice":
        weeks_key = "practice_weeks"
    elif milestone_type == "signoff":
        weeks_key = "signoff_weeks"
    else:  # invoice
        weeks_key = "invoice_weeks"

    # Get the weeks array
    weeks_array = milestone.get(weeks_key, [])

    # Find and update the week, or create it if it doesn't exist
    updated = False
    for i, week in enumerate(weeks_array):
        if week.get("week_number") == week_number:
            # Update existing week with new data
            weeks_array[i]["status"] = payload.get("week_status", week.get("status"))
            weeks_array[i]["color"] = payload.get("color", week.get("color"))
            weeks_array[i]["week_label"] = payload.get("week_label", week.get("week_label"))
            weeks_array[i]["date"] = payload.get("date", week.get("date"))
            updated = True
            break

    if not updated:
        # Create new week if it doesn't exist
        new_week = {
            "week_number": week_number,
            "status": payload.get("week_status"),
            "color": payload.get("color"),
            "week_label": payload.get("week_label"),
            "date": payload.get("date")
        }
        weeks_array.append(new_week)
        # Sort by week_number to maintain chronological order
        weeks_array.sort(key=lambda w: w.get("week_number", 0))

    # Save updated weeks array
    database["milestones"].update_one(
        {"id": milestone_id},
        {"$set": {weeks_key: weeks_array, "updated_at": utc_now_iso()}}
    )

    # Return updated milestone
    updated_milestone = database["milestones"].find_one({"id": milestone_id})
    return to_plain_document(updated_milestone)


def patch_record(database: Database, table: str, record_id: str, changes: Any) -> dict[str, Any] | None:
    ensure_table(table)

    if not isinstance(changes, dict):
        raise HTTPException(status_code=400, detail="Invalid body")

    next_changes = deepcopy(changes)
    if table in {"projects", "milestones", "team_members"}:
        next_changes["updated_at"] = utc_now_iso()

    database[table].update_one({"id": str(record_id)}, {"$set": next_changes})
    updated = database[table].find_one({"id": str(record_id)})

    # Regenerate week data for milestones if relevant fields changed
    if table == "milestones" and updated:
        project_id = updated.get("project_id")
        # Check if any week-generating fields were modified
        week_fields = {"actual_start", "actual_end_eta", "status", "client_signoff_status",
                      "signedoff_date", "invoice_status", "invoice_raised_date"}
        if project_id and any(field in next_changes for field in week_fields):
            regenerate_project_milestone_weeks(database, project_id)

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

    # Regenerate week data for milestones
    if table == "milestones" and updated:
        project_id = updated.get("project_id")
        if project_id:
            regenerate_project_milestone_weeks(database, project_id)

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