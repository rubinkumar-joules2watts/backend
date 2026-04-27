from __future__ import annotations

import logging
import re
from typing import Any

from .dependencies import get_ai_service
from .gemini_service import GeminiGenerateConfig

logger = logging.getLogger("delivery_tracker.ai.insight")

SYSTEM_PROMPT = """You are a senior PMO Director specializing in high-stakes corporate reporting.
Your goal is to synthesize raw project logs into concise, evidence-based status reports.

STRICT STYLE RULES:
1. NO EMOJIS: Never use emojis, symbols, or pictograms. Use plain, professional text ONLY.
2. BULLET POINTS: Present all insights as a series of succinct bullet points starting with "- ".
3. EXECUTIVE TONE: Use formal, objective, and high-impact corporate language (e.g., "Synthesized," "Expedited," "Mitigated," "At-Risk").
4. EVIDENCE-BASED: Use ONLY facts present in the provided logs/context. Do not invent metrics, dates, scope, or decisions.
5. DATE DISCIPLINE: If a date is referenced, use the date provided in the logs; do not infer.
6. STRUCTURE: Prefer labeled bullets (e.g., "- Achievements: ...", "- In progress: ...", "- Risks/Dependencies: ...", "- Next steps: ...").
7. CONCISENESS: Keep each point punchy and direct (1 sentence per bullet). Avoid fluff and repetition.
8. NO QUOTING: Do not copy long passages from logs; summarize instead.
9. SAFETY: If evidence is insufficient, state "Not specified in the provided logs" rather than guessing."""

PROJECT_REPORT_SYSTEM_PROMPT = """You are a senior project reporting assistant.

Follow the user prompt exactly.
Do not add any sections or prose outside the requested bullet output format.
Use only provided evidence and do not invent details."""


def _window_label(start_date: str | None, end_date: str | None) -> str:
    if start_date and end_date:
        return f"{start_date} to {end_date}"
    if start_date:
        return f"from {start_date}"
    if end_date:
        return f"until {end_date}"
    return "unspecified"


def _clean_event_text(raw: str) -> str:
    text = re.sub(r"\s+", " ", (raw or "").strip())
    # Remove any existing emojis from raw text to ensure they don't leak in
    text = text.encode('ascii', 'ignore').decode('ascii')
    if not text:
        return "update logged"
    words = text.split()
    if len(words) > 60:
        return " ".join(words[:60]).rstrip(",") + "..."
    return text


def _extract_date_and_text(log_line: str) -> tuple[str, str]:
    if ":" not in log_line:
        return "", _clean_event_text(log_line)
    left, right = log_line.split(":", 1)
    date_part = left.strip()
    text_part = _clean_event_text(right)
    return date_part, text_part


def _contains_risk_signal(text: str) -> bool:
    lower = text.lower()
    risk_tokens = (
        "action needed",
        "blocker",
        "blocked",
        "awaiting",
        "pending",
        "issue",
        "limitation",
        "workaround",
        "failed",
        "risk",
    )
    return any(token in lower for token in risk_tokens)


def _fallback_milestone_story(name: str, description: str, status: str, logs: list[str]) -> str:
    if not logs:
        return (
            f"- Milestone: {name} | Status: {status}.\n"
            "- Progress: No in-window progress updates were recorded.\n"
            "- Update cadence: Not specified in the provided logs.\n"
            "- Risks/Blockers: Not specified in the provided logs.\n"
            "- Next steps: Confirm upcoming deliverables and required stakeholder actions."
        )

    first_date, first_text = _extract_date_and_text(logs[0])
    last_date, last_text = _extract_date_and_text(logs[-1])

    risk_count = 0
    for entry in logs:
        _, et = _extract_date_and_text(entry)
        if _contains_risk_signal(et):
            risk_count += 1

    cadence = f"{len(logs)} update(s) captured; latest on {last_date or 'window end'}."

    lines: list[str] = []
    lines.append(f"- Milestone: {name} | Status: {status}.")
    lines.append(f"- Progress: {first_text} (first logged {first_date or 'window start'}).")
    lines.append(f"- Latest: {last_text} (logged {last_date or 'window end'}).")
    lines.append(f"- Update cadence: {cadence}")
    if risk_count > 0:
        lines.append("- Risks/Blockers: Risks and dependencies are referenced in-window and require active follow-up.")
    else:
        lines.append("- Risks/Blockers: No explicit in-window blockers were recorded in the provided logs.")
    return "\n".join(lines)


def _fallback_project_summary(
    project_name: str,
    milestone_summaries: list[dict[str, str]],
    start_date: str | None,
    end_date: str | None,
) -> str:
    window = _window_label(start_date, end_date)
    if not milestone_summaries:
        return f"- No captured milestone activity identified for the reporting window ({window})."

    summary_texts = [str(item.get("summary") or "") for item in milestone_summaries]
    risk_mentions = sum(1 for s in summary_texts if _contains_risk_signal(s))
    completed_mentions = sum(
        1
        for s in summary_texts
        if any(token in s.lower() for token in ("completed", "done", "finalized", "signed off"))
    )
    total = len(milestone_summaries)

    lines: list[str] = []
    lines.append(f"- Reporting window: {window}.")
    lines.append(f"- Delivery posture: {project_name} shows in-window activity across {total} milestone workstream(s).")
    if completed_mentions > 0:
        lines.append(f"- Achievements: {completed_mentions} milestone summary(s) reference completion/finalization signals.")
    
    if risk_mentions > 0:
        lines.append("- Risks/Dependencies: In-window risks, blockers, or dependencies are present and require follow-up.")
    else:
        lines.append("- Risks/Dependencies: No major in-window blockers are explicitly captured in the provided summaries.")

    lines.append("- Next steps: Close open dependencies, confirm decisions, and maintain weekly reporting cadence.")
    
    return "\n".join(lines)


def _truncate_bullet_text(text: str, max_words: int = 18) -> str:
    clean = re.sub(r"\s+", " ", (text or "").strip())
    if not clean:
        return ""
    words = clean.split(" ")
    if len(words) <= max_words:
        return clean
    return " ".join(words[:max_words]).rstrip(",.;:") + "..."


def _compact_project_report(summary: str) -> str:
    """
    Force concise two-section output:
    - Project Updates: max 4 bullets
    - Milestones: max 5 bullets
    Also trims verbose bullets to keep PDF output compact.
    """
    raw_lines = [ln.rstrip() for ln in (summary or "").splitlines()]

    project_bullets: list[str] = []
    milestone_bullets: list[str] = []
    section: str | None = None

    for line in raw_lines:
        stripped = line.strip()
        if not stripped:
            continue

        lower = stripped.lower()
        if "project updates" in lower:
            section = "project"
            continue
        if "milestones" in lower:
            section = "milestone"
            continue

        if stripped.startswith("- "):
            bullet = _truncate_bullet_text(stripped[2:], max_words=18)
            if not bullet:
                continue
            if section == "project":
                if len(project_bullets) < 4:
                    project_bullets.append(f"- {bullet}")
            elif section == "milestone":
                if len(milestone_bullets) < 5:
                    milestone_bullets.append(f"- {bullet}")
            else:
                if len(project_bullets) < 4:
                    project_bullets.append(f"- {bullet}")

    if not project_bullets:
        project_bullets.append("- Not specified in the provided logs.")
    if not milestone_bullets:
        milestone_bullets.append("- Not specified in the provided logs.")

    out: list[str] = []
    out.append("## Project Updates")
    out.extend(project_bullets)
    out.append("")
    out.append("## Milestones")
    out.extend(milestone_bullets)
    return "\n".join(out).strip()


async def generate_milestone_insight(
    milestone_name: str,
    description: str,
    status: str,
    logs: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    milestone_meta: dict[str, Any] | None = None,
) -> str:
    """
    Generate a meaningful summary for a specific milestone based on logs and current status.
    """
    meta = milestone_meta or {}
    milestone_code = meta.get("milestone_code") or ""
    planned_start = meta.get("planned_start")
    planned_end = meta.get("planned_end") or meta.get("planned_end_eta")
    actual_start = meta.get("actual_start")
    actual_end = meta.get("actual_end_eta") or meta.get("actual_end")
    completion_pct = meta.get("completion_pct")
    invoice_status = meta.get("invoice_status")
    signoff_status = meta.get("client_signoff_status")
    blocker = meta.get("blocker")

    prompt = (
        f"Milestone: {milestone_name}\n"
        f"Description: {description}\n"
        f"Current Status: {status}\n"
        f"Milestone Code: {milestone_code}\n"
        f"Planned Start: {planned_start}\n"
        f"Planned End: {planned_end}\n"
        f"Actual Start: {actual_start}\n"
        f"Actual End/Eta: {actual_end}\n"
        f"Completion %: {completion_pct}\n"
        f"Invoice Status: {invoice_status}\n"
        f"Client Signoff Status: {signoff_status}\n"
        f"Blocker: {blocker}\n"
        f"Reporting Window: {_window_label(start_date, end_date)}\n"
        "Raw Event Logs for this window:\n"
        + ("\n".join(logs) if logs else "No specific progress logs were recorded during this reporting window.") + "\n\n"
        "Task:\n"
        "Generate EXACTLY 6 bullet points (each must start with '- ').\n"
        "Use this structure and keep each bullet to one sentence:\n"
        "- Milestone: name/code, status, and reporting window.\n"
        "- Schedule: planned vs actual (if values exist); flag if inconsistent.\n"
        "- Progress: 1-2 concrete in-window outcomes.\n"
        "- Update cadence: summarize frequency and latest update date based on logs.\n"
        "- Risks/Blockers: blockers, approvals, access issues, or uncertainties evidenced in-window.\n"
        "- Commercials: invoice/signoff/completion status (or state not specified).\n"
        "Rules: Use ONLY the provided fields and in-window logs; do not invent; no emojis; no long quotes."
    )
    
    try:
        ai = get_ai_service()
        summary = await ai.generate_text(
            prompt=prompt,
            system_instruction=SYSTEM_PROMPT,
            config=GeminiGenerateConfig(temperature=0.1, max_output_tokens=250)
        )
        cleaned = (summary or "").strip()
        if cleaned:
            return cleaned
        return _fallback_milestone_story(milestone_name, description, status, logs)
    except Exception as e:
        logger.error(f"Failed to generate insight for {milestone_name}: {e}")
        return _fallback_milestone_story(milestone_name, description, status, logs)

async def generate_project_executive_summary(
    project_name: str,
    milestone_summaries: list[dict[str, str]],
    project_updates: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> str:
    """
    Synthesize a high-level project summary based on milestone insights.
    """
    window = _window_label(start_date, end_date)
    if not milestone_summaries:
        return f"- No captured milestone activity identified for the reporting window ({window})."

    milestone_context = "\n".join([f"- {m['name']}: {m['summary']}" for m in milestone_summaries])
    updates = project_updates or []
    updates_context = "\n".join([f"- {line}" for line in updates]) if updates else "- (none provided)"
    prompt = (
        "You are a senior project reporting assistant.\n\n"
        "Your task is to generate a clean, professional project summary from the given data.\n\n"
        "STRICT INSTRUCTIONS:\n"
        "- Output ONLY bullet points (no paragraphs).\n"
        "- Keep language concise, clear, and business-friendly.\n"
        "- Keep output short and crisp.\n"
        "- Do NOT repeat raw text; summarize meaningfully.\n"
        "- Remove unnecessary words, keep it sharp.\n"
        "- Maintain chronological and logical flow.\n"
        "- Do NOT hallucinate or add extra assumptions.\n\n"
        "FORMAT:\n\n"
        "## 🔹 Project Updates\n"
        "- [Summarize key updates in 1 line each]\n"
        "- [Focus on actions, decisions, issues, outcomes]\n"
        "- [Max 4 bullets]\n\n"
        "## 🔹 Milestones\n"
        "- [Milestone name + status + duration]\n"
        "- [Progress summary]\n"
        "- [Weekly status trend]\n"
        "- [Blockers if any]\n"
        "- [Invoice or completion status]\n\n"
        "GUIDELINES:\n"
        "- Merge similar updates into one bullet.\n"
        "- Highlight risks or concerns clearly.\n"
        "- Convert long updates into crisp action-oriented statements.\n"
        "- If completion % seems incorrect vs status, flag it briefly.\n\n"
        "INPUT DATA:\n"
        "<Project Details>\n"
        f"- Project: {project_name}\n"
        f"- Reporting Window: {window}\n\n"
        "<Project Updates>\n"
        f"{updates_context}\n\n"
        "<Milestones>\n"
        f"{milestone_context}\n\n"
        "Tone: Executive summary for leadership (C-level), extremely crisp."
    )
    
    try:
        ai = get_ai_service()
        summary = await ai.generate_text(
            prompt=prompt,
            system_instruction=PROJECT_REPORT_SYSTEM_PROMPT,
            config=GeminiGenerateConfig(temperature=0.1, max_output_tokens=400)
        )
        cleaned = (summary or "").strip()
        if cleaned:
            return _compact_project_report(cleaned)
        return _fallback_project_summary(project_name, milestone_summaries, start_date, end_date)
    except Exception as e:
        logger.error(f"Failed to generate executive summary for {project_name}: {e}")
        return _fallback_project_summary(project_name, milestone_summaries, start_date, end_date)
