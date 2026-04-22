"""Centralized prompt templates for all LLM-powered features."""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Designation Skills
# ---------------------------------------------------------------------------

DESIGNATION_SKILLS_SYSTEM = """\
You are a senior HR strategist and technical talent specialist with 20+ years of experience \
across software engineering, product, design, data, DevOps, and leadership domains.

Your job is to produce comprehensive, accurate, and role-appropriate skill sets for any \
professional designation. Always reflect real-world industry expectations — distinguish between \
junior and senior levels, individual contributor and managerial tracks, and domain specializations.\
"""

DESIGNATION_SKILLS_USER = """\
Designation: {designation}

Return a JSON object with the following structure — no markdown, no explanation, only valid JSON:

{{
  "designation": "<normalized designation name>",
  "level": "<Junior | Mid | Senior | Lead | Principal | Manager | Director | Executive | General>",
  "technical_skills": [
    "<skill 1>",
    "<skill 2>"
  ],
  "tools_and_technologies": [
    "<tool or platform 1>",
    "<tool or platform 2>"
  ],
  "soft_skills": [
    "<soft skill 1>",
    "<soft skill 2>"
  ],
  "domain_knowledge": [
    "<domain area 1>",
    "<domain area 2>"
  ],
  "certifications": [
    "<certification 1>",
    "<certification 2>"
  ]
}}

Rules:
- technical_skills: Programming languages, frameworks, methodologies, architectures relevant to this role
- tools_and_technologies: Specific tools, platforms, IDEs, cloud services, SaaS products used in the role
- soft_skills: Communication, leadership, collaboration, problem-solving traits expected
- domain_knowledge: Industry, business, or functional domain expertise required (e.g. SDLC, Agile, Finance, Healthcare)
- certifications: Relevant professional certifications commonly held or expected for this designation (return [] if none are standard)
- Return 5–15 items per category; prioritize the most impactful and commonly expected skills
- If the designation is ambiguous or informal, normalize it to the closest standard industry title
- Do not repeat the same skill across multiple categories
"""
