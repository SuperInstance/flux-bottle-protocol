"""
Bottle schema definitions: types, frontmatter, body validation.

Uses only Python stdlib — YAML frontmatter is parsed with regex.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class BottleType(str, Enum):
    """Canonical bottle types per BOTTLE-SPEC.md §3."""

    INTRODUCTION = "INTRODUCTION"
    CLAIM = "CLAIM"
    MESSAGE = "MESSAGE"
    RESPONSE = "RESPONSE"
    STATUS_UPDATE = "STATUS_UPDATE"
    BROADCAST = "BROADCAST"
    RFC_SUBMISSION = "RFC_SUBMISSION"
    TASK_COMPLETION = "TASK_COMPLETION"


class Priority(str, Enum):
    PRIORITY_CRITICAL = "critical"
    PRIORITY_HIGH = "high"
    PRIORITY_MEDIUM = "medium"
    PRIORITY_LOW = "low"


class TrustLevel(str, Enum):
    TRUST_VERIFIED = "verified"
    TRUST_STANDARD = "standard"
    TRUST_UNVERIFIED = "unverified"


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ValidationIssue:
    """A single validation finding."""

    severity: Severity
    field: str
    message: str
    suggestion: str = ""

    def __str__(self) -> str:
        prefix = f"[{self.severity.value.upper()}]"
        parts = [f"{prefix} {self.field}: {self.message}"]
        if self.suggestion:
            parts.append(f"  → {self.suggestion}")
        return "\n".join(parts)


@dataclass
class BottleFrontmatter:
    """Structured frontmatter for a bottle."""

    from_agent: str
    to: str
    type: BottleType
    date: str  # ISO 8601
    subject: str

    # Optional fields
    priority: Priority = Priority.PRIORITY_MEDIUM
    reply_to: Optional[str] = None
    task_refs: List[str] = field(default_factory=list)
    repo_refs: List[str] = field(default_factory=list)
    trust_level: TrustLevel = TrustLevel.TRUST_STANDARD

    # Raw dict for round-trip fidelity
    raw: dict = field(default_factory=dict, repr=False)

    # The source filename this frontmatter came from (if parsed from file)
    source_file: Optional[str] = None

    def to_dict(self) -> dict:
        """Serialize back to a YAML-compatible dict."""
        d: dict = {
            "from": self.from_agent,
            "to": self.to,
            "type": self.type.value,
            "date": self.date,
            "subject": self.subject,
        }
        if self.priority != Priority.PRIORITY_MEDIUM:
            d["priority"] = self.priority.value
        if self.reply_to:
            d["reply_to"] = self.reply_to
        if self.task_refs:
            d["task_refs"] = list(self.task_refs)
        if self.repo_refs:
            d["repo_refs"] = list(self.repo_refs)
        if self.trust_level != TrustLevel.TRUST_STANDARD:
            d["trust_level"] = self.trust_level.value
        return d


@dataclass
class Bottle:
    """A complete bottle: frontmatter + markdown body."""

    frontmatter: BottleFrontmatter
    body: str
    source_path: Optional[Path] = None

    @property
    def bottle_id(self) -> str:
        """Derive the canonical filename from frontmatter."""
        fm = self.frontmatter
        try:
            dt = datetime.fromisoformat(fm.date.replace("Z", "+00:00"))
            ts = dt.strftime("%Y%m%d-%H%M%S")
        except (ValueError, AttributeError):
            ts = "unknown"
        return f"{fm.type.value}-{fm.from_agent}-{ts}"


# ---------------------------------------------------------------------------
# Frontmatter parser (no external deps — regex-based)
# ---------------------------------------------------------------------------

_FRONTMATTER_RE = re.compile(
    r"^---[ \t]*\n(.*?)\n---[ \t]*\n(.*)$",
    re.DOTALL,
)

# Also match empty frontmatter: ---\n---\nBody
_FRONTMATTER_EMPTY_RE = re.compile(
    r"^---[ \t]*\n---[ \t]*\n(.*)$",
    re.DOTALL,
)


def _parse_yaml_simple(raw: str) -> dict:
    """
    Extremely minimal YAML-like parser for flat or shallow structures.

    Handles:
      - key: value  (strings, numbers, booleans)
      - key: "quoted string"
      - key:
          - item1
          - item2
    """
    result: dict = {}
    lines = raw.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Skip empty lines and comments
        if not stripped or stripped.startswith("#"):
            i += 1
            continue

        # Check for list continuation (starts with "- ")
        if stripped.startswith("- "):
            i += 1
            continue

        # Key: value
        m = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.*)", stripped)
        if not m:
            i += 1
            continue

        key = m.group(1)
        value_raw = m.group(2).strip()

        # Check if next lines form a list under this key
        list_items: list = []
        j = i + 1
        while j < len(lines) and lines[j].strip().startswith("- "):
            item = re.sub(r"^\s*-\s*", "", lines[j]).strip().strip("\"'")
            list_items.append(item)
            j += 1

        if list_items:
            result[key] = list_items
            i = j
        else:
            result[key] = _parse_scalar(value_raw)
            i += 1

    return result


def _parse_scalar(value: str) -> Any:
    """Parse a YAML scalar value."""
    if not value:
        return ""
    # Quoted string
    if (value.startswith('"') and value.endswith('"')) or \
       (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    # Boolean
    if value.lower() in ("true", "yes"):
        return True
    if value.lower() in ("false", "no"):
        return False
    # Integer
    try:
        return int(value)
    except ValueError:
        pass
    # Float
    try:
        return float(value)
    except ValueError:
        pass
    return value


def parse_frontmatter(text: str) -> tuple[dict | None, str]:
    """
    Extract YAML frontmatter and body from a bottle file.

    Returns (frontmatter_dict, body_markdown).
    If no frontmatter found, returns (None, text).
    """
    m = _FRONTMATTER_RE.match(text)
    if not m:
        m = _FRONTMATTER_EMPTY_RE.match(text)
        if not m:
            return None, text
        return {}, m.group(1)
    yaml_raw = m.group(1)
    body = m.group(2)
    return _parse_yaml_simple(yaml_raw), body


# ---------------------------------------------------------------------------
# Bottle Validator
# ---------------------------------------------------------------------------

class BottleValidator:
    """Validates bottles against the protocol specification."""

    def __init__(
        self,
        known_agents: list[str] | None = None,
        known_roles: list[str] | None = None,
        known_caps: list[str] | None = None,
    ):
        self.known_agents = set(known_agents or [])
        self.known_roles = set(known_roles or [])
        self.known_caps = set(known_caps or [])

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(self, bottle: Bottle) -> list[ValidationIssue]:
        """Run all validations on a bottle. Returns list of issues."""
        issues: list[ValidationIssue] = []
        issues.extend(self.validate_frontmatter(bottle.frontmatter))
        issues.extend(self.validate_body(bottle.body, bottle.frontmatter.type))
        return issues

    def validate_frontmatter(self, fm: BottleFrontmatter) -> list[ValidationIssue]:
        """Validate frontmatter fields."""
        issues: list[ValidationIssue] = []

        # --- required fields ---
        if not fm.from_agent or not fm.from_agent.strip():
            issues.append(ValidationIssue(
                Severity.ERROR, "from",
                "'from' field is required and must be non-empty.",
                "Set 'from' to your agent identifier, e.g. 'Quill'.",
            ))

        if not fm.to or not fm.to.strip():
            issues.append(ValidationIssue(
                Severity.ERROR, "to",
                "'to' field is required and must be non-empty.",
                "Set 'to' to 'fleet', an agent name, 'role:<role>', or 'cap:<capability>'.",
            ))
        else:
            # Validate target format
            issues.extend(self._validate_target(fm.to))

        if not fm.subject or not fm.subject.strip():
            issues.append(ValidationIssue(
                Severity.ERROR, "subject",
                "'subject' field is required and must be non-empty.",
                "Provide a short 1-line summary.",
            ))

        # --- date ---
        if not fm.date:
            issues.append(ValidationIssue(
                Severity.ERROR, "date",
                "'date' field is required.",
                "Use ISO 8601 UTC format, e.g. '2026-04-12T15:30:00Z'.",
            ))
        else:
            issues.extend(self._validate_date(fm.date))

        # --- type-specific rules ---
        if fm.type == BottleType.RESPONSE and not fm.reply_to:
            issues.append(ValidationIssue(
                Severity.ERROR, "reply_to",
                "RESPONSE bottles MUST include 'reply_to'.",
                "Set 'reply_to' to the filename of the bottle you are responding to.",
            ))

        if fm.type == BottleType.CLAIM and not fm.task_refs:
            issues.append(ValidationIssue(
                Severity.WARNING, "task_refs",
                "CLAIM bottles should include 'task_refs'.",
                "Add 'task_refs: [R3]' referencing the task being claimed.",
            ))

        if fm.type == BottleType.TASK_COMPLETION and not fm.task_refs:
            issues.append(ValidationIssue(
                Severity.WARNING, "task_refs",
                "TASK_COMPLETION bottles should include 'task_refs'.",
                "Add 'task_refs' referencing the completed task(s).",
            ))

        if fm.type == BottleType.BROADCAST and fm.to != "fleet":
            issues.append(ValidationIssue(
                Severity.WARNING, "to",
                "BROADCAST bottles typically target 'fleet'.",
                "Set 'to: fleet' for fleet-wide announcements.",
            ))

        # --- known agents warning ---
        if (
            self.known_agents
            and fm.from_agent
            and fm.from_agent not in self.known_agents
        ):
            issues.append(ValidationIssue(
                Severity.WARNING, "from",
                f"Agent '{fm.from_agent}' is not in the known agents list.",
                f"Known agents: {', '.join(sorted(self.known_agents))}",
            ))

        return issues

    def validate_body(self, body: str, bottle_type: BottleType) -> list[ValidationIssue]:
        """Type-specific body validation."""
        issues: list[ValidationIssue] = []
        stripped = body.strip()

        if not stripped:
            issues.append(ValidationIssue(
                Severity.ERROR, "body",
                "Body must not be empty.",
                "Add markdown content after the frontmatter.",
            ))
            return issues

        # Type-specific checks
        if bottle_type == BottleType.INTRODUCTION:
            # Should mention capabilities or identity
            if len(stripped) < 50:
                issues.append(ValidationIssue(
                    Severity.WARNING, "body",
                    "INTRODUCTION body seems too short (< 50 chars).",
                    "Include agent identity, capabilities, and purpose.",
                ))

        if bottle_type == BottleType.CLAIM:
            # Should have some structure (headings, lists, etc.)
            if not re.search(r"^#{1,3}\s", stripped, re.MULTILINE):
                issues.append(ValidationIssue(
                    Severity.INFO, "body",
                    "CLAIM body has no markdown headings.",
                    "Use headings to structure approach, timeline, and deliverables.",
                ))

        if bottle_type == BottleType.RFC_SUBMISSION:
            if not re.search(r"^#{1,3}\s", stripped, re.MULTILINE):
                issues.append(ValidationIssue(
                    Severity.INFO, "body",
                    "RFC_SUBMISSION body has no markdown headings.",
                    "Use headings for: Motivation, Proposal, Open Questions.",
                ))

        if bottle_type == BottleType.TASK_COMPLETION:
            # Should reference deliverables
            has_links = bool(re.search(r"https?://", stripped))
            has_code_refs = bool(re.search(r"`[^`]+`|```", stripped))
            if not has_links and not has_code_refs:
                issues.append(ValidationIssue(
                    Severity.INFO, "body",
                    "TASK_COMPLETION body has no links or code references.",
                    "Include links to deliverables or code changes.",
                ))

        return issues

    def validate_format(self, file_content: str) -> list[ValidationIssue]:
        """Validate raw file content has proper frontmatter + markdown format."""
        issues: list[ValidationIssue] = []

        if not file_content.startswith("---"):
            issues.append(ValidationIssue(
                Severity.ERROR, "format",
                "File must start with '---' frontmatter delimiter.",
                "Ensure the first line of the file is '---'.",
            ))
            return issues

        fm_data, body = parse_frontmatter(file_content)
        if fm_data is None:
            issues.append(ValidationIssue(
                Severity.ERROR, "format",
                "Could not parse frontmatter block.",
                "Ensure frontmatter is delimited by '---\\n...\\n---'.",
            ))
            return issues

        if "from" not in fm_data:
            issues.append(ValidationIssue(
                Severity.ERROR, "frontmatter.from",
                "Missing required field 'from'.",
            ))
        if "to" not in fm_data:
            issues.append(ValidationIssue(
                Severity.ERROR, "frontmatter.to",
                "Missing required field 'to'.",
            ))
        if "type" not in fm_data:
            issues.append(ValidationIssue(
                Severity.ERROR, "frontmatter.type",
                "Missing required field 'type'.",
            ))
        else:
            valid_types = {t.value for t in BottleType}
            if fm_data["type"] not in valid_types:
                issues.append(ValidationIssue(
                    Severity.ERROR, "frontmatter.type",
                    f"Invalid type '{fm_data['type']}'.",
                    f"Valid types: {', '.join(sorted(valid_types))}",
                ))
        if "date" not in fm_data:
            issues.append(ValidationIssue(
                Severity.ERROR, "frontmatter.date",
                "Missing required field 'date'.",
            ))
        if "subject" not in fm_data:
            issues.append(ValidationIssue(
                Severity.ERROR, "frontmatter.subject",
                "Missing required field 'subject'.",
            ))

        if not body.strip():
            issues.append(ValidationIssue(
                Severity.ERROR, "body",
                "Body is empty.",
            ))

        return issues

    def parse_bottle(self, file_path: str | Path) -> Bottle:
        """
        Read a bottle file, parse frontmatter, validate, and return a Bottle.

        Raises ValueError if parsing fails.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Bottle file not found: {path}")

        content = path.read_text(encoding="utf-8")
        format_issues = self.validate_format(content)
        errors = [i for i in format_issues if i.severity == Severity.ERROR]
        if errors:
            error_msgs = "\n".join(str(e) for e in errors)
            raise ValueError(f"Invalid bottle format:\n{error_msgs}")

        fm_data, body = parse_frontmatter(content)
        assert fm_data is not None

        fm = self._build_frontmatter(fm_data, path.name)
        bottle = Bottle(frontmatter=fm, body=body.strip(), source_path=path.resolve())

        # Validate and warn (don't raise — consumers can check issues)
        issues = self.validate(bottle)
        errs = [i for i in issues if i.severity == Severity.ERROR]
        if errs:
            error_msgs = "\n".join(str(e) for e in errs)
            raise ValueError(f"Validation errors:\n{error_msgs}")

        return bottle

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_target(self, to: str) -> list[ValidationIssue]:
        """Validate the 'to' target specification."""
        issues: list[ValidationIssue] = []
        targets = [t.strip() for t in to.split(",")]

        for target in targets:
            if target == "fleet":
                continue
            if target.startswith("role:"):
                role = target[5:]
                if self.known_roles and role not in self.known_roles:
                    issues.append(ValidationIssue(
                        Severity.WARNING, "to",
                        f"Unknown role '{role}'.",
                        f"Known roles: {', '.join(sorted(self.known_roles))}",
                    ))
                continue
            if target.startswith("cap:"):
                cap = target[4:]
                if self.known_caps and cap not in self.known_caps:
                    issues.append(ValidationIssue(
                        Severity.WARNING, "to",
                        f"Unknown capability '{cap}'.",
                        f"Known capabilities: {', '.join(sorted(self.known_caps))}",
                    ))
                continue
            # Plain agent name
            if self.known_agents and target not in self.known_agents:
                issues.append(ValidationIssue(
                    Severity.WARNING, "to",
                    f"Unknown agent '{target}'.",
                    f"Known agents: {', '.join(sorted(self.known_agents))}",
                ))

        return issues

    def _validate_date(self, date_str: str) -> list[ValidationIssue]:
        """Validate ISO 8601 date."""
        issues: list[ValidationIssue] = []
        try:
            normalized = date_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                issues.append(ValidationIssue(
                    Severity.WARNING, "date",
                    "Date has no timezone info. UTC is recommended.",
                    "Use 'Z' suffix or '+00:00' for UTC.",
                ))
        except ValueError:
            issues.append(ValidationIssue(
                Severity.ERROR, "date",
                f"Invalid date format: '{date_str}'.",
                "Use ISO 8601 UTC format, e.g. '2026-04-12T15:30:00Z'.",
            ))
        return issues

    def _build_frontmatter(self, data: dict, filename: str) -> BottleFrontmatter:
        """Convert raw dict to BottleFrontmatter, with safe defaults."""
        # Parse type
        raw_type = data.get("type", "MESSAGE")
        try:
            bottle_type = BottleType(raw_type)
        except ValueError:
            bottle_type = BottleType.MESSAGE

        # Parse priority
        raw_priority = data.get("priority", "medium")
        try:
            priority = Priority(raw_priority)
        except ValueError:
            priority = Priority.PRIORITY_MEDIUM

        # Parse trust_level
        raw_trust = data.get("trust_level", "standard")
        try:
            trust = TrustLevel(raw_trust)
        except ValueError:
            trust = TrustLevel.TRUST_STANDARD

        # Lists
        task_refs = data.get("task_refs", [])
        if isinstance(task_refs, str):
            task_refs = [task_refs]
        repo_refs = data.get("repo_refs", [])
        if isinstance(repo_refs, str):
            repo_refs = [repo_refs]

        return BottleFrontmatter(
            from_agent=data.get("from", ""),
            to=data.get("to", ""),
            type=bottle_type,
            date=data.get("date", ""),
            subject=data.get("subject", ""),
            priority=priority,
            reply_to=data.get("reply_to"),
            task_refs=list(task_refs),
            repo_refs=list(repo_refs),
            trust_level=trust,
            raw=data,
            source_file=filename,
        )


# ---------------------------------------------------------------------------
# Convenience
# ---------------------------------------------------------------------------

def make_bottle(
    from_agent: str,
    to: str,
    bottle_type: str | BottleType,
    subject: str,
    body: str,
    date: str | None = None,
    priority: str | None = None,
    reply_to: str | None = None,
    task_refs: list[str] | None = None,
    repo_refs: list[str] | None = None,
    trust_level: str | None = None,
) -> Bottle:
    """
    Factory to create a Bottle with minimal boilerplate.

    If date is not provided, uses current UTC time.
    """
    if isinstance(bottle_type, str):
        bottle_type = BottleType(bottle_type)

    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    _priority = Priority(priority) if priority else Priority.PRIORITY_MEDIUM
    _trust = TrustLevel(trust_level) if trust_level else TrustLevel.TRUST_STANDARD

    fm = BottleFrontmatter(
        from_agent=from_agent,
        to=to,
        type=bottle_type,
        date=date,
        subject=subject,
        priority=_priority,
        reply_to=reply_to,
        task_refs=task_refs or [],
        repo_refs=repo_refs or [],
        trust_level=_trust,
    )
    return Bottle(frontmatter=fm, body=body.strip())


def serialize_bottle(bottle: Bottle) -> str:
    """Serialize a Bottle to its file format (frontmatter + body)."""
    fm_dict = bottle.frontmatter.to_dict()
    lines = ["---"]
    for key, value in fm_dict.items():
        if isinstance(value, list):
            lines.append(f"{key}:")
            for item in value:
                lines.append(f"  - {item}")
        elif isinstance(value, str) and (":" in value or "#" in value or value.startswith(" ")):
            lines.append(f'{key}: "{value}"')
        else:
            lines.append(f"{key}: {value}")
    lines.append("---")
    lines.append("")
    lines.append(bottle.body)
    lines.append("")
    return "\n".join(lines)
