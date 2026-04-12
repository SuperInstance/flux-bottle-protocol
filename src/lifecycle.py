"""
Bottle lifecycle management: state tracking, history, reports.

Uses only Python stdlib. State is persisted to a JSON ledger file.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class BottleState(str, Enum):
    """Bottle lifecycle states per BOTTLE-SPEC.md §8."""

    DRAFT = "DRAFT"
    SENT = "SENT"
    DELIVERED = "DELIVERED"
    READ = "READ"
    RESPONDED = "RESPONDED"
    ARCHIVED = "ARCHIVED"
    EXPIRED = "EXPIRED"


# Valid state transitions (source → set of valid targets)
_VALID_TRANSITIONS: dict[BottleState, set[BottleState]] = {
    BottleState.DRAFT: {BottleState.SENT},
    BottleState.SENT: {BottleState.DELIVERED, BottleState.ARCHIVED},
    BottleState.DELIVERED: {BottleState.READ, BottleState.EXPIRED, BottleState.ARCHIVED},
    BottleState.READ: {BottleState.RESPONDED, BottleState.EXPIRED, BottleState.ARCHIVED},
    BottleState.RESPONDED: {BottleState.ARCHIVED},
    BottleState.ARCHIVED: set(),
    BottleState.EXPIRED: {BottleState.ARCHIVED},
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class StateTransition:
    """A recorded state change."""

    state: BottleState
    timestamp: str  # ISO 8601

    def to_dict(self) -> dict:
        return {"state": self.state.value, "timestamp": self.timestamp}

    @classmethod
    def from_dict(cls, data: dict) -> StateTransition:
        return cls(
            state=BottleState(data["state"]),
            timestamp=data["timestamp"],
        )


@dataclass
class BottleRecord:
    """Complete lifecycle record for a single bottle."""

    bottle_id: str
    current_state: BottleState
    history: List[StateTransition] = field(default_factory=list)
    from_agent: str = ""
    to: str = ""
    bottle_type: str = ""
    subject: str = ""

    def to_dict(self) -> dict:
        return {
            "bottle_id": self.bottle_id,
            "current_state": self.current_state.value,
            "history": [t.to_dict() for t in self.history],
            "from_agent": self.from_agent,
            "to": self.to,
            "bottle_type": self.bottle_type,
            "subject": self.subject,
        }

    @classmethod
    def from_dict(cls, data: dict) -> BottleRecord:
        return cls(
            bottle_id=data["bottle_id"],
            current_state=BottleState(data["current_state"]),
            history=[StateTransition.from_dict(h) for h in data.get("history", [])],
            from_agent=data.get("from_agent", ""),
            to=data.get("to", ""),
            bottle_type=data.get("bottle_type", ""),
            subject=data.get("subject", ""),
        )


# ---------------------------------------------------------------------------
# Bottle Ledger
# ---------------------------------------------------------------------------

class BottleLedger:
    """
    Tracks bottle lifecycle state transitions.

    Persists state to a JSON ledger file. Thread-safe for single-writer scenarios.
    """

    DEFAULT_LEDGER_NAME = "ledger.json"
    DEFAULT_STATE_DIR = ".bottle-state"

    def __init__(
        self,
        repo_path: str | Path | None = None,
        ledger_file: str | Path | None = None,
    ):
        if ledger_file:
            self.ledger_path = Path(ledger_file)
        elif repo_path:
            self.ledger_path = (
                Path(repo_path)
                / "message-in-a-bottle"
                / self.DEFAULT_STATE_DIR
                / self.DEFAULT_LEDGER_NAME
            )
        else:
            # In-memory only
            self.ledger_path = None

        self._records: dict[str, BottleRecord] = {}
        self._load()

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def record(
        self,
        bottle_id: str,
        state: BottleState,
        from_agent: str = "",
        to: str = "",
        bottle_type: str = "",
        subject: str = "",
    ) -> None:
        """
        Record a state transition for a bottle.

        Validates that the transition is legal.
        """
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        transition = StateTransition(state=state, timestamp=now)

        if bottle_id in self._records:
            rec = self._records[bottle_id]
            # Validate transition
            valid_targets = _VALID_TRANSITIONS.get(rec.current_state, set())
            if state not in valid_targets and rec.current_state != state:
                # Allow re-recording the same state (idempotent)
                raise ValueError(
                    f"Invalid transition: {rec.current_state.value} → {state.value} "
                    f"for bottle '{bottle_id}'. "
                    f"Valid targets: {', '.join(s.value for s in valid_targets)}"
                )
            rec.current_state = state
            rec.history.append(transition)
            # Update metadata if provided
            if from_agent:
                rec.from_agent = from_agent
            if to:
                rec.to = to
            if bottle_type:
                rec.bottle_type = bottle_type
            if subject:
                rec.subject = subject
        else:
            # New bottle
            # If first state isn't DRAFT, still allow it (bottle may have been
            # created in a later state)
            rec = BottleRecord(
                bottle_id=bottle_id,
                current_state=state,
                history=[transition],
                from_agent=from_agent,
                to=to,
                bottle_type=bottle_type,
                subject=subject,
            )
            self._records[bottle_id] = rec

        self._save()

    def get_history(self, bottle_id: str) -> List[StateTransition]:
        """Get the full state history for a bottle."""
        rec = self._records.get(bottle_id)
        if not rec:
            return []
        return list(rec.history)

    def get_state(self, bottle_id: str) -> Optional[BottleState]:
        """Get the current state of a bottle, or None if not tracked."""
        rec = self._records.get(bottle_id)
        return rec.current_state if rec else None

    def get_record(self, bottle_id: str) -> Optional[BottleRecord]:
        """Get the full record for a bottle."""
        return self._records.get(bottle_id)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_pending(self, agent_name: str) -> List[BottleRecord]:
        """
        Get bottles awaiting response from a specific agent.

        Returns bottles that are DELIVERED or READ and were sent TO this agent.
        """
        results = []
        for rec in self._records.values():
            if rec.to == agent_name and rec.current_state in (
                BottleState.DELIVERED,
                BottleState.READ,
            ):
                results.append(rec)
        return sorted(results, key=lambda r: r.history[-1].timestamp)

    def get_overdue(
        self,
        max_age_days: int = 7,
    ) -> List[BottleRecord]:
        """
        Get bottles that have been in DELIVERED or READ state longer than max_age_days.

        These need follow-up.
        """
        now = datetime.now(timezone.utc)
        results = []

        for rec in self._records.values():
            if rec.current_state in (BottleState.DELIVERED, BottleState.READ):
                if not rec.history:
                    continue
                last_ts = rec.history[-1].timestamp
                try:
                    last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                    age = (now - last_dt).days
                    if age > max_age_days:
                        results.append(rec)
                except ValueError:
                    continue

        return sorted(results, key=lambda r: r.history[-1].timestamp)

    def get_all_records(self) -> List[BottleRecord]:
        """Get all tracked bottle records."""
        return list(self._records.values())

    def get_by_agent(self, agent_name: str) -> List[BottleRecord]:
        """Get all records involving a specific agent (sender or receiver)."""
        results = []
        for rec in self._records.values():
            if rec.from_agent == agent_name or rec.to == agent_name:
                results.append(rec)
        return sorted(results, key=lambda r: r.history[-1].timestamp)

    # ------------------------------------------------------------------
    # Reports
    # ------------------------------------------------------------------

    def generate_status_report(self) -> str:
        """
        Generate a markdown summary of all tracked bottles.
        """
        lines = [
            "# Bottle Status Report",
            f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
            f"**Total tracked:** {len(self._records)}",
            "",
        ]

        # State distribution
        state_counts: dict[BottleState, int] = {}
        for rec in self._records.values():
            state_counts[rec.current_state] = state_counts.get(rec.current_state, 0) + 1

        lines.append("## State Distribution")
        lines.append("")
        lines.append("| State | Count |")
        lines.append("|-------|-------|")
        for state in BottleState:
            count = state_counts.get(state, 0)
            lines.append(f"| {state.value} | {count} |")
        lines.append("")

        # Overdue
        overdue = self.get_overdue()
        if overdue:
            lines.append(f"## Overdue (>7 days in DELIVERED/READ)")
            lines.append("")
            for rec in overdue:
                last_ts = rec.history[-1].timestamp if rec.history else "unknown"
                lines.append(
                    f"- **{rec.bottle_id}** — {rec.current_state.value} "
                    f"since {last_ts} — {rec.subject}"
                )
            lines.append("")

        # Pending responses by agent
        agents = sorted({rec.to for rec in self._records.values() if rec.to})
        if agents:
            lines.append("## Pending by Agent")
            lines.append("")
            for agent in agents:
                pending = self.get_pending(agent)
                if pending:
                    lines.append(f"### {agent}")
                    for rec in pending:
                        lines.append(
                            f"- **{rec.bottle_id}**: {rec.subject} "
                            f"({rec.current_state.value})"
                        )
                    lines.append("")

        # Recent activity
        recent_transitions: list[tuple[str, str, str]] = []
        for rec in self._records.values():
            for t in rec.history:
                recent_transitions.append((t.timestamp, rec.bottle_id, t.state.value))
        recent_transitions.sort(reverse=True)
        recent_transitions = recent_transitions[:20]

        if recent_transitions:
            lines.append("## Recent Activity (last 20)")
            lines.append("")
            lines.append("| Time | Bottle | State |")
            lines.append("|------|--------|-------|")
            for ts, bid, st in recent_transitions:
                lines.append(f"| {ts} | {bid} | {st} |")
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Load ledger from disk."""
        if not self.ledger_path or not self.ledger_path.exists():
            return

        try:
            data = json.loads(self.ledger_path.read_text(encoding="utf-8"))
            for bottle_id, record_data in data.items():
                self._records[bottle_id] = BottleRecord.from_dict(record_data)
            logger.debug("Loaded %d bottle records from %s", len(self._records), self.ledger_path)
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.warning(
                "Corrupted ledger at %s, starting fresh: %s",
                self.ledger_path, exc,
            )
            self._records = {}

    def _save(self) -> None:
        """Persist ledger to disk."""
        if not self.ledger_path:
            return  # In-memory only

        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            bid: rec.to_dict()
            for bid, rec in self._records.items()
        }
        try:
            self.ledger_path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.error("Failed to save ledger to %s: %s", self.ledger_path, exc)
