"""
Bottle routing logic: target resolution, inbox/outbox paths, scanning.

Uses only Python stdlib.
"""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

try:
    from .schema import Bottle, BottleType, BottleValidator, Severity
except ImportError:
    from schema import Bottle, BottleType, BottleValidator, Severity


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RepoRef:
    """Reference to a fleet repository."""

    name: str            # e.g. "flux-state-manager"
    agent: str           # owning agent, e.g. "Quill"
    path: str            # local filesystem path
    roles: List[str] = field(default_factory=list)
    capabilities: List[str] = field(default_factory=list)


@dataclass
class RouteTarget:
    """A resolved delivery target."""

    repo: RepoRef
    inbox_path: Path     # full path where the bottle file should be placed
    sender_dir: str      # subdirectory under from-fleet/ (sender agent name)


# ---------------------------------------------------------------------------
# Bottle Router
# ---------------------------------------------------------------------------

class BottleRouter:
    """
    Routes bottles to their target inboxes based on the 'to' field.

    Supports:
      - Direct agent name: "Quill"
      - Fleet-wide: "fleet"
      - Role targeting: "role:architect"
      - Capability targeting: "cap:writing"
      - Multi-target: "Quill,Cipher"
    """

    BOTTLE_DIR = "message-in-a-bottle"
    OUTBOX = "for-fleet"
    INBOX = "from-fleet"
    ARCHIVE = "archive"

    def __init__(
        self,
        repos: List[RepoRef] | None = None,
        base_path: str | Path | None = None,
    ):
        self.repos: list[RepoRef] = list(repos or [])
        self.base_path = Path(base_path) if base_path else None

    def register_repo(self, repo: RepoRef) -> None:
        """Register a repository for routing."""
        self.repos.append(repo)

    # ------------------------------------------------------------------
    # Target resolution
    # ------------------------------------------------------------------

    def resolve_target(self, target_spec: str) -> List[RepoRef]:
        """
        Resolve a target specification to a list of matching RepoRef.

        Formats:
          "fleet"        → all repos
          "Quill"        → repos owned by Quill
          "role:architect" → repos whose agent has the 'architect' role
          "cap:writing"  → repos whose agent has the 'writing' capability
          "Quill,Cipher" → union of both agents' repos
        """
        targets = [t.strip() for t in target_spec.split(",")]
        result: list[RepoRef] = []
        seen: set[str] = set()

        for spec in targets:
            if spec == "fleet":
                matched = list(self.repos)
            elif spec.startswith("role:"):
                role = spec[5:]
                matched = [r for r in self.repos if role in r.roles]
            elif spec.startswith("cap:"):
                cap = spec[4:]
                matched = [r for r in self.repos if cap in r.capabilities]
            else:
                # Direct agent name
                matched = [r for r in self.repos if r.agent == spec]

            for repo in matched:
                if repo.name not in seen:
                    seen.add(repo.name)
                    result.append(repo)

        return result

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def route(self, bottle: Bottle) -> List[RouteTarget]:
        """
        Determine where a bottle should be delivered.

        Returns a list of RouteTarget objects.
        """
        fm = bottle.frontmatter
        target_repos = self.resolve_target(fm.to)
        sender = fm.from_agent

        routes: list[RouteTarget] = []
        for repo in target_repos:
            inbox_base = self.get_inbox_path(repo.path)
            sender_dir = inbox_base / sender
            routes.append(RouteTarget(
                repo=repo,
                inbox_path=sender_dir,
                sender_dir=sender,
            ))
        return routes

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def get_inbox_path(self, repo_path: str | Path) -> Path:
        """
        Get the inbox base path for a repository.

        Returns: {repo}/message-in-a-bottle/from-fleet/
        """
        p = Path(repo_path) / self.BOTTLE_DIR / self.INBOX
        return p

    def get_outbox_path(self, repo_path: str | Path) -> Path:
        """
        Get the outbox path for a repository.

        Returns: {repo}/message-in-a-bottle/for-fleet/
        """
        p = Path(repo_path) / self.BOTTLE_DIR / self.OUTBOX
        return p

    def get_archive_path(self, repo_path: str | Path) -> Path:
        """Get the archive path for a repository."""
        p = Path(repo_path) / self.BOTTLE_DIR / self.ARCHIVE
        return p

    # ------------------------------------------------------------------
    # Inbox scanning
    # ------------------------------------------------------------------

    def scan_inbox(
        self,
        repo_path: str | Path,
        validator: BottleValidator | None = None,
    ) -> List[Bottle]:
        """
        Read all bottles from a repository's inbox.

        Scans message-in-a-bottle/from-fleet/ recursively for .md files.
        Skips files that fail validation (logs warnings).
        """
        inbox = self.get_inbox_path(repo_path)
        if not inbox.exists():
            return []

        bottles: list[Bottle] = []
        if validator is None:
            validator = BottleValidator()

        for md_file in sorted(inbox.rglob("*.md")):
            try:
                bottle = validator.parse_bottle(md_file)
                bottles.append(bottle)
            except (ValueError, FileNotFoundError) as exc:
                # Skip invalid bottles but could log
                pass

        return bottles

    def scan_unread(
        self,
        repo_path: str | Path,
        read_file: Path | None = None,
    ) -> List[Path]:
        """
        Find unread bottle files in inbox.

        Compares against a read tracking file (one filename per line).
        If read_file is None, uses {repo}/message-in-a-bottle/.read-tracker
        """
        inbox = self.get_inbox_path(repo_path)
        if not inbox.exists():
            return []

        all_files = set(f.name for f in inbox.rglob("*.md"))

        if read_file is None:
            read_file = Path(repo_path) / self.BOTTLE_DIR / ".read-tracker"

        if read_file.exists():
            read_names = set(read_file.read_text(encoding="utf-8").strip().split("\n"))
            read_names.discard("")  # remove empty
        else:
            read_names = set()

        unread = all_files - read_names
        return sorted(
            f for f in inbox.rglob("*.md") if f.name in unread
        )

    # ------------------------------------------------------------------
    # Read tracking
    # ------------------------------------------------------------------

    def mark_read(
        self,
        repo_path: str | Path,
        bottle_filename: str,
    ) -> bool:
        """
        Mark a bottle as read by appending its filename to the read tracker.

        Returns True if newly marked, False if already read.
        """
        read_file = Path(repo_path) / self.BOTTLE_DIR / ".read-tracker"
        read_file.parent.mkdir(parents=True, exist_ok=True)

        existing = set()
        if read_file.exists():
            content = read_file.read_text(encoding="utf-8")
            existing = set(content.strip().split("\n"))
            existing.discard("")

        if bottle_filename in existing:
            return False

        existing.add(bottle_filename)
        read_file.write_text("\n".join(sorted(existing)) + "\n", encoding="utf-8")
        return True

    # ------------------------------------------------------------------
    # Archival
    # ------------------------------------------------------------------

    def archive_old(
        self,
        repo_path: str | Path,
        max_age_days: int = 30,
        validator: BottleValidator | None = None,
    ) -> List[str]:
        """
        Archive bottles older than max_age_days.

        Moves files from for-fleet/ and from-fleet/ to archive/.
        Returns list of archived filenames.
        """
        if validator is None:
            validator = BottleValidator()

        repo = Path(repo_path)
        bottle_dir = repo / self.BOTTLE_DIR
        archive = self.get_archive_path(repo)
        archive.mkdir(parents=True, exist_ok=True)

        now = datetime.now(timezone.utc)
        archived: list[str] = []

        for sub_dir in [self.OUTBOX, self.INBOX]:
            source_dir = bottle_dir / sub_dir
            if not source_dir.exists():
                continue

            for md_file in sorted(source_dir.rglob("*.md")):
                try:
                    bottle = validator.parse_bottle(md_file)
                    fm = bottle.frontmatter
                    dt = datetime.fromisoformat(fm.date.replace("Z", "+00:00"))
                    age = (now - dt).days

                    if age > max_age_days:
                        # Determine relative path within sub_dir for archive
                        rel = md_file.relative_to(source_dir)
                        dest = archive / sub_dir / rel
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(md_file), str(dest))
                        archived.append(md_file.name)

                except (ValueError, FileNotFoundError):
                    # Can't parse — skip
                    pass

        return archived
