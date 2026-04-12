"""
Comprehensive tests for the Bottle Protocol: schema, routing, lifecycle.

Uses only Python stdlib (unittest). No external deps.
"""

import os
import shutil
import tempfile
import textwrap
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from schema import (
    Bottle,
    BottleFrontmatter,
    BottleType,
    BottleValidator,
    Priority,
    TrustLevel,
    Severity,
    ValidationIssue,
    make_bottle,
    parse_frontmatter,
    serialize_bottle,
)
from router import BottleRouter, RepoRef, RouteTarget
from lifecycle import BottleState, BottleLedger, BottleRecord, StateTransition


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tmp_repo() -> str:
    """Create a temporary repo directory with bottle structure and return its path."""
    d = tempfile.mkdtemp(prefix="bottle-test-")
    bottle_dir = Path(d) / "message-in-a-bottle"
    (bottle_dir / "for-fleet").mkdir(parents=True)
    (bottle_dir / "from-fleet").mkdir(parents=True)
    (bottle_dir / "archive").mkdir(parents=True)
    return d


def _write_bottle_file(
    directory: str,
    filename: str,
    frontmatter: dict,
    body: str = "This is the body content.",
) -> str:
    """Write a bottle file and return its full path."""
    lines = ["---"]
    for k, v in frontmatter.items():
        if isinstance(v, list):
            lines.append(f"{k}:")
            for item in v:
                lines.append(f"  - {item}")
        elif isinstance(v, str) and (":" in v or len(v) > 60):
            lines.append(f'{k}: "{v}"')
        else:
            lines.append(f"{k}: {v}")
    lines.append("---")
    lines.append("")
    lines.append(body)
    lines.append("")

    path = Path(directory) / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# Tests: Frontmatter Parsing
# ---------------------------------------------------------------------------

class TestFrontmatterParsing(unittest.TestCase):
    """Tests for YAML frontmatter parsing."""

    def test_basic_frontmatter(self):
        text = textwrap.dedent("""\
            ---
            from: Quill
            to: fleet
            type: CLAIM
            date: 2026-04-12T15:30:00Z
            subject: Test claim
            ---
            Body content here.
        """)
        fm, body = parse_frontmatter(text)
        self.assertIsNotNone(fm)
        self.assertEqual(fm["from"], "Quill")
        self.assertEqual(fm["to"], "fleet")
        self.assertEqual(fm["type"], "CLAIM")
        self.assertEqual(fm["subject"], "Test claim")
        self.assertEqual(body.strip(), "Body content here.")

    def test_frontmatter_with_lists(self):
        text = textwrap.dedent("""\
            ---
            from: Quill
            to: fleet
            type: CLAIM
            date: 2026-04-12T15:30:00Z
            subject: Test
            task_refs:
              - R3
              - R4
            repo_refs:
              - SuperInstance/flux-bottle-protocol
            ---
            Body here.
        """)
        fm, _ = parse_frontmatter(text)
        self.assertIsNotNone(fm)
        self.assertEqual(fm["task_refs"], ["R3", "R4"])
        self.assertEqual(fm["repo_refs"], ["SuperInstance/flux-bottle-protocol"])

    def test_frontmatter_with_optional_fields(self):
        text = textwrap.dedent("""\
            ---
            from: Quill
            to: Cipher
            type: RESPONSE
            date: 2026-04-12T15:30:00Z
            subject: Re: Your claim
            reply_to: CLAIM-Cipher-20260412-140000.md
            priority: high
            trust_level: verified
            ---
            I accept your claim.
        """)
        fm, _ = parse_frontmatter(text)
        self.assertIsNotNone(fm)
        self.assertEqual(fm["reply_to"], "CLAIM-Cipher-20260412-140000.md")
        self.assertEqual(fm["priority"], "high")
        self.assertEqual(fm["trust_level"], "verified")

    def test_no_frontmatter(self):
        text = "Just some text without frontmatter."
        fm, body = parse_frontmatter(text)
        self.assertIsNone(fm)
        self.assertEqual(body.strip(), "Just some text without frontmatter.")

    def test_empty_frontmatter(self):
        text = textwrap.dedent("""\
            ---
            ---
            Body.
        """)
        fm, body = parse_frontmatter(text)
        self.assertIsNotNone(fm)
        self.assertEqual(fm, {})
        self.assertEqual(body.strip(), "Body.")

    def test_frontmatter_with_quotes(self):
        text = textwrap.dedent("""\
            ---
            from: Quill
            to: fleet
            type: MESSAGE
            date: 2026-04-12T15:30:00Z
            subject: "Hello: world"
            ---
            Test.
        """)
        fm, _ = parse_frontmatter(text)
        self.assertIsNotNone(fm)
        self.assertEqual(fm["subject"], "Hello: world")


# ---------------------------------------------------------------------------
# Tests: Bottle Validation
# ---------------------------------------------------------------------------

class TestBottleValidation(unittest.TestCase):
    """Tests for BottleValidator."""

    def setUp(self):
        self.validator = BottleValidator(
            known_agents=["Quill", "Cipher", "Atlas"],
            known_roles=["architect", "engineer"],
            known_caps=["writing", "coding", "analysis"],
        )

    def test_valid_bottle_no_issues(self):
        bottle = make_bottle(
            from_agent="Quill",
            to="fleet",
            bottle_type="CLAIM",
            subject="Test claim",
            body="## Approach\n\nI will do the thing.\n\n## Timeline\n\n1 day.",
            task_refs=["R3"],
        )
        issues = self.validator.validate(bottle)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertEqual(len(errors), 0, f"Unexpected errors: {errors}")

    def test_missing_from_field(self):
        fm = BottleFrontmatter(
            from_agent="", to="fleet", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="Test",
        )
        bottle = Bottle(frontmatter=fm, body="Hello.")
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(any(i.field == "from" for i in errors))

    def test_missing_to_field(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="Test",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(any(i.field == "to" for i in errors))

    def test_response_without_reply_to(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="Cipher", type=BottleType.RESPONSE,
            date="2026-04-12T15:30:00Z", subject="Re: something",
            reply_to=None,
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(any(i.field == "reply_to" for i in errors))

    def test_invalid_date(self):
        issues = self.validator.validate_frontmatter(BottleFrontmatter(
            from_agent="Quill", to="fleet", type=BottleType.MESSAGE,
            date="not-a-date", subject="Test",
        ))
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(any(i.field == "date" for i in errors))

    def test_claim_without_task_refs_warning(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="fleet", type=BottleType.CLAIM,
            date="2026-04-12T15:30:00Z", subject="Claim",
            task_refs=[],
        )
        issues = self.validator.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING]
        self.assertTrue(any(i.field == "task_refs" for i in warnings))

    def test_broadcast_not_to_fleet_warning(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="Cipher", type=BottleType.BROADCAST,
            date="2026-04-12T15:30:00Z", subject="Announcement",
        )
        issues = self.validator.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING]
        self.assertTrue(any(i.field == "to" for i in warnings))

    def test_empty_body_error(self):
        issues = self.validator.validate_body("", BottleType.MESSAGE)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_introduction_short_body_warning(self):
        issues = self.validator.validate_body("Hi", BottleType.INTRODUCTION)
        warnings = [i for i in issues if i.severity == Severity.WARNING]
        self.assertTrue(len(warnings) > 0)

    def test_format_validation_valid(self):
        content = textwrap.dedent("""\
            ---
            from: Quill
            to: fleet
            type: MESSAGE
            date: 2026-04-12T15:30:00Z
            subject: Test
            ---
            Body.
        """)
        issues = self.validator.validate_format(content)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertEqual(len(errors), 0)

    def test_format_validation_missing_frontmatter_delimiter(self):
        issues = self.validator.validate_format("Just text, no frontmatter.")
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_format_validation_invalid_type(self):
        content = textwrap.dedent("""\
            ---
            from: Quill
            to: fleet
            type: INVALID_TYPE
            date: 2026-04-12T15:30:00Z
            subject: Test
            ---
            Body.
        """)
        issues = self.validator.validate_format(content)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(any("type" in i.field for i in errors))

    def test_parse_bottle_from_file(self):
        tmpdir = tempfile.mkdtemp()
        try:
            path = _write_bottle_file(tmpdir, "MESSAGE-Quill-20260412-153000.md", {
                "from": "Quill",
                "to": "fleet",
                "type": "MESSAGE",
                "date": "2026-04-12T15:30:00Z",
                "subject": "Hello fleet",
            }, "This is a test message.")
            bottle = self.validator.parse_bottle(path)
            self.assertEqual(bottle.frontmatter.from_agent, "Quill")
            self.assertEqual(bottle.frontmatter.to, "fleet")
            self.assertEqual(bottle.frontmatter.type, BottleType.MESSAGE)
            self.assertEqual(bottle.body, "This is a test message.")
        finally:
            shutil.rmtree(tmpdir)

    def test_parse_bottle_invalid_raises(self):
        tmpdir = tempfile.mkdtemp()
        try:
            path = os.path.join(tmpdir, "bad.md")
            Path(path).write_text("no frontmatter here", encoding="utf-8")
            with self.assertRaises(ValueError):
                self.validator.parse_bottle(path)
        finally:
            shutil.rmtree(tmpdir)


# ---------------------------------------------------------------------------
# Tests: All Bottle Types
# ---------------------------------------------------------------------------

class TestBottleTypes(unittest.TestCase):
    """Test creation and validation for all 8 bottle types."""

    def setUp(self):
        self.validator = BottleValidator()

    def _make(self, btype: str, **kwargs) -> Bottle:
        return make_bottle(
            from_agent="Quill", to="fleet", bottle_type=btype,
            subject=f"Test {btype}",
            body=f"# Test {btype}\n\nThis is a {btype} bottle.\n\n## Details\n\nSome content.",
            **kwargs,
        )

    def test_introduction(self):
        bottle = self._make("INTRODUCTION")
        self.assertEqual(bottle.frontmatter.type, BottleType.INTRODUCTION)

    def test_claim(self):
        bottle = self._make("CLAIM", task_refs=["R3"])
        self.assertEqual(bottle.frontmatter.type, BottleType.CLAIM)

    def test_message(self):
        bottle = self._make("MESSAGE")
        self.assertEqual(bottle.frontmatter.type, BottleType.MESSAGE)

    def test_response(self):
        bottle = self._make("RESPONSE", reply_to="CLAIM-Atlas-20260412-140000.md")
        self.assertEqual(bottle.frontmatter.type, BottleType.RESPONSE)
        self.assertEqual(bottle.frontmatter.reply_to, "CLAIM-Atlas-20260412-140000.md")

    def test_status_update(self):
        bottle = self._make("STATUS_UPDATE")
        self.assertEqual(bottle.frontmatter.type, BottleType.STATUS_UPDATE)

    def test_broadcast(self):
        bottle = self._make("BROADCAST")
        self.assertEqual(bottle.frontmatter.type, BottleType.BROADCAST)

    def test_rfc_submission(self):
        bottle = self._make("RFC_SUBMISSION")
        self.assertEqual(bottle.frontmatter.type, BottleType.RFC_SUBMISSION)

    def test_task_completion(self):
        bottle = self._make("TASK_COMPLETION", task_refs=["R1"])
        self.assertEqual(bottle.frontmatter.type, BottleType.TASK_COMPLETION)


# ---------------------------------------------------------------------------
# Tests: Serialization round-trip
# ---------------------------------------------------------------------------

class TestSerialization(unittest.TestCase):
    """Test bottle serialization and deserialization."""

    def test_serialize_and_parse(self):
        bottle = make_bottle(
            from_agent="Quill", to="fleet", bottle_type="CLAIM",
            subject="Test claim", body="## Approach\n\nBuild the thing.",
            task_refs=["R3"], priority="high",
        )
        text = serialize_bottle(bottle)
        self.assertTrue(text.startswith("---"))
        self.assertIn("from: Quill", text)
        self.assertIn("type: CLAIM", text)
        self.assertIn("priority: high", text)
        self.assertIn("task_refs:", text)
        self.assertIn("## Approach", text)

    def test_bottle_id_derivation(self):
        bottle = make_bottle(
            from_agent="Quill", to="fleet", bottle_type="CLAIM",
            subject="Test", body="Body.",
            date="2026-04-12T15:30:00Z",
        )
        self.assertEqual(bottle.bottle_id, "CLAIM-Quill-20260412-153000")


# ---------------------------------------------------------------------------
# Tests: Routing
# ---------------------------------------------------------------------------

class TestRouting(unittest.TestCase):
    """Tests for BottleRouter."""

    def setUp(self):
        self.router = BottleRouter(repos=[
            RepoRef(name="repo-quill", agent="Quill", path="/tmp/quill",
                    roles=["architect"], capabilities=["writing", "analysis"]),
            RepoRef(name="repo-cipher", agent="Cipher", path="/tmp/cipher",
                    roles=["engineer"], capabilities=["coding"]),
            RepoRef(name="repo-atlas", agent="Atlas", path="/tmp/atlas",
                    roles=["architect", "engineer"], capabilities=["writing", "coding"]),
        ])

    def test_route_to_fleet(self):
        targets = self.router.resolve_target("fleet")
        names = {r.name for r in targets}
        self.assertEqual(names, {"repo-quill", "repo-cipher", "repo-atlas"})

    def test_route_to_specific_agent(self):
        targets = self.router.resolve_target("Quill")
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].agent, "Quill")

    def test_route_to_role(self):
        targets = self.router.resolve_target("role:architect")
        names = {r.name for r in targets}
        self.assertEqual(names, {"repo-quill", "repo-atlas"})

    def test_route_to_capability(self):
        targets = self.router.resolve_target("cap:writing")
        names = {r.name for r in targets}
        self.assertEqual(names, {"repo-quill", "repo-atlas"})

    def test_route_multi_target(self):
        targets = self.router.resolve_target("Quill,Cipher")
        names = {r.name for r in targets}
        self.assertEqual(names, {"repo-quill", "repo-cipher"})

    def test_route_no_match(self):
        targets = self.router.resolve_target("NonExistent")
        self.assertEqual(len(targets), 0)

    def test_route_bottle(self):
        bottle = make_bottle("Quill", "fleet", "BROADCAST", "Announcement", "Hello!")
        routes = self.router.route(bottle)
        self.assertEqual(len(routes), 3)

    def test_route_bottle_to_specific(self):
        bottle = make_bottle("Quill", "Cipher", "MESSAGE", "Hey", "Hello Cipher.")
        routes = self.router.route(bottle)
        self.assertEqual(len(routes), 1)
        self.assertEqual(routes[0].repo.agent, "Cipher")

    def test_get_inbox_path(self):
        path = self.router.get_inbox_path("/tmp/quill")
        self.assertEqual(str(path), "/tmp/quill/message-in-a-bottle/from-fleet")

    def test_get_outbox_path(self):
        path = self.router.get_outbox_path("/tmp/quill")
        self.assertEqual(str(path), "/tmp/quill/message-in-a-bottle/for-fleet")

    def test_inbox_scanning(self):
        repo_path = _tmp_repo()
        validator = BottleValidator()
        try:
            # Write a valid bottle in inbox
            _write_bottle_file(
                repo_path + "/message-in-a-bottle/from-fleet/Cipher",
                "MESSAGE-Cipher-20260412-170000.md",
                {
                    "from": "Cipher",
                    "to": "Quill",
                    "type": "MESSAGE",
                    "date": "2026-04-12T17:00:00Z",
                    "subject": "Hello Quill",
                },
                "Hey Quill, how are you?",
            )
            bottles = self.router.scan_inbox(repo_path, validator)
            self.assertEqual(len(bottles), 1)
            self.assertEqual(bottles[0].frontmatter.from_agent, "Cipher")
        finally:
            shutil.rmtree(repo_path)

    def test_scan_empty_inbox(self):
        repo_path = _tmp_repo()
        validator = BottleValidator()
        try:
            bottles = self.router.scan_inbox(repo_path, validator)
            self.assertEqual(len(bottles), 0)
        finally:
            shutil.rmtree(repo_path)

    def test_mark_read(self):
        repo_path = _tmp_repo()
        try:
            result = self.router.mark_read(repo_path, "MESSAGE-Cipher-20260412-170000.md")
            self.assertTrue(result)
            # Second call should return False (already read)
            result = self.router.mark_read(repo_path, "MESSAGE-Cipher-20260412-170000.md")
            self.assertFalse(result)
        finally:
            shutil.rmtree(repo_path)

    def test_scan_unread(self):
        repo_path = _tmp_repo()
        try:
            _write_bottle_file(
                repo_path + "/message-in-a-bottle/from-fleet/Atlas",
                "MESSAGE-Atlas-20260413-100000.md",
                {
                    "from": "Atlas",
                    "to": "Quill",
                    "type": "MESSAGE",
                    "date": "2026-04-13T10:00:00Z",
                    "subject": "Status check",
                },
                "Checking in.",
            )
            _write_bottle_file(
                repo_path + "/message-in-a-bottle/from-fleet/Cipher",
                "MESSAGE-Cipher-20260412-170000.md",
                {
                    "from": "Cipher",
                    "to": "Quill",
                    "type": "MESSAGE",
                    "date": "2026-04-12T17:00:00Z",
                    "subject": "Hello",
                },
                "Hey.",
            )

            # Both unread
            unread = self.router.scan_unread(repo_path)
            self.assertEqual(len(unread), 2)

            # Mark one as read
            self.router.mark_read(repo_path, "MESSAGE-Cipher-20260412-170000.md")
            unread = self.router.scan_unread(repo_path)
            self.assertEqual(len(unread), 1)
            self.assertIn("Atlas", str(unread[0]))
        finally:
            shutil.rmtree(repo_path)


# ---------------------------------------------------------------------------
# Tests: Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle(unittest.TestCase):
    """Tests for BottleLedger and lifecycle state transitions."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.ledger = BottleLedger(repo_path=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_initial_state_draft(self):
        self.ledger.record("TEST-001", BottleState.DRAFT, from_agent="Quill")
        state = self.ledger.get_state("TEST-001")
        self.assertEqual(state, BottleState.DRAFT)

    def test_valid_transition_sequence(self):
        self.ledger.record("TEST-002", BottleState.DRAFT, from_agent="Quill")
        self.ledger.record("TEST-002", BottleState.SENT)
        self.ledger.record("TEST-002", BottleState.DELIVERED)
        self.ledger.record("TEST-002", BottleState.READ)
        self.ledger.record("TEST-002", BottleState.RESPONDED)
        self.ledger.record("TEST-002", BottleState.ARCHIVED)
        self.assertEqual(self.ledger.get_state("TEST-002"), BottleState.ARCHIVED)

    def test_invalid_transition_raises(self):
        self.ledger.record("TEST-003", BottleState.DRAFT)
        with self.assertRaises(ValueError):
            self.ledger.record("TEST-003", BottleState.ARCHIVED)

    def test_idempotent_same_state(self):
        self.ledger.record("TEST-004", BottleState.DRAFT)
        # Re-recording same state should not raise
        self.ledger.record("TEST-004", BottleState.DRAFT)
        self.assertEqual(self.ledger.get_state("TEST-004"), BottleState.DRAFT)

    def test_get_history(self):
        self.ledger.record("TEST-005", BottleState.DRAFT, from_agent="Quill")
        self.ledger.record("TEST-005", BottleState.SENT)
        history = self.ledger.get_history("TEST-005")
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0].state, BottleState.DRAFT)
        self.assertEqual(history[1].state, BottleState.SENT)

    def test_get_pending(self):
        self.ledger.record("TEST-006", BottleState.DELIVERED,
                           from_agent="Quill", to="Cipher", subject="Need response")
        self.ledger.record("TEST-007", BottleState.READ,
                           from_agent="Atlas", to="Cipher", subject="Also pending")
        self.ledger.record("TEST-008", BottleState.RESPONDED,
                           from_agent="Quill", to="Cipher", subject="Already done")

        pending = self.ledger.get_pending("Cipher")
        self.assertEqual(len(pending), 2)
        subjects = {r.subject for r in pending}
        self.assertIn("Need response", subjects)
        self.assertIn("Also pending", subjects)

    def test_get_overdue(self):
        # Create a bottle that was DELIVERED a long time ago by manipulating history
        self.ledger.record("TEST-009", BottleState.DELIVERED,
                           from_agent="Quill", to="Cipher", subject="Old bottle")
        # Manually set an old timestamp
        old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        rec = self.ledger.get_record("TEST-009")
        assert rec is not None
        rec.history[-1] = StateTransition(state=BottleState.DELIVERED, timestamp=old_ts)
        self.ledger._save()

        overdue = self.ledger.get_overdue(max_age_days=7)
        self.assertEqual(len(overdue), 1)
        self.assertEqual(overdue[0].bottle_id, "TEST-009")

    def test_no_overdue_for_recent(self):
        self.ledger.record("TEST-010", BottleState.DELIVERED,
                           from_agent="Quill", to="Cipher", subject="Recent")
        overdue = self.ledger.get_overdue(max_age_days=7)
        self.assertEqual(len(overdue), 0)

    def test_status_report(self):
        self.ledger.record("TEST-011", BottleState.SENT,
                           from_agent="Quill", to="fleet", subject="Report test",
                           bottle_type="MESSAGE")
        report = self.ledger.generate_status_report()
        self.assertIn("# Bottle Status Report", report)
        self.assertIn("TEST-011", report)
        self.assertIn("State Distribution", report)

    def test_persistence_across_instances(self):
        self.ledger.record("TEST-012", BottleState.DRAFT, from_agent="Quill")
        self.ledger.record("TEST-012", BottleState.SENT)

        # Create new ledger from same path
        ledger2 = BottleLedger(repo_path=self.tmpdir)
        state = ledger2.get_state("TEST-012")
        self.assertEqual(state, BottleState.SENT)
        history = ledger2.get_history("TEST-012")
        self.assertEqual(len(history), 2)

    def test_get_by_agent(self):
        self.ledger.record("A1", BottleState.SENT, from_agent="Quill", to="Cipher")
        self.ledger.record("A2", BottleState.SENT, from_agent="Cipher", to="Quill")
        self.ledger.record("A3", BottleState.SENT, from_agent="Atlas", to="fleet")

        quill_records = self.ledger.get_by_agent("Quill")
        self.assertEqual(len(quill_records), 2)  # A1 (from) and A2 (to)

    def test_expired_state(self):
        self.ledger.record("TEST-013", BottleState.DELIVERED)
        self.ledger.record("TEST-013", BottleState.EXPIRED)
        self.assertEqual(self.ledger.get_state("TEST-013"), BottleState.EXPIRED)

    def test_expired_to_archived(self):
        self.ledger.record("TEST-014", BottleState.EXPIRED)
        self.ledger.record("TEST-014", BottleState.ARCHIVED)
        self.assertEqual(self.ledger.get_state("TEST-014"), BottleState.ARCHIVED)


# ---------------------------------------------------------------------------
# Tests: Invalid Bottle Rejection
# ---------------------------------------------------------------------------

class TestInvalidBottleRejection(unittest.TestCase):
    """Test that various invalid bottles are properly rejected."""

    def setUp(self):
        self.validator = BottleValidator()

    def test_completely_empty_file(self):
        issues = self.validator.validate_format("")
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_file_without_frontmatter(self):
        issues = self.validator.validate_format("# Just a markdown file\n\nNo frontmatter.")
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_frontmatter_missing_required_fields(self):
        content = textwrap.dedent("""\
            ---
            from: Quill
            ---
            Body.
        """)
        issues = self.validator.validate_format(content)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        # Missing: to, type, date, subject
        self.assertTrue(len(errors) >= 4)

    def test_invalid_type_rejected(self):
        content = textwrap.dedent("""\
            ---
            from: Quill
            to: fleet
            type: NOT_A_REAL_TYPE
            date: 2026-04-12T15:30:00Z
            subject: Bad type
            ---
            Body.
        """)
        issues = self.validator.validate_format(content)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(any("type" in i.field for i in errors))

    def test_response_missing_reply_to_rejected(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="Cipher", type=BottleType.RESPONSE,
            date="2026-04-12T15:30:00Z", subject="Re:",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(any(i.field == "reply_to" for i in errors))


# ---------------------------------------------------------------------------
# Tests: Archive Cleanup
# ---------------------------------------------------------------------------

class TestArchiveCleanup(unittest.TestCase):
    """Test bottle archival for old bottles."""

    def test_archive_old_bottles(self):
        repo_path = _tmp_repo()
        router = BottleRouter()
        validator = BottleValidator()

        try:
            # Write an old bottle (31 days ago)
            old_date = (datetime.now(timezone.utc) - timedelta(days=35)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            _write_bottle_file(
                repo_path + "/message-in-a-bottle/for-fleet",
                "CLAIM-Quill-20260312-153000.md",
                {
                    "from": "Quill",
                    "to": "fleet",
                    "type": "CLAIM",
                    "date": old_date,
                    "subject": "Old claim",
                },
                "This is old.",
            )

            # Write a recent bottle (should NOT be archived)
            recent_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            _write_bottle_file(
                repo_path + "/message-in-a-bottle/for-fleet",
                "MESSAGE-Quill-20260412-153000.md",
                {
                    "from": "Quill",
                    "to": "fleet",
                    "type": "MESSAGE",
                    "date": recent_date,
                    "subject": "Recent message",
                },
                "This is recent.",
            )

            archived = router.archive_old(repo_path, max_age_days=30, validator=validator)
            self.assertEqual(len(archived), 1)
            self.assertIn("CLAIM-Quill-20260312-153000", archived[0])

            # Verify the old one is in archive
            archive_dir = Path(repo_path) / "message-in-a-bottle" / "archive" / "for-fleet"
            archived_files = list(archive_dir.rglob("*.md"))
            self.assertEqual(len(archived_files), 1)

            # Verify the recent one is still in outbox
            outbox_dir = Path(repo_path) / "message-in-a-bottle" / "for-fleet"
            remaining = list(outbox_dir.rglob("*.md"))
            self.assertEqual(len(remaining), 1)
            self.assertIn("MESSAGE-Quill-20260412", remaining[0].name)

        finally:
            shutil.rmtree(repo_path)

    def test_archive_empty_repo(self):
        repo_path = _tmp_repo()
        router = BottleRouter()
        validator = BottleValidator()
        try:
            archived = router.archive_old(repo_path, validator=validator)
            self.assertEqual(len(archived), 0)
        finally:
            shutil.rmtree(repo_path)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
