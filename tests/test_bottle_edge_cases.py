"""
Edge case tests for the Bottle Protocol: state transitions, persistence, routing,
parsing, validation, and serialization.

40+ new tests beyond the core suite.
"""

import json
import os
import shutil
import tempfile
import textwrap
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

import sys

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
    _parse_scalar,
)
from router import BottleRouter, RepoRef, RouteTarget
from lifecycle import (
    BottleState,
    BottleLedger,
    BottleRecord,
    StateTransition,
    _VALID_TRANSITIONS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tmp_repo() -> str:
    d = tempfile.mkdtemp(prefix="bottle-edge-")
    bottle_dir = Path(d) / "message-in-a-bottle"
    (bottle_dir / "for-fleet").mkdir(parents=True)
    (bottle_dir / "from-fleet").mkdir(parents=True)
    (bottle_dir / "archive").mkdir(parents=True)
    return d


# ---------------------------------------------------------------------------
# 1. Individual valid state transitions
# ---------------------------------------------------------------------------

class TestAllValidTransitions(unittest.TestCase):
    """Test every single valid transition edge individually."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.ledger = BottleLedger(repo_path=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_draft_to_sent(self):
        self.ledger.record("T1", BottleState.DRAFT)
        self.ledger.record("T1", BottleState.SENT)
        self.assertEqual(self.ledger.get_state("T1"), BottleState.SENT)

    def test_sent_to_delivered(self):
        self.ledger.record("T2", BottleState.SENT)
        self.ledger.record("T2", BottleState.DELIVERED)
        self.assertEqual(self.ledger.get_state("T2"), BottleState.DELIVERED)

    def test_sent_to_archived(self):
        self.ledger.record("T3", BottleState.SENT)
        self.ledger.record("T3", BottleState.ARCHIVED)
        self.assertEqual(self.ledger.get_state("T3"), BottleState.ARCHIVED)

    def test_delivered_to_read(self):
        self.ledger.record("T4", BottleState.DELIVERED)
        self.ledger.record("T4", BottleState.READ)
        self.assertEqual(self.ledger.get_state("T4"), BottleState.READ)

    def test_delivered_to_expired(self):
        self.ledger.record("T5", BottleState.DELIVERED)
        self.ledger.record("T5", BottleState.EXPIRED)
        self.assertEqual(self.ledger.get_state("T5"), BottleState.EXPIRED)

    def test_delivered_to_archived(self):
        self.ledger.record("T6", BottleState.DELIVERED)
        self.ledger.record("T6", BottleState.ARCHIVED)
        self.assertEqual(self.ledger.get_state("T6"), BottleState.ARCHIVED)

    def test_read_to_responded(self):
        self.ledger.record("T7", BottleState.READ)
        self.ledger.record("T7", BottleState.RESPONDED)
        self.assertEqual(self.ledger.get_state("T7"), BottleState.RESPONDED)

    def test_read_to_expired(self):
        self.ledger.record("T8", BottleState.READ)
        self.ledger.record("T8", BottleState.EXPIRED)
        self.assertEqual(self.ledger.get_state("T8"), BottleState.EXPIRED)

    def test_read_to_archived(self):
        self.ledger.record("T9", BottleState.READ)
        self.ledger.record("T9", BottleState.ARCHIVED)
        self.assertEqual(self.ledger.get_state("T9"), BottleState.ARCHIVED)

    def test_responded_to_archived(self):
        self.ledger.record("T10", BottleState.RESPONDED)
        self.ledger.record("T10", BottleState.ARCHIVED)
        self.assertEqual(self.ledger.get_state("T10"), BottleState.ARCHIVED)

    def test_expired_to_archived(self):
        self.ledger.record("T11", BottleState.EXPIRED)
        self.ledger.record("T11", BottleState.ARCHIVED)
        self.assertEqual(self.ledger.get_state("T11"), BottleState.ARCHIVED)


# ---------------------------------------------------------------------------
# 2. Invalid transition attempts for every state
# ---------------------------------------------------------------------------

class TestAllInvalidTransitions(unittest.TestCase):
    """Verify that every invalid transition raises ValueError."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.ledger = BottleLedger(repo_path=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _assert_invalid(self, bottle_id, start, target):
        self.ledger.record(bottle_id, start)
        with self.assertRaises(ValueError, msg=f"{start.value} -> {target.value} should be invalid"):
            self.ledger.record(bottle_id, target)

    # DRAFT can only go to SENT
    def test_draft_to_delivered_invalid(self):
        self._assert_invalid("D1", BottleState.DRAFT, BottleState.DELIVERED)

    def test_draft_to_read_invalid(self):
        self._assert_invalid("D2", BottleState.DRAFT, BottleState.READ)

    def test_draft_to_responded_invalid(self):
        self._assert_invalid("D3", BottleState.DRAFT, BottleState.RESPONDED)

    def test_draft_to_archived_invalid(self):
        self._assert_invalid("D4", BottleState.DRAFT, BottleState.ARCHIVED)

    def test_draft_to_expired_invalid(self):
        self._assert_invalid("D5", BottleState.DRAFT, BottleState.EXPIRED)

    # SENT can only go to DELIVERED or ARCHIVED
    def test_sent_to_read_invalid(self):
        self._assert_invalid("S1", BottleState.SENT, BottleState.READ)

    def test_sent_to_responded_invalid(self):
        self._assert_invalid("S2", BottleState.SENT, BottleState.RESPONDED)

    def test_sent_to_expired_invalid(self):
        self._assert_invalid("S3", BottleState.SENT, BottleState.EXPIRED)

    # ARCHIVED is terminal
    def test_archived_to_any_invalid(self):
        for target in BottleState:
            if target == BottleState.ARCHIVED:
                continue
            self.ledger.record("A1", BottleState.ARCHIVED)
            if target != BottleState.ARCHIVED:
                with self.assertRaises(ValueError):
                    self.ledger.record("A1", target)
                # Reset
                self.ledger._records.clear()

    # RESPONDED can only go to ARCHIVED
    def test_responded_to_delivered_invalid(self):
        self._assert_invalid("R1", BottleState.RESPONDED, BottleState.DELIVERED)

    def test_responded_to_read_invalid(self):
        self._assert_invalid("R2", BottleState.RESPONDED, BottleState.READ)

    def test_responded_to_expired_invalid(self):
        self._assert_invalid("R3", BottleState.RESPONDED, BottleState.EXPIRED)


# ---------------------------------------------------------------------------
# 3. Ledger persistence edge cases
# ---------------------------------------------------------------------------

class TestLedgerPersistenceEdgeCases(unittest.TestCase):
    """Test ledger with corrupted JSON, empty file, missing directory."""

    def test_corrupted_json_starts_fresh(self):
        tmpdir = tempfile.mkdtemp()
        try:
            ledger_path = Path(tmpdir) / "message-in-a-bottle" / ".bottle-state" / "ledger.json"
            ledger_path.parent.mkdir(parents=True)
            ledger_path.write_text("{invalid json!!!", encoding="utf-8")

            ledger = BottleLedger(repo_path=tmpdir)
            # Should start fresh with no records
            self.assertEqual(len(ledger.get_all_records()), 0)
            # Should be able to record new bottles
            ledger.record("NEW-1", BottleState.DRAFT)
            self.assertEqual(ledger.get_state("NEW-1"), BottleState.DRAFT)
        finally:
            shutil.rmtree(tmpdir)

    def test_empty_ledger_file(self):
        tmpdir = tempfile.mkdtemp()
        try:
            ledger_path = Path(tmpdir) / "message-in-a-bottle" / ".bottle-state" / "ledger.json"
            ledger_path.parent.mkdir(parents=True)
            ledger_path.write_text("", encoding="utf-8")

            ledger = BottleLedger(repo_path=tmpdir)
            self.assertEqual(len(ledger.get_all_records()), 0)
        finally:
            shutil.rmtree(tmpdir)

    def test_missing_directory_creates_on_save(self):
        tmpdir = tempfile.mkdtemp()
        try:
            # Ledger dir doesn't exist yet
            ledger_path = Path(tmpdir) / "nonexistent" / "subdir" / "ledger.json"
            ledger = BottleLedger(ledger_file=ledger_path)
            ledger.record("X1", BottleState.DRAFT)
            # File should be created
            self.assertTrue(ledger_path.exists())
            data = json.loads(ledger_path.read_text())
            self.assertIn("X1", data)
        finally:
            shutil.rmtree(tmpdir)

    def test_in_memory_ledger(self):
        """Ledger with no path should work in-memory."""
        ledger = BottleLedger()
        ledger.record("MEM-1", BottleState.DRAFT)
        self.assertEqual(ledger.get_state("MEM-1"), BottleState.DRAFT)
        self.assertEqual(len(ledger.get_all_records()), 1)

    def test_ledger_with_missing_keys_in_json(self):
        """JSON with wrong structure (not a dict) should start fresh."""
        tmpdir = tempfile.mkdtemp()
        try:
            ledger_path = Path(tmpdir) / "message-in-a-bottle" / ".bottle-state" / "ledger.json"
            ledger_path.parent.mkdir(parents=True)
            ledger_path.write_text('"just a string"', encoding="utf-8")

            ledger = BottleLedger(repo_path=tmpdir)
            self.assertEqual(len(ledger.get_all_records()), 0)
        finally:
            shutil.rmtree(tmpdir)

    def test_ledger_with_partial_record_data(self):
        """JSON with a record missing required keys should start fresh."""
        tmpdir = tempfile.mkdtemp()
        try:
            ledger_path = Path(tmpdir) / "message-in-a-bottle" / ".bottle-state" / "ledger.json"
            ledger_path.parent.mkdir(parents=True)
            ledger_path.write_text('{"BAD-1": {"no_state_field": true}}', encoding="utf-8")

            ledger = BottleLedger(repo_path=tmpdir)
            self.assertEqual(len(ledger.get_all_records()), 0)
        finally:
            shutil.rmtree(tmpdir)


# ---------------------------------------------------------------------------
# 4. Router edge cases
# ---------------------------------------------------------------------------

class TestRouterEdgeCases(unittest.TestCase):
    """Empty repo list, duplicate repos, case sensitivity."""

    def test_empty_repo_list(self):
        router = BottleRouter(repos=[])
        targets = router.resolve_target("fleet")
        self.assertEqual(len(targets), 0)
        targets = router.resolve_target("Quill")
        self.assertEqual(len(targets), 0)

    def test_duplicate_repos_deduplicated(self):
        router = BottleRouter(repos=[
            RepoRef(name="repo-quill", agent="Quill", path="/tmp/quill"),
            RepoRef(name="repo-quill", agent="Quill", path="/tmp/quill2"),
        ])
        targets = router.resolve_target("Quill")
        self.assertEqual(len(targets), 1)

    def test_case_sensitive_agent_matching(self):
        router = BottleRouter(repos=[
            RepoRef(name="repo-quill", agent="Quill", path="/tmp/quill"),
        ])
        targets = router.resolve_target("quill")
        self.assertEqual(len(targets), 0)  # lowercase doesn't match

    def test_case_sensitive_role_matching(self):
        router = BottleRouter(repos=[
            RepoRef(name="repo-quill", agent="Quill", path="/tmp/quill",
                    roles=["Architect"]),
        ])
        targets = router.resolve_target("role:architect")
        self.assertEqual(len(targets), 0)  # lowercase role doesn't match

    def test_fleet_deduplication(self):
        router = BottleRouter(repos=[
            RepoRef(name="repo-quill", agent="Quill", path="/tmp/quill", capabilities=["writing"]),
        ])
        # fleet + Quill should not duplicate
        targets = router.resolve_target("fleet,Quill")
        self.assertEqual(len(targets), 1)

    def test_unknown_role_returns_empty(self):
        router = BottleRouter(repos=[
            RepoRef(name="repo-quill", agent="Quill", path="/tmp/quill", roles=["architect"]),
        ])
        targets = router.resolve_target("role:nonexistent")
        self.assertEqual(len(targets), 0)

    def test_unknown_capability_returns_empty(self):
        router = BottleRouter(repos=[
            RepoRef(name="repo-quill", agent="Quill", path="/tmp/quill", capabilities=["coding"]),
        ])
        targets = router.resolve_target("cap:nonexistent")
        self.assertEqual(len(targets), 0)

    def test_register_repo_adds_to_list(self):
        router = BottleRouter(repos=[])
        router.register_repo(RepoRef(name="repo-new", agent="New", path="/tmp/new"))
        targets = router.resolve_target("New")
        self.assertEqual(len(targets), 1)


# ---------------------------------------------------------------------------
# 5. Frontmatter parsing edge cases
# ---------------------------------------------------------------------------

class TestFrontmatterParsingEdgeCases(unittest.TestCase):
    """Multiline values, special characters, numeric strings, boolean parsing."""

    def test_numeric_string_value(self):
        text = "---\nfrom: Agent42\nto: fleet\ntype: MESSAGE\ndate: 2026-04-12T15:30:00Z\nsubject: 12345\n---\nBody.\n"
        fm, _ = parse_frontmatter(text)
        self.assertEqual(fm["subject"], 12345)  # parsed as int

    def test_boolean_true_value(self):
        text = "---\nfrom: Agent42\nto: fleet\ntype: MESSAGE\ndate: 2026-04-12T15:30:00Z\nsubject: true\n---\nBody.\n"
        fm, _ = parse_frontmatter(text)
        self.assertEqual(fm["subject"], True)

    def test_boolean_false_value(self):
        text = "---\nfrom: Agent42\nto: fleet\ntype: MESSAGE\ndate: 2026-04-12T15:30:00Z\nsubject: false\n---\nBody.\n"
        fm, _ = parse_frontmatter(text)
        self.assertEqual(fm["subject"], False)

    def test_boolean_yes_value(self):
        text = "---\nfrom: Agent42\nto: fleet\ntype: MESSAGE\ndate: 2026-04-12T15:30:00Z\nsubject: yes\n---\nBody.\n"
        fm, _ = parse_frontmatter(text)
        self.assertEqual(fm["subject"], True)

    def test_boolean_no_value(self):
        text = "---\nfrom: Agent42\nto: fleet\ntype: MESSAGE\ndate: 2026-04-12T15:30:00Z\nsubject: no\n---\nBody.\n"
        fm, _ = parse_frontmatter(text)
        self.assertEqual(fm["subject"], False)

    def test_special_characters_in_subject(self):
        text = textwrap.dedent("""\
            ---
            from: Quill
            to: fleet
            type: MESSAGE
            date: 2026-04-12T15:30:00Z
            subject: "Hello <world> & 'friends' — \"quotes\""
            ---
            Body.
        """)
        fm, _ = parse_frontmatter(text)
        self.assertIn("<world>", fm["subject"])
        self.assertIn("&", fm["subject"])

    def test_single_quoted_string(self):
        text = "---\nfrom: Agent42\nto: fleet\ntype: MESSAGE\ndate: 2026-04-12T15:30:00Z\nsubject: 'quoted value'\n---\nBody.\n"
        fm, _ = parse_frontmatter(text)
        self.assertEqual(fm["subject"], "quoted value")

    def test_colon_in_quoted_value(self):
        text = textwrap.dedent("""\
            ---
            from: Quill
            to: fleet
            type: MESSAGE
            date: 2026-04-12T15:30:00Z
            subject: "RE: Task R3: Done"
            ---
            Body.
        """)
        fm, _ = parse_frontmatter(text)
        self.assertEqual(fm["subject"], "RE: Task R3: Done")

    def test_float_number_value(self):
        text = "---\nfrom: Agent42\nto: fleet\ntype: MESSAGE\ndate: 2026-04-12T15:30:00Z\npriority: 3.14\n---\nBody.\n"
        fm, _ = parse_frontmatter(text)
        self.assertEqual(fm["priority"], 3.14)

    def test_empty_value(self):
        text = "---\nfrom: Agent42\nto: fleet\ntype: MESSAGE\ndate: 2026-04-12T15:30:00Z\nsubject:\n---\nBody.\n"
        fm, _ = parse_frontmatter(text)
        self.assertEqual(fm["subject"], "")

    def test_yaml_comments_ignored(self):
        text = textwrap.dedent("""\
            ---
            from: Quill
            # This is a comment
            to: fleet
            type: MESSAGE
            date: 2026-04-12T15:30:00Z
            subject: Test
            ---
            Body.
        """)
        fm, _ = parse_frontmatter(text)
        self.assertIsNotNone(fm)
        self.assertEqual(fm["from"], "Quill")

    def test_scalar_parser_quoted(self):
        self.assertEqual(_parse_scalar('"hello world"'), "hello world")
        self.assertEqual(_parse_scalar("'hello world'"), "hello world")

    def test_scalar_parser_bools(self):
        self.assertEqual(_parse_scalar("true"), True)
        self.assertEqual(_parse_scalar("True"), True)
        self.assertEqual(_parse_scalar("TRUE"), True)
        self.assertEqual(_parse_scalar("false"), False)
        self.assertEqual(_parse_scalar("False"), False)
        self.assertEqual(_parse_scalar("yes"), True)
        self.assertEqual(_parse_scalar("no"), False)

    def test_scalar_parser_numbers(self):
        self.assertEqual(_parse_scalar("42"), 42)
        self.assertEqual(_parse_scalar("3.14"), 3.14)
        self.assertEqual(_parse_scalar("0"), 0)

    def test_scalar_parser_plain_string(self):
        self.assertEqual(_parse_scalar("hello"), "hello")
        self.assertEqual(_parse_scalar(""), "")


# ---------------------------------------------------------------------------
# 6. Bottle creation with all Priority and TrustLevel enum values
# ---------------------------------------------------------------------------

class TestAllPriorityAndTrustLevelValues(unittest.TestCase):

    def test_all_priorities(self):
        for p in Priority:
            bottle = make_bottle("Agent", "fleet", "MESSAGE", f"Test {p.value}", "Body.",
                                 priority=p.value)
            self.assertEqual(bottle.frontmatter.priority, p)

    def test_all_trust_levels(self):
        for t in TrustLevel:
            bottle = make_bottle("Agent", "fleet", "MESSAGE", f"Test {t.value}", "Body.",
                                 trust_level=t.value)
            self.assertEqual(bottle.frontmatter.trust_level, t)

    def test_default_priority_is_medium(self):
        bottle = make_bottle("Agent", "fleet", "MESSAGE", "Test", "Body.")
        self.assertEqual(bottle.frontmatter.priority, Priority.PRIORITY_MEDIUM)

    def test_default_trust_is_standard(self):
        bottle = make_bottle("Agent", "fleet", "MESSAGE", "Test", "Body.")
        self.assertEqual(bottle.frontmatter.trust_level, TrustLevel.TRUST_STANDARD)


# ---------------------------------------------------------------------------
# 7. Serialization round-trip for complex bottles
# ---------------------------------------------------------------------------

class TestSerializationRoundTrip(unittest.TestCase):
    """Lists, special chars in subject, full round-trip fidelity."""

    def test_round_trip_with_lists(self):
        bottle = make_bottle(
            "Quill", "fleet", "CLAIM", "Complex claim",
            "## Approach\n\nBuild it.",
            task_refs=["R1", "R2", "R3"],
            repo_refs=["org/repo1", "org/repo2"],
            priority="high",
            trust_level="verified",
            reply_to="MESSAGE-Atlas-20260410-120000.md",
        )
        text = serialize_bottle(bottle)
        self.assertIn("task_refs:", text)
        self.assertIn("repo_refs:", text)
        self.assertIn("R1", text)
        self.assertIn("org/repo1", text)
        self.assertIn("reply_to:", text)

    def test_special_chars_in_subject(self):
        bottle = make_bottle(
            "Quill", "fleet", "MESSAGE",
            "Subject with: colons, <HTML>, & symbols, and \"quotes\"",
            "Body content.",
        )
        text = serialize_bottle(bottle)
        # Subject with special chars should be quoted
        self.assertIn("Subject with:", text)
        # Re-parse to verify
        fm_data, body = parse_frontmatter(text)
        self.assertIsNotNone(fm_data)
        self.assertIn("Subject with:", str(fm_data["subject"]))

    def test_round_trip_preserves_all_fields(self):
        bottle = make_bottle(
            "Quill", "Cipher", "RESPONSE", "Re: Hello",
            "I agree.",
            priority="critical",
            trust_level="verified",
            reply_to="MESSAGE-Cipher-20260412-140000.md",
            task_refs=["R5"],
        )
        text = serialize_bottle(bottle)
        fm_data, body = parse_frontmatter(text)
        self.assertEqual(fm_data["from"], "Quill")
        self.assertEqual(fm_data["to"], "Cipher")
        self.assertEqual(fm_data["type"], "RESPONSE")
        self.assertEqual(fm_data["priority"], "critical")
        self.assertEqual(fm_data["trust_level"], "verified")
        self.assertEqual(fm_data["reply_to"], "MESSAGE-Cipher-20260412-140000.md")


# ---------------------------------------------------------------------------
# 8. get_overdue with various max_age_days values
# ---------------------------------------------------------------------------

class TestGetOverdueVariousMaxAge(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.ledger = BottleLedger(repo_path=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _make_old_delivered(self, bid, days_old):
        self.ledger.record(bid, BottleState.DELIVERED, from_agent="Quill", to="Cipher")
        old_ts = (datetime.now(timezone.utc) - timedelta(days=days_old)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        rec = self.ledger.get_record(bid)
        assert rec is not None
        rec.history[-1] = StateTransition(state=BottleState.DELIVERED, timestamp=old_ts)
        self.ledger._save()

    def test_max_age_zero_same_day_not_overdue(self):
        """A bottle created today is not >0 days old, so not overdue with max_age_days=0."""
        self._make_old_delivered("Z1", 0)
        overdue = self.ledger.get_overdue(max_age_days=0)
        self.assertEqual(len(overdue), 0)

    def test_max_age_one_day(self):
        self._make_old_delivered("Z2", 2)
        overdue = self.ledger.get_overdue(max_age_days=1)
        self.assertEqual(len(overdue), 1)

    def test_max_age_thirty_days(self):
        self._make_old_delivered("Z3", 5)
        overdue = self.ledger.get_overdue(max_age_days=30)
        self.assertEqual(len(overdue), 0)

    def test_max_age_exactly_at_threshold(self):
        """Bottle exactly max_age_days old should NOT be overdue (uses > not >=)."""
        self._make_old_delivered("Z4", 7)
        overdue = self.ledger.get_overdue(max_age_days=7)
        self.assertEqual(len(overdue), 0)

    def test_read_state_also_tracked_for_overdue(self):
        self.ledger.record("Z5", BottleState.READ, from_agent="Quill", to="Cipher")
        old_ts = (datetime.now(timezone.utc) - timedelta(days=15)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        rec = self.ledger.get_record("Z5")
        assert rec is not None
        rec.history[-1] = StateTransition(state=BottleState.READ, timestamp=old_ts)
        self.ledger._save()

        overdue = self.ledger.get_overdue(max_age_days=7)
        self.assertEqual(len(overdue), 1)
        self.assertEqual(overdue[0].current_state, BottleState.READ)

    def test_archived_not_overdue(self):
        self.ledger.record("Z6", BottleState.DELIVERED)
        self.ledger.record("Z6", BottleState.ARCHIVED)
        overdue = self.ledger.get_overdue(max_age_days=0)
        self.assertEqual(len(overdue), 0)


# ---------------------------------------------------------------------------
# 9. generate_status_report with different state distributions
# ---------------------------------------------------------------------------

class TestStatusReportEdgeCases(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.ledger = BottleLedger(repo_path=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_empty_ledger_report(self):
        report = self.ledger.generate_status_report()
        self.assertIn("# Bottle Status Report", report)
        self.assertIn("Total tracked:** 0", report)
        self.assertIn("State Distribution", report)

    def test_all_states_represented(self):
        """Create at least one bottle in each state."""
        for i, state in enumerate(BottleState):
            self.ledger.record(f"ALL-{i}", state, from_agent="Quill", to="fleet",
                               subject=f"Bottle in {state.value}")
        report = self.ledger.generate_status_report()
        for state in BottleState:
            self.assertIn(state.value, report)

    def test_report_shows_overdue_section(self):
        self.ledger.record("O1", BottleState.DELIVERED, from_agent="Quill", to="Cipher",
                           subject="Overdue bottle")
        old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        rec = self.ledger.get_record("O1")
        assert rec is not None
        rec.history[-1] = StateTransition(state=BottleState.DELIVERED, timestamp=old_ts)
        self.ledger._save()

        report = self.ledger.generate_status_report()
        self.assertIn("Overdue", report)
        self.assertIn("O1", report)

    def test_report_shows_pending_by_agent(self):
        self.ledger.record("P1", BottleState.DELIVERED, from_agent="Quill", to="Cipher",
                           subject="Pending for Cipher")
        report = self.ledger.generate_status_report()
        self.assertIn("Pending by Agent", report)
        self.assertIn("Cipher", report)


# ---------------------------------------------------------------------------
# 10. BottleValidator with known_agents filtering
# ---------------------------------------------------------------------------

class TestValidatorKnownAgentsFiltering(unittest.TestCase):

    def test_known_agent_passes(self):
        v = BottleValidator(known_agents=["Quill", "Cipher"])
        bottle = make_bottle("Quill", "fleet", "MESSAGE", "Test",
                             "# Introduction\n\nI am Quill, a writing agent with many capabilities for the fleet.")
        issues = v.validate(bottle)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "from"]
        self.assertEqual(len(warnings), 0)

    def test_unknown_agent_warning(self):
        v = BottleValidator(known_agents=["Quill", "Cipher"])
        bottle = make_bottle("Stranger", "fleet", "MESSAGE", "Test", "Body content here.")
        issues = v.validate(bottle)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "from"]
        self.assertTrue(len(warnings) > 0)
        self.assertIn("Stranger", str(warnings[0]))

    def test_no_known_agents_no_warning(self):
        v = BottleValidator()  # No known agents
        bottle = make_bottle("Anyone", "fleet", "MESSAGE", "Test", "Body content here.")
        issues = v.validate(bottle)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "from"]
        self.assertEqual(len(warnings), 0)

    def test_known_roles_validation(self):
        v = BottleValidator(known_roles=["architect", "engineer"])
        fm = BottleFrontmatter(
            from_agent="Quill", to="role:manager", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="Test",
        )
        issues = v.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "to"]
        self.assertTrue(any("manager" in str(w) for w in warnings))

    def test_known_caps_validation(self):
        v = BottleValidator(known_caps=["writing", "coding"])
        fm = BottleFrontmatter(
            from_agent="Quill", to="cap:flying", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="Test",
        )
        issues = v.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "to"]
        self.assertTrue(any("flying" in str(w) for w in warnings))


# ---------------------------------------------------------------------------
# 11. Date validation edge cases
# ---------------------------------------------------------------------------

class TestDateValidationEdgeCases(unittest.TestCase):

    def setUp(self):
        self.validator = BottleValidator()

    def test_iso_with_z_suffix(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="fleet", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="Test",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR and i.field == "date"]
        self.assertEqual(len(errors), 0)

    def test_iso_with_offset(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="fleet", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00+05:30", subject="Test",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR and i.field == "date"]
        self.assertEqual(len(errors), 0)

    def test_iso_with_utc_offset(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="fleet", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00+00:00", subject="Test",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR and i.field == "date"]
        self.assertEqual(len(errors), 0)

    def test_no_timezone_warning(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="fleet", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00", subject="Test",
        )
        issues = self.validator.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "date"]
        self.assertTrue(len(warnings) > 0)

    def test_invalid_date_string(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="fleet", type=BottleType.MESSAGE,
            date="not-even-close", subject="Test",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR and i.field == "date"]
        self.assertTrue(len(errors) > 0)

    def test_empty_date(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="fleet", type=BottleType.MESSAGE,
            date="", subject="Test",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR and i.field == "date"]
        self.assertTrue(len(errors) > 0)

    def test_negative_offset(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="fleet", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00-08:00", subject="Test",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR and i.field == "date"]
        self.assertEqual(len(errors), 0)


# ---------------------------------------------------------------------------
# 12. Multiple bottles tracked simultaneously
# ---------------------------------------------------------------------------

class TestMultipleBottlesSimultaneously(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.ledger = BottleLedger(repo_path=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_many_bottles_tracked(self):
        for i in range(50):
            self.ledger.record(f"BULK-{i:03d}", BottleState.DRAFT, from_agent="Quill")
        self.assertEqual(len(self.ledger.get_all_records()), 50)

    def test_different_agents_interleaved(self):
        agents = ["Quill", "Cipher", "Atlas", "Nova", "Echo"]
        for i, agent in enumerate(agents):
            self.ledger.record(f"INT-{i}", BottleState.SENT, from_agent=agent, to="fleet")
        quill_records = self.ledger.get_by_agent("Quill")
        self.assertEqual(len(quill_records), 1)

    def test_get_all_records_returns_all(self):
        self.ledger.record("M1", BottleState.DRAFT)
        self.ledger.record("M2", BottleState.SENT)
        self.ledger.record("M3", BottleState.ARCHIVED)
        self.assertEqual(len(self.ledger.get_all_records()), 3)

    def test_get_pending_across_multiple_agents(self):
        self.ledger.record("MP1", BottleState.DELIVERED, from_agent="Quill", to="Cipher")
        self.ledger.record("MP2", BottleState.READ, from_agent="Atlas", to="Cipher")
        self.ledger.record("MP3", BottleState.DELIVERED, from_agent="Quill", to="Atlas")

        cipher_pending = self.ledger.get_pending("Cipher")
        self.assertEqual(len(cipher_pending), 2)
        atlas_pending = self.ledger.get_pending("Atlas")
        self.assertEqual(len(atlas_pending), 1)


# ---------------------------------------------------------------------------
# 13. Idempotent state recording
# ---------------------------------------------------------------------------

class TestIdempotentStateRecording(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.ledger = BottleLedger(repo_path=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_draft_repeated(self):
        self.ledger.record("ID1", BottleState.DRAFT)
        self.ledger.record("ID1", BottleState.DRAFT)
        self.ledger.record("ID1", BottleState.DRAFT)
        state = self.ledger.get_state("ID1")
        self.assertEqual(state, BottleState.DRAFT)
        history = self.ledger.get_history("ID1")
        self.assertEqual(len(history), 3)  # Each re-record appends to history

    def test_archived_repeated(self):
        self.ledger.record("ID2", BottleState.ARCHIVED)
        self.ledger.record("ID2", BottleState.ARCHIVED)
        self.assertEqual(self.ledger.get_state("ID2"), BottleState.ARCHIVED)
        history = self.ledger.get_history("ID2")
        self.assertEqual(len(history), 2)

    def test_sent_repeated_then_progress(self):
        self.ledger.record("ID3", BottleState.SENT)
        self.ledger.record("ID3", BottleState.SENT)
        self.ledger.record("ID3", BottleState.DELIVERED)  # valid transition
        self.assertEqual(self.ledger.get_state("ID3"), BottleState.DELIVERED)
        history = self.ledger.get_history("ID3")
        self.assertEqual(len(history), 3)

    def test_metadata_update_on_idempotent_record(self):
        self.ledger.record("ID4", BottleState.DRAFT, from_agent="Quill")
        # Re-record with more metadata
        self.ledger.record("ID4", BottleState.DRAFT, to="Cipher", subject="Updated")
        rec = self.ledger.get_record("ID4")
        assert rec is not None
        self.assertEqual(rec.to, "Cipher")
        self.assertEqual(rec.subject, "Updated")


# ---------------------------------------------------------------------------
# 14. Empty body validation per bottle type
# ---------------------------------------------------------------------------

class TestEmptyBodyValidationPerType(unittest.TestCase):

    def setUp(self):
        self.validator = BottleValidator()

    def test_empty_body_message(self):
        issues = self.validator.validate_body("", BottleType.MESSAGE)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_empty_body_introduction(self):
        issues = self.validator.validate_body("", BottleType.INTRODUCTION)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_empty_body_claim(self):
        issues = self.validator.validate_body("", BottleType.CLAIM)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_empty_body_response(self):
        issues = self.validator.validate_body("", BottleType.RESPONSE)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_empty_body_status_update(self):
        issues = self.validator.validate_body("", BottleType.STATUS_UPDATE)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_empty_body_broadcast(self):
        issues = self.validator.validate_body("", BottleType.BROADCAST)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_empty_body_rfc_submission(self):
        issues = self.validator.validate_body("", BottleType.RFC_SUBMISSION)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_empty_body_task_completion(self):
        issues = self.validator.validate_body("", BottleType.TASK_COMPLETION)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)

    def test_whitespace_only_body(self):
        issues = self.validator.validate_body("   \n\t\n   ", BottleType.MESSAGE)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(len(errors) > 0)


# ---------------------------------------------------------------------------
# 15. TARGET validation with various role: and cap: formats
# ---------------------------------------------------------------------------

class TestTargetValidationEdgeCases(unittest.TestCase):

    def setUp(self):
        self.validator = BottleValidator(
            known_agents=["Quill", "Cipher"],
            known_roles=["architect", "engineer"],
            known_caps=["writing", "coding"],
        )

    def test_fleet_target_no_issues(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="fleet", type=BottleType.BROADCAST,
            date="2026-04-12T15:30:00Z", subject="Announcement",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertEqual(len(errors), 0)

    def test_multi_target_known_agents(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="Quill,Cipher", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="Hello both",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertEqual(len(errors), 0)

    def test_multi_target_with_unknown_agent(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="Quill,Unknown", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="Hello",
        )
        issues = self.validator.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "to"]
        self.assertTrue(any("Unknown" in str(w) for w in warnings))

    def test_role_target_known(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="role:architect", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="To architects",
        )
        issues = self.validator.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "to"]
        self.assertEqual(len(warnings), 0)

    def test_role_target_unknown(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="role:manager", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="To managers",
        )
        issues = self.validator.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "to"]
        self.assertTrue(len(warnings) > 0)

    def test_cap_target_known(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="cap:writing", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="To writers",
        )
        issues = self.validator.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "to"]
        self.assertEqual(len(warnings), 0)

    def test_cap_target_unknown(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="cap:flying", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="To flyers",
        )
        issues = self.validator.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "to"]
        self.assertTrue(len(warnings) > 0)

    def test_empty_to_field_error(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="Test",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertTrue(any(i.field == "to" for i in errors))

    def test_no_known_agents_no_target_warnings(self):
        v = BottleValidator()  # No known agents
        fm = BottleFrontmatter(
            from_agent="Quill", to="Anyone", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="Test",
        )
        issues = v.validate_frontmatter(fm)
        warnings = [i for i in issues if i.severity == Severity.WARNING and i.field == "to"]
        self.assertEqual(len(warnings), 0)

    def test_mixed_targets_fleet_and_role(self):
        fm = BottleFrontmatter(
            from_agent="Quill", to="fleet,role:architect", type=BottleType.MESSAGE,
            date="2026-04-12T15:30:00Z", subject="Mixed targets",
        )
        issues = self.validator.validate_frontmatter(fm)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        self.assertEqual(len(errors), 0)


# ---------------------------------------------------------------------------
# 16. BottleRecord and StateTransition serialization
# ---------------------------------------------------------------------------

class TestRecordSerialization(unittest.TestCase):

    def test_state_transition_round_trip(self):
        st = StateTransition(state=BottleState.DRAFT, timestamp="2026-04-12T15:30:00Z")
        d = st.to_dict()
        st2 = StateTransition.from_dict(d)
        self.assertEqual(st2.state, BottleState.DRAFT)
        self.assertEqual(st2.timestamp, "2026-04-12T15:30:00Z")

    def test_bottle_record_round_trip(self):
        rec = BottleRecord(
            bottle_id="TEST-1",
            current_state=BottleState.SENT,
            history=[
                StateTransition(state=BottleState.DRAFT, timestamp="2026-04-12T15:00:00Z"),
                StateTransition(state=BottleState.SENT, timestamp="2026-04-12T15:30:00Z"),
            ],
            from_agent="Quill",
            to="Cipher",
            bottle_type="MESSAGE",
            subject="Hello",
        )
        d = rec.to_dict()
        rec2 = BottleRecord.from_dict(d)
        self.assertEqual(rec2.bottle_id, "TEST-1")
        self.assertEqual(rec2.current_state, BottleState.SENT)
        self.assertEqual(len(rec2.history), 2)
        self.assertEqual(rec2.from_agent, "Quill")
        self.assertEqual(rec2.subject, "Hello")

    def test_bottle_record_defaults(self):
        rec = BottleRecord(bottle_id="EMPTY", current_state=BottleState.DRAFT)
        d = rec.to_dict()
        rec2 = BottleRecord.from_dict(d)
        self.assertEqual(rec2.from_agent, "")
        self.assertEqual(rec2.to, "")
        self.assertEqual(rec2.history, [])


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
