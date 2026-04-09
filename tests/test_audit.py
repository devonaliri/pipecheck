"""Tests for pipecheck.audit."""
import json
from pathlib import Path

import pytest

from pipecheck.audit import (
    AuditEntry,
    clear_history,
    get_history,
    record,
)


@pytest.fixture()
def audit_dir(tmp_path: Path) -> Path:
    return tmp_path / "audit"


class TestAuditEntry:
    def test_str_format(self):
        entry = AuditEntry(
            pipeline="orders",
            action="diff",
            timestamp="2024-01-01T00:00:00+00:00",
        )
        text = str(entry)
        assert "orders" in text
        assert "diff" in text
        assert "2024-01-01" in text

    def test_roundtrip(self):
        entry = AuditEntry(
            pipeline="sales",
            action="save_snapshot",
            timestamp="2024-06-15T12:00:00+00:00",
            details={"version": "1.2"},
        )
        restored = AuditEntry.from_dict(entry.to_dict())
        assert restored.pipeline == entry.pipeline
        assert restored.action == entry.action
        assert restored.details == entry.details

    def test_from_dict_defaults_details(self):
        entry = AuditEntry.from_dict(
            {"pipeline": "p", "action": "a", "timestamp": "t"}
        )
        assert entry.details == {}


class TestRecord:
    def test_creates_file(self, audit_dir):
        record("orders", "diff", audit_dir=audit_dir)
        files = list(audit_dir.iterdir())
        assert len(files) == 1

    def test_appends_multiple(self, audit_dir):
        record("orders", "diff", audit_dir=audit_dir)
        record("orders", "save_snapshot", audit_dir=audit_dir)
        history = get_history("orders", audit_dir=audit_dir)
        assert len(history) == 2

    def test_entry_fields(self, audit_dir):
        entry = record("orders", "diff", details={"env": "prod"}, audit_dir=audit_dir)
        assert entry.pipeline == "orders"
        assert entry.action == "diff"
        assert entry.details == {"env": "prod"}
        assert entry.timestamp  # non-empty

    def test_separate_pipelines(self, audit_dir):
        record("orders", "diff", audit_dir=audit_dir)
        record("users", "diff", audit_dir=audit_dir)
        assert len(get_history("orders", audit_dir=audit_dir)) == 1
        assert len(get_history("users", audit_dir=audit_dir)) == 1


class TestGetHistory:
    def test_empty_when_no_log(self, audit_dir):
        result = get_history("nonexistent", audit_dir=audit_dir)
        assert result == []

    def test_order_preserved(self, audit_dir):
        record("p", "first", audit_dir=audit_dir)
        record("p", "second", audit_dir=audit_dir)
        history = get_history("p", audit_dir=audit_dir)
        assert history[0].action == "first"
        assert history[1].action == "second"


class TestClearHistory:
    def test_returns_true_when_deleted(self, audit_dir):
        record("orders", "diff", audit_dir=audit_dir)
        assert clear_history("orders", audit_dir=audit_dir) is True

    def test_returns_false_when_missing(self, audit_dir):
        assert clear_history("ghost", audit_dir=audit_dir) is False

    def test_history_empty_after_clear(self, audit_dir):
        record("orders", "diff", audit_dir=audit_dir)
        clear_history("orders", audit_dir=audit_dir)
        assert get_history("orders", audit_dir=audit_dir) == []
