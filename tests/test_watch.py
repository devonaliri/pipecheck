"""Tests for pipecheck.watch."""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from pipecheck.watch import WatchConfig, WatchEvent, watch, _file_hash
from pipecheck.drift import DriftReport
from pipecheck.differ import SchemaDiff


def _make_schema_dict(name: str = "orders", extra_col: bool = False) -> dict:
    cols = [{"name": "id", "type": "integer", "nullable": False}]
    if extra_col:
        cols.append({"name": "amount", "type": "float", "nullable": True})
    return {"name": name, "version": "1.0", "columns": cols}


def _write(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data))


# ---------------------------------------------------------------------------
# WatchEvent
# ---------------------------------------------------------------------------

class TestWatchEvent:
    def _make_report(self, has_drift: bool = False) -> DriftReport:
        diff = MagicMock(spec=SchemaDiff)
        diff.has_changes = has_drift
        return DriftReport(pipeline="orders", diff=diff, has_drift=has_drift)

    def test_str_no_drift(self, tmp_path):
        report = self._make_report(has_drift=False)
        event = WatchEvent(path=tmp_path / "s.json", report=report, iteration=1)
        assert "no drift" in str(event)
        assert "iter=1" in str(event)

    def test_str_drift(self, tmp_path):
        report = self._make_report(has_drift=True)
        event = WatchEvent(path=tmp_path / "s.json", report=report, iteration=3)
        assert "DRIFT DETECTED" in str(event)


# ---------------------------------------------------------------------------
# _file_hash
# ---------------------------------------------------------------------------

def test_file_hash_changes_on_edit(tmp_path):
    f = tmp_path / "schema.json"
    f.write_text(json.dumps(_make_schema_dict()))
    h1 = _file_hash(f)
    f.write_text(json.dumps(_make_schema_dict(extra_col=True)))
    h2 = _file_hash(f)
    assert h1 != h2


def test_file_hash_stable(tmp_path):
    f = tmp_path / "schema.json"
    f.write_text("hello")
    assert _file_hash(f) == _file_hash(f)


# ---------------------------------------------------------------------------
# watch()
# ---------------------------------------------------------------------------

class TestWatch:
    def _config(self, path: Path, iterations: int = 1, on_event=None) -> WatchConfig:
        return WatchConfig(
            path=path,
            interval=0.0,
            max_iterations=iterations,
            on_event=on_event,
        )

    def test_raises_when_file_missing(self, tmp_path):
        cfg = self._config(tmp_path / "missing.json")
        with pytest.raises(FileNotFoundError):
            watch(cfg)

    def test_no_event_when_file_unchanged(self, tmp_path):
        f = tmp_path / "schema.json"
        _write(f, _make_schema_dict())
        with patch("pipecheck.watch.time.sleep"):
            events = watch(self._config(f, iterations=2))
        assert events == []

    def test_event_emitted_on_change(self, tmp_path):
        f = tmp_path / "schema.json"
        _write(f, _make_schema_dict())

        call_count = 0

        def mutate_on_second_call(*_):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                _write(f, _make_schema_dict(extra_col=True))

        with patch("pipecheck.watch.time.sleep", side_effect=mutate_on_second_call):
            events = watch(self._config(f, iterations=2))

        assert len(events) == 1
        assert isinstance(events[0], WatchEvent)
        assert events[0].iteration == 1

    def test_on_event_callback_called(self, tmp_path):
        f = tmp_path / "schema.json"
        _write(f, _make_schema_dict())
        received = []

        def mutate(*_):
            _write(f, _make_schema_dict(extra_col=True))

        with patch("pipecheck.watch.time.sleep", side_effect=mutate):
            watch(self._config(f, iterations=1, on_event=received.append))

        assert len(received) == 1
        assert isinstance(received[0], WatchEvent)
