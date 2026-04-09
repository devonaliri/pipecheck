"""Tests for schema drift detection."""

import pytest
import tempfile
import os
from datetime import datetime

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.drift import detect_drift, set_baseline, DriftReport
from pipecheck.snapshot import save_snapshot


def _make_schema(name="test", version="1.0", columns=None):
    """Helper to create test schemas."""
    if columns is None:
        columns = [
            ColumnSchema(name="id", data_type="integer", nullable=False),
            ColumnSchema(name="name", data_type="string", nullable=True)
        ]
    return PipelineSchema(name=name, version=version, columns=columns)


class TestDetectDrift:
    """Test drift detection functionality."""
    
    def test_no_drift_when_identical(self):
        """Should report no drift when schemas are identical."""
        with tempfile.TemporaryDirectory() as tmpdir:
            schema = _make_schema()
            save_snapshot(schema, "baseline", tmpdir)
            
            report = detect_drift(schema, "baseline", tmpdir)
            
            assert not report.has_drift
            assert report.schema_name == "test"
            assert report.baseline_version == "1.0"
            assert report.current_version == "1.0"
    
    def test_drift_when_column_added(self):
        """Should detect drift when column is added."""
        with tempfile.TemporaryDirectory() as tmpdir:
            baseline = _make_schema(version="1.0")
            save_snapshot(baseline, "baseline", tmpdir)
            
            current = _make_schema(
                version="1.1",
                columns=baseline.columns + [
                    ColumnSchema(name="email", data_type="string", nullable=True)
                ]
            )
            
            report = detect_drift(current, "baseline", tmpdir)
            
            assert report.has_drift
            assert len(report.drift.added) == 1
            assert report.drift.added[0].name == "email"
    
    def test_drift_when_column_removed(self):
        """Should detect drift when column is removed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            baseline = _make_schema(version="1.0")
            save_snapshot(baseline, "baseline", tmpdir)
            
            current = _make_schema(
                version="1.1",
                columns=[baseline.columns[0]]  # Only keep first column
            )
            
            report = detect_drift(current, "baseline", tmpdir)
            
            assert report.has_drift
            assert len(report.drift.removed) == 1
            assert report.drift.removed[0].name == "name"
    
    def test_raises_when_baseline_missing(self):
        """Should raise error when baseline doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            schema = _make_schema()
            
            with pytest.raises(ValueError, match="Baseline snapshot.*not found"):
                detect_drift(schema, "baseline", tmpdir)
    
    def test_report_str_format(self):
        """Should format report as readable string."""
        with tempfile.TemporaryDirectory() as tmpdir:
            baseline = _make_schema(version="1.0")
            save_snapshot(baseline, "baseline", tmpdir)
            
            report = detect_drift(baseline, "baseline", tmpdir)
            report_str = str(report)
            
            assert "Drift Report: test" in report_str
            assert "Baseline: 1.0" in report_str
            assert "No drift detected" in report_str


class TestSetBaseline:
    """Test baseline setting functionality."""
    
    def test_saves_as_baseline_snapshot(self):
        """Should save schema with 'baseline' name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            schema = _make_schema()
            
            set_baseline(schema, tmpdir)
            
            # Should be able to detect drift against it
            report = detect_drift(schema, "baseline", tmpdir)
            assert not report.has_drift
