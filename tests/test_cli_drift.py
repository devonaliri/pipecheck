"""Tests for drift CLI commands."""

import json
import tempfile
from pathlib import Path
from argparse import Namespace

import pytest

from pipecheck.cli_drift import cmd_drift
from pipecheck.schema import PipelineSchema, ColumnSchema


def _create_schema_file(directory: Path, schema_dict: dict) -> Path:
    """Helper to create a schema file."""
    schema_file = directory / "schema.json"
    schema_file.write_text(json.dumps(schema_dict))
    return schema_file


def _make_schema_dict(name: str = "test_pipeline", columns: list = None) -> dict:
    """Helper to create a schema dictionary."""
    if columns is None:
        columns = [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "name", "type": "string", "nullable": True}
        ]
    return {
        "name": name,
        "version": "1.0.0",
        "columns": columns
    }


class TestCmdDrift:
    """Tests for cmd_drift function."""
    
    def test_set_baseline_creates_baseline(self):
        """Test setting a baseline."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            baseline_dir = tmppath / "baselines"
            
            schema_file = _create_schema_file(tmppath, _make_schema_dict())
            
            args = Namespace(
                schema_file=str(schema_file),
                pipeline="test_pipeline",
                baseline_dir=str(baseline_dir),
                set_baseline=True
            )
            
            exit_code = cmd_drift(args)
            
            assert exit_code == 0
            assert baseline_dir.exists()
            assert (baseline_dir / "test_pipeline.json").exists()
    
    def test_no_baseline_returns_error(self):
        """Test detecting drift with no baseline returns error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            baseline_dir = tmppath / "baselines"
            
            schema_file = _create_schema_file(tmppath, _make_schema_dict())
            
            args = Namespace(
                schema_file=str(schema_file),
                pipeline="test_pipeline",
                baseline_dir=str(baseline_dir),
                set_baseline=False
            )
            
            exit_code = cmd_drift(args)
            
            assert exit_code == 2
    
    def test_no_drift_returns_zero(self):
        """Test no drift returns exit code 0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            baseline_dir = tmppath / "baselines"
            
            schema_dict = _make_schema_dict()
            schema_file = _create_schema_file(tmppath, schema_dict)
            
            # Set baseline first
            args_baseline = Namespace(
                schema_file=str(schema_file),
                pipeline="test_pipeline",
                baseline_dir=str(baseline_dir),
                set_baseline=True
            )
            cmd_drift(args_baseline)
            
            # Check for drift with identical schema
            args_check = Namespace(
                schema_file=str(schema_file),
                pipeline="test_pipeline",
                baseline_dir=str(baseline_dir),
                set_baseline=False
            )
            
            exit_code = cmd_drift(args_check)
            
            assert exit_code == 0
    
    def test_drift_detected_returns_one(self):
        """Test drift detection returns exit code 1."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            baseline_dir = tmppath / "baselines"
            
            # Create baseline schema
            baseline_schema = _make_schema_dict()
            schema_file = _create_schema_file(tmppath, baseline_schema)
            
            args_baseline = Namespace(
                schema_file=str(schema_file),
                pipeline="test_pipeline",
                baseline_dir=str(baseline_dir),
                set_baseline=True
            )
            cmd_drift(args_baseline)
            
            # Create drifted schema with additional column
            drifted_schema = _make_schema_dict()
            drifted_schema["columns"].append(
                {"name": "email", "type": "string", "nullable": True}
            )
            drifted_file = _create_schema_file(tmppath / "drifted", drifted_schema)
            
            args_check = Namespace(
                schema_file=str(drifted_file),
                pipeline="test_pipeline",
                baseline_dir=str(baseline_dir),
                set_baseline=False
            )
            
            exit_code = cmd_drift(args_check)
            
            assert exit_code == 1
