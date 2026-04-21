"""Tests for pipecheck.trace."""
import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.trace import TraceStep, TraceResult, trace_column


def _col(name: str, data_type: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=data_type, nullable=nullable)


def _make_schema(name: str, columns) -> PipelineSchema:
    return PipelineSchema(name=name, version="1.0", columns=columns)


# ---------------------------------------------------------------------------
# TraceStep
# ---------------------------------------------------------------------------

class TestTraceStep:
    def test_str_not_null(self):
        step = TraceStep(pipeline="raw", column="id", data_type="integer", nullable=False)
        assert str(step) == "raw.id (integer, not null)"

    def test_str_nullable(self):
        step = TraceStep(pipeline="clean", column="email", data_type="string", nullable=True)
        assert str(step) == "clean.email (string, nullable)"


# ---------------------------------------------------------------------------
# TraceResult
# ---------------------------------------------------------------------------

class TestTraceResult:
    def test_found_false_when_empty(self):
        result = TraceResult(column_name="x")
        assert not result.found()

    def test_found_true_when_steps(self):
        step = TraceStep(pipeline="a", column="x", data_type="string", nullable=False)
        result = TraceResult(column_name="x", steps=[step])
        assert result.found()

    def test_len(self):
        steps = [
            TraceStep("a", "x", "string", False),
            TraceStep("b", "x", "string", True),
        ]
        result = TraceResult(column_name="x", steps=steps)
        assert len(result) == 2

    def test_str_no_steps(self):
        result = TraceResult(column_name="missing")
        assert "No trace found" in str(result)
        assert "missing" in str(result)

    def test_str_with_steps(self):
        steps = [
            TraceStep("raw", "user_id", "integer", False),
            TraceStep("clean", "user_id", "integer", False),
        ]
        result = TraceResult(column_name="user_id", steps=steps)
        text = str(result)
        assert "user_id" in text
        assert "raw.user_id" in text
        assert "clean.user_id" in text


# ---------------------------------------------------------------------------
# trace_column
# ---------------------------------------------------------------------------

class TestTraceColumn:
    def _build_pipeline(self):
        s1 = _make_schema("raw", [_col("id", "integer"), _col("name", "string")])
        s2 = _make_schema("clean", [_col("id", "integer"), _col("name", "string"), _col("score", "float")])
        s3 = _make_schema("agg", [_col("id", "integer"), _col("score", "float")])
        return [s1, s2, s3]

    def test_column_present_in_all_schemas(self):
        schemas = self._build_pipeline()
        result = trace_column("id", schemas)
        assert result.found()
        assert len(result) == 3

    def test_column_present_in_subset(self):
        schemas = self._build_pipeline()
        result = trace_column("score", schemas)
        assert result.found()
        assert len(result) == 2
        assert result.steps[0].pipeline == "clean"
        assert result.steps[1].pipeline == "agg"

    def test_column_not_present(self):
        schemas = self._build_pipeline()
        result = trace_column("unknown_col", schemas)
        assert not result.found()
        assert len(result) == 0

    def test_empty_schema_list(self):
        result = trace_column("id", [])
        assert not result.found()

    def test_step_preserves_nullable_flag(self):
        s = _make_schema("src", [_col("email", "string", nullable=True)])
        result = trace_column("email", [s])
        assert result.steps[0].nullable is True

    def test_step_preserves_data_type(self):
        s = _make_schema("src", [_col("amount", "decimal")])
        result = trace_column("amount", [s])
        assert result.steps[0].data_type == "decimal"
