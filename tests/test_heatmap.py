"""Tests for pipecheck.heatmap."""
import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.heatmap import HeatmapCell, HeatmapResult, build_heatmap


def _col(name, *, desc="", tags=None, dtype="string"):
    return ColumnSchema(name=name, data_type=dtype, description=desc, tags=tags or [])


def _make_schema(name, cols):
    return PipelineSchema(name=name, version="1", columns=cols)


class TestHeatmapCell:
    def test_score_all_present(self):
        cell = HeatmapCell("p", "c", True, True, True)
        assert cell.score == 3

    def test_score_none_present(self):
        cell = HeatmapCell("p", "c", False, False, False)
        assert cell.score == 0

    def test_str_full(self):
        cell = HeatmapCell("pipe", "col", True, True, True)
        assert "[DTY]" in str(cell)
        assert "3/3" in str(cell)

    def test_str_partial(self):
        cell = HeatmapCell("pipe", "col", True, False, False)
        assert "[D..]" in str(cell)
        assert "1/3" in str(cell)


class TestBuildHeatmap:
    def test_empty_schemas_returns_empty(self):
        result = build_heatmap([])
        assert result.total_cells == 0

    def test_cell_count_matches_columns(self):
        s = _make_schema("pipe", [_col("a"), _col("b")])
        result = build_heatmap([s])
        assert result.total_cells == 2

    def test_has_description_detected(self):
        s = _make_schema("p", [_col("a", desc="hello")])
        result = build_heatmap([s])
        assert result.cells[0].has_description is True

    def test_missing_description_detected(self):
        s = _make_schema("p", [_col("a", desc="")])
        result = build_heatmap([s])
        assert result.cells[0].has_description is False

    def test_tags_detected(self):
        s = _make_schema("p", [_col("a", tags=["pii"])])
        result = build_heatmap([s])
        assert result.cells[0].has_tags is True

    def test_pipelines_list(self):
        s1 = _make_schema("alpha", [_col("x")])
        s2 = _make_schema("beta", [_col("y")])
        result = build_heatmap([s1, s2])
        assert result.pipelines() == ["alpha", "beta"]

    def test_cells_for_filters_by_pipeline(self):
        s1 = _make_schema("alpha", [_col("x"), _col("y")])
        s2 = _make_schema("beta", [_col("z")])
        result = build_heatmap([s1, s2])
        assert len(result.cells_for("alpha")) == 2
        assert len(result.cells_for("beta")) == 1

    def test_average_score_full(self):
        s = _make_schema("p", [_col("a", desc="d", tags=["t"], dtype="int")])
        result = build_heatmap([s])
        assert result.average_score == pytest.approx(1.0)

    def test_average_score_empty(self):
        result = build_heatmap([])
        assert result.average_score == 0.0

    def test_str_no_data(self):
        result = HeatmapResult(cells=[])
        assert str(result) == "No data."

    def test_str_contains_pipeline_name(self):
        s = _make_schema("mypipe", [_col("a", desc="d", tags=["t"])])
        result = build_heatmap([s])
        assert "mypipe" in str(result)
