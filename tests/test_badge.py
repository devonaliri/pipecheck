"""Tests for pipecheck.badge."""
from __future__ import annotations

import pytest

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.badge import (
    BadgeResult,
    _color_for_score,
    generate_badge,
    generate_coverage_badge,
)


def _col(name: str, dtype: str = "string", nullable: bool = False,
         description: str = "", tags: list[str] | None = None) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        data_type=dtype,
        nullable=nullable,
        description=description,
        tags=tags or [],
    )


def _make_schema(name: str = "orders", cols: list[ColumnSchema] | None = None) -> PipelineSchema:
    return PipelineSchema(
        name=name,
        version="1.0",
        description="Test schema",
        columns=cols or [_col("id", "integer")],
    )


class TestColorForScore:
    def test_high_score_green(self):
        assert _color_for_score(95) == "4c1"

    def test_good_score_light_green(self):
        assert _color_for_score(80) == "97ca00"

    def test_medium_score_yellow(self):
        assert _color_for_score(60) == "dfb317"

    def test_low_score_red(self):
        assert _color_for_score(30) == "e05d44"

    def test_boundary_90_is_green(self):
        assert _color_for_score(90) == "4c1"

    def test_boundary_75_is_light_green(self):
        assert _color_for_score(75) == "97ca00"


class TestBadgeResult:
    def test_str_contains_label_and_message(self):
        b = BadgeResult(label="pipecheck", message="85%", color="97ca00",
                        schema_name="orders", score=85)
        text = str(b)
        assert "pipecheck" in text
        assert "85%" in text

    def test_str_contains_schema_name(self):
        b = BadgeResult(label="pipecheck", message="85%", color="97ca00",
                        schema_name="orders", score=85)
        assert "orders" in str(b)

    def test_shields_url_format(self):
        b = BadgeResult(label="pipecheck", message="85%", color="97ca00",
                        schema_name="orders", score=85)
        url = b.to_shields_url()
        assert url.startswith("https://img.shields.io/badge/")
        assert "pipecheck" in url
        assert "85%25" in url or "85%" in url

    def test_svg_contains_label(self):
        b = BadgeResult(label="cov", message="90%", color="4c1",
                        schema_name="users", score=90)
        svg = b.to_svg()
        assert "<svg" in svg
        assert "cov" in svg
        assert "90%" in svg


class TestGenerateBadge:
    def test_returns_badge_result(self):
        schema = _make_schema()
        result = generate_badge(schema)
        assert isinstance(result, BadgeResult)

    def test_label_default(self):
        schema = _make_schema()
        result = generate_badge(schema)
        assert result.label == "pipecheck"

    def test_custom_label(self):
        schema = _make_schema()
        result = generate_badge(schema, label="health")
        assert result.label == "health"

    def test_schema_name_preserved(self):
        schema = _make_schema(name="events")
        result = generate_badge(schema)
        assert result.schema_name == "events"

    def test_message_ends_with_percent(self):
        schema = _make_schema()
        result = generate_badge(schema)
        assert result.message.endswith("%")

    def test_score_is_integer(self):
        schema = _make_schema()
        result = generate_badge(schema)
        assert isinstance(result.score, int)


class TestGenerateCoverageBadge:
    def test_returns_badge_result(self):
        schema = _make_schema()
        result = generate_coverage_badge(schema)
        assert isinstance(result, BadgeResult)

    def test_label_default(self):
        schema = _make_schema()
        result = generate_coverage_badge(schema)
        assert result.label == "coverage"

    def test_message_ends_with_percent(self):
        schema = _make_schema()
        result = generate_coverage_badge(schema)
        assert result.message.endswith("%")

    def test_fully_documented_schema_high_score(self):
        cols = [
            _col("id", "integer", description="Primary key", tags=["pk"]),
            _col("name", "string", description="Full name", tags=["pii"]),
        ]
        schema = _make_schema(cols=cols)
        result = generate_coverage_badge(schema)
        assert result.score > 0
