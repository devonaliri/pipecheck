"""Tests for pipecheck.owners."""
import pytest

from pipecheck.owners import (
    OwnerEntry,
    OwnershipReport,
    get_owners,
    set_owner,
)
from pipecheck.schema import ColumnSchema, PipelineSchema


def _make_schema(name: str = "orders") -> PipelineSchema:
    return PipelineSchema(
        name=name,
        version="1.0",
        columns=[
            ColumnSchema(name="id", data_type="integer"),
            ColumnSchema(name="email", data_type="string"),
        ],
    )


class TestOwnerEntry:
    def test_str_pipeline_level(self):
        e = OwnerEntry(pipeline="orders", team="data-eng", contacts=["alice@x.com"])
        s = str(e)
        assert "orders" in s
        assert "data-eng" in s
        assert "alice@x.com" in s

    def test_str_column_level(self):
        e = OwnerEntry(pipeline="orders", team="security", column="email")
        s = str(e)
        assert "orders.email" in s
        assert "security" in s

    def test_str_no_contacts(self):
        e = OwnerEntry(pipeline="orders", team="data-eng")
        assert "none" in str(e)

    def test_roundtrip(self):
        e = OwnerEntry(pipeline="orders", team="data-eng", contacts=["a@b.com"], column="id")
        assert OwnerEntry.from_dict(e.to_dict()) == e

    def test_from_dict_defaults(self):
        e = OwnerEntry.from_dict({"pipeline": "orders", "team": "eng"})
        assert e.contacts == []
        assert e.column is None


class TestOwnershipReport:
    def test_has_owners_false_when_empty(self):
        r = OwnershipReport(schema_name="orders")
        assert not r.has_owners()

    def test_has_owners_true_when_present(self):
        e = OwnerEntry(pipeline="orders", team="eng")
        r = OwnershipReport(schema_name="orders", entries=[e])
        assert r.has_owners()

    def test_teams_sorted_unique(self):
        entries = [
            OwnerEntry(pipeline="orders", team="security"),
            OwnerEntry(pipeline="orders", team="data-eng"),
            OwnerEntry(pipeline="orders", team="security", column="email"),
        ]
        r = OwnershipReport(schema_name="orders", entries=entries)
        assert r.teams() == ["data-eng", "security"]

    def test_str_no_owners(self):
        r = OwnershipReport(schema_name="orders")
        assert "No owners"n    def test_str_with_owners(self):
        e = OwnerEntry(pipeline="orders", team="data-eng")
        r = OwnershipReport(schema_name="orders",s = str(r)
        assert "orders" in s
        assert "data-eng" in s


class TestSetAndGetOwners:
    def test_set_and_get_pipeline_owner(self, tmp_path):
        schema = _make_schema()
        set_owner(schema, team="data-eng", contacts=["alice@x.com"], base_dir=str(tmp_path))
        report = get_owners(schema, base_dir=str(tmp_path))
        assert report.has_owners()
        assert report.entries[0].team == "data-eng"

    def test_set_column_owner(self, tmp_path):
        schema = _make_schema()
        set_owner(schema, team="security", column="email", base_dir=str(tmp_path))
        report = get_owners(schema, base_dir=str(tmp_path))
        assert report.entries[0].column == "email"

    def test_overwrite_existing_owner(self, tmp_path):
        schema = _make_schema()
        set_owner(schema, team="old-team", base_dir=str(tmp_path))
        set_owner(schema, team="new-team", base_dir=str(tmp_path))
        report = get_owners(schema, base_dir=str(tmp_path))
        assert len(report.entries) == 1
        assert report.entries[0].team == "new-team"

    def test_multiple_column_owners(self, tmp_path):
        schema = _make_schema()
        set_owner(schema, team="eng", base_dir=str(tmp_path))
        set_owner(schema, team="security", column="email", base_dir=str(tmp_path))
        report = get_owners(schema, base_dir=str(tmp_path))
        assert len(report.entries) == 2

    def test_get_owners_no_file(self, tmp_path):
        schema = _make_schema()
        report = get_owners(schema, base_dir=str(tmp_path))
        assert not report.has_owners()

    def test_get_owners_filters_by_pipeline(self, tmp_path):
        s1 = _make_schema("orders")
        s2 = _make_schema("users")
        set_owner(s1, team="eng", base_dir=str(tmp_path))
        set_owner(s2, team="infra", base_dir=str(tmp_path))
        report = get_owners(s1, base_dir=str(tmp_path))
        assert all(e.pipeline == "orders" for e in report.entries)
