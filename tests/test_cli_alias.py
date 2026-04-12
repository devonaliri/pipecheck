"""Tests for pipecheck.cli_alias."""
import argparse
import pytest

from pipecheck.cli_alias import cmd_alias


def _args(tmp_path, action, pipeline="orders", alias="ord", column=None, reason=""):
    ns = argparse.Namespace(
        alias_dir=str(tmp_path),
        pipeline=pipeline,
        alias_action=action,
        alias=alias,
        column=column,
        reason=reason,
    )
    return ns


class TestCmdAlias:
    def test_add_exits_zero(self, tmp_path, capsys):
        code = cmd_alias(_args(tmp_path, "add"))
        assert code == 0
        out = capsys.readouterr().out
        assert "ord" in out

    def test_add_with_column(self, tmp_path, capsys):
        code = cmd_alias(_args(tmp_path, "add", column="customer_id"))
        assert code == 0
        out = capsys.readouterr().out
        assert "customer_id" in out

    def test_add_with_reason(self, tmp_path, capsys):
        code = cmd_alias(_args(tmp_path, "add", reason="legacy name"))
        assert code == 0
        out = capsys.readouterr().out
        assert "legacy name" in out

    def test_list_no_aliases(self, tmp_path, capsys):
        code = cmd_alias(_args(tmp_path, "list"))
        assert code == 0
        out = capsys.readouterr().out
        assert "No aliases" in out

    def test_list_shows_entries(self, tmp_path, capsys):
        cmd_alias(_args(tmp_path, "add", alias="ord"))
        cmd_alias(_args(tmp_path, "add", alias="order_data"))
        capsys.readouterr()  # flush
        code = cmd_alias(_args(tmp_path, "list"))
        assert code == 0
        out = capsys.readouterr().out
        assert "ord" in out
        assert "order_data" in out

    def test_remove_existing_exits_zero(self, tmp_path, capsys):
        cmd_alias(_args(tmp_path, "add", alias="ord"))
        capsys.readouterr()
        code = cmd_alias(_args(tmp_path, "remove", alias="ord"))
        assert code == 0

    def test_remove_nonexistent_exits_one(self, tmp_path, capsys):
        code = cmd_alias(_args(tmp_path, "remove", alias="ghost"))
        assert code == 1
        err = capsys.readouterr().err
        assert "ghost" in err

    def test_unknown_action_exits_two(self, tmp_path):
        ns = argparse.Namespace(
            alias_dir=str(tmp_path),
            pipeline="orders",
            alias_action="unknown",
        )
        code = cmd_alias(ns)
        assert code == 2
