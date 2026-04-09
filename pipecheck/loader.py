"""Schema file loader supporting JSON and YAML formats."""

import json
import os
from typing import Union

try:
    import yaml
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

from pipecheck.schema import PipelineSchema


class LoadError(Exception):
    """Raised when a schema file cannot be loaded or parsed."""


def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        try:
            return json.load(fh)
        except json.JSONDecodeError as exc:
            raise LoadError(f"Invalid JSON in '{path}': {exc}") from exc


def _load_yaml(path: str) -> dict:
    if not _YAML_AVAILABLE:
        raise LoadError(
            f"Cannot load '{path}': PyYAML is not installed. "
            "Run `pip install pyyaml` to enable YAML support."
        )
    with open(path, "r", encoding="utf-8") as fh:
        try:
            data = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise LoadError(f"Invalid YAML in '{path}': {exc}") from exc
    if not isinstance(data, dict):
        raise LoadError(f"Expected a mapping at the top level of '{path}'.")
    return data


def load_file(path: str) -> PipelineSchema:
    """Load a PipelineSchema from a JSON or YAML file.

    Args:
        path: Filesystem path to a ``.json``, ``.yaml``, or ``.yml`` file.

    Returns:
        A :class:`~pipecheck.schema.PipelineSchema` instance.

    Raises:
        LoadError: If the file is missing, has an unsupported extension, or
            contains invalid content.
    """
    if not os.path.exists(path):
        raise LoadError(f"File not found: '{path}'")

    ext = os.path.splitext(path)[1].lower()
    if ext == ".json":
        raw = _load_json(path)
    elif ext in (".yaml", ".yml"):
        raw = _load_yaml(path)
    else:
        raise LoadError(
            f"Unsupported file extension '{ext}' for '{path}'. "
            "Expected .json, .yaml, or .yml."
        )

    try:
        return PipelineSchema.from_dict(raw)
    except (KeyError, TypeError, ValueError) as exc:
        raise LoadError(f"Schema parse error in '{path}': {exc}") from exc
