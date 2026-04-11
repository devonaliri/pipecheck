"""Watch a schema file for changes and report drift automatically."""
from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from pipecheck.loader import load_file
from pipecheck.schema import PipelineSchema
from pipecheck.drift import detect_drift, set_baseline, DriftReport


@dataclass
class WatchEvent:
    """Emitted each time a watched file changes."""

    path: Path
    report: DriftReport
    iteration: int

    def __str__(self) -> str:  # pragma: no cover
        status = "DRIFT DETECTED" if self.report.has_drift else "no drift"
        return f"[watch iter={self.iteration}] {self.path.name}: {status}"


@dataclass
class WatchConfig:
    """Configuration for a watch session."""

    path: Path
    interval: float = 2.0
    max_iterations: Optional[int] = None  # None = run forever
    on_event: Optional[Callable[[WatchEvent], None]] = field(default=None, repr=False)


def _file_hash(path: Path) -> str:
    """Return a hex digest of the file contents."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_schema(path: Path) -> PipelineSchema:
    data = load_file(str(path))
    return PipelineSchema.from_dict(data)


def watch(config: WatchConfig) -> list[WatchEvent]:
    """Poll *path* every *interval* seconds and yield WatchEvents.

    Returns the list of all events emitted (useful for testing).
    Stops after *max_iterations* polls when set.
    """
    path = config.path
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")

    # Establish initial baseline from current file state.
    initial_schema = _load_schema(path)
    set_baseline(initial_schema, base_dir=path.parent)

    last_hash = _file_hash(path)
    events: list[WatchEvent] = []
    iteration = 0

    while True:
        time.sleep(config.interval)
        iteration += 1

        current_hash = _file_hash(path)
        if current_hash != last_hash:
            last_hash = current_hash
            current_schema = _load_schema(path)
            report = detect_drift(current_schema, base_dir=path.parent)
            event = WatchEvent(path=path, report=report, iteration=iteration)
            events.append(event)
            if config.on_event:
                config.on_event(event)
            # Update baseline so next comparison is against latest.
            set_baseline(current_schema, base_dir=path.parent)

        if config.max_iterations is not None and iteration >= config.max_iterations:
            break

    return events
