"""Small data models used by the Day 3 benchmark pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ReadBenchmarkResult:
    """Timing results for one reader and one file format."""

    reader: str
    file_format: str
    rows_read: int
    run_count: int
    average_ms: float
    median_ms: float
    min_ms: float
    max_ms: float


@dataclass(frozen=True)
class FileArtifact:
    """Metadata for one exported file."""

    file_format: str
    path: Path
    size_bytes: int

    @property
    def size_mb(self) -> float:
        """Return the artifact size in megabytes."""

        return self.size_bytes / 1024 / 1024
