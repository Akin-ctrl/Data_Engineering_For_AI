"""CSV and Parquet export helpers for Day 3."""

from __future__ import annotations

import pandas as pd

from day3.pipeline.config import Day3Config
from day3.pipeline.logging_utils import LOGGER
from day3.pipeline.models import FileArtifact


def write_exports(config: Day3Config, frame: pd.DataFrame) -> tuple[FileArtifact, FileArtifact]:
    """Write the export DataFrame to CSV and Parquet, overwriting deterministically."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    csv_path = config.csv_path
    parquet_path = config.parquet_path

    frame.to_csv(csv_path, index=False, encoding="utf-8")
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, parquet_path, compression="snappy")

    csv_artifact = FileArtifact(
        file_format="csv",
        path=csv_path,
        size_bytes=csv_path.stat().st_size,
    )
    parquet_artifact = FileArtifact(
        file_format="parquet",
        path=parquet_path,
        size_bytes=parquet_path.stat().st_size,
    )

    LOGGER.info(
        "Export files written",
        extra={
            "context": {
                "csv_path": str(csv_artifact.path),
                "csv_size_mb": round(csv_artifact.size_mb, 3),
                "parquet_path": str(parquet_artifact.path),
                "parquet_size_mb": round(parquet_artifact.size_mb, 3),
            }
        },
    )
    return csv_artifact, parquet_artifact
