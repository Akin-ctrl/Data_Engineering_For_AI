"""Runtime configuration for the Day 1 pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration required to run the Day 1 pipeline."""

    hf_csv_url: str
    pghost: str
    pgport: int
    pgdatabase: str
    pguser: str
    pgpassword: str

    @property
    def sqlalchemy_url(self) -> str:
        """Build SQLAlchemy connection URL for PostgreSQL."""

        return (
            f"postgresql+psycopg://{self.pguser}:{self.pgpassword}"
            f"@{self.pghost}:{self.pgport}/{self.pgdatabase}"
        )


def load_config() -> PipelineConfig:
    """Load and validate environment configuration from .env."""

    load_dotenv()

    required = [
        "HF_CSV_URL",
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
    ]

    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return PipelineConfig(
        hf_csv_url=os.environ["HF_CSV_URL"],
        pghost=os.environ["PGHOST"],
        pgport=int(os.environ["PGPORT"]),
        pgdatabase=os.environ["PGDATABASE"],
        pguser=os.environ["PGUSER"],
        pgpassword=os.environ["PGPASSWORD"],
    )
