"""Runtime configuration for the Day 2 ArXiv pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

from day2.pipeline.constants import DEFAULT_ARXIV_CATEGORIES
from day2.pipeline.logging_utils import LOGGER


@dataclass(frozen=True)
class PipelineConfig:
    """Typed configuration for the Day 2 ArXiv ingestion pipeline."""

    arxiv_base_url: str
    arxiv_search_query: str
    arxiv_categories: list[str]
    arxiv_source_name: str
    arxiv_max_papers: int
    arxiv_page_size: int
    arxiv_sleep_seconds: int
    arxiv_default_lookback_days: int
    arxiv_overlap_minutes: int
    arxiv_request_timeout_seconds: int
    arxiv_request_max_retries: int
    arxiv_retry_backoff_seconds: int
    arxiv_user_agent: str
    pghost: str
    pgport: int
    pgdatabase: str
    pguser: str
    pgpassword: str

    @property
    def sqlalchemy_url(self) -> str:
        """Build SQLAlchemy URL from environment-driven PostgreSQL settings."""

        return (
            f"postgresql+psycopg://{self.pguser}:{self.pgpassword}"
            f"@{self.pghost}:{self.pgport}/{self.pgdatabase}"
        )


def parse_csv_list(value: str | None) -> list[str]:
    """Parse comma-separated config values into a clean, deterministic list."""

    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_positive_int(value: str, env_name: str) -> int:
    """Parse and validate positive integer environment values."""

    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{env_name} must be an integer, got: {value}") from exc

    if parsed <= 0:
        raise ValueError(f"{env_name} must be greater than zero, got: {parsed}")

    return parsed


def load_config() -> PipelineConfig:
    """Load and validate pipeline configuration from the environment."""

    load_dotenv()

    required = [
        "ARXIV_BASE_URL",
        "ARXIV_SOURCE_NAME",
        "ARXIV_MAX_PAPERS",
        "ARXIV_PAGE_SIZE",
        "ARXIV_SLEEP_SECONDS",
        "ARXIV_DEFAULT_LOOKBACK_DAYS",
        "ARXIV_OVERLAP_MINUTES",
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
    ]

    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    categories = parse_csv_list(os.getenv("ARXIV_CATEGORIES"))
    if not categories:
        categories = DEFAULT_ARXIV_CATEGORIES.copy()

    search_query = os.getenv("ARXIV_SEARCH_QUERY", "").strip()
    if not search_query:
        search_query = " OR ".join(f"cat:{category}" for category in categories)

    arxiv_max_papers = parse_positive_int(os.environ["ARXIV_MAX_PAPERS"], "ARXIV_MAX_PAPERS")
    if arxiv_max_papers < 10000:
        LOGGER.warning(
            "ARXIV_MAX_PAPERS is below the Day 2 deep-learning target",
            extra={"context": {"arxiv_max_papers": arxiv_max_papers, "recommended_min": 10000}},
        )

    arxiv_page_size = parse_positive_int(os.environ["ARXIV_PAGE_SIZE"], "ARXIV_PAGE_SIZE")
    arxiv_sleep_seconds = parse_positive_int(os.environ["ARXIV_SLEEP_SECONDS"], "ARXIV_SLEEP_SECONDS")
    arxiv_default_lookback_days = parse_positive_int(
        os.environ["ARXIV_DEFAULT_LOOKBACK_DAYS"],
        "ARXIV_DEFAULT_LOOKBACK_DAYS",
    )
    arxiv_overlap_minutes = parse_positive_int(
        os.environ["ARXIV_OVERLAP_MINUTES"],
        "ARXIV_OVERLAP_MINUTES",
    )
    arxiv_request_timeout_seconds = parse_positive_int(
        os.getenv("ARXIV_REQUEST_TIMEOUT_SECONDS", "90"),
        "ARXIV_REQUEST_TIMEOUT_SECONDS",
    )
    arxiv_request_max_retries = parse_positive_int(
        os.getenv("ARXIV_REQUEST_MAX_RETRIES", "4"),
        "ARXIV_REQUEST_MAX_RETRIES",
    )
    arxiv_retry_backoff_seconds = parse_positive_int(
        os.getenv("ARXIV_RETRY_BACKOFF_SECONDS", "5"),
        "ARXIV_RETRY_BACKOFF_SECONDS",
    )
    arxiv_user_agent = os.getenv(
        "ARXIV_USER_AGENT",
        "DataEngineeringForAI-Day2/1.0 (contact: instructor@example.com)",
    ).strip()
    if not arxiv_user_agent:
        raise ValueError("ARXIV_USER_AGENT must not be empty")

    if arxiv_sleep_seconds < 3:
        LOGGER.warning(
            "ARXIV_SLEEP_SECONDS is below the recommended polite threshold",
            extra={"context": {"arxiv_sleep_seconds": arxiv_sleep_seconds, "recommended_min": 3}},
        )

    return PipelineConfig(
        arxiv_base_url=os.environ["ARXIV_BASE_URL"],
        arxiv_search_query=search_query,
        arxiv_categories=categories,
        arxiv_source_name=os.environ["ARXIV_SOURCE_NAME"],
        arxiv_max_papers=arxiv_max_papers,
        arxiv_page_size=arxiv_page_size,
        arxiv_sleep_seconds=arxiv_sleep_seconds,
        arxiv_default_lookback_days=arxiv_default_lookback_days,
        arxiv_overlap_minutes=arxiv_overlap_minutes,
        arxiv_request_timeout_seconds=arxiv_request_timeout_seconds,
        arxiv_request_max_retries=arxiv_request_max_retries,
        arxiv_retry_backoff_seconds=arxiv_retry_backoff_seconds,
        arxiv_user_agent=arxiv_user_agent,
        pghost=os.environ["PGHOST"],
        pgport=parse_positive_int(os.environ["PGPORT"], "PGPORT"),
        pgdatabase=os.environ["PGDATABASE"],
        pguser=os.environ["PGUSER"],
        pgpassword=os.environ["PGPASSWORD"],
    )
