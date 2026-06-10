"""ArXiv API request and pagination helpers."""

from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

import requests
from requests import Response
from requests.exceptions import RequestException

from day2.pipeline.config import PipelineConfig
from day2.pipeline.constants import ARXIV_NS, OPENSEARCH_NS
from day2.pipeline.logging_utils import LOGGER
from day2.pipeline.parse import entry_element_to_record


def build_search_query(base_query: str, start_dt: datetime, end_dt: datetime) -> str:
    """Append a submittedDate range filter to an ArXiv query."""

    start_token = start_dt.strftime("%Y%m%d%H%M")
    end_token = end_dt.strftime("%Y%m%d%H%M")
    return f"({base_query}) AND submittedDate:[{start_token} TO {end_token}]"


def request_feed_page(config: PipelineConfig, params: dict[str, Any]) -> Response:
    """Fetch one feed page with bounded retries and linear backoff."""

    last_error: Exception | None = None
    for attempt in range(1, config.arxiv_request_max_retries + 1):
        try:
            response = requests.get(
                config.arxiv_base_url,
                params=params,
                headers={"User-Agent": config.arxiv_user_agent},
                timeout=config.arxiv_request_timeout_seconds,
            )
            if response.status_code == 429:
                raise requests.HTTPError("429 Too Many Requests", response=response)
            response.raise_for_status()
            return response
        except RequestException as exc:
            last_error = exc
            if attempt == config.arxiv_request_max_retries:
                break

            is_rate_limited = getattr(getattr(exc, "response", None), "status_code", None) == 429
            if is_rate_limited:
                sleep_seconds = max(30, config.arxiv_retry_backoff_seconds * attempt * 6)
            else:
                sleep_seconds = config.arxiv_retry_backoff_seconds * attempt
            LOGGER.warning(
                "ArXiv request failed; retrying",
                extra={
                    "context": {
                        "attempt": attempt,
                        "max_retries": config.arxiv_request_max_retries,
                        "rate_limited": is_rate_limited,
                        "sleep_seconds": sleep_seconds,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                },
            )
            time.sleep(sleep_seconds)

    raise RuntimeError("ArXiv request retries exhausted") from last_error


def fetch_feed_page(
    config: PipelineConfig,
    search_query: str,
    start: int,
    max_results: int,
) -> tuple[int, list[dict[str, Any]]]:
    """Fetch one page of ArXiv feed results and parse records."""

    params = {
        "search_query": search_query,
        "start": start,
        "max_results": max_results,
        "sortBy": "submittedDate",
        "sortOrder": "ascending",
    }
    response = request_feed_page(config, params)

    root = ET.fromstring(response.text)
    total_results_text = root.findtext(f"{{{OPENSEARCH_NS}}}totalResults", default="0")
    total_results = int(total_results_text)

    entries = [entry_element_to_record(entry) for entry in root.findall(f"{{{ARXIV_NS}}}entry")]
    return total_results, entries
