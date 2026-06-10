"""Intentional failure hooks used to teach retries."""

from __future__ import annotations

from datetime import datetime, timezone

from day4.pipeline.config import Day4Config


def maybe_trigger_sabotage(stage: str, *, config: Day4Config) -> None:
    """Fail a stage on purpose so retry behavior is visible in the lab."""

    if config.sabotage_target == "off" or config.sabotage_target != stage:
        return

    config.output_dir.mkdir(parents=True, exist_ok=True)

    if config.sabotage_mode == "always":
        raise RuntimeError(f"Intentional Day 4 sabotage triggered for stage: {stage}")

    marker_path = config.sabotage_marker_path
    if not marker_path.exists():
        marker_path.write_text(f"triggered_at={datetime.now(timezone.utc).isoformat()}\n", encoding="utf-8")
        raise RuntimeError(f"Intentional Day 4 sabotage triggered for stage: {stage}")
