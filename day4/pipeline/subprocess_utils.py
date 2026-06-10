"""Subprocess helpers for running earlier lab scripts."""

from __future__ import annotations

import subprocess

from day4.pipeline.config import Day4Config
from day4.pipeline.constants import REPO_ROOT
from day4.pipeline.logging_utils import LOGGER


def run_subprocess(command: list[str], *, config: Day4Config, step_name: str) -> None:
    """Run a child lab command with the project's environment."""

    LOGGER.info(
        "Running step",
        extra={
            "context": {
                "step": step_name,
                "command": " ".join(command),
            }
        },
    )
    subprocess.run(command, cwd=REPO_ROOT, env=config.subprocess_env, check=True)
