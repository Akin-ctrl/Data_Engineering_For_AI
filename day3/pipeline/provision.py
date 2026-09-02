"""SQL view provisioning helpers for Day 3.

This is the Transform step of the Day 3 ELT story. The SQL lives in
`day3/day3_agent_query_views.sql` so learners can read the views as plain SQL,
and this module is the small amount of Python needed to run that file.
"""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from day3.pipeline.constants import DAY3_VIEWS_SQL
from day3.pipeline.logging_utils import LOGGER


def read_views_sql() -> str:
    """Read the Day 3 views SQL file and return its contents.

    Returns:
        The full SQL script as one string.

    Raises:
        FileNotFoundError: If the SQL file is missing from the repository.
    """

    if not DAY3_VIEWS_SQL.is_file():
        raise FileNotFoundError(
            f"Day 3 SQL views file is missing: {DAY3_VIEWS_SQL}. "
            "Check that day3/day3_agent_query_views.sql is present."
        )

    return DAY3_VIEWS_SQL.read_text(encoding="utf-8")


def provision_views(sqlalchemy_url: str) -> None:
    """Create the Day 3 query views and materialized views in PostgreSQL.

    The SQL file opens its own transaction with BEGIN and closes it with COMMIT,
    so the connection is opened in autocommit mode and the file stays in charge
    of its own transaction. If any statement fails, PostgreSQL rolls the whole
    script back and nothing is left half-created.

    Args:
        sqlalchemy_url: PostgreSQL connection URL, as built by a day config.

    Raises:
        RuntimeError: If PostgreSQL rejects any statement in the SQL file.
    """

    sql_script = read_views_sql()

    LOGGER.info(
        "Provisioning Day 3 SQL views",
        extra={"context": {"sql_file": str(DAY3_VIEWS_SQL)}},
    )

    engine = create_engine(sqlalchemy_url, future=True, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as connection:
            # exec_driver_sql sends the SQL straight to the driver, so colons and
            # other SQL punctuation are never mistaken for bind parameters.
            connection.exec_driver_sql(sql_script)
    except SQLAlchemyError as exc:
        raise RuntimeError(f"Day 3 SQL view provisioning failed: {exc}") from exc
    finally:
        engine.dispose()

    LOGGER.info("Day 3 SQL views provisioned")
