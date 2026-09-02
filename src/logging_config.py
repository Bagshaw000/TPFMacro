"""Central logging setup - call once per process.

The API calls `configure_logging()` from main.py; the arq worker calls it from
worker.py. Every other module just does:

    import logging
    logger = logging.getLogger(__name__)

and logs through `logger`; the records propagate to the root handlers set here,
so the module name shows up in each line (`%(name)s`).

The file handler writes to $LOG_FILE (default "logs/app.log", relative to the
process working directory). If that directory can't be created / opened
(read-only fs, missing mount) the app still runs - it just logs to stderr only.
"""

import logging
import os


def configure_logging(level: int = logging.INFO) -> None:
    """Safe to call more than once. Sets a stderr handler and, if possible, a
    file handler on the root logger. `force=True` drops any handlers arq /
    uvicorn installed first, so formatting stays consistent - which is also why
    the worker calls this from `on_startup` (after arq configures its own)."""
    handlers: list[logging.Handler] = [logging.StreamHandler()]

    log_file = os.getenv("LOG_FILE", "logs/app.log")
    try:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    except OSError as exc:  # read-only fs, permission, bad path - non-fatal
        logging.getLogger(__name__).warning(
            "File logging disabled (%s): %s", log_file, exc
        )

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )
