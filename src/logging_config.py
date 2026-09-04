"""Central logging setup - call once per process.

The API calls `configure_logging()` from main.py; the arq worker calls it from
worker.py. Every other module just does:

    import logging
    logger = logging.getLogger(__name__)

and logs through `logger`; the records propagate to the root handlers set here,
so the module name shows up in each line (`%(name)s`).

Logs go to stderr by default (containers capture that). File logging is
OPT-IN via $LOG_FILE - and the path must sit OUTSIDE any directory uvicorn
`--reload` / watchfiles watches, or every log line retriggers a reload. In
docker set e.g. `LOG_FILE=/var/log/tpfmacro/app.log`; leave it unset in local
dev.
"""

import logging
import os


def configure_logging(level: int = logging.INFO) -> None:
    """Safe to call more than once. Always adds a stderr handler; adds a file
    handler only when $LOG_FILE is set. `force=True` drops any handlers arq /
    uvicorn installed first, so formatting stays consistent - which is also why
    the worker calls this from `on_startup` (after arq configures its own)."""
    handlers: list[logging.Handler] = [logging.StreamHandler()]

    log_file = os.getenv("LOG_FILE")
    if log_file:
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
