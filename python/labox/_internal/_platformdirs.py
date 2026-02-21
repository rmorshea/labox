import atexit
import os
import shutil

from platformdirs import user_cache_path

LABOX_USER_CACHE_DIR = user_cache_path("labox", "labox")
"""The user cache directory for Labox, where temporary files and data are stored."""

LABOX_USER_CACHE_DIR.mkdir(exist_ok=True, parents=True)

LABOX_USER_PROC_CACHE_DIR = LABOX_USER_CACHE_DIR / f"proc-{os.getpid()}"
"""The process-specific user cache directory for Labox that is deleted when the process exits."""

LABOX_USER_PROC_CACHE_DIR.mkdir(exist_ok=True, parents=True)


def _cleanup_user_proc_cache_dir() -> None:
    if LABOX_USER_PROC_CACHE_DIR.exists():
        shutil.rmtree(LABOX_USER_PROC_CACHE_DIR, ignore_errors=True)


atexit.register(_cleanup_user_proc_cache_dir)
