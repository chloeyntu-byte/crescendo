from __future__ import print_function

import hashlib
import json
import math
import os
import tempfile
import time

from .storage_errors import annotate_storage_error


def ensure_dir(path):
    path = os.path.abspath(os.path.expanduser(path))
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except OSError as error:
            annotate_storage_error(error, path, "create directory")
            raise
    return path


def cds_lib_token(value):
    """Encode one library name or path without changing cds.lib token boundaries."""
    if not isinstance(value, str) or not value or any(char in value for char in "\0\r\n"):
        raise ValueError("cds.lib tokens must be nonempty text without line breaks or NUL")
    if any(char.isspace() or char in "\"'\\#" for char in value):
        return json.dumps(value, ensure_ascii=False)
    return value


def _replace_file(source, target):
    replace = getattr(os, "replace", None)
    if replace is not None:
        replace(source, target)
    else:
        os.rename(source, target)


def _fsync_directory(path):
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_write_text(path, text):
    path = os.path.abspath(path)
    tmp_path = None
    operation = "create write directory"
    try:
        directory = ensure_dir(os.path.dirname(path))
        operation = "create temporary file"
        handle, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=directory)
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            operation = "write and flush file"
            stream.write(text)
            stream.flush()
            operation = "sync file"
            os.fsync(stream.fileno())
        operation = "replace file"
        _replace_file(tmp_path, path)
        operation = "sync directory"
        _fsync_directory(directory)
    except Exception as error:
        annotate_storage_error(error, path, operation)
        if tmp_path is not None:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        raise


def atomic_write_json(path, value):
    atomic_write_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")
    if os.path.basename(path) in ("status.json", "startup_failure.json", "server_error.json", "worker_shutdown.json", "candidate.json"):
        try:
            from .run_log import observe_saved_json
            observe_saved_json(path, value)
        except Exception:
            # The durable machine record is authoritative. Logging must not
            # replace a native/storage error after that record was committed.
            pass


def load_json(path):
    with open(path, "r") as stream:
        return json.load(stream)


def sha256_file(path):
    # cadence imports this module; resolve the optional prepare cancellation
    # hook at call time to keep import order independent.
    from .cadence import check_command_cancellation
    check_command_cancellation()
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while True:
            check_command_cancellation()
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    check_command_cancellation()
    return digest.hexdigest()


def utc_timestamp():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def run_id(prefix="run"):
    return "%s_%s" % (prefix, time.strftime("%Y%m%d_%H%M%S", time.localtime()))


def finite_number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def is_under(path, root):
    path = os.path.realpath(os.path.abspath(path))
    root = os.path.realpath(os.path.abspath(root))
    return path == root or path.startswith(root + os.sep)


def require_under(path, root, label="path"):
    if not is_under(path, root):
        raise ValueError("%s escapes safe root: %s (root %s)" % (label, path, root))
    return os.path.abspath(path)
