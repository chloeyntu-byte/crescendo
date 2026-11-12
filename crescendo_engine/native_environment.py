"""Keep native caller environment states separate from Python/Tk overrides.

Snapshots carry None for an unset variable and an empty string for a variable
set to empty. Values are working launch data and must not be logged.
"""
from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import json
import os

GUI_ENV_NAMES = (
    "LD_LIBRARY_PATH", "LD_PRELOAD", "PYTHONHOME", "PYTHONPATH",
    "TCL_LIBRARY", "TK_LIBRARY", "QT_PLUGIN_PATH",
)
LEGACY_NATIVE_ENV_NAMES = ("PATH", "CDS_LIC_FILE", "LM_LICENSE_FILE") + GUI_ENV_NAMES
HOME_NATIVE_ENV_NAMES = LEGACY_NATIVE_ENV_NAMES + ("HOME",)
MODEL_ENV_NAMES = ("PDK", _contract_text("CRESCENDO_SPECTRE_MODEL_DIR"), _contract_text("CRESCENDO_ALPS_MODEL_DIR"))
NATIVE_ENV_NAMES = HOME_NATIVE_ENV_NAMES + MODEL_ENV_NAMES
SCHEDULER_HOME_SET = _contract_text("CRESCENDO_SCHEDULER_HOME_SET")
SCHEDULER_HOME_VALUE = _contract_text("CRESCENDO_SCHEDULER_HOME_VALUE")
NATIVE_ENV_SCHEMA = _contract_text("crescendo.native_environment.v1")
NATIVE_ENV_SNAPSHOT_VAR = _contract_text("CRESCENDO_NATIVE_ENV_SNAPSHOT")
MAX_SNAPSHOT_BYTES = 1024 * 1024
MAX_VALUE_CHARS = 16384


def validate_native_values(values, require_complete=False):
    if not isinstance(values, dict):
        raise ValueError("Native environment snapshot values must be an object")
    unknown = set(values) - set(NATIVE_ENV_NAMES)
    if unknown:
        raise ValueError("Native environment snapshot contains unsupported keys")
    if require_complete and set(values) not in (set(NATIVE_ENV_NAMES), set(HOME_NATIVE_ENV_NAMES), set(LEGACY_NATIVE_ENV_NAMES)):
        raise ValueError("Native environment snapshot is incomplete; reopen from the caller")
    result = {}
    for name, value in values.items():
        if value is not None and not isinstance(value, str):
            raise ValueError("Native environment state must be null or a string")
        if value is not None and (len(value) > MAX_VALUE_CHARS or any(ord(char) < 32 for char in value)):
            raise ValueError("Native environment value contains controls or exceeds its size limit")
        result[name] = value
    return result


def parse_native_environment_snapshot(raw):
    if not isinstance(raw, str) or not raw or len(raw.encode("utf-8")) > MAX_SNAPSHOT_BYTES:
        raise ValueError("Native environment snapshot is empty or exceeds its size limit")
    try:
        snapshot = json.loads(raw)
    except (ValueError, TypeError):
        raise ValueError("Native environment snapshot is not valid JSON")
    if not isinstance(snapshot, dict) or snapshot.get("schema") != NATIVE_ENV_SCHEMA or set(snapshot) != {"schema", "values"}:
        raise ValueError("Native environment snapshot schema is not recognized")
    return validate_native_values(snapshot.get("values"), require_complete=True)


def clean_gui_environment(environ=None):
    result = dict(os.environ if environ is None else environ)
    for name in GUI_ENV_NAMES:
        result.pop(name, None)
    return result


def restore_native_environment(environ, values):
    """Restore an explicit snapshot without borrowing GUI dependency values.

    Older partial Maestro records may lack PATH/license states. Keep their
    previous discovery behavior while clearing all unknown GUI override states.
    A complete current snapshot preserves all three states exactly.
    """
    result = dict(os.environ if environ is None else environ)
    if values is None:
        return result
    values = validate_native_values(values)
    result = clean_gui_environment(result)
    for name, value in values.items():
        if value is None:
            result.pop(name, None)
        else:
            result[name] = value
    return result


def native_snapshot_warnings(values):
    missing = sorted(set(NATIVE_ENV_NAMES) - set(values or {}))
    if not missing:
        return []
    return [
        _contract_text("The caller environment snapshot lacks %s. Reopen Crescendo from the updated "
        "Maestro menu; unknown GUI override states are cleared.") % ", ".join(missing)
    ]


def preserve_scheduler_home(environ):
    """Record caller HOME before Ocean receives its owned working HOME.

    Never inspect or copy credential files. An explicit unset or empty HOME is
    preserved, as are existing markers from an outer native launch boundary.
    Marker values are working launch data and must not be logged.
    """
    result = dict(environ)
    if SCHEDULER_HOME_SET not in result:
        result[SCHEDULER_HOME_SET] = "x" if "HOME" in result else ""
        result[SCHEDULER_HOME_VALUE] = result.get("HOME", "")
    return result


def scheduler_environment(environ):
    """Restore caller HOME only for the native scheduler submission/query CLI.

    Ocean and compute-side simulation keep their working HOME. Scheduler
    credentials stay at the original caller location; no login, token read or
    credential copying is performed at this boundary.
    """
    result = dict(environ)
    marker = result.pop(SCHEDULER_HOME_SET, None)
    value = result.pop(SCHEDULER_HOME_VALUE, "")
    if marker is not None:
        result[_contract_text("CRESCENDO_SIMULATION_HOME_SET")] = "x" if "HOME" in result else ""
        result[_contract_text("CRESCENDO_SIMULATION_HOME_VALUE")] = result.get("HOME", "")
    if marker == "x":
        result["HOME"] = value
    elif marker == "":
        result.pop("HOME", None)
    elif marker is not None:
        raise ValueError("Scheduler HOME marker is invalid")
    return result
