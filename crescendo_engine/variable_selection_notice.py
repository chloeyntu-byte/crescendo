"""Advisories for large optimization variable selections."""

import re
import signal


VARIABLE_NOTICE_THRESHOLD = 50
VARIABLE_NOTICE_STAGE = "variable_selection_notice"
_TEMPLATE_ADVICE = (
    "Template Netlist reference capture can cause Virtuoso to quit unexpectedly "
    "with many variables in some environments. If this happens, select fewer "
    "variables or use Maestro Netlist or Native Run."
)
_RUNTIME_ADVICE = (
    "Many variables can significantly increase the time per generation and the "
    "number of generations needed to converge. Consider narrowing the selection "
    "before starting."
)
_CRASH_SIGNALS = (signal.SIGSEGV, signal.SIGBUS, signal.SIGABRT)
_CRASH_LINE = re.compile(r"\bProcess was terminated with (?:SIGSEGV|SIGBUS|SIGABRT) signal\b"
                         r"|\bSegmentation fault\b", re.IGNORECASE)


def variable_selection_notice(backend, count):
    if count <= VARIABLE_NOTICE_THRESHOLD:
        return ""
    if backend == "private_netlist":
        advice = _TEMPLATE_ADVICE
    elif backend in ("maestro_baseline", "maestro_netlist", "auto"):
        advice = _RUNTIME_ADVICE
    else:
        return ""
    return "Selected {} variables. {}".format(count, advice)


def native_capture_crashed(detail):
    """Recognize a crash of the native capture process from its command receipt."""
    if not detail.get("capture_diagnostics") or detail.get("failure_stage") == "native_netlist_cancelled":
        return False
    command = detail.get("command") or {}
    if command.get("cancelled") or command.get("timed_out"):
        return False
    code = command.get("returncode")
    if any(code in (-sig, 128 + sig) for sig in _CRASH_SIGNALS):
        return True
    return any(_CRASH_LINE.search(command.get(key) or "") for key in ("stdout_tail", "stderr_tail"))
