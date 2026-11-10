"""One compact report when Setup cannot reach the normal run diagnostic."""
import json
import hashlib
import os

from .utils import atomic_write_text


def write_setup_diagnostic_log(reference, error=None):
    from .setup_private_session import _owner, _load, private_session_info
    from .native_diagnostics import skill_command_diagnostics, _owned_binary_stream, MAX_GUARD_SCRIPT_BYTES
    owner = _owner(reference)
    info = private_session_info(reference)
    state = _load(os.path.join(owner["root"], "session_state.json"))
    operations = state.get("operations") or []
    if not isinstance(operations, list) or len(operations) > 4096:
        raise ValueError("Setup operation inventory is not bounded")
    summary = {"schema_version": "crescendo.setup_private_diagnostics.v1",
        "session_id": owner["session_id"], "status": info.get("status"),
        "maestro": info.get("maestro"), "configs": [{key: row.get(key) for key in
            ("library", "cell", "view", "path", "top")} for row in info.get("configs") or []],
        "owned_views": len(info.get("views") or []), "operation_count": len(operations),
        "binding_verification": info.get("binding_verification"),
        "native_run_invoked": False, "solver_invocations": 0,
        "scope": "Setup creates and binds private views; candidate capture belongs to its run log"}
    lines = ["Crescendo Setup 私有化诊断", "setup_summary " + json.dumps(summary, ensure_ascii=False, sort_keys=True)]
    if error is not None:
        lines.append("first_cause " + json.dumps({"type": type(error).__name__, "message": str(error)[:8192]}, ensure_ascii=False))
        if operations:
            control = operations[-1]
            parent = os.path.join(owner["root"], "operations")
            if (not isinstance(control, str) or os.path.dirname(control) != parent
                    or os.path.realpath(control) != control or not os.path.isdir(control)):
                raise ValueError("Setup command root is not owned")
            command_path = os.path.join(control, "command.json")
            script_path = os.path.join(control, "setup.il")
            if os.path.isfile(command_path):
                command = _load(command_path)
                evidence = {key: command.get(key) for key in
                    ("argv", "cwd", "returncode", "timed_out", "cancelled", "termination_failed", "guardian_cleanup")}
                evidence["script_path"] = script_path
                with _owned_binary_stream(script_path, control) as stream:
                    if stream is not None:
                        raw = stream.read(MAX_GUARD_SCRIPT_BYTES + 1)
                        if len(raw) <= MAX_GUARD_SCRIPT_BYTES:
                            evidence["script_sha256"] = hashlib.sha256(raw).hexdigest()
                        else:
                            evidence["script_hash_omitted"] = "script exceeds the diagnostic byte bound"
                evidence["native_error"] = skill_command_diagnostics(command, script_path,
                    required_marker="CRESCENDO_SETUP_PRIVATE_BINDING_DONE", owned_root=control)
                lines.append("native_binding_failure " + json.dumps(evidence, ensure_ascii=False, sort_keys=True))
            else:
                lines.append("native_binding_failure " + json.dumps({"state": "command_not_registered",
                    "command_root": control}, ensure_ascii=False))
    path = os.path.join(owner["root"], "environment_test.log")
    atomic_write_text(path, "\n".join(lines) + "\n")
    return path
