from __future__ import print_function

import os
import shlex
import stat
import time
import uuid

from .adexl_runtime import (
    AdeXLRuntimeBuilder, finalize_adexl_candidate, matching_adexl_protocol,
)
from .cadence import build_guardian_command, child_environment, run_command
from .legacy_guardian import (
    acquire_legacy_guardian_launch_lock, close_legacy_guardian_launch_lock,
    legacy_guardian_parent_identity, write_legacy_guardian_manifest,
)
from .resources import guardian_script_path, submit_spectre_script_path
from .technology import (
    TechnologyBindingError, validate_workspace_technology,
    workspace_technology_validation_required,
)
from .utils import atomic_write_json, atomic_write_text, ensure_dir
from .workspace import (
    CADENCE_STARTUP_LOCK, ParameterApplicationInfrastructureFailure,
    reset_results, summarize_spectre_timing, workspace_test_design_lines,
)


def _spectre_wrapper(workspace, config):
    local_bin = ensure_dir(os.path.join(workspace["root"], "bin"))
    wrapper_path = os.path.join(local_bin, "spectre")
    real_spectre = config["cadence"]["spectre_bin"]
    submit_script = config.get("runtime", {}).get(
        "spectre_submit_script"
    ) or submit_spectre_script_path()
    content = '''#!/bin/sh
real_spectre="${CRESCENDO_REAL_SPECTRE_BIN:-}"
submit_script="${CRESCENDO_SPECTRE_SUBMIT_SCRIPT:-}"
if [ -z "$real_spectre" ]; then real_spectre=%s; fi
if [ -z "$submit_script" ]; then submit_script=%s; fi
if [ ! -x "$submit_script" ]; then
  echo "Crescendo Spectre submit script is missing or not executable: $submit_script" >&2
  exit 126
fi
case "${CRESCENDO_SIMULATION_HOME_SET-unrecorded}" in
  x) HOME=${CRESCENDO_SIMULATION_HOME_VALUE-}; export HOME ;;
  '') unset HOME ;;
  unrecorded) : ;;
  *) echo "Invalid Crescendo simulation HOME marker" >&2; exit 64 ;;
esac
unset CRESCENDO_SIMULATION_HOME_SET CRESCENDO_SIMULATION_HOME_VALUE
exec "$submit_script" "$real_spectre" "$@"
''' % (shlex.quote(str(real_spectre)), shlex.quote(str(submit_script)))
    atomic_write_text(wrapper_path, content)
    os.chmod(
        wrapper_path,
        os.stat(wrapper_path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH,
    )
    return wrapper_path, submit_script


def _workspace_binding(workspace):
    return {
        "workspace_root": workspace["root"],
        "simulation_root": workspace["simulation_root"],
        "setup_path": workspace["setup_sdb"],
        "library_name": workspace["primary_library"],
        "cell_name": workspace["setup_cell"],
        "view_name": workspace["setup_view"],
    }


def _candidate_id(candidate_root):
    parent = os.path.dirname(os.path.abspath(candidate_root))
    if os.path.basename(parent) == "attempts":
        return os.path.basename(os.path.dirname(parent)) or "candidate"
    return os.path.basename(os.path.abspath(candidate_root)) or "candidate"


def run_adexl(workspace, config, candidate_root, timeout_seconds=None,
              retained_log_root=None):
    if workspace.get("setup_backend") != "adexl":
        raise ValueError("run_adexl requires an ADE XL private workspace")
    candidate_root = ensure_dir(candidate_root)
    reset_results(workspace)
    candidate_timeout = int(
        timeout_seconds if timeout_seconds is not None
        else config["runtime"].get("candidate_timeout_seconds", 600)
    )
    runtime_root = ensure_dir(os.path.join(candidate_root, "adexl_runtime"))
    builder = AdeXLRuntimeBuilder(
        workspace["setup"], runtime_root, epoch=uuid.uuid4().hex,
        options={
            "candidate_timeout_seconds": candidate_timeout,
            "poll_interval_seconds": 1,
            "close_after_candidate": True,
        },
        workspace_binding=_workspace_binding(workspace),
        require_private_workspace=True,
    )
    session = builder.build_session()
    candidate = builder.build_candidate(
        _candidate_id(candidate_root),
        os.path.join(runtime_root, "candidates", "run"),
        timeout_seconds=candidate_timeout,
    )
    driver_path = os.path.join(runtime_root, "adexl_driver.il")
    driver_lines = [
        'load("%s")' % session["startup_script_path"].replace("\\", "\\\\").replace('"', '\\"'),
    ]
    binding_lines = workspace_test_design_lines(workspace)
    if binding_lines:
        # A private OA patch must never be followed by simulation of the source
        # design. Public AXL API errors close the owned session before candidate
        # execution; no fallback silently discards this identity proof.
        binding_path = os.path.join(runtime_root, "adexl_private_binding.il")
        atomic_write_text(binding_path, '\n'.join([
            'unless(boundp(\'crescAxlStartupResult) && crescAxlStartupResult && !crescAxlTainted exit(1))',
            'crescAxlPrivateBindingResult = errset(let((mainSDB crescPrivateTest crescPrivateArgs crescPrivateReadback)',
            '  mainSDB = crescAxlMainSetupDB',
        ] + ['  ' + line for line in binding_lines] + [
            '  t) t)',
            'unless(crescAxlPrivateBindingResult && car(crescAxlPrivateBindingResult)',
            '  crescAxlTainted = t',
            '  crescAxlSafeClose()',
            '  printf("CRESCENDO_ADEXL_PRIVATE_BINDING_FAILED\\n")',
            '  exit(1)', ')',
        ]) + '\n')
        driver_lines.append('load("%s")' % binding_path.replace("\\", "\\\\").replace('"', '\\"'))
    driver_lines.append('load("%s")' % candidate["script_path"].replace("\\", "\\\\").replace('"', '\\"'))
    atomic_write_text(driver_path, '\n'.join(driver_lines) + '\n')
    wrapper_path, submit_script = _spectre_wrapper(workspace, config)
    base_environment = child_environment(config, workspace["root"])
    spectre_timing_path = os.path.join(candidate_root, "spectre_timeout.log")
    environment = child_environment(
        config, workspace["root"], include_runtime_ld=True, extra={
            "PATH": os.path.dirname(wrapper_path) + ":" + base_environment["PATH"],
            "CRESCENDO_SIMULATION_TIMEOUT_SECONDS": config["runtime"].get(
                "simulation_timeout_seconds", 900,
            ),
            "CRESCENDO_REAL_SPECTRE_BIN": config["cadence"]["spectre_bin"],
            "CRESCENDO_CADENCE_ROOT": config["cadence"].get("cadence_root") or "",
            "CRESCENDO_SPECTRE_TIMEOUT_LOG": spectre_timing_path,
            "CRESCENDO_SPECTRE_RETAIN_ROOT": os.path.abspath(
                retained_log_root or candidate_root
            ),
            "CRESCENDO_SPECTRE_SUBMIT_SCRIPT": submit_script,
        },
    )
    command_argv = [
        config["cadence"]["virtuoso_bin"], "-nograph", "-restore", driver_path,
        "-log", os.path.join(candidate_root, "adexl.log"),
    ]
    guardian_state_path = os.path.join(candidate_root, "guardian_state.json")
    guardian_token = "adexl-%s" % uuid.uuid4().hex
    stdout_path = os.path.join(candidate_root, "ocean.stdout")
    stderr_path = os.path.join(candidate_root, "ocean.stderr")
    launch_lock_fd, launch_lock_path = acquire_legacy_guardian_launch_lock(candidate_root)
    started_epoch = time.time()
    try:
        parent_identity = legacy_guardian_parent_identity()
        guardian_argv = build_guardian_command(
            command_argv, workspace["root"], stdout_path, stderr_path,
            guardian_state_path, guardian_token,
            guardian_launch_lock_fd=launch_lock_fd,
        )
        guardian_manifest = write_legacy_guardian_manifest(
            candidate_root, workspace["root"], guardian_state_path,
            stdout_path, stderr_path, guardian_token, command_argv,
            parent_identity["pid"], guardian_script_path(),
            launch_lock_path=launch_lock_path,
            expected_parent_boot_id=parent_identity["boot_id"],
            expected_parent_start_ticks=parent_identity["start_ticks"],
            expected_uid=parent_identity["uid"],
            guardian_argv=guardian_argv,
        )
        command_launch_lock_fd = launch_lock_fd
        launch_lock_fd = None
        command = run_command(
            command_argv, cwd=workspace["root"], env=environment,
            timeout_seconds=candidate_timeout, stdout_path=stdout_path,
            stderr_path=stderr_path, startup_lock=CADENCE_STARTUP_LOCK,
            ready_path=candidate["marker_path"],
            startup_timeout_seconds=min(
                candidate_timeout,
                int(config["runtime"].get("ocean_startup_timeout_seconds", 90)),
            ),
            lock_timeout_seconds=config["runtime"].get("command_timeout_seconds", 1800),
            guardian_state_path=guardian_state_path,
            guardian_token=guardian_token,
            guardian_launch_lock_fd=command_launch_lock_fd,
        )
    finally:
        close_legacy_guardian_launch_lock(launch_lock_fd)
    command.update({
        "backend": "adexl",
        "started_epoch": started_epoch,
        "finished_epoch": time.time(),
        "driver_path": driver_path,
        "session_artifact": session,
        "candidate_artifact": candidate,
        "guardian_manifest_path": os.path.join(candidate_root, "legacy_guardian_launch.json"),
        "guardian_protocol": guardian_manifest["protocol"],
        "spectre_submit_script": submit_script,
        "spectre_timing": summarize_spectre_timing(
            spectre_timing_path, workspace["root"],
        ),
    })
    protocol_rows = matching_adexl_protocol(
        candidate["marker_path"], candidate["protocol_identity"],
    )
    command["adexl_protocol_states"] = [row["state"] for row in protocol_rows]
    if command.get("returncode") != 0:
        command["infrastructure_failure"] = True
    if command.get("returncode") == 0 and not command.get("timed_out"):
        try:
            finalized = finalize_adexl_candidate(candidate)
        except Exception as error:
            command["finalization_error"] = "%s: %s" % (
                type(error).__name__, str(error),
            )
            command["infrastructure_failure"] = True
            atomic_write_json(os.path.join(candidate_root, "adexl_command.json"), command)
        else:
            command.update({
                "history_name": finalized["history_name"],
                "history_identity": finalized["history_identity"],
                "rdb_path": finalized["rdb_path"],
                "results_dir": finalized["results_dir"],
                "results_location": finalized["results_location"],
                "run_id": finalized["run_id"],
                "run_status": finalized["run_status"],
                "adexl_protocol_states": finalized["states"],
                "infrastructure_failure": False,
            })
    technology_manifest = workspace.get("technology") or {}
    if (
            command.get("returncode") == 0 and not command.get("timed_out")
            and not command.get("infrastructure_failure")
            and workspace_technology_validation_required(technology_manifest)):
        try:
            command["technology_validation"] = validate_workspace_technology(
                technology_manifest, require_complete=True,
            )
        except TechnologyBindingError as error:
            command["technology_validation"] = {
                "ok": False, "status": "failed", "error": str(error),
            }
            atomic_write_json(os.path.join(candidate_root, "adexl_command.json"), command)
            raise ParameterApplicationInfrastructureFailure(str(error))
    atomic_write_json(os.path.join(candidate_root, "adexl_command.json"), command)
    return command
