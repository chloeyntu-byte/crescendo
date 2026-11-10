from __future__ import print_function

import copy
import os

from .cadence import discover_eda_environment
from .native_environment import (
    NATIVE_ENV_SNAPSHOT_VAR, parse_native_environment_snapshot,
    restore_native_environment,
)
from .pdk import normalize_pdk_profiles
from .resources import submit_spectre_script_path
from .utils import atomic_write_json, ensure_dir, load_json


def default_run_root(environ=None, work_dir=None):
    # Persistent TB results belong to the editor's project, not its home quota.
    # Menu launches replace this generic root with a named TB namespace.
    return os.path.join(os.path.realpath(os.path.abspath(work_dir or os.getcwd())), ".crescendo", "runs")


DEFAULTS = {
    "cadence": {
        "virtuoso_bin": None,
        "ocean_bin": None,
        "spectre_bin": None,
        "alps_bin": None,
        "alps_root": None,
        "alps_search_roots": ["/software/empyrean/alps", "/opt/eda/empyrean/alps", "/opt/empyrean/alps"],
        "simulator": "spectre",
        "alps_model_dir": None,
        "spectre_model_dir": None,
        "alps_model_file_map": {},
        "simulator_threads": None,
        "simulator_result_reader": "spectre",
        "spectre_result_reader": None,
        "alps_result_reader": None,
        "cadence_root": None,
        "spectre_root": None,
        "cds_lib": None,
        "license_file": None,
        "extra_binary_paths": [],
        "extra_library_paths": [],
        "search_roots": ["/opt/eda/cadence", "/opt/cadence",
                         "/software/cadence", "/software/cadence/spectre"],
        "pdk_name": None,
        "pdk_dir": None,
        "pdk_adapter": "generic-cdf",
        "pdk_adapter_policy": None,
        "pdk_profiles": [],
        "model_roots": [],
        "model_path_remaps": [],
        "startup_extensions": [],
        "setup_mode": "auto",
    },
    "runtime": {
        "run_root": default_run_root(),
        "run_root_origin": "derived",
        "execution_backend": "auto",
        "command_scheduler": "auto",
        "scheduler_receipt_grace_seconds": 10,
        "execution_commands": {"solver_templates": {"spectre": "", "alps": ""},
                               "submission_template": "", "scheduler_query_template": "",
                               "scheduler_cancel_template": ""},
        "data_root_override": None,
        "local_scratch_budget_bytes": 128 * 1024 * 1024,
        "max_parallel_jobs": 5,
        "persistent_worker_lifetime": "run",
        "spectre_submit_script": submit_spectre_script_path(),
        "simulation_timeout_seconds": 900,
        "candidate_timeout_seconds": 600,
        "scheduler_watchdog_grace_seconds": 30,
        "ocean_startup_timeout_seconds": 90,
        "controller_startup_timeout_seconds": 30,
        "controller_commit_timeout_seconds": 60,
        "persistent_cleanup_timeout_seconds": 20,
        "persistent_cleanup_termination_margin_seconds": 20,
        "controller_close_timeout_seconds": 30,
        "controller_term_grace_seconds": 5,
        "controller_kill_grace_seconds": 5,
        "controller_worker_cleanup_timeout_seconds": 15,
        "persistent_health_enabled": True,
        "persistent_active_health_interval_seconds": 2.0,
        "persistent_identity_startup_timeout_seconds": 90,
        "persistent_workspace_prepare_timeout_seconds": 1800,
        "persistent_startup_total_timeout_seconds": 1890,
        "maestro_batch_candidate_limit": 0,
        "cluster_queue_timeout_seconds": 3600,
        "cluster_stop_timeout_seconds": 15,
        "persistent_health_warmup_samples": 5,
        "persistent_health_baseline_samples": 3,
        "persistent_health_consecutive_limit": 3,
        "persistent_rss_growth_limit_mib": 512,
        "persistent_fd_growth_limit": 128,
        "persistent_min_free_disk_mib": 1024,
        "persistent_log_growth_limit_mib": 256,
        "persistent_resource_guard_max_restarts": 8,
        "infrastructure_retries": 1,
        "max_infrastructure_replacements": 0,
        "command_timeout_seconds": 1800,
        "range_validation_timeout_seconds": 120,
        "range_validation_shutdown_timeout_seconds": 15,
    },
    "safety": {
        "allow_source_writes": False,
        "require_private_workspace": True,
        "protect_existing_processes": True,
    },
}

MANUAL_KEYS = (
    "virtuoso_bin", "ocean_bin", "spectre_bin", "alps_bin", "alps_root", "alps_search_roots",
    "simulator", "alps_model_dir", "spectre_model_dir", "alps_model_file_map",
    "simulator_threads", "simulator_result_reader", "spectre_result_reader", "alps_result_reader", "cadence_root", "spectre_root", "cds_lib",
    "license_file", "extra_binary_paths", "extra_library_paths", "search_roots", "pdk_name",
    "pdk_dir", "pdk_adapter", "pdk_adapter_policy", "pdk_profiles", "model_roots",
    "model_path_remaps", "startup_extensions",
)

PDK_KEYS = (
    "pdk_name", "pdk_dir", "pdk_adapter", "pdk_adapter_policy", "pdk_profiles",
    "model_roots", "model_path_remaps", "startup_extensions",
    "alps_model_dir", "spectre_model_dir", "alps_model_file_map",
)

CADENCE_SCALAR_PATH_KEYS = (
    "virtuoso_bin", "ocean_bin", "spectre_bin", "alps_bin", "alps_root", "alps_model_dir", "spectre_model_dir", "cadence_root", "spectre_root",
    "cds_lib", "pdk_dir",
)

CADENCE_LIST_PATH_KEYS = (
    "extra_binary_paths", "extra_library_paths", "search_roots", "alps_search_roots", "model_roots",
    "startup_extensions",
)


def deep_merge(base, overlay):
    result = copy.deepcopy(base)
    for key, value in (overlay or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def user_config_path(environ=None):
    environ = os.environ if environ is None else environ
    if environ.get("CRESCENDO_USER_CONFIG"):
        return os.path.abspath(os.path.expanduser(environ["CRESCENDO_USER_CONFIG"]))
    base = environ.get("XDG_CONFIG_HOME") or os.path.join(os.path.expanduser("~"), ".config")
    return os.path.join(os.path.abspath(os.path.expanduser(base)), "crescendo", "config.json")


def _load_optional(path):
    return load_json(path) if path and os.path.isfile(path) else {}


def _user_setup(path):
    return _load_optional(path).get("eda_setup", {})


def _manual_values(cadence):
    return dict((key, copy.deepcopy(cadence.get(key))) for key in MANUAL_KEYS if cadence.get(key) not in (None, [], ""))


def _config_relative_path(value, base_dir):
    value = os.path.expandvars(os.path.expanduser(str(value)))
    if not os.path.isabs(value):
        value = os.path.join(base_dir, value)
    return os.path.abspath(value)


def _resolve_file_overrides(values, base_dir):
    result = copy.deepcopy(values or {})
    if not base_dir:
        return result
    for key in CADENCE_SCALAR_PATH_KEYS:
        if result.get(key):
            result[key] = _config_relative_path(result[key], base_dir)
    for key in CADENCE_LIST_PATH_KEYS:
        if key in result:
            result[key] = [_config_relative_path(value, base_dir) for value in result.get(key) or []]
    if "alps_model_file_map" in result:
        mapping = result.get("alps_model_file_map")
        if not isinstance(mapping, dict):
            raise ValueError("ALPS model mapping must be a source-to-target object")
        normalized = {}
        for source, target in mapping.items():
            if isinstance(target, dict):
                target = dict(target)
                if not target.get("path"):
                    raise ValueError("ALPS model mapping target path is missing")
                target["path"] = _config_relative_path(target["path"], base_dir)
            elif isinstance(target, str):
                target = _config_relative_path(target, base_dir)
            else:
                raise ValueError("ALPS model mapping target must be a path or path/section object")
            normalized[_config_relative_path(source, base_dir)] = target
        result["alps_model_file_map"] = normalized
    adapter = result.get("pdk_adapter")
    if adapter and adapter != "generic-cdf":
        result["pdk_adapter"] = _config_relative_path(adapter, base_dir)
    policy = result.get("pdk_adapter_policy")
    if isinstance(policy, dict) and policy.get("path"):
        policy = copy.deepcopy(policy)
        policy["path"] = _config_relative_path(policy["path"], base_dir)
        result["pdk_adapter_policy"] = policy
    if "pdk_profiles" in result:
        result["pdk_profiles"] = normalize_pdk_profiles(result, base_dir)
    if "model_path_remaps" in result:
        normalized = normalize_pdk_profiles({
            "pdk_name": result.get("pdk_name") or "manual-pdk",
            "pdk_dir": result.get("pdk_dir"),
            "model_path_remaps": result.get("model_path_remaps"),
        }, base_dir)
        result["model_path_remaps"] = normalized[0]["model_path_remaps"]
    license_value = result.get("license_file")
    if license_value:
        result["license_file"] = os.pathsep.join(
            item if "@" in item else _config_relative_path(item, base_dir)
            for item in str(license_value).split(os.pathsep) if item
        )
    return result


def persist_eda_setup(mode, manual, discovery, path=None):
    if mode not in ("auto", "manual"):
        raise ValueError("EDA setup mode must be auto or manual")
    path = os.path.abspath(os.path.expanduser(path or user_config_path()))
    payload = _load_optional(path)
    payload["schema_version"] = "crescendo.user_config.v1"
    payload["eda_setup"] = {
        "mode": mode,
        "manual": dict((key, copy.deepcopy(value)) for key, value in (manual or {}).items() if key in MANUAL_KEYS),
        "auto_detected": copy.deepcopy((discovery or {}).get("effective", {})),
        "last_discovery": copy.deepcopy(discovery or {}),
    }
    atomic_write_json(path, payload)
    return path


def configure_eda(manual=None, mode="manual", path=None, validate=True,
                  base_cadence=None, persist=True):
    if mode not in ("auto", "manual"):
        raise ValueError("EDA setup mode must be auto or manual")
    if mode == "auto":
        overrides = _manual_values(base_cadence or DEFAULTS["cadence"])
        for key in MANUAL_KEYS:
            if key != "search_roots" and key not in PDK_KEYS:
                overrides.pop(key, None)
    else:
        unknown = sorted(set((manual or {}).keys()) - set(MANUAL_KEYS))
        if unknown:
            raise ValueError("unsupported EDA override fields: %s" % ", ".join(unknown))
        overrides = _manual_values(base_cadence or DEFAULTS["cadence"])
        overrides.update(dict((key, value) for key, value in (manual or {}).items() if value not in (None, "")))
    launch_values = None
    if NATIVE_ENV_SNAPSHOT_VAR in os.environ:
        launch_values = parse_native_environment_snapshot(os.environ[NATIVE_ENV_SNAPSHOT_VAR])
    discovery = discover_eda_environment(
        overrides, validate=validate,
        environ=restore_native_environment(os.environ, launch_values),
    )
    if mode == "manual" and not discovery["ok"]:
        failures = list(discovery.get("missing") or [])
        failures.extend(row["name"] for row in discovery.get("version_checks", []) if not row.get("ok"))
        raise ValueError("EDA override validation failed: %s. %s" % (
            ", ".join(sorted(set(failures))) or "unknown check",
            " ".join(discovery.get("recommendations") or []),
        ))
    config_path = os.path.abspath(os.path.expanduser(path or user_config_path()))
    if persist:
        config_path = persist_eda_setup(mode, manual or {}, discovery, path=config_path)
    return discovery, config_path


def load_config(path=None, persist_discovery=True, validate_discovery=True):
    config = copy.deepcopy(DEFAULTS)
    explicit_path = path or os.environ.get("CRESCENDO_CONFIG")
    user_path = user_config_path()
    explicit_payload = {}
    if explicit_path:
        explicit_path = os.path.abspath(os.path.expanduser(explicit_path))
        explicit_payload = load_json(explicit_path)
        config = deep_merge(config, explicit_payload)
    saved_setup = _user_setup(user_path)
    explicit_cadence = explicit_payload.get("cadence", {})
    explicit_mode = explicit_cadence.get("setup_mode")
    if not explicit_mode and any(
            explicit_cadence.get(key) not in (None, [], "")
            for key in MANUAL_KEYS if key != "search_roots" and key not in PDK_KEYS
    ):
        explicit_mode = "manual"
    mode = explicit_mode or saved_setup.get("mode") or config["cadence"].get("setup_mode") or "auto"
    if mode not in ("auto", "manual"):
        raise ValueError("cadence.setup_mode must be auto or manual")
    explicit_base = os.path.dirname(explicit_path) if explicit_path else None
    file_overrides = _resolve_file_overrides(_manual_values(explicit_cadence), explicit_base)
    saved_manual = _resolve_file_overrides(
        saved_setup.get("manual", {}) if mode == "manual" else {},
        os.path.dirname(user_path),
    )
    overrides = deep_merge(saved_manual, file_overrides)
    if mode == "auto":
        for key in MANUAL_KEYS:
            if key != "search_roots" and key not in PDK_KEYS:
                overrides.pop(key, None)
    launch_values = None
    if NATIVE_ENV_SNAPSHOT_VAR in os.environ:
        launch_values = parse_native_environment_snapshot(os.environ[NATIVE_ENV_SNAPSHOT_VAR])
    native_environ = restore_native_environment(os.environ, launch_values)
    discovery = discover_eda_environment(overrides, validate=validate_discovery, environ=native_environ)
    config["cadence"] = deep_merge(config["cadence"], discovery["effective"])
    config["cadence"]["setup_mode"] = mode
    config["cadence"]["discovery"] = discovery
    if launch_values is not None:
        config["cadence"]["launch_environment"] = launch_values
    config["cadence"]["user_config_path"] = user_path
    # Keep persisted user overrides separate from effective discovery values.
    # The native UI may display both, but must never turn auto-discovered host
    # paths into manual configuration merely because the settings page was
    # opened and saved.
    config["cadence"]["_manual_overrides"] = copy.deepcopy(saved_manual)
    config["cadence"]["_file_overrides"] = copy.deepcopy(file_overrides)
    config["_explicit_config_path"] = explicit_path
    if persist_discovery:
        persist_eda_setup(mode, saved_manual, discovery, path=user_path)
    environment_run_root = os.environ.get("CRESCENDO_RUN_ROOT")
    explicit_runtime = explicit_payload.get("runtime", {})
    configured_run_root = environment_run_root or explicit_runtime.get("run_root") or default_run_root()
    run_root_base = os.getcwd() if environment_run_root else (explicit_base or os.getcwd())
    config["runtime"]["run_root"] = ensure_dir(_config_relative_path(configured_run_root, run_root_base))
    config["runtime"]["storage_base_override"] = (
        config["runtime"]["run_root"] if environment_run_root or explicit_runtime.get("run_root") else None
    )
    config["runtime"]["run_root_override"] = config["runtime"]["storage_base_override"]
    config["runtime"]["run_root_origin"] = "explicit" if config["runtime"]["storage_base_override"] else "derived"
    override = config["runtime"].get("data_root_override")
    if override is not None:
        from .storage_policy import resolve_source_path
        config["runtime"]["data_root_override"] = resolve_source_path(override, explicit_base or os.getcwd(), os.environ.get("HOME"))
    environment_submit_script = os.environ.get("CRESCENDO_SPECTRE_SUBMIT_SCRIPT")
    submit_script = environment_submit_script or config["runtime"].get("spectre_submit_script")
    if not submit_script:
        submit_script = submit_spectre_script_path()
    submit_script = os.path.expandvars(os.path.expanduser(str(submit_script)))
    if not os.path.isabs(submit_script):
        base_dir = os.getcwd() if environment_submit_script else (
            os.path.dirname(explicit_path) if explicit_path else os.getcwd()
        )
        submit_script = os.path.join(base_dir, submit_script)
    config["runtime"]["spectre_submit_script"] = os.path.abspath(submit_script)
    if config["safety"].get("allow_source_writes"):
        raise ValueError("safety.allow_source_writes must remain false")
    return config


def doctor(config):
    cadence = config["cadence"]
    checks = []
    for key in ("virtuoso_bin", "ocean_bin", "spectre_bin"):
        path = cadence.get(key)
        checks.append({"name": key, "ok": bool(path and os.path.isfile(path) and os.access(path, os.X_OK)), "path": path})
    discovery = cadence.get("discovery", {})
    for row in discovery.get("checks", []):
        if row["name"] not in ("virtuoso_bin", "ocean_bin", "spectre_bin"):
            checks.append({"name": row["name"], "ok": row["ok"], "path": row.get("value")})
    for row in discovery.get("version_checks", []):
        checks.append({"name": "%s_version" % row["name"], "ok": row["ok"], "path": row.get("binary")})
    checks.append({"name": "run_root", "ok": os.path.isdir(config["runtime"]["run_root"]), "path": config["runtime"]["run_root"]})
    submit_script = config["runtime"].get("spectre_submit_script")
    checks.append({
        "name": "spectre_submit_script",
        "ok": bool(submit_script and os.path.isfile(submit_script) and os.access(submit_script, os.X_OK)),
        "path": submit_script,
    })
    return {
        "ok": all(row["ok"] for row in checks),
        "checks": checks,
        "discovery": discovery,
        "recommendations": discovery.get("recommendations", []),
        "config": config,
    }
