"""Candidate batching and Maestro job capacity are independent quantities."""
from __future__ import print_function

import math
import copy
import re


def candidate_execution_plan(spec, prepared, config):
    mode = str(spec.get("maestro_candidate_mode", "auto")).strip().lower()
    if mode not in ("auto", "serial", "batch"):
        raise ValueError("maestro_candidate_mode must be auto, serial or batch")
    overrides = spec.get("maestro_job_overrides") or {}
    policy = prepared.get("job_policy") or {}
    properties = (policy.get("global") or {}).get("properties") or {}
    distribution = str(properties.get("distributionmethod") or "").strip().lower()
    if overrides.get("command"):
        distribution = "command"
    max_jobs = overrides.get("max_jobs", properties.get("maxjobs"))
    try:
        if isinstance(max_jobs, bool):
            raise ValueError()
        max_jobs = int(max_jobs)
        if max_jobs <= 0:
            raise ValueError()
    except (TypeError, ValueError, OverflowError):
        max_jobs = None
    cluster_distributions = ("command", "remote-host", "remote", "lsf", "sge")
    is_cluster = distribution in cluster_distributions
    enabled_tests = set(row.get("name") for row in prepared.get("setup", {}).get("tests", [])
                        if row.get("enabled", True))
    test_policies = []
    for test in policy.get("tests", []):
        if enabled_tests and test.get("name") not in enabled_tests:
            continue
        effective = dict(properties, **(test.get("properties") or {}))
        effective_distribution = ("command" if overrides.get("command") else
                                  str(effective.get("distributionmethod") or "").lower())
        is_cluster = is_cluster or effective_distribution in cluster_distributions
        test_policies.append({"test": test.get("name"), "distribution": effective_distribution,
                              "max_jobs": overrides.get("max_jobs", effective.get("maxjobs"))})
    enabled = mode == "batch" or (mode == "auto" and is_cluster)
    limit = (config.get("runtime") or {}).get("maestro_batch_candidate_limit", 0)
    if isinstance(limit, bool):
        raise ValueError("maestro_batch_candidate_limit must be a non-negative integer")
    numeric = float(limit)
    if not math.isfinite(numeric) or numeric < 0 or int(numeric) != numeric:
        raise ValueError("maestro_batch_candidate_limit must be a non-negative integer")
    return {
        "requested_mode": mode, "enabled": enabled,
        "effective_mode": "batch" if enabled else "serial",
        "reason": "explicit_batch" if mode == "batch" else (
            "cluster_job_policy" if enabled else "serial_or_no_parallel_cluster_policy"
        ),
        "candidate_limit": int(numeric), "max_jobs": max_jobs,
        "distribution": distribution or None,
        "test_policies": test_policies,
        "job_policy_source": policy.get("source"),
        "capacity_policy": "native_maestro_job_policy",
        "native_cluster_stop_required": bool(is_cluster or policy.get("available") is not True),
    }


def candidate_batches(planned, limit=0):
    """Do not pad, drop or regenerate population to match an ICRP count."""
    planned = list(planned)
    if not planned:
        return []
    size = int(limit) if limit else len(planned)
    if size < 1:
        raise ValueError("batch candidate limit must be positive or zero for the whole generation")
    return [planned[start:start + size] for start in range(0, len(planned), size)]


def batch_timeout_seconds(candidate_count, spec, config):
    runtime = config.get("runtime") or {}
    queue = float(runtime.get("cluster_queue_timeout_seconds", 3600))
    per_candidate = float(spec.get("candidate_timeout_seconds", 600))
    preparation = float(runtime.get("persistent_workspace_prepare_timeout_seconds", 1800))
    startup = max(float(runtime.get("ocean_startup_timeout_seconds", 90)),
                  float(runtime.get("persistent_identity_startup_timeout_seconds", 90)))
    grace = float(runtime.get("scheduler_watchdog_grace_seconds", 30))
    values = (queue, per_candidate, preparation, startup, grace)
    if any(not math.isfinite(value) or value < 0 for value in values) or per_candidate <= 0:
        raise ValueError("batch timeouts must be finite and bounded")
    # Worst-case serialized execution remains legal when maxjobs is smaller
    # than a generation, or the cluster currently has no free capacity.
    return preparation + startup + queue + max(1, int(candidate_count)) * per_candidate + grace


def slice_candidate_spectre_timing(summary, test_names):
    """Keep shared wrapper failures and timing bound to one physical test set."""
    test_names = set(test_names)
    runs = [copy.deepcopy(row) for row in summary.get("runs") or []
            if test_names.intersection(re.split(r"[/\\\\]+", str(row.get("cwd") or "")))]
    return {
        "run_count": len(runs), "reported_end_count": len(runs),
        "completed_run_count": sum(row.get("returncode") == 0 for row in runs),
        "unclosed_start_count": 0,
        "total_seconds": sum(float(row.get("elapsed_seconds", 0.0)) for row in runs),
        "runs": runs, "scope": "candidate_physical_tests",
        # Unmatched START records cannot be assigned without a complete CWD;
        # retain that batch-level diagnostic rather than blame another point.
        "shared_unclosed_start_count": summary.get("unclosed_start_count", 0),
    }


def native_job_state_quiescent(state):
    if not isinstance(state, dict):
        return False
    if state.get("simulation_submitted") is False:
        return state.get("no_remote_jobs_started") is True
    if state.get("simulation_submitted") is True:
        return (state.get("native_completion_proven") is True or
                state.get("native_stop_proven") is True)
    return False


def serial_timeout_seconds(spec, config):
    runtime = config.get("runtime") or {}
    candidate = float(spec.get("candidate_timeout_seconds", 600))
    queue = float(runtime.get("cluster_queue_timeout_seconds", 3600)) if runtime.get("native_cluster_stop_required") else 0.0
    if not math.isfinite(candidate) or candidate <= 0 or not math.isfinite(queue) or queue < 0:
        raise ValueError("serial candidate/queue budgets must be finite and bounded")
    return candidate + queue
