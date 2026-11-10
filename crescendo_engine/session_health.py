from __future__ import print_function

import math
import os
import resource
import stat
import statistics


DEFAULT_RSS_GROWTH_LIMIT_KIB = 512 * 1024
DEFAULT_FD_GROWTH_LIMIT = 128
DEFAULT_MIN_FREE_DISK_MIB = 1024
DEFAULT_LOG_GROWTH_LIMIT_BYTES = 256 * 1024 * 1024


class PersistentSessionHealthLimit(RuntimeError):
    def __init__(self, decision):
        self.decision = decision
        detail = decision
        for key in ("decision", "health_decision", "resource_guard_decision"):
            if isinstance(decision.get(key), dict):
                detail = decision[key]
                break
        limits = detail.get("limits") or {}
        sample = detail.get("sample") or {}
        state = decision.get("state") or "limit_exceeded"
        underlying_state = detail.get("state") or state
        reason = detail.get("identity_unproven_reason") or {}
        if not isinstance(reason, dict):
            reason = {"code": str(reason)}
        violations = detail.get("violations") or []
        if isinstance(violations, str):
            violations = [violations]
        reason_text = reason.get("code") or ",".join(str(value) for value in violations) or underlying_state
        stage = detail.get("startup_phase") or detail.get("phase") or "session_health"
        if underlying_state in (
                "identity_startup_timeout", "workspace_prepare_timeout",
                "startup_total_timeout", "startup_request_identity_changed"):
            message = (
                "persistent Maestro startup guard failed: state=%s, stage=%s, "
                "reason=%s, elapsed=%s seconds, phase_timeout=%s seconds, "
                "total_elapsed=%s seconds, total_timeout=%s seconds; checkpoint is current"
                % (state, stage, reason_text, detail.get("identity_pending_seconds"),
                   detail.get("startup_phase_timeout_seconds") or detail.get("identity_startup_timeout_seconds"),
                   detail.get("startup_total_seconds"), detail.get("startup_total_timeout_seconds"))
            )
        else:
            message = (
                "persistent Maestro resource guard failed: state=%s, stage=%s, reason=%s, "
                "consecutive_samples=%s; checkpoint is current "
                "(rss=%s KiB, limit=%s KiB, fds=%s, limit=%s, max_fd=%s, limit=%s, "
                "free_disk=%s MiB, minimum=%s MiB, logs=%s bytes, limit=%s bytes)"
                % (
                    state, stage, reason_text, detail.get("violation_streak", "unknown"),
                    sample.get("rss_kib"), limits.get("rss_kib"),
                    sample.get("fd_count"), limits.get("fd_count"),
                    sample.get("max_fd_count"), limits.get("max_fd_count"),
                    sample.get("free_disk_mib"), limits.get("min_free_disk_mib"),
                    sample.get("log_bytes"), limits.get("log_bytes"),
                )
            )
        RuntimeError.__init__(self, message)


def _nonnegative_integer(value):
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed >= 0 else None


def _nofile_soft_limit():
    try:
        soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except (ValueError, OSError):
        return None
    if soft in (resource.RLIM_INFINITY, -1):
        return None
    return _nonnegative_integer(soft)


def _rss_absolute_limit_details():
    """An active process tree needs a machine/cgroup cap, not an idle delta."""
    limits = []
    try:
        with open("/proc/meminfo", "r") as stream:
            for line in stream:
                if line.startswith("MemTotal:"):
                    limits.append({"source": "/proc/meminfo:MemTotal", "bytes": int(line.split()[1]) * 1024})
                    break
    except (IOError, OSError, ValueError, IndexError):
        pass
    try:
        with open("/proc/self/cgroup", "r") as stream:
            rows = list(stream)
        for row in rows:
            hierarchy, controllers, relative = row.rstrip().split(":", 2)
            if hierarchy == "0" and not controllers:
                base, name = "/sys/fs/cgroup", "memory.max"
            elif "memory" in controllers.split(","):
                base, name = "/sys/fs/cgroup/memory", "memory.limit_in_bytes"
            else:
                continue
            directory = os.path.abspath(os.path.join(base, relative.lstrip("/")))
            if directory != base and not directory.startswith(base + os.sep):
                continue
            # A leaf can be unlimited while a parent enforces the effective cap.
            while directory == base or directory.startswith(base + os.sep):
                try:
                    with open(os.path.join(directory, name), "r") as stream:
                        value = stream.read(64).strip()
                    if value != "max" and int(value) > 0:
                        limits.append({"source": os.path.join(directory, name), "bytes": int(value)})
                except (IOError, OSError, ValueError):
                    pass
                if directory == base:
                    break
                directory = os.path.dirname(directory)
    except (IOError, OSError, ValueError, IndexError):
        pass
    return {"fraction": 0.80, "sources": limits,
            "limit_kib": int(min(row["bytes"] for row in limits) * 0.80 // 1024) if limits else None}


def _process_stat(pid):
    try:
        with open("/proc/%d/stat" % int(pid), "r") as stream:
            raw = stream.read()
    except (IOError, OSError, TypeError, ValueError):
        return None
    closing = raw.rfind(")")
    if closing < 0:
        return None
    fields = raw[closing + 2:].split()
    if len(fields) <= 19:
        return None
    try:
        return {
            "pid": int(pid), "state": fields[0], "ppid": int(fields[1]),
            "start_ticks": int(fields[19]),
        }
    except (TypeError, ValueError):
        return None


def process_identity(pid):
    observed = _process_stat(pid)
    if not observed or observed.get("state") == "Z":
        return None
    return {"pid": observed["pid"], "start_ticks": observed["start_ticks"]}


def _process_table():
    try:
        entries = os.listdir("/proc")
    except OSError:
        return {}
    result = {}
    for entry in entries:
        if not entry.isdigit():
            continue
        observed = _process_stat(int(entry))
        if observed and observed.get("state") != "Z":
            result[observed["pid"]] = observed
    return result


def _process_resources(pid, expected_start_ticks):
    before = _process_stat(pid)
    if (
            not before or before.get("state") == "Z"
            or before.get("start_ticks") != int(expected_start_ticks)):
        return None
    rss_kib = None
    fd_count = None
    try:
        with open("/proc/%d/status" % int(pid), "r") as stream:
            for line in stream:
                if line.startswith("VmRSS:"):
                    rss_kib = int(line.split()[1])
                    break
    except (IOError, OSError, ValueError, IndexError):
        pass
    try:
        fd_count = len(os.listdir("/proc/%d/fd" % int(pid)))
    except (IOError, OSError):
        pass
    after = _process_stat(pid)
    if (
            not after or after.get("state") == "Z"
            or after.get("start_ticks") != int(expected_start_ticks)):
        return None
    return {
        "pid": int(pid), "ppid": after.get("ppid"),
        "start_ticks": int(expected_start_ticks),
        "rss_kib": rss_kib, "fd_count": fd_count,
    }


def _safe_storage_sample(paths):
    rows = []
    seen_devices = set()
    complete = True
    for path in sorted(set(os.path.abspath(value) for value in (paths or []) if value)):
        try:
            value = os.lstat(path)
            if stat.S_ISLNK(value.st_mode) or not stat.S_ISDIR(value.st_mode):
                complete = False
                rows.append({"path": path, "error": "path is not a real directory"})
                continue
            if value.st_dev in seen_devices:
                continue
            filesystem = os.statvfs(path)
            free_bytes = int(filesystem.f_bavail) * int(filesystem.f_frsize)
            total_bytes = int(filesystem.f_blocks) * int(filesystem.f_frsize)
            seen_devices.add(value.st_dev)
            rows.append({
                "path": path, "device": int(value.st_dev),
                "free_bytes": free_bytes, "total_bytes": total_bytes,
            })
        except (IOError, OSError, TypeError, ValueError) as error:
            complete = False
            rows.append({"path": path, "error": "%s: %s" % (error.__class__.__name__, error)})
    free_values = [row["free_bytes"] for row in rows if row.get("free_bytes") is not None]
    return {
        "storage_complete": bool(complete and free_values),
        "free_disk_mib": (
            int(min(free_values) // (1024 * 1024)) if free_values else None
        ),
        "filesystems": rows,
    }


def _safe_log_sample(paths):
    rows = []
    complete = True
    total = 0
    for path in sorted(set(os.path.abspath(value) for value in (paths or []) if value)):
        if not os.path.lexists(path):
            continue
        try:
            value = os.lstat(path)
            if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
                complete = False
                rows.append({"path": path, "error": "log is not a regular file"})
                continue
            size = int(value.st_size)
            total += size
            rows.append({"path": path, "bytes": size})
        except (IOError, OSError, TypeError, ValueError) as error:
            complete = False
            rows.append({"path": path, "error": "%s: %s" % (error.__class__.__name__, error)})
    return {"logs_complete": complete, "log_bytes": total, "logs": rows}


def sample_owned_process_tree(root_identities, storage_paths=None, log_paths=None):
    roots = []
    for identity in root_identities or []:
        identity = identity if isinstance(identity, dict) else {}
        pid = _nonnegative_integer(identity.get("pid"))
        start_ticks = _nonnegative_integer(identity.get("start_ticks"))
        if not pid or pid <= 1 or not start_ticks:
            continue
        roots.append({
            "pid": pid, "start_ticks": start_ticks,
            "role": str(identity.get("role") or "root"),
        })
    result = {
        "schema_version": "crescendo.owned_process_tree_health.v1",
        "alive": False, "tree_complete": False,
        "pid": roots[0]["pid"] if roots else None,
        "pid_start_ticks": roots[0]["start_ticks"] if roots else None,
        "rss_kib": None, "fd_count": None, "process_count": 0,
        "max_fd_count": None,
        "roots": roots, "processes": [],
    }
    table = _process_table()
    exact_roots = []
    for root in roots:
        observed = table.get(root["pid"])
        if observed and observed.get("start_ticks") == root["start_ticks"]:
            exact_roots.append(root)
    if roots and len(exact_roots) == len(roots):
        owned = set(root["pid"] for root in exact_roots)
        changed = True
        while changed:
            changed = False
            for pid, observed in table.items():
                if pid not in owned and observed.get("ppid") in owned:
                    owned.add(pid)
                    changed = True
        samples = []
        unavailable = []
        for pid in sorted(owned):
            sample = _process_resources(pid, table[pid]["start_ticks"])
            if sample is None:
                unavailable.append(pid)
            else:
                samples.append(sample)
        sampled_roots = dict((row["pid"], row["start_ticks"]) for row in samples)
        root_complete = all(
            sampled_roots.get(root["pid"]) == root["start_ticks"] for root in exact_roots
        )
        resource_complete = bool(
            root_complete and not unavailable and all(
                row.get("rss_kib") is not None and row.get("fd_count") is not None
                for row in samples
            )
        )
        result.update({
            "alive": root_complete,
            "tree_complete": resource_complete,
            "rss_kib": sum(row["rss_kib"] for row in samples) if resource_complete else None,
            "fd_count": sum(row["fd_count"] for row in samples) if resource_complete else None,
            "max_fd_count": max(row["fd_count"] for row in samples) if resource_complete else None,
            "process_count": len(samples), "processes": samples,
            "unavailable_pids": unavailable,
        })
    result.update(_safe_storage_sample(storage_paths))
    result.update(_safe_log_sample(log_paths))
    return result


class PersistentSessionHealthGuard(object):
    def __init__(self, warmup_samples=5, baseline_samples=3, consecutive_limit=3,
                 rss_growth_limit_kib=DEFAULT_RSS_GROWTH_LIMIT_KIB,
                 fd_growth_limit=DEFAULT_FD_GROWTH_LIMIT, nofile_soft_limit=None,
                 min_free_disk_mib=DEFAULT_MIN_FREE_DISK_MIB,
                 log_growth_limit_bytes=DEFAULT_LOG_GROWTH_LIMIT_BYTES,
                 rss_absolute_limit_kib=None):
        self.warmup_samples = max(1, int(warmup_samples))
        self.baseline_samples = max(1, min(int(baseline_samples), self.warmup_samples))
        self.consecutive_limit = max(1, int(consecutive_limit))
        self.rss_growth_limit_kib = max(1, int(rss_growth_limit_kib))
        self.fd_growth_limit = max(1, int(fd_growth_limit))
        self.min_free_disk_mib = max(1, int(min_free_disk_mib))
        self.log_growth_limit_bytes = max(1, int(log_growth_limit_bytes))
        self.rss_absolute_limit_details = (
            _rss_absolute_limit_details() if rss_absolute_limit_kib is None
            else {"source": "explicit", "limit_kib": _nonnegative_integer(rss_absolute_limit_kib)}
        )
        self.rss_absolute_limit_kib = self.rss_absolute_limit_details["limit_kib"]
        self.nofile_soft_limit = (
            _nofile_soft_limit() if nofile_soft_limit is None
            else _nonnegative_integer(nofile_soft_limit)
        )
        self.samples = []
        self.total_samples = 0
        self.growth_comparable = True
        self.rss_baseline_kib = None
        self.fd_baseline = None
        self.log_baseline_bytes = None
        self.violation_streak = 0

    def _sample(self, health):
        health = health if isinstance(health, dict) else {}
        tree_complete = health.get("tree_complete", True) is not False
        storage_complete = health.get("storage_complete", True) is not False
        logs_complete = health.get("logs_complete", True) is not False
        return {
            "alive": health.get("alive") is True,
            "tree_complete": tree_complete,
            "storage_complete": storage_complete,
            "logs_complete": logs_complete,
            "session_identity_complete": (
                health.get("session_identity_complete", True) is not False
            ),
            "rss_kib": _nonnegative_integer(
                health.get("rss_kib", health.get("rss_kb"))
            ) if tree_complete else None,
            "fd_count": (
                _nonnegative_integer(health.get("fd_count")) if tree_complete else None
            ),
            "max_fd_count": (
                _nonnegative_integer(
                    health.get("max_fd_count", health.get("fd_count"))
                ) if tree_complete else None
            ),
            "free_disk_mib": (
                _nonnegative_integer(health.get("free_disk_mib")) if storage_complete else None
            ),
            "log_bytes": (
                _nonnegative_integer(health.get("log_bytes")) if logs_complete else None
            ),
        }

    def _set_baseline(self):
        baseline_window = self.samples[-self.baseline_samples:]
        rss_values = [sample["rss_kib"] for sample in baseline_window
                      if sample["rss_kib"] is not None]
        fd_values = [sample["fd_count"] for sample in baseline_window
                     if sample["fd_count"] is not None]
        log_values = [sample["log_bytes"] for sample in baseline_window
                      if sample["log_bytes"] is not None]
        if rss_values:
            self.rss_baseline_kib = int(statistics.median(rss_values))
        if fd_values:
            self.fd_baseline = int(statistics.median(fd_values))
        if log_values:
            self.log_baseline_bytes = int(statistics.median(log_values))

    def _limits(self):
        rss_limit = None
        if self.rss_baseline_kib is not None:
            rss_limit = self.rss_baseline_kib + self.rss_growth_limit_kib
        fd_limit = None
        if self.fd_baseline is not None:
            fd_limit = self.fd_baseline + self.fd_growth_limit
        max_fd_limit = (
            int(math.floor(self.nofile_soft_limit * 0.70))
            if self.nofile_soft_limit else None
        )
        log_limit = None
        if self.log_baseline_bytes is not None:
            log_limit = self.log_baseline_bytes + self.log_growth_limit_bytes
        return rss_limit, fd_limit, max_fd_limit, log_limit

    def observe(self, health, comparable=True):
        # Only idle boundaries can be compared for retained-session growth.
        # Active netlisting/simulation has an intentionally different tree.
        self.growth_comparable = bool(comparable)
        self.total_samples += 1
        sample = self._sample(health)
        monitoring_violations = []
        if not sample["alive"]:
            monitoring_violations.append("owned_process_tree_dead")
        elif not sample["tree_complete"]:
            monitoring_violations.append("owned_process_tree_incomplete")
        if not sample["storage_complete"]:
            monitoring_violations.append("storage_health_unavailable")
        if not sample["logs_complete"]:
            monitoring_violations.append("log_health_unavailable")
        if not sample["session_identity_complete"]:
            monitoring_violations.append("session_identity_unproven")
        if (
            not monitoring_violations
            and sample["rss_kib"] is None and sample["fd_count"] is None
            and sample["free_disk_mib"] is None and sample["log_bytes"] is None
        ):
            self.violation_streak = 0
            return self._result(sample, "unavailable", [])

        if comparable:
            self.samples.append(sample)
        if self.log_baseline_bytes is None and sample["log_bytes"] is not None:
            self.log_baseline_bytes = sample["log_bytes"]
        if (
                sample["free_disk_mib"] is not None
                and sample["free_disk_mib"] < self.min_free_disk_mib):
            self.violation_streak = max(1, self.violation_streak + 1)
            return self._result(sample, "limit_exceeded", ["disk_low_watermark"])
        if comparable and len(self.samples) == self.warmup_samples:
            self._set_baseline()
        rss_limit, fd_limit, max_fd_limit, log_limit = self._limits()
        violations = list(monitoring_violations)
        if self.rss_absolute_limit_kib is not None and sample["rss_kib"] is not None:
            if sample["rss_kib"] > self.rss_absolute_limit_kib:
                violations.append("rss_absolute_limit")
        if max_fd_limit is not None and sample["max_fd_count"] is not None:
            if sample["max_fd_count"] > max_fd_limit:
                violations.append("fd_per_process_limit")
        if comparable and len(self.samples) > self.warmup_samples:
            if rss_limit is not None and sample["rss_kib"] is not None:
                if sample["rss_kib"] > rss_limit:
                    violations.append("rss_growth")
            if fd_limit is not None and sample["fd_count"] is not None:
                if sample["fd_count"] > fd_limit:
                    violations.append("fd_growth")
        if (not comparable or len(self.samples) > self.warmup_samples) and log_limit is not None and sample["log_bytes"] is not None:
            if sample["log_bytes"] > log_limit:
                violations.append("log_growth")
        self.violation_streak = self.violation_streak + 1 if violations else 0
        if self.violation_streak >= self.consecutive_limit:
            state = "limit_exceeded"
        elif violations:
            state = "warning"
        elif comparable and len(self.samples) <= self.warmup_samples:
            state = "warming"
        else:
            state = "healthy"
        return self._result(sample, state, violations)

    def _result(self, sample, state, violations):
        rss_limit, fd_limit, max_fd_limit, log_limit = self._limits()
        return {
            "schema_version": "crescendo.persistent_session_health.v1",
            "state": state,
            "triggered": state == "limit_exceeded",
            "sample_count": self.total_samples,
            "growth_sample_count": len(self.samples),
            "growth_comparable": self.growth_comparable,
            "warmup_samples": self.warmup_samples,
            "consecutive_limit": self.consecutive_limit,
            "violation_streak": self.violation_streak,
            "violations": list(violations),
            "sample": sample,
            "baseline": {
                "rss_kib": self.rss_baseline_kib,
                "fd_count": self.fd_baseline,
                "log_bytes": self.log_baseline_bytes,
            },
            "limits": {
                "rss_kib": rss_limit if self.growth_comparable else self.rss_absolute_limit_kib,
                "rss_growth_kib": rss_limit,
                "rss_absolute_kib": self.rss_absolute_limit_kib,
                "rss_absolute_source": self.rss_absolute_limit_details,
                "fd_count": fd_limit,
                "max_fd_count": max_fd_limit,
                "nofile_soft": self.nofile_soft_limit,
                "min_free_disk_mib": self.min_free_disk_mib,
                "log_bytes": log_limit,
            },
        }
