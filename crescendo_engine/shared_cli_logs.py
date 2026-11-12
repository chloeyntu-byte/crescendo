"""Bounded actual solver log retention, with no completion/stop authority."""
import hashlib
import os
import re
import stat

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .owned_solver_logs import OwnedSolverLogError, owned_solver_log_reader
from .utils import atomic_write_json, atomic_write_text, utc_timestamp


RETAINED_SCHEMA = _contract_text("crescendo.shared_cli_retained_log.v1")
MAX_LOG_BYTES = 64 * 1024


def retained_cli_name(name):
    return isinstance(name, str) and re.fullmatch(r"retained_cli_[0-9a-f]{32}\.(?:log|meta)", name) is not None


def retain_shared_cli_task_logs(task, run_root, candidate_id, launch_epoch=None):
    """Retain a tail once after solver exit, before committed raw PSF deletion.

    Every source is in this exact candidate/task directory. No recursive search
    or long-lived descriptor survives the native reader barrier. Retained text
    is explicitly a snapshot, never simulator completion or termination proof.
    """
    result = {"schema_version": RETAINED_SCHEMA, "candidate_id": candidate_id,
              "task_id": task.get("task_id"), "files": [], "warnings": []}
    if not isinstance(candidate_id, str) or not re.fullmatch(r"g[0-9]{3,}_c[0-9]{4,}", candidate_id):
        return result  # Template qualification has no optimizer candidate.
    task_id = task.get("task_id")
    if (not isinstance(task_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", task_id)
            or task_id in (".", "..")):
        result["warnings"].append("invalid_task_identity")
        return result
    run_root = os.path.abspath(run_root)
    candidate_root = os.path.join(run_root, "candidates", candidate_id)
    task_root = os.path.join(candidate_root, "simulation", "tasks", task_id)
    psf = task.get("psf_dir")
    if (os.path.realpath(candidate_root) != candidate_root or not os.path.isdir(candidate_root)
            or not isinstance(psf, str) or os.path.dirname(os.path.abspath(psf)) != task_root
            or os.path.realpath(task_root) != task_root):
        result["warnings"].append("task_directory_identity_disagrees")
        return result
    simulator = task.get("execution_simulator", "spectre")
    if launch_epoch is None:
        launch_epoch = task.get('launch_epoch')
    paths = [(simulator, task.get("simulator_log") or task.get("spectre_log")),
             (simulator + "_stdout", task.get("solver_stdout") or os.path.join(task_root, "execution", "simulator.stdout")),
             (simulator + "_stderr", task.get("solver_stderr") or os.path.join(task_root, "execution", "simulator.stderr")),
             (simulator + "_psf", os.path.join(psf, "logFile"))]
    if simulator == "alps":
        paths += [("alps_companion", psf + ".log"), ("alps_warning", psf + ".warn")]
    paths += [("scheduler_stdout", task.get("scheduler_stdout")), ("scheduler_stderr", task.get("scheduler_stderr"))]
    seen = set()
    for kind, source in paths:
        if not isinstance(source, str) or not source or source in seen:
            continue
        seen.add(source)
        source = os.path.abspath(source)
        if not source.startswith(task_root + os.sep):
            result["warnings"].append(kind + ": unowned_source")
            continue
        try:
            with owned_solver_log_reader(source, task_root, launch_epoch, task=task) as (fd, source_metadata):
                info = os.fstat(fd)
                os.lseek(fd, max(0, info.st_size - MAX_LOG_BYTES), os.SEEK_SET)
                raw = os.read(fd, MAX_LOG_BYTES)
                final = os.fstat(fd)
            if not raw:
                continue
            text = raw.decode("utf-8", "replace")
            data = text.encode("utf-8")
            text_clipped = len(data) > MAX_LOG_BYTES
            if text_clipped:
                text = data[-MAX_LOG_BYTES:].decode("utf-8", "ignore")
                data = text.encode("utf-8")
            token = hashlib.sha256(source.encode("utf-8")).hexdigest()[:32]
            path = os.path.join(candidate_root, "retained_cli_" + token + ".log")
            meta_path = path[:-4] + ".meta"
            for destination in (path, meta_path):
                if os.path.lexists(destination):
                    existing = os.lstat(destination)
                    if not stat.S_ISREG(existing.st_mode) or existing.st_nlink != 1:
                        raise ValueError("retained destination is unsafe")
            entry = {"schema_version": RETAINED_SCHEMA, "candidate_id": candidate_id, "task_id": task_id,
                "test": task.get("test"), "corner": task.get("corner"), "point_id": task.get("point_id"),
                "execution_simulator": simulator, "kind": kind, "source_path": source,
                "path": path, "file_name": os.path.basename(source), "original_bytes": info.st_size,
                "retained_bytes": len(data), "truncated": info.st_size > len(raw) or text_clipped,
                "snapshot": True, "retained_at": utc_timestamp(), "sha256": hashlib.sha256(data).hexdigest(),
                "completion_authority": False, "termination_authority": False}
            entry.update(resolved_source_path=source_metadata['resolved_source_path'],
                leaf_link=source_metadata['leaf_link'],
                source_device=source_metadata['source_device'], source_inode=source_metadata['source_inode'],
                target_device=source_metadata['target_device'], target_inode=source_metadata['target_inode'],
                changed_during_read=(info.st_size, info.st_mtime) != (final.st_size, final.st_mtime))
            atomic_write_text(path, text)
            atomic_write_json(meta_path, entry)
            result["files"].append(entry)
        except FileNotFoundError:
            continue
        except OwnedSolverLogError as error:
            reason = {'unowned_or_linked_path': 'unowned_source',
                      'linked_or_nonregular_log': 'nonregular_or_linked_source',
                      'stale_log': 'stale_source'}.get(error.reason, error.reason)
            result['warnings'].append(kind + ': ' + reason)
        except (OSError, ValueError, TypeError) as error:
            result["warnings"].append(kind + ": " + type(error).__name__)
    return result
