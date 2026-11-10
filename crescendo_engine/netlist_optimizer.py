"""One optimizer adapter for numeric Maestro and frozen netlist frontends.

This module owns no population algorithm and performs no optional PDK range
validation.  Preparation, CLI simulation and measurement retain their own
identity and process-ownership contracts.
"""
from __future__ import print_function

import copy
import os
import re
import threading
import time
import traceback

from .metrics import evaluate_metrics
from .netlist_execution import NetlistExecutionContext, NetlistExecutionError, execution_policy
from .netlist_providers import prepare_maestro_netlists
from .netlist_run_ownership import reconcile_run_preparations
from .private_netlist import (materialize_candidate, prepare_private_netlist,
                             reconcile_preparation, compact_preparation_artifacts,
                             PrivateNetlistUnsupported)
from .utils import atomic_write_json, ensure_dir, utc_timestamp
from .workspace import verify_sources_unchanged


PROTOCOL = "crescendo.private_netlist_optimizer.v1"


class PrivateNetlistRunError(RuntimeError):
    pass


class PrivateNetlistOptimizer(object):
    def __init__(self, run_root, spec, prepared, selected, config,
                 failure_metrics, cancel_check=None, prior_ownership_roots=None,
                 environment_check=None):
        self.run_root = os.path.abspath(run_root)
        self.root = ensure_dir(os.path.join(self.run_root, "private_netlist"))
        self.spec, self.prepared = spec, prepared
        self.source_mode = ("maestro_netlist" if spec.get("execution_backend") == "maestro_netlist"
                            else "private_netlist")
        self.selected, self.config = selected, config
        self.failure_metrics = failure_metrics
        self.cancel_check = cancel_check
        self.prior_ownership_roots = list(prior_ownership_roots or [])
        self.environment_check = environment_check
        self.policy = execution_policy(prepared, spec, config)
        self.parallel_candidates = min(int(self.policy["max_jobs"]), int(spec["population"]))
        self.lock = threading.RLock()
        self.prepared_event = threading.Event()
        self.preparing = False
        self.prepare_error = None
        self.preparation_failure = None
        self.fatal_error = None
        self.context, self.manifest = None, None
        self.maestro_producer = None
        self.native_fallback = None
        self.native_manifests = {}
        self.native_roots = set()
        self.progress = {"schema_version": PROTOCOL, "sequence": 0, "commands": [], "tasks": []}
        self.candidate_progress = {}
        self.point_commit_callback = None

    def _cancelled(self):
        return bool(self.cancel_check and self.cancel_check())

    def _uses_native_generation(self):
        return self.source_mode == "maestro_netlist" or self.native_fallback is not None

    def _start_native_producer(self):
        from .shared_maestro import SharedMaestroProducer
        self.maestro_producer = SharedMaestroProducer(
            self.run_root, self.prepared, self.selected, self.config, self.spec,
            cancel_check=self._cancelled, progress_callback=self._progress)
        self._progress({"stage": "netlist_provider_ready", "netlist_source": "maestro",
                        "execution_backend": self.source_mode,
                        "native_generation_required": True, "template_fallback": self.native_fallback,
                        "message": ("Numeric derived DPAR has no native expression; generating a fresh native netlist for every candidate"
                                    if self.native_fallback else
                                    "Numeric Maestro frontend uses the shared CLI execution context")})

    def _progress(self, event):
        event = copy.deepcopy(event) if isinstance(event, dict) else {"message": str(event)}
        with self.lock:
            candidate_id = event.get("candidate_id")
            previous_gate = (self.candidate_progress.get(candidate_id) or {}).get("gate_progress")
            gate = event.get("gate_progress") or previous_gate
            if gate is not None:
                gate = copy.deepcopy(gate)
                if (previous_gate and previous_gate.get("scope") == gate.get("scope")
                        and previous_gate["passed"] > gate["passed"]):
                    gate = copy.deepcopy(previous_gate)
                event["gate_progress"] = gate
                self.progress["gate_progress"] = copy.deepcopy(gate)
            self.progress["sequence"] += 1
            self.progress["updated_at"] = utc_timestamp()
            self.progress["latest"] = event
            if event.get("argv"):
                self.progress["commands"].append(event)
                self.progress["commands"] = self.progress["commands"][-24:]
            if event.get("candidate_id"):
                self.progress["tasks"].append(event)
                self.progress["tasks"] = self.progress["tasks"][-24:]
                self._publish_candidate_progress_locked(event)
            atomic_write_json(os.path.join(self.root, "progress.json"), self.progress)
        from .run_log import append_run_event
        append_run_event(self.run_root, event.get("stage") or "private_netlist",
                         event.get("message") or event.get("state") or "Private Netlist progress",
                         details=event, level="ERROR" if event.get("state") == "infrastructure_failed" else "INFO")

    def _candidate_gate(self, candidate_id, passed, current):
        # Only callers immediately after the actual boundary may advance this.
        current = candidate_id + ": " + current
        try:
            self._progress({"stage": "candidate_gate", "candidate_id": candidate_id,
                "message": current, "gate_progress": {"scope": os.path.basename(self.run_root) + ":" + candidate_id,
                    "passed": passed, "total": 5, "current": current}})
        except (IOError, OSError) as error:
            # Display telemetry cannot overturn a verified/durable candidate.
            # Authoritative result/checkpoint writes retain their own failures.
            from .run_log import append_run_event
            try:
                append_run_event(self.run_root, "gate_progress_warning",
                    "Gate display update could not be saved: %s" % error, level="WARN")
            except (IOError, OSError):
                pass

    def _publish_candidate_progress_locked(self, event):
        """Publish display state separately from asynchronously completed rows.

        A live log reader cannot change optimizer status, metrics, stop authority
        or completed point evidence. All task records are kept, rather than the
        last 24 events which can drop still-running jobs from a large sweep.
        """
        candidate_id = event.get("candidate_id")
        if not isinstance(candidate_id, str) or not re.fullmatch(r"g[0-9]{3,}_c[0-9]{4,}", candidate_id):
            return  # Baseline/reference capture events have no optimizer candidate.
        root = os.path.join(self.run_root, "candidates", candidate_id)
        if os.path.realpath(root) != root or not os.path.isdir(root):
            raise ValueError("Shared CLI progress candidate directory is unowned")
        snapshot = self.candidate_progress.setdefault(candidate_id, {
            "schema_version": "crescendo.shared_cli_progress.v1", "candidate_id": candidate_id,
            "stage": "preparing", "execution_backend": self.source_mode,
            "execution_simulator": self.spec.get("simulator", "spectre"), "tasks": []})
        snapshot.update(stage=event.get("stage") or snapshot["stage"],
                        updated_at=utc_timestamp(), sequence=self.progress["sequence"])
        if event.get("message"):
            snapshot["message"] = str(event["message"])[-4096:]
        if event.get("gate_progress") is not None:
            snapshot["gate_progress"] = copy.deepcopy(event["gate_progress"])
        task_id = event.get("task_id")
        task = None
        if task_id:
            task = next((row for row in snapshot["tasks"] if row.get("task_id") == task_id), None)
            if task is None:
                task = {"candidate_id": candidate_id, "task_id": task_id}
                snapshot["tasks"].append(task)
        elif event.get("point_id") is not None:
            matches = [row for row in snapshot["tasks"] if row.get("test") == event.get("test")
                       and row.get("corner") == event.get("corner")
                       and str(row.get("point_id")) == str(event["point_id"])]
            task = matches[0] if len(matches) == 1 else None
        if task is not None:
            for field in ("test", "artifact_test", "corner", "point_id", "execution_simulator",
                          "task_root", "psf_dir", "simulator_log", "spectre_log", "solver_stdout", "solver_stderr",
                          "scheduler_stdout", "scheduler_stderr", "launch_epoch", "cwd", "argv", "command",
                          "job_id", "scheduler_job_id", "scheduler_state", "execution_host", "job_exit_code",
                          "solver_command_plan", "submission_command_plan", "task_timeouts", "log_paths", "retained_logs"):
                if field in event:
                    task[field] = copy.deepcopy(event[field])
            task["state"] = event.get("scheduler_state") or event.get("state") or event.get("stage") or task.get("state")
            if event.get("scheduler_job_id"):
                task["job_id"] = event["scheduler_job_id"]
            if isinstance(event.get("timeout_state"), dict):
                task["task_timeouts"] = copy.deepcopy(event["timeout_state"])
            observation = event.get("scheduler_observation")
            if isinstance(observation, dict):
                task["job_exit_code"] = observation.get("job_exit_code")
                task["execution_host"] = (observation.get("metadata") or {}).get("exec_node")
        snapshot["task_count"] = len(snapshot["tasks"])
        atomic_write_json(os.path.join(root, "cli_progress.json"), snapshot)

    def _prepare_once(self):
        with self.lock:
            first = not self.preparing
            if first:
                self.preparing = True
        if first:
            try:
                # Reconcile prior cluster ownership before creating any new job.
                self.context = NetlistExecutionContext(
                    self.config, self.prepared, self.spec,
                    cancel_check=self._cancelled, progress_callback=self._progress,
                    ownership_root=os.path.join(self.root, "execution_ownership"),
                    owned_run_root=self.run_root,
                    prior_ownership_roots=self.prior_ownership_roots,
                    point_commit_callback=self.point_commit_callback,
                )
                for boundary in self.prior_ownership_roots + [{"owned_run_root": self.run_root}]:
                    previous = reconcile_run_preparations(boundary["owned_run_root"], request_cancel=True, solver_cleanup_proven=True)
                    if not previous.get("cleanup_proven"):
                        raise PrivateNetlistRunError(
                            "Previous native netlist preparation is not proven stopped; retained evidence blocks reuse: %s"
                            % previous.get("root")
                        )
                if self.source_mode == "maestro_netlist":
                    self._start_native_producer()
                    return None
                template_root = os.path.join(self.root, "templates")
                try:
                    self.manifest = prepare_private_netlist(
                        self.prepared, self.selected, self.config, template_root,
                        cancel_check=self._cancelled, progress_callback=self._progress, spec=self.spec,
                    )
                except PrivateNetlistUnsupported as error:
                    detail = getattr(error, "detail", {})
                    qualified_reason = (
                        detail.get("failure_stage") == "native_numeric_derived_parameters" and bool(detail.get("numeric_derived_parameters"))
                        or detail.get("failure_stage") == "native_selected_parameter_binding" and bool(detail.get("selected_parameter_bindings")))
                    if (self._cancelled() or not qualified_reason
                            or detail.get("native_generation_required") is not True
                            or detail.get("template_qualified") is not False
                            or detail.get("template_root") != template_root
                            or detail.get("cleanup_pending") or detail.get("termination_failed")
                            or not (detail.get("source_guard") or {}).get("ok")):
                        raise
                    proof = reconcile_preparation(template_root, request_cancel=True)
                    if not proof.get("cleanup_proven"):
                        failure = PrivateNetlistRunError("Native template capture cleanup is unproven; native fallback blocked")
                        failure.detail = dict(copy.deepcopy(detail), failure_stage="native_template_capture_cleanup",
                                              preparation_cleanup=proof, cleanup_pending=True, termination_failed=True)
                        failure.termination_failed = True
                        raise failure
                    if self._cancelled():
                        raise PrivateNetlistRunError("Native fallback cancelled before generation")
                    self.native_fallback = dict(copy.deepcopy(detail), reason=str(error), preparation_cleanup=proof,
                                                requested_backend=self.source_mode, actual_source_mode="maestro_netlist")
                    atomic_write_json(os.path.join(self.root, "native_generation_required.json"), self.native_fallback)
                    self._start_native_producer()
                    return None
                if not (self.manifest.get("source_guard") or {}).get("ok"):
                    raise PrivateNetlistRunError("Source identity was not verified during netlist preparation")
                from .diagnosis_catalog import diagnostic_run
                if diagnostic_run(self.spec):
                    # Preparation has already reconciled native processes and
                    # owned views. Keep only its filesystem payload for this
                    # explicit diagnostic; Stop and close still run normally.
                    preparation_proof = reconcile_preparation(self.manifest["root"], request_cancel=False)
                    if not preparation_proof.get("cleanup_proven"):
                        raise PrivateNetlistRunError("Diagnostic capture termination/view cleanup is unproven; evidence retained")
                    artifact_cleanup = {"ok": True, "removed": [], "preparation_cleanup": preparation_proof,
                        "retained": [{"path": self.manifest["root"],
                                      "reason": "environment_test_capture_manual_inspection"}],
                        "retention_policy": "diagnostic_capture_retained"}
                else:
                    artifact_cleanup = compact_preparation_artifacts(self.manifest)
                self._progress({"stage": "native_preparation_artifacts",
                                "message": "Native preparation payload cleanup checked",
                                "artifact_cleanup": artifact_cleanup})
                self._progress({"stage": "template_ready", "message": "Private native template qualified",
                                "template_sha256": self.manifest.get("manifest_sha256"),
                                "task_count": len(self.manifest.get("tasks") or [])})
            except Exception as error:
                self.prepare_error = error
                self.fatal_error = "%s: %s" % (error.__class__.__name__, error)
                path = os.path.join(self.root, "preparation_failure.json")
                self.preparation_failure = {
                    "schema_version": "crescendo.shared_template_failure.v1",
                    "failure_stage": getattr(error, "detail", {}).get("failure_stage") or "native_template_preparation",
                    "error_type": error.__class__.__name__, "message": str(error),
                    "diagnostic_path": path, "execution_started": False,
                    "shared_preparation_failed": True, "preparation_attempt_count": 1,
                    "affected_candidate_count": int(self.spec["population"]),
                }
                atomic_write_json(path, dict(self.preparation_failure,
                                            detail=getattr(error, "detail", {}),
                                            traceback=traceback.format_exc()))
                self._progress({"stage": "shared_template_preparation_failed", "state": "infrastructure_failed",
                                "message": self.fatal_error, "no_candidate_execution": True,
                                "shared_preparation_failure": self.preparation_failure,
                                "native_error_detail": getattr(error, "detail", {})})
            finally:
                self.prepared_event.set()
        else:
            while not self.prepared_event.wait(0.1):
                if self._cancelled():
                    raise PrivateNetlistRunError("Private netlist preparation cancelled")
        if self.prepare_error is not None:
            raise self.prepare_error
        if self._cancelled():
            raise PrivateNetlistRunError("Private netlist preparation cancelled")
        if self.fatal_error:
            raise PrivateNetlistRunError(self.fatal_error)
        return self.manifest

    def verify_sources(self):
        manifests = list(self.native_manifests.values()) if self._uses_native_generation() else [self.manifest]
        manifests = [manifest for manifest in manifests if manifest is not None]
        if not manifests:
            return {"ok": True, "state": "no_native_template_created"}
        if self.environment_check is not None:
            with self.lock:
                # A distinct boundary forces the existing full identity check
                # after the generation's last simulator, before its terminal
                # checkpoint; subsequent close still checks path metadata.
                external = self.environment_check(-1)
                if not external.get("ok"):
                    self.fatal_error = external.get("error") or "External EDA environment changed"
                    if self.context is not None:
                        self.context.cancel()
                    raise PrivateNetlistRunError(self.fatal_error)
        guards = [verify_sources_unchanged(manifest["workspace"]) for manifest in manifests]
        guard = guards[0] if len(guards) == 1 else {"ok": all(row.get("ok") for row in guards),
                                                   "preparations": guards}
        if not guard.get("ok"):
            with self.lock:
                self.fatal_error = "Source design identity changed during private netlist optimization"
            if self.context is not None:
                self.context.cancel()
            raise PrivateNetlistRunError(self.fatal_error)
        atomic_write_json(os.path.join(self.root, "source_guard.json"), guard)
        return guard

    def evaluate(self, index, generation, values):
        candidate_id = "g%03d_c%04d" % (generation, index)
        root = ensure_dir(os.path.join(self.run_root, "candidates", candidate_id))
        started = time.monotonic()
        row = {"candidate_id": candidate_id, "generation": generation, "index": index,
               "values": dict(values), "status": "running", "started_at": utc_timestamp(),
               "execution_backend": self.source_mode}
        timing = {"schema_version": "crescendo.candidate_timing.v1", "candidate_id": candidate_id,
                  "started_epoch": time.time(), "phases": {}}
        atomic_write_json(os.path.join(root, "candidate.json"), row)
        atomic_write_json(os.path.join(root, "timing.json"), timing)
        diagnostic = {"schema_version": PROTOCOL, "candidate_id": candidate_id}
        execution = None
        materialized = None
        try:
            self._candidate_gate(candidate_id, 0, "Preparing native execution")
            manifest = self._prepare_once()
            if self.environment_check is not None:
                with self.lock:
                    guard = self.environment_check(generation)
                    if not guard.get("ok"):
                        raise PrivateNetlistRunError(guard.get("error") or "External EDA environment changed")
            self._candidate_gate(candidate_id, 1, "Execution context verified; generating netlist")
            if self._uses_native_generation():
                preparation_root = os.path.join(root, "simulation")
                with self.lock:
                    self.native_roots.add(preparation_root)
                materialized = self.maestro_producer.generate(values, preparation_root, candidate_id)
                if self.native_fallback is not None:
                    from .private_netlist import _manifest_digest, validate_manifest
                    metadata = {"requested_backend": self.source_mode, "native_generation_required": True,
                                "template_qualified": False, "template_fallback": copy.deepcopy(self.native_fallback)}
                    materialized["netlist_generation"].update(metadata)
                    for task in materialized["tasks"]:
                        task.setdefault("netlist_generation", {}).update(copy.deepcopy(metadata),
                            source_mode=materialized["netlist_generation"]["source_mode"])
                    materialized["manifest_sha256"] = _manifest_digest(materialized)
                    validate_manifest(materialized)
                    for name in ("private_netlist.json", "private_netlist_candidate.json"):
                        atomic_write_json(os.path.join(materialized["root"], name), materialized)
                manifest = materialized
                with self.lock:
                    self.native_manifests[candidate_id] = materialized
                    if self.manifest is None:
                        self.manifest = materialized
                row["parameter_application"] = copy.deepcopy(materialized["netlist_generation"]["parameter_application"])
            else:
                materialized = materialize_candidate(
                    manifest, values, os.path.join(root, "simulation"), candidate_id,
                )
            row["source_guard"] = {"ok": True, "state": "verified_numeric_maestro" if self._uses_native_generation() else "verified_frozen_template",
                                   "fingerprint": (manifest.get("source_identity") or {}).get("fingerprint"),
                                   "verification_policy": "native_preparation_and_each_generation_and_final"}
            row["netlist_generation"] = copy.deepcopy(materialized.get("netlist_generation") or {})
            from .simulator_profiles import require_frozen_model_profile
            with self.lock:
                require_frozen_model_profile(self.spec, materialized)
            self._candidate_gate(candidate_id, 2, "Fresh netlist and model profile verified; solving")
            execution = self.context.evaluate(materialized, cancel_check=self._cancelled)
            completion = copy.deepcopy(execution["completion"])
            completion.update(ok=True,
                              tests=sorted(set(str(task["test"]) for task in materialized["tasks"])),
                              corners=sorted(set(str(task["corner"]) for task in materialized["tasks"])),
                              points=[{"test": task["test"], "corner": task["corner"],
                                       "point_id": task["point_id"]} for task in materialized["tasks"]],
                              parameters=dict(materialized.get("applied_values") or values), optimizer_protocol=PROTOCOL)
            completion["template_sha256"] = manifest.get("manifest_sha256")
            completion["netlist_generation"] = copy.deepcopy(materialized.get("netlist_generation") or {})
            row["execution_pipeline"] = copy.deepcopy(completion.get("execution_pipeline") or {})
            self._candidate_gate(candidate_id, 3, "Solver and point readback complete; qualifying metrics")
            if self._uses_native_generation():
                cleanup = reconcile_preparation(materialized["root"], request_cancel=True)
                if not cleanup.get("cleanup_proven"):
                    raise NetlistExecutionError("Numeric Maestro capture cleanup is unproven", {"termination_failed": True,
                                               "preparation_cleanup": cleanup})
                row["preparation_cleanup"] = cleanup
            row["completion_evidence"] = completion
            row["rdb_path"] = os.path.join(root, "simulation", "cli_results.json")
            row["native_job_state"] = execution["native_job_state"]
            row["metrics"] = evaluate_metrics(
                execution["parsed"], self.spec["objectives"], self.spec.get("constraints", []),
            )
            row["status"] = "completed"
            self._candidate_gate(candidate_id, 4, "Metrics and capture cleanup verified; saving candidate")
            runs = execution.get("task_results") or []
            timing["phases"]["simulator"] = {
                "simulator": self.spec.get("simulator", "spectre"),
                "run_count": len(runs), "completed_run_count": len(runs),
                "total_seconds": sum(float(task.get("elapsed_seconds", 0)) for task in runs),
                "runs": [{"test": task["task"]["test"], "corner": task["task"]["corner"],
                          "elapsed_seconds": task.get("elapsed_seconds")} for task in runs],
            }
            diagnostic["completion"] = completion
        except Exception as error:
            infrastructure = not isinstance(error, NetlistExecutionError) or error.infrastructure
            stopped = self._cancelled()
            row.update(status="cancelled" if stopped else "infrastructure_failed" if infrastructure else "failed",
                       failure_class="user_stop" if stopped else "private_netlist_infrastructure" if infrastructure else "design_or_simulation",
                       failure_stage=(getattr(error, "detail", {}).get("failure_stage") or
                                      ("netlist_preparation" if materialized is None else "solver_submission_or_measurement")),
                       spectre_state="not_started" if materialized is None else "see_execution_diagnostics",
                       error="%s: %s" % (error.__class__.__name__, error))
            shared_failure = self.prepare_error is error and self.preparation_failure is not None
            if shared_failure:
                row.update(execution_started=False, shared_preparation_failed=True,
                           shared_preparation_failure=dict(self.preparation_failure))
                diagnostic.update(error=row["error"], detail={
                    "shared_preparation_failure_path": self.preparation_failure["diagnostic_path"],
                    "execution_started": False,
                })
            else:
                diagnostic.update(error=row["error"], detail=getattr(error, "detail", {}),
                                  traceback=traceback.format_exc())
            self._progress({"stage": row["failure_stage"], "candidate_id": candidate_id,
                            "message": "Candidate not executed: shared native template preparation failed" if shared_failure else row["error"],
                            "state": row["status"], "no_candidate_execution": shared_failure,
                            "shared_preparation_failure_path": self.preparation_failure["diagnostic_path"] if shared_failure else None})
            if infrastructure and not stopped:
                with self.lock:
                    self.fatal_error = row["error"]
                if self.context is not None:
                    self.context.cancel()
            elif not stopped:
                row["metrics"] = self.failure_metrics(self.spec, error)
            if getattr(error, "termination_failed", False) or (
                    self.context is not None and self.context.unsafe.is_set()):
                row["cleanup_pending"] = True
            if self.manifest is not None and "source_guard" not in row:
                row["source_guard"] = {"ok": True, "state": "verified_numeric_maestro" if self._uses_native_generation() else "verified_frozen_template"}
        finally:
            row["finished_at"] = utc_timestamp()
            with self.lock:
                live = self.candidate_progress.get(candidate_id)
                if live is not None:
                    row["execution_progress"] = copy.deepcopy(live)
                    row["execution_simulator"] = live["execution_simulator"]
            timing.update(status=row["status"], finished_epoch=time.time(),
                          elapsed_seconds=max(0.0, time.monotonic() - started))
            timing["phases"]["preparation"] = {
                "shared_run_template": not self._uses_native_generation(),
                "netlist_source": "maestro" if self._uses_native_generation() else "template",
                "native_generation_required": self._uses_native_generation(),
                "elapsed_seconds": (materialized or self.manifest or {}).get("preparation", {}).get("elapsed_seconds"),
            }
            atomic_write_json(os.path.join(root, "execution_diagnostics.json"), diagnostic)
            if row.get("completion_evidence"):
                atomic_write_json(os.path.join(root, "completion_evidence.json"), row["completion_evidence"])
            atomic_write_json(os.path.join(root, "timing.json"), timing)
            atomic_write_json(os.path.join(root, "candidate.json"), row)
        if row["status"] == "completed":
            self._candidate_gate(candidate_id, 5, "Qualified candidate saved")
            with self.lock:
                row["execution_progress"] = copy.deepcopy(self.candidate_progress[candidate_id])
        return row

    def cancel(self):
        if self.maestro_producer is not None:
            self.maestro_producer.cancel()
        if self.context is not None:
            self.context.cancel()

    def close(self, cancel=False):
        result = (self.context.close(cancel=cancel) if self.context is not None else
                  {"cleanup_proven": True, "retained": [], "active_task_count": 0, "unsafe": False})
        result["optimizer_protocol"] = PROTOCOL
        if self.maestro_producer is not None:
            generation = self.maestro_producer.close(cancel=cancel)
            result["shared_generation_close"] = generation
            result["cleanup_proven"] = bool(result.get("cleanup_proven") and generation.get("cleanup_proven"))
        atomic_write_json(os.path.join(self.root, "shutdown.json"), result)
        guard_error = None
        try:
            result["source_guard"] = self.verify_sources()
        except Exception as error:
            # A source/environment mismatch must still fail the run, while
            # exact owned views can be cleaned after native termination proof.
            # Do not leave a completed capture's temporary views solely because
            # an unrelated source changed after it exited.
            guard_error = error
            result["source_guard"] = {"ok": False, "error": "%s: %s" % (error.__class__.__name__, error)}
        preparation = (reconcile_run_preparations(self.run_root, request_cancel=True,
                                                   solver_cleanup_proven=(self.context is not None and
                                                                          result.get("cleanup_proven") is True))
                       if result.get("cleanup_proven") else
                       {"root": self.run_root, "cleanup_proven": False,
                        "state": "retained_until_solver_termination_proven"})
        result["preparation_cleanup"] = preparation
        result["cleanup_proven"] = bool(result.get("cleanup_proven") and preparation.get("cleanup_proven"))
        atomic_write_json(os.path.join(self.root, "shutdown.json"), result)
        if guard_error is not None:
            raise guard_error
        return result
