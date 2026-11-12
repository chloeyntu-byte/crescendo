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

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .candidate_timing import elapsed, measure, save_timing, timed_lock, timestamp
from .metrics import evaluate_metrics
from .netlist_execution import NetlistExecutionContext, NetlistExecutionError, execution_policy
from .netlist_providers import prepare_maestro_netlists
from .netlist_run_ownership import reconcile_run_preparations
from .private_netlist import (materialize_candidate, prepare_private_netlist,
                             reconcile_preparation, compact_preparation_artifacts,
                             PrivateNetlistUnsupported)
from .utils import atomic_write_json, ensure_dir, utc_timestamp
from .workspace import verify_sources_unchanged


PROTOCOL = _contract_text("crescendo.private_netlist_optimizer.v1")


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
        self.primary_failure = None
        self.context, self.manifest = None, None
        self.maestro_producer = None
        self.native_fallback = None
        self.native_manifests = {}
        self.shared_history_references = {}
        self.native_roots = set()
        self.progress = {"schema_version": PROTOCOL, "sequence": 0, "commands": [], "tasks": []}
        self.candidate_progress = {}
        self.point_commit_callback = None
        from .execution_settings import normalize_execution_settings
        settings = dict(config.get("runtime") or {})
        settings.update((key, spec[key]) for key in ("progress_flush_interval_seconds",) if key in spec)
        self.progress_flush_interval = normalize_execution_settings(settings)["progress_flush_interval_seconds"]
        self.progress_flush_times = {}

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
            self._flush_progress_file(os.path.join(self.root, _contract_text("progress.json")), self.progress, event)
        from .run_log import append_run_event
        append_run_event(self.run_root, event.get("stage") or "private_netlist",
                         event.get("message") or event.get("state") or "Template Netlist progress",
                         details=event, level="ERROR" if event.get("state") == "infrastructure_failed" else "INFO")

    def _flush_progress_file(self, path, value, event, force=False):
        stage = event.get("stage", "")
        urgent = (force or event.get("state") in ("failed", "infrastructure_failed", "cancelled", "completed")
                  or stage in ("task_completed", "solve_completed", "solver_terminal", "measurement_terminal",
                               "task_timeout", "point_raw_cleanup")
                  or (event.get("gate_progress") or {}).get("passed") == 5
                  or "failed" in stage or "error" in stage)
        now = time.monotonic()
        last = self.progress_flush_times.get(path)
        if urgent or last is None or now - last >= self.progress_flush_interval:
            atomic_write_json(path, value)
            self.progress_flush_times[path] = now
            return True
        return False

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
            "schema_version": _contract_text("crescendo.shared_cli_progress.v1"), "candidate_id": candidate_id,
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
        self._flush_progress_file(os.path.join(root, _contract_text("cli_progress.json")), snapshot, event)

    def _prepare_once(self, phases=None):
        phases = phases if phases is not None else {}
        with timed_lock(self.lock, phases, "shared_setup_lock_wait"):
            first = not self.preparing
            if first:
                self.preparing = True
        if first:
            setup_started = timestamp()
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
                    with measure(phases, "template_capture", scope="shared_run"):
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
                    atomic_write_json(os.path.join(self.root, _contract_text("native_generation_required.json")), self.native_fallback)
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
                self._progress({"stage": "template_ready", "message": "Working native template qualified",
                                "template_sha256": self.manifest.get("manifest_sha256"),
                                "task_count": len(self.manifest.get("tasks") or [])})
            except Exception as error:
                self.prepare_error = error
                self.fatal_error = "%s: %s" % (error.__class__.__name__, error)
                path = os.path.join(self.root, _contract_text("preparation_failure.json"))
                self.preparation_failure = {
                    "schema_version": _contract_text("crescendo.shared_template_failure.v1"),
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
                phases["shared_setup"] = {"elapsed_seconds": elapsed(setup_started, timestamp()),
                                          "scope": "shared_run"}
        else:
            with measure(phases, "shared_setup_wait"):
                while not self.prepared_event.wait(0.1):
                    if self._cancelled():
                        raise PrivateNetlistRunError("Template Netlist preparation cancelled")
        if self.prepare_error is not None:
            raise self.prepare_error
        if self._cancelled():
            raise PrivateNetlistRunError("Template Netlist preparation cancelled")
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
                self.fatal_error = "Source design identity changed during Template Netlist optimization"
            if self.context is not None:
                self.context.cancel()
            raise PrivateNetlistRunError(self.fatal_error)
        atomic_write_json(os.path.join(self.root, _contract_text("source_guard.json")), guard)
        return guard

    def _apply_native_fallback(self, materialized):
        if self.native_fallback is None:
            return
        from .private_netlist import _manifest_digest, validate_manifest
        metadata = {"requested_backend": self.source_mode, "native_generation_required": True,
                    "template_qualified": False, "template_fallback": copy.deepcopy(self.native_fallback)}
        materialized["netlist_generation"].update(metadata)
        for task in materialized["tasks"]:
            task.setdefault("netlist_generation", {}).update(copy.deepcopy(metadata),
                source_mode=materialized["netlist_generation"]["source_mode"])
        materialized["manifest_sha256"] = _manifest_digest(materialized)
        validate_manifest(materialized)
        for name in (_contract_text("private_netlist.json"), _contract_text("private_netlist_candidate.json")):
            atomic_write_json(os.path.join(materialized["root"], name), materialized)

    def prepare_generation(self, generation, pending):
        """Called once by the generation barrier, before candidate futures."""
        from concurrent.futures import Future
        from .generation_batch import build_generation_plan, BatchUnsupported, HistoryReferences
        from .private_netlist import validate_manifest
        from .simulator_profiles import require_frozen_model_profile
        self._prepare_once()
        if not self._uses_native_generation():
            return {}
        if self.environment_check is not None:
            guard = self.environment_check(generation)
            if not guard.get('ok'):
                raise PrivateNetlistRunError(guard.get('error') or 'External EDA environment changed')
        plan = build_generation_plan(generation, pending, self.selected,
            {'source_identity': self.prepared.get('source_identity'), 'backend': self.source_mode})
        root = os.path.join(self.run_root, 'candidates', 'batch_' + plan['batch_epoch'])
        self.native_roots.add(root)
        for candidate in plan['candidates']:
            ensure_dir(os.path.join(self.run_root, 'candidates', candidate['candidate_id']))
        try:
            manifests = self.maestro_producer.generate_batch(plan)
        except BatchUnsupported as error:
            if plan.get('submitted') or self._cancelled():
                raise
            fallback = {'schema_version': plan['schema_version'], 'generation': generation,
                'batch_epoch': plan['batch_epoch'], 'reason': str(error), 'submitted': False}
            atomic_write_json(os.path.join(self.run_root, 'batch_fallback_' + plan['batch_epoch'] + '.json'), fallback)
            self._progress(dict(fallback, stage='native_batch_fallback', message=str(error)))
            result = {}
            # Even the downgraded generation freezes every input before CLI.
            for candidate in plan['candidates']:
                cid = candidate['candidate_id']
                candidate_root = os.path.join(self.run_root, 'candidates', cid, 'simulation')
                self.native_roots.add(candidate_root)
                manifest = self.maestro_producer.generate(candidate['values'], candidate_root, cid)
                result[cid] = {'manifest': manifest, 'future': Future()}
        else:
            refs = HistoryReferences(os.path.join(root, _contract_text('history_references.json')), plan)
            self.shared_history_references[plan['batch_epoch']] = refs
            result, futures = {}, {row['row_id']: Future() for row in plan['rows']}
            for candidate in plan['candidates']:
                cid = candidate['candidate_id']
                result[cid] = {'manifest': manifests[candidate['row_id']],
                    'future': futures[candidate['row_id']], 'references': refs, 'plan': plan,
                    'batch_reference': {'schema_version': plan['schema_version'],
                        'batch_epoch': plan['batch_epoch'], 'root': root,
                        'manifest': os.path.join(root, _contract_text('batch_manifest.json')),
                        'timing': os.path.join(root, _contract_text('batch_timing.json')),
                        'physical_ids': plan['candidate_to_physical'][cid]}}
        # All closures and selected execution models pass before publishing.
        for item in result.values():
            self._apply_native_fallback(item['manifest'])
            validate_manifest(item['manifest'])
            require_frozen_model_profile(self.spec, item['manifest'])
            self.context.prepare_candidate(item['manifest'])
        self.native_manifests.update({cid: item['manifest'] for cid, item in result.items()})
        self.generation_inputs = result
        return result

    def commit_candidate(self, row):
        reference = row.get('batch_reference') or {}
        refs = self.shared_history_references.get(reference.get('batch_epoch'))
        if refs is not None and row.get('status') in ('completed', 'failed', 'infrastructure_failed'):
            refs.commit(row['candidate_id'])

    def shared_cleanup_ready(self, row):
        reference = row.get('batch_reference')
        if not reference:
            return True
        refs = self.shared_history_references.get(reference.get('batch_epoch'))
        if refs is not None:
            return refs.data['remaining'] == 0
        from .generation_batch import require_history_released
        try:
            require_history_released(reference, row['candidate_id'])
            return True
        except (OSError, ValueError):
            return False

    def evaluate(self, index, generation, values, pre_generated=None):
        if pre_generated is None:
            return self._evaluate_candidate(index, generation, values)
        future = pre_generated['future']
        manifest = pre_generated['manifest']
        with self.lock:
            owner = not future.running() and not future.done()
            if owner:
                plan = pre_generated.get('plan')
                if plan is not None and not plan['submitted']:
                    atomic_write_json(pre_generated['batch_reference']['manifest'], dict(plan, submitted=True))
                    plan['submitted'] = True
                future.set_running_or_notify_cancel()
        if owner:
            try:
                row = self._evaluate_candidate(index, generation, values, manifest)
                future.set_result(copy.deepcopy(row))
            except BaseException as error:
                future.set_exception(error)
                raise
        else:
            row = copy.deepcopy(future.result())
        cid = 'g%03d_c%04d' % (generation, index)
        row.update(candidate_id=cid, index=index, generation=generation, values=dict(values))
        if pre_generated.get('batch_reference'):
            row['batch_reference'] = copy.deepcopy(pre_generated['batch_reference'])
            row['physical_execution_candidate'] = manifest['candidate_id']
            if cid != manifest['candidate_id']:
                row['shared_with'] = manifest['candidate_id']
            row['timing'] = {'schema_version': _contract_text('crescendo.candidate_timing.v1'),
                'candidate_id': cid, 'phases': {}, 'shared_batch': row['batch_reference'],
                'physical_execution_candidate': manifest['candidate_id']}
        root = ensure_dir(os.path.join(self.run_root, 'candidates', cid))
        if row.get('completion_evidence'):
            row['completion_evidence']['candidate_id'] = cid
            for key in ('batch_reference', 'physical_execution_candidate', 'shared_with'):
                if key in row:
                    row['completion_evidence'][key] = copy.deepcopy(row[key])
            atomic_write_json(os.path.join(root, _contract_text('completion_evidence.json')), row['completion_evidence'])
        atomic_write_json(os.path.join(root, _contract_text('candidate.json')), row)
        atomic_write_json(os.path.join(root, _contract_text('timing.json')), row.get('timing') or {})
        return row

    def _evaluate_candidate(self, index, generation, values, pre_generated=None):
        candidate_id = "g%03d_c%04d" % (generation, index)
        root = ensure_dir(os.path.join(self.run_root, "candidates", candidate_id))
        started = timestamp()
        row = {"candidate_id": candidate_id, "generation": generation, "index": index,
               "values": dict(values), "status": "running", "started_at": utc_timestamp(),
               "execution_backend": self.source_mode}
        phases = {}
        timing = {"schema_version": _contract_text("crescendo.candidate_timing.v1"), "candidate_id": candidate_id,
                  "started_epoch": timestamp(time.time), "phases": phases}
        timing_path = os.path.join(root, _contract_text("timing.json"))
        atomic_write_json(os.path.join(root, _contract_text("candidate.json")), row)
        save_timing(timing_path, timing)
        diagnostic = {"schema_version": PROTOCOL, "candidate_id": candidate_id}
        execution = None
        materialized = None
        try:
            self._candidate_gate(candidate_id, 0, "Preparing native execution")
            manifest = self._prepare_once(phases)
            with measure(phases, "build"):
                if self.environment_check is not None:
                    with timed_lock(self.lock, phases, "environment_lock_wait"):
                        guard = self.environment_check(generation)
                        if not guard.get("ok"):
                            raise PrivateNetlistRunError(guard.get("error") or "External EDA environment changed")
                self._candidate_gate(candidate_id, 1, "Execution context verified; generating netlist")
                if self._uses_native_generation():
                    preparation_root = os.path.join(root, "simulation")
                    with self.lock:
                        self.native_roots.add(preparation_root)
                    with measure(phases, "native_generation"):
                        materialized = pre_generated
                        if materialized is None:
                            materialized = self.maestro_producer.generate(values, preparation_root, candidate_id,
                                                                          phase_timings=phases)
                    self._apply_native_fallback(materialized)
                    manifest = materialized
                    with self.lock:
                        self.native_manifests[candidate_id] = materialized
                        if self.manifest is None:
                            self.manifest = materialized
                    row["parameter_application"] = copy.deepcopy(materialized["netlist_generation"]["parameter_application"])
                else:
                    with measure(phases, "template_materialization"):
                        materialized = materialize_candidate(
                            manifest, values, os.path.join(root, "simulation"), candidate_id,
                        )
                row["source_guard"] = {"ok": True, "state": "verified_numeric_maestro" if self._uses_native_generation() else "verified_frozen_template",
                                       "fingerprint": (manifest.get("source_identity") or {}).get("fingerprint"),
                                       "verification_policy": "native_preparation_and_each_generation_and_final"}
                row["netlist_generation"] = copy.deepcopy(materialized.get("netlist_generation") or {})
                from .simulator_profiles import require_frozen_model_profile
                with timed_lock(self.lock, phases, "model_profile_lock_wait"):
                    require_frozen_model_profile(self.spec, materialized)
            self._candidate_gate(candidate_id, 2, "Fresh netlist and model profile verified; solving")
            with measure(phases, "execution"):
                execution = self.context.evaluate(materialized, cancel_check=self._cancelled, timing=timing)
            with measure(phases, "collection"):
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
                row["rdb_path"] = os.path.join(materialized.get("root") or os.path.join(root, "simulation"), _contract_text("cli_results.json"))
                row["native_job_state"] = execution["native_job_state"]
                row["metrics"] = evaluate_metrics(
                    execution["parsed"], self.spec["objectives"], self.spec.get("constraints", []),
                )
            row["status"] = "completed"
            self._candidate_gate(candidate_id, 4, "Metrics and capture cleanup verified; saving candidate")
            runs = execution.get("task_results") or []
            timing["phases"]["simulator"] = {
                "simulator": self.spec.get("simulator", "spectre"),
                "scope": "task_execution_with_submission_and_qualification",
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
            if infrastructure and not stopped:
                with self.lock:
                    cancelled_error = bool(getattr(error, "detail", {}).get("cancelled") or
                                           "cancelled" in str(error).lower())
                    prior_cancel = bool(self.primary_failure and (
                        self.primary_failure.get("cancelled") or
                        "cancelled" in self.primary_failure.get("error", "").lower()))
                    if self.primary_failure is None or (prior_cancel and not cancelled_error):
                        # unsafe can become visible to a peer before the
                        # originating worker finishes its cleanup/raises.
                        self.primary_failure = dict(
                            (key, row[key]) for key in ("candidate_id", "error", "failure_class", "failure_stage"))
                        self.primary_failure["error_type"] = error.__class__.__name__
                        if cancelled_error:
                            self.primary_failure["cancelled"] = True
                        self.fatal_error = row["error"]
                    else:
                        # A failed point cancels its peers. Their later queue/
                        # capture errors cannot replace the originating failure.
                        row["secondary_failure"] = True
                        if cancelled_error:
                            row["failure_stage"] = "peer_cancellation"
                    row["primary_failure"] = copy.deepcopy(self.primary_failure)
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
            timing.update(status=row["status"], finished_epoch=timestamp(time.time),
                          elapsed_seconds=elapsed(started, timestamp()))
            preparation = (materialized if self._uses_native_generation() else self.manifest) or {}
            timing["phases"]["preparation"] = {
                "shared_run_template": not self._uses_native_generation(),
                "netlist_source": "maestro" if self._uses_native_generation() else "template",
                "native_generation_required": self._uses_native_generation(),
                "elapsed_seconds": preparation.get("preparation", {}).get("elapsed_seconds"),
            }
            atomic_write_json(os.path.join(root, _contract_text("execution_diagnostics.json")), diagnostic)
            if row.get("completion_evidence"):
                atomic_write_json(os.path.join(root, _contract_text("completion_evidence.json")), row["completion_evidence"])
            atomic_write_json(os.path.join(root, _contract_text("candidate.json")), row)
            row["timing"] = timing
            save_timing(timing_path, timing)
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
        with self.lock:
            self._flush_progress_file(os.path.join(self.root, _contract_text("progress.json")), self.progress, {}, force=True)
            for candidate_id, snapshot in self.candidate_progress.items():
                candidate_root = os.path.join(self.run_root, "candidates", candidate_id)
                if (not os.path.isdir(candidate_root) or
                        os.path.isfile(os.path.join(candidate_root, _contract_text("artifact_summary.json")))):
                    continue
                self._flush_progress_file(os.path.join(self.run_root, "candidates", candidate_id, _contract_text("cli_progress.json")),
                                          snapshot, {}, force=True)
        result["optimizer_protocol"] = PROTOCOL
        if self.maestro_producer is not None:
            generation = self.maestro_producer.close(cancel=cancel)
            result["shared_generation_close"] = generation
            result["cleanup_proven"] = bool(result.get("cleanup_proven") and generation.get("cleanup_proven"))
        atomic_write_json(os.path.join(self.root, _contract_text("shutdown.json")), result)
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
        atomic_write_json(os.path.join(self.root, _contract_text("shutdown.json")), result)
        if guard_error is not None:
            raise guard_error
        return result
