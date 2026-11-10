"""Startup phase evidence and deadlines; no Cadence or cluster jobs are launched."""
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.persistent_controller import (
    SCHEMA_VERSION, PersistentControllerProxy, _publish_controller_startup_phase,
)
from crescendo_engine.runner import _WholeRunPersistentWorker, run_optimization
from crescendo_engine.session_health import PersistentSessionHealthLimit
from crescendo_engine.utils import atomic_write_json, load_json
from tests.test_runner import _proven_controller_shutdown, _proven_session_health


CURRENT_ID = "current-token"


class PersistentStartupPhaseTests(unittest.TestCase):
    def _run(self, root, prepare_seconds=3.0, ready_after=3.4,
             authenticated=True, free_disk_mib=4096, changing_timestamps=False,
             config_updates=None):
        clock = {"value": 0.0}
        prepared = {
            "setup": {"tests": [], "corners": []}, "hierarchy": {"designs": []},
            "parameters": [{"parameter_id": "x", "backend": "schematic", "optimizable": True}],
            "outputs": [{"key": "main::gain"}],
        }
        config = {"runtime": {
            "run_root": root, "execution_backend": "auto", "max_parallel_jobs": 1,
            "candidate_timeout_seconds": 60, "ocean_startup_timeout_seconds": 1,
            "persistent_identity_startup_timeout_seconds": 1,
            "persistent_workspace_prepare_timeout_seconds": 8,
            "persistent_startup_total_timeout_seconds": 9,
            "persistent_active_health_interval_seconds": 0.5,
            "persistent_health_consecutive_limit": 99,
        }}
        config["runtime"].update(config_updates or {})
        spec = {
            "algorithm": "random", "generations": 1, "population": 1, "jobs": 1,
            "variables": [{"parameter_id": "x", "minimum": 0, "maximum": 1}],
            "objectives": [{"output_key": "main::gain", "direction": "maximize"}],
            "constraints": [],
        }

        class FakeController(object):
            def __init__(self, run_root, worker_index, *_args):
                self.run_root = run_root
                self.worker_index = worker_index
                self.epoch_index = 0
                self._start_ticks = 456
                self.process = mock.Mock(pid=123)
                self.request_start = None

            def start(self):
                return self

            def submit_evaluate(self, index, generation, values, _spec):
                self.request_start = clock["value"]
                return {"index": index, "generation": generation, "values": values}

            def poll(self, request):
                if clock["value"] - self.request_start < ready_after + 0.7:
                    return None
                value = request["values"]["x"]
                return {
                    "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                    "values": request["values"], "status": "completed",
                    "persistent_worker": {"worker_index": 0, "cleanup_pending": True},
                    "metrics": {"objectives": [{"score": -value, "value": value}],
                                "constraints": [], "feasible": True,
                                "total_violation": 0, "merit": -value},
                }

            def sample_health(self):
                elapsed = clock["value"] - self.request_start
                if elapsed >= ready_after:
                    return _proven_session_health()
                phase = "workspace_prepare" if elapsed < prepare_seconds else "ocean_starting"
                first_ocean = None if elapsed < prepare_seconds else self.request_start + prepare_seconds
                return {
                    "alive": True, "tree_complete": True,
                    "storage_complete": True, "logs_complete": True,
                    "pid": 123, "pid_start_ticks": 456, "controller_epoch": 0,
                    "free_disk_mib": free_disk_mib, "log_bytes": 0,
                    "session_identity_proven": False,
                    "session_identity": {"proven": False, "reason": "active worker session is not reusable"},
                    "startup_phase": {
                        "proven": authenticated, "stage": phase, "epoch": "worker-000-epoch-000",
                        "controller_epoch": 0, "controller_pid": 123,
                        "controller_pid_start_ticks": 456,
                        "request_sequence": 1, "request_nonce": "one-request",
                        "request_started_monotonic": self.request_start,
                        "started_monotonic": clock["value"] if changing_timestamps else self.request_start,
                        "stage_started_monotonic": clock["value"] if changing_timestamps else self.request_start,
                        "first_ocean_started_monotonic": first_ocean,
                    },
                }

            def commit_candidate(self, _candidate_id):
                return {"ok": True, "state": "cleaned", "health": _proven_session_health()}

            def terminate_and_prove(self, _reason):
                return _proven_controller_shutdown()

            def close(self):
                return _proven_controller_shutdown()

        def monotonic():
            value = clock["value"]
            clock["value"] += 0.05
            return value

        with mock.patch("crescendo_engine.runner.PersistentControllerProxy", FakeController), mock.patch(
                "crescendo_engine.runner.time.monotonic", side_effect=monotonic), mock.patch(
                "crescendo_engine.runner.time.sleep", return_value=None):
            return run_optimization(spec, config, prepared=prepared, run_root=os.path.join(root, "run"))

    def _failure(self, **kwargs):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(PersistentSessionHealthLimit) as captured:
                self._run(root, **kwargs)
            checkpoint = load_json(os.path.join(root, "run", "checkpoint.json"))
            return captured.exception, checkpoint["state"]["persistent_session_health"]

    def test_authenticated_slow_workspace_does_not_consume_ocean_deadline(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(self._run(root)["state"], "completed")

    def test_unproven_phase_retains_short_legacy_identity_timeout(self):
        error, decision = self._failure(authenticated=False)
        self.assertEqual(decision["state"], "identity_startup_timeout")
        self.assertFalse(decision["startup_phase_authenticated"])
        self.assertIn("startup guard failed", str(error))
        self.assertNotIn("0 consecutive samples", str(error))

    def test_ocean_starting_still_has_its_own_bounded_timeout(self):
        error, decision = self._failure(ready_after=100)
        self.assertEqual(decision["state"], "identity_startup_timeout")
        self.assertEqual(decision["startup_phase"], "ocean_starting")
        self.assertLess(decision["identity_pending_seconds"], 2)
        self.assertGreater(decision["startup_total_seconds"], 3)
        self.assertIn("stage=ocean_starting", str(error))

    def test_workspace_timeout_cannot_be_extended_by_repeated_timestamps(self):
        _error, decision = self._failure(prepare_seconds=100, ready_after=101,
                                       changing_timestamps=True)
        self.assertEqual(decision["state"], "workspace_prepare_timeout")
        self.assertIn("workspace_prepare_timeout", decision["violations"])
        self.assertGreaterEqual(decision["startup_total_seconds"], 8)

    def test_total_startup_deadline_can_be_stricter_than_phase_deadlines(self):
        _error, decision = self._failure(config_updates={"persistent_startup_total_timeout_seconds": 2})
        self.assertEqual(decision["state"], "startup_total_timeout")
        self.assertIn("persistent_startup_total_timeout", decision["violations"])

    def test_disk_protection_remains_active_during_authenticated_prepare(self):
        _error, decision = self._failure(free_disk_mib=10)
        self.assertIn("disk_low_watermark", decision["violations"])
        self.assertLess(decision["startup_total_seconds"], 1)

    def test_restart_wrapper_reports_underlying_startup_reason(self):
        error = PersistentSessionHealthLimit({
            "state": "restart_limit_exhausted", "decision": {
                "state": "identity_startup_timeout", "startup_phase": "ocean_starting",
                "identity_unproven_reason": {"code": "worker_process_identity_incomplete"},
                "identity_pending_seconds": 90, "identity_startup_timeout_seconds": 90,
            },
        })
        self.assertIn("worker_process_identity_incomplete", str(error))
        self.assertIn("restart_limit_exhausted", str(error))
        self.assertNotIn("0 consecutive samples", str(error))

    def test_workspace_phase_precedes_hash_and_copy(self):
        with tempfile.TemporaryDirectory() as root:
            worker = _WholeRunPersistentWorker(root, 0, {"setup": {}, "hierarchy": {}}, [],
                                               {"runtime": {}, "cadence": {}})
            events = []
            worker.startup_phase_callback = lambda stage, _epoch: events.append(stage)

            class FakeOcean(object):
                def __init__(self, *args, **kwargs):
                    self.epoch = kwargs["epoch"]

                def start(self):
                    events.append("ocean_start")

            def guard(*_args):
                events.append("external_hash")
                return {"ok": True}

            def build(*_args, **_kwargs):
                events.append("copy")
                return {"root": root}

            with mock.patch("crescendo_engine.runner._reverify_prepared_external_environment", side_effect=guard), mock.patch(
                    "crescendo_engine.runner.build_workspace", side_effect=build), mock.patch(
                    "crescendo_engine.runner.PersistentMaestroWorker", FakeOcean):
                worker._new_epoch()
            self.assertEqual(events, ["workspace_prepare", "external_hash", "copy", "ocean_starting",
                                      "ocean_start", "session_ready"])

    def test_proxy_phase_requires_current_rpc_and_owned_controller(self):
        with tempfile.TemporaryDirectory() as root:
            proxy = PersistentControllerProxy(root, 0, {}, [], {})
            proxy.epoch_root = os.path.join(root, "controller")
            os.makedirs(proxy.epoch_root)
            proxy.epoch_index = 0
            proxy.token = CURRENT_ID
            proxy.process = mock.Mock(pid=123)
            proxy.process.poll.return_value = None
            proxy._start_ticks = 456
            proxy._pgid = 123
            request = {"schema_version": SCHEMA_VERSION, "token": proxy.token,
                       "controller_epoch": 0, "sequence": 1, "nonce": "current-request",
                       "operation": "EVALUATE", "started_monotonic": 1.0}
            proxy._outstanding = request
            phase = {"stage": "workspace_prepare", "epoch": None,
                     "started_monotonic": 2.0, "stage_started_monotonic": 2.0,
                     "first_ocean_started_monotonic": None}
            identity = dict((key, request[key]) for key in (
                "schema_version", "token", "controller_epoch", "sequence", "nonce", "operation"))
            proxy._write_state("processing", active_request=identity, startup_phase=phase)
            document = load_json(proxy.state_path)
            health = {"alive": True, "tree_complete": True}
            with mock.patch("crescendo_engine.persistent_controller.time.monotonic", return_value=10), mock.patch(
                    "crescendo_engine.persistent_controller._process_stat",
                    return_value={"start_ticks": 456, "pgid": 123}):
                self.assertTrue(proxy._authenticated_startup_phase(health)["proven"])
                for key, value in (("token", "foreign-token"), ("pid_start_ticks", 999)):
                    altered = dict(document)
                    altered[key] = value
                    atomic_write_json(proxy.state_path, altered)
                    self.assertFalse(proxy._authenticated_startup_phase(health)["proven"])
                altered = dict(document)
                altered["active_request"] = dict(identity, nonce="old-request")
                atomic_write_json(proxy.state_path, altered)
                self.assertFalse(proxy._authenticated_startup_phase(health)["proven"])
                atomic_write_json(proxy.state_path, document)
                self.assertFalse(proxy._authenticated_startup_phase(dict(health, tree_complete=False))["proven"])
                other = proxy.state_path + ".real"
                os.rename(proxy.state_path, other)
                os.symlink(other, proxy.state_path)
                self.assertFalse(proxy._authenticated_startup_phase(health)["proven"])

    def test_published_phase_does_not_reset_fixed_start_or_ocean_time(self):
        with tempfile.TemporaryDirectory() as root:
            bootstrap = {"token": "test-token", "worker_index": 0, "controller_epoch": 0,
                         "parent_pid": os.getpid(), "parent_start_ticks": 1, "run_root": root,
                         "state_path": os.path.join(root, "controller_state.json")}
            identity = {"sequence": 1, "nonce": "test-request"}
            context = {}
            with mock.patch("crescendo_engine.persistent_controller.time.monotonic",
                            side_effect=[20, 25, 30, 35, 40]):
                _publish_controller_startup_phase(bootstrap, identity, "workspace_prepare", None, context)
                _publish_controller_startup_phase(bootstrap, identity, "workspace_prepare", "epoch", context)
                self.assertEqual(context["stage_started_monotonic"], 20)
                _publish_controller_startup_phase(bootstrap, identity, "ocean_starting", "epoch", context)
                _publish_controller_startup_phase(bootstrap, identity, "ocean_starting", "epoch", context)
                _publish_controller_startup_phase(bootstrap, identity, "workspace_prepare", "next-epoch", context)
            phase = load_json(bootstrap["state_path"])["startup_phase"]
            self.assertEqual(phase["started_monotonic"], 20)
            self.assertEqual(phase["first_ocean_started_monotonic"], 30)


if __name__ == "__main__":
    unittest.main()


# Historical v0.3.0 native-ADE scheduling, never the current shared CLI route.
from tests.legacy_native_fixture import historical_native_loop


class LegacyNativePersistentStartupPhaseTests(unittest.TestCase):
    """Retired native scheduling regression assertions."""
    _run = PersistentStartupPhaseTests._run
    _failure = PersistentStartupPhaseTests._failure


_HISTORICAL_NATIVE_TESTS = (
    'test_authenticated_slow_workspace_does_not_consume_ocean_deadline',
    'test_unproven_phase_retains_short_legacy_identity_timeout',
    'test_ocean_starting_still_has_its_own_bounded_timeout',
    'test_workspace_timeout_cannot_be_extended_by_repeated_timestamps',
    'test_total_startup_deadline_can_be_stricter_than_phase_deadlines',
    'test_disk_protection_remains_active_during_authenticated_prepare',
)
for _historical_name in _HISTORICAL_NATIVE_TESTS:
    setattr(LegacyNativePersistentStartupPhaseTests, _historical_name, historical_native_loop(getattr(PersistentStartupPhaseTests, _historical_name)))
    delattr(PersistentStartupPhaseTests, _historical_name)
