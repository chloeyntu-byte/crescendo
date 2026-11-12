import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.continuation import (
    ContinuationLease, build_continuation_pending, continuation_checkpoint_path,
    continuation_pending_path, continuation_schema_sha256,
    persist_continuation_pending,
)
from crescendo_engine.runner import (
    _checkpoint_checksum, checkpoint_candidates, checkpoint_with_candidates,
    run_optimization,
)
from crescendo_engine.server import (
    OptimizerService, _persist_reconciled_origin_checkpoint,
    _checkpoint_continuation_schema_payload, _reconcile_continuation_anchor,
)


class CheckpointOriginGuardTest(unittest.TestCase):
    def _checkpoint(self, origin_root, revision, rows=None, marker=None):
        continuation_id = "continuation-test"
        base = {
            "schema_version": "crescendo.checkpoint.v1",
            "manifest": {
                "origin_run_root": origin_root,
                "origin_run_id": os.path.basename(origin_root),
                "continuation_id": continuation_id,
            },
            "parameter_ids": ["x"],
            "candidate_layout": [
                "id", "generation", "index", "status", "values", "metrics",
                "failure_class", "error", "started_at", "finished_at",
            ],
            "spec": {
                "evaluator": "mock",
                "population": 1,
                "variables": [{"parameter_id": "x"}],
                "objectives": [],
                "constraints": [],
            },
            "candidates": [],
            "state": {
                "next_generation": 1,
                "active_plan": None,
                "generator": {"previous": [], "de_targets": []},
                "selection_history": [] if marker is None else [{"marker": marker}],
                "checkpoint_revision": revision - 1,
                "completed_candidates": 0,
                "failed_candidates": 0,
                "infrastructure_failed_candidates": 0,
            },
        }
        return checkpoint_with_candidates(base, rows or [])

    def _fixture(self):
        temporary = tempfile.TemporaryDirectory()
        origin_root = os.path.join(temporary.name, "origin")
        os.makedirs(origin_root)
        with open(os.path.join(origin_root, "status.json"), "w") as stream:
            json.dump({
                "schema_version": "crescendo.run_status.v1",
                "run_id": "origin",
                "run_root": origin_root,
                "continuation_id": "continuation-test",
                "state": "failed",
            }, stream)
        service = OptimizerService({"runtime": {"run_root": temporary.name}})
        return temporary, service, origin_root

    def test_newer_origin_blocks_stale_loaded_checkpoint(self):
        temporary, service, origin_root = self._fixture()
        try:
            loaded = self._checkpoint(origin_root, 5)
            origin = self._checkpoint(origin_root, 7, marker="advanced optimizer state")
            with open(os.path.join(origin_root, "checkpoint.json"), "w") as stream:
                json.dump(origin, stream)

            with self.assertRaisesRegex(RuntimeError, "older than the authoritative origin"):
                service._cleanup_checkpoint_origin(loaded, inspect_only=True)
        finally:
            temporary.cleanup()

    def test_same_revision_semantic_fork_fails_closed(self):
        temporary, service, origin_root = self._fixture()
        try:
            loaded = self._checkpoint(origin_root, 5, marker="branch-a")
            origin = self._checkpoint(origin_root, 5, marker="branch-b")
            with open(os.path.join(origin_root, "checkpoint.json"), "w") as stream:
                json.dump(origin, stream)

            with self.assertRaisesRegex(RuntimeError, "fork at the same revision"):
                service._cleanup_checkpoint_origin(loaded, inspect_only=True)
        finally:
            temporary.cleanup()

    def test_same_revision_ignores_only_volatile_timestamp_and_checksum(self):
        temporary, service, origin_root = self._fixture()
        try:
            loaded = self._checkpoint(origin_root, 5)
            origin = copy.deepcopy(loaded)
            origin["created_at"] = "different export timestamp"
            origin.pop("payload_sha256", None)
            origin = checkpoint_with_candidates(
                dict(origin, state=dict(origin["state"], checkpoint_revision=4)),
                checkpoint_candidates(origin),
            )
            self.assertEqual(origin["state"]["checkpoint_revision"], 5)
            with open(os.path.join(origin_root, "checkpoint.json"), "w") as stream:
                json.dump(origin, stream)

            result = service._cleanup_checkpoint_origin(loaded, inspect_only=True)

            self.assertEqual(result["checkpoint_selection"]["state"], "same_revision")
        finally:
            temporary.cleanup()

    def test_strict_reconciliation_descendant_is_promoted_for_repeat_resume(self):
        temporary, service, origin_root = self._fixture()
        try:
            loaded = self._checkpoint(origin_root, 5)
            recovered = {
                "candidate_id": "g000_c0000",
                "generation": 0,
                "index": 0,
                "status": "failed",
                "values": {"x": 0.25},
                "metrics": None,
            }
            origin = checkpoint_with_candidates(loaded, [recovered])
            with open(os.path.join(origin_root, "checkpoint.json"), "w") as stream:
                json.dump(origin, stream)

            result = service._cleanup_checkpoint_origin(loaded, inspect_only=True)

            self.assertEqual(
                result["checkpoint_selection"]["state"],
                "origin_reconciliation_promoted",
            )
            self.assertEqual(checkpoint_candidates(loaded)[0]["candidate_id"], "g000_c0000")
        finally:
            temporary.cleanup()

    def test_global_anchor_promotes_latest_checkpoint_when_original_is_loaded_again(self):
        with tempfile.TemporaryDirectory() as root:
            original = self._checkpoint(os.path.join(root, "origin-a"), 5)
            selection = _reconcile_continuation_anchor(original, root)
            self.assertEqual(selection["state"], "portable_anchor_created")

            advanced = copy.deepcopy(original)
            advanced["manifest"]["origin_run_id"] = "origin-b"
            advanced["manifest"]["origin_run_root"] = os.path.join(root, "origin-b")
            advanced = checkpoint_with_candidates(advanced, checkpoint_candidates(advanced))
            promoted = _reconcile_continuation_anchor(advanced, root)
            self.assertEqual(promoted["state"], "loaded_checkpoint_promoted")

            repeated_original = copy.deepcopy(original)
            selected = _reconcile_continuation_anchor(repeated_original, root)
            self.assertEqual(selected["state"], "anchor_promoted")
            self.assertEqual(
                repeated_original["payload_sha256"], advanced["payload_sha256"],
            )

    def test_global_anchor_rejects_same_revision_fork_and_unsafe_file(self):
        with tempfile.TemporaryDirectory() as root:
            checkpoint = self._checkpoint(os.path.join(root, "origin"), 5)
            _reconcile_continuation_anchor(checkpoint, root)
            fork = copy.deepcopy(checkpoint)
            fork["state"]["selection_history"] = [{"marker": "fork"}]
            fork["payload_sha256"] = _checkpoint_checksum(fork)
            with self.assertRaisesRegex(RuntimeError, "forks at revision"):
                _reconcile_continuation_anchor(fork, root)

        with tempfile.TemporaryDirectory() as root:
            checkpoint = self._checkpoint(os.path.join(root, "origin"), 5)
            anchor_path = continuation_checkpoint_path(
                root, "continuation-test", create=True,
            )
            target = os.path.join(root, "target.json")
            with open(target, "w") as stream:
                json.dump(checkpoint, stream)
            os.symlink(target, anchor_path)
            with self.assertRaisesRegex(RuntimeError, "safe regular file"):
                _reconcile_continuation_anchor(checkpoint, root)

    def test_old_checkpoint_recovers_two_successive_pending_descendants(self):
        with tempfile.TemporaryDirectory() as root:
            original = self._checkpoint(os.path.join(root, "origin-a"), 5)
            _reconcile_continuation_anchor(original, root)
            advanced = checkpoint_with_candidates(original, checkpoint_candidates(original))
            pending = build_continuation_pending(
                advanced, "continuation-test",
                advanced["state"]["checkpoint_revision"], original["payload_sha256"],
                continuation_schema_sha256(
                    _checkpoint_continuation_schema_payload(advanced)
                ),
            )
            persist_continuation_pending(root, "continuation-test", pending)

            first_loaded = copy.deepcopy(original)
            result = _reconcile_continuation_anchor(first_loaded, root)
            self.assertEqual(result["state"], "pending_journal_recovered")
            self.assertEqual(first_loaded["payload_sha256"], advanced["payload_sha256"])

            newest = checkpoint_with_candidates(advanced, checkpoint_candidates(advanced))
            pending = build_continuation_pending(
                newest, "continuation-test",
                newest["state"]["checkpoint_revision"], advanced["payload_sha256"],
                continuation_schema_sha256(
                    _checkpoint_continuation_schema_payload(newest)
                ),
            )
            persist_continuation_pending(root, "continuation-test", pending)
            second_loaded = copy.deepcopy(original)
            result = _reconcile_continuation_anchor(second_loaded, root)
            self.assertEqual(result["state"], "pending_journal_recovered")
            self.assertEqual(second_loaded["payload_sha256"], newest["payload_sha256"])
            self.assertFalse(os.path.lexists(
                continuation_pending_path(root, "continuation-test"),
            ))

    def test_uncoordinated_direct_resume_is_rejected_before_pending_reconciliation(self):
        with tempfile.TemporaryDirectory() as root:
            original = self._checkpoint(os.path.join(root, "origin"), 5)
            _reconcile_continuation_anchor(original, root)
            advanced = checkpoint_with_candidates(original, checkpoint_candidates(original))
            pending = build_continuation_pending(
                advanced, "continuation-test",
                advanced["state"]["checkpoint_revision"], original["payload_sha256"],
                continuation_schema_sha256(
                    _checkpoint_continuation_schema_payload(advanced)
                ),
            )
            persist_continuation_pending(root, "continuation-test", pending)
            loaded = copy.deepcopy(original)
            lease = ContinuationLease(
                root, "continuation-test", run_root=os.path.join(root, "resumed"),
            ).acquire()
            with mock.patch(
                "crescendo_engine.runner._run_optimization_unleased",
                side_effect=lambda *_args, **kwargs: kwargs["resume_checkpoint"]["payload_sha256"],
            ) as unleased:
                try:
                    with self.assertRaisesRegex(RuntimeError, "internal"):
                        run_optimization(
                            {}, {"runtime": {"run_root": root}},
                            run_root=os.path.join(root, "resumed"),
                            resume_checkpoint=loaded, continuation_lease=lease,
                        )
                finally:
                    lease.release()
            unleased.assert_not_called()
            self.assertTrue(os.path.lexists(
                continuation_pending_path(root, "continuation-test"),
            ))

    def test_pending_journal_symlink_checksum_and_parent_fork_fail_closed(self):
        with tempfile.TemporaryDirectory() as root:
            original = self._checkpoint(os.path.join(root, "origin"), 5)
            _reconcile_continuation_anchor(original, root)
            pending_path = continuation_pending_path(
                root, "continuation-test", create=True,
            )
            target = os.path.join(root, "malicious.json")
            with open(target, "w") as stream:
                json.dump({}, stream)
            os.symlink(target, pending_path)
            with self.assertRaisesRegex(RuntimeError, "safe regular file"):
                _reconcile_continuation_anchor(copy.deepcopy(original), root)

        with tempfile.TemporaryDirectory() as root:
            original = self._checkpoint(os.path.join(root, "origin"), 5)
            _reconcile_continuation_anchor(original, root)
            advanced = checkpoint_with_candidates(original, checkpoint_candidates(original))
            pending = build_continuation_pending(
                advanced, "continuation-test",
                advanced["state"]["checkpoint_revision"], original["payload_sha256"],
                continuation_schema_sha256(
                    _checkpoint_continuation_schema_payload(advanced)
                ),
            )
            pending["created_at"] = "tampered"
            with open(continuation_pending_path(root, "continuation-test"), "w") as stream:
                json.dump(pending, stream)
            with self.assertRaisesRegex(RuntimeError, "checksum does not match"):
                _reconcile_continuation_anchor(copy.deepcopy(original), root)

        with tempfile.TemporaryDirectory() as root:
            original = self._checkpoint(os.path.join(root, "origin"), 5)
            _reconcile_continuation_anchor(original, root)
            advanced = checkpoint_with_candidates(original, checkpoint_candidates(original))
            pending = build_continuation_pending(
                advanced, "continuation-test",
                advanced["state"]["checkpoint_revision"], "f" * 64,
                continuation_schema_sha256(
                    _checkpoint_continuation_schema_payload(advanced)
                ),
            )
            persist_continuation_pending(root, "continuation-test", pending)
            with self.assertRaisesRegex(RuntimeError, "parent checksum does not match"):
                _reconcile_continuation_anchor(copy.deepcopy(original), root)

    def test_external_newer_checkpoint_remains_selected(self):
        temporary, service, origin_root = self._fixture()
        try:
            loaded = self._checkpoint(origin_root, 7)
            origin = self._checkpoint(origin_root, 5)
            with open(os.path.join(origin_root, "checkpoint.json"), "w") as stream:
                json.dump(origin, stream)

            result = service._cleanup_checkpoint_origin(loaded, inspect_only=True)

            self.assertEqual(result["checkpoint_selection"]["state"], "external_newer")
        finally:
            temporary.cleanup()

    def test_external_newer_requires_exact_immutable_manifest_and_candidate_extension(self):
        temporary, service, origin_root = self._fixture()
        try:
            origin_row = {
                "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                "status": "completed", "values": {"x": 0.25},
                "metrics": {"feasible": True},
            }
            origin = self._checkpoint(origin_root, 5, rows=[origin_row])
            with open(os.path.join(origin_root, "checkpoint.json"), "w") as stream:
                json.dump(origin, stream)

            changed_manifest = self._checkpoint(origin_root, 7, rows=[origin_row])
            changed_manifest["manifest"]["source_design_identity"] = {
                "schema_version": "crescendo.source_design_identity.v1",
                "fingerprint": "different",
            }
            changed_manifest.pop("payload_sha256", None)
            with self.assertRaisesRegex(RuntimeError, "immutable optimization schema"):
                service._cleanup_checkpoint_origin(changed_manifest, inspect_only=True)

            changed_row = copy.deepcopy(origin_row)
            changed_row["values"] = {"x": 0.75}
            forked = self._checkpoint(origin_root, 7, rows=[changed_row])
            with self.assertRaisesRegex(RuntimeError, "exact candidate extension"):
                service._cleanup_checkpoint_origin(forked, inspect_only=True)
        finally:
            temporary.cleanup()

    def test_external_newer_candidate_superset_is_persisted_before_origin_cleanup(self):
        temporary, service, origin_root = self._fixture()
        try:
            origin = self._checkpoint(origin_root, 5)
            with open(os.path.join(origin_root, "checkpoint.json"), "w") as stream:
                json.dump(origin, stream)
            row = {
                "candidate_id": "g000_c0000", "generation": 0, "index": 0,
                "status": "completed", "values": {"x": 0.25},
                "metrics": {"feasible": True},
            }
            loaded = self._checkpoint(origin_root, 7, rows=[row])

            result = service._cleanup_checkpoint_origin(loaded)

            self.assertTrue(result["checkpoint_updated"])
            with open(os.path.join(origin_root, "checkpoint.json"), "r") as stream:
                persisted = json.load(stream)
            self.assertEqual(
                persisted, loaded,
            )
        finally:
            temporary.cleanup()

    def test_reconciled_write_refuses_to_downgrade_origin(self):
        temporary, _service, origin_root = self._fixture()
        try:
            newer = self._checkpoint(origin_root, 9, marker="newer")
            stale = self._checkpoint(origin_root, 7)
            with open(os.path.join(origin_root, "checkpoint.json"), "w") as stream:
                json.dump(newer, stream)

            with self.assertRaisesRegex(RuntimeError, "different optimization state|newer origin"):
                _persist_reconciled_origin_checkpoint(origin_root, stale)
        finally:
            temporary.cleanup()

    def test_malformed_revision_is_rejected_before_ordering(self):
        temporary, service, origin_root = self._fixture()
        try:
            loaded = self._checkpoint(origin_root, 5)
            origin = self._checkpoint(origin_root, 6)
            origin["state"]["checkpoint_revision"] = True
            origin.pop("payload_sha256", None)
            with open(os.path.join(origin_root, "checkpoint.json"), "w") as stream:
                json.dump(origin, stream)

            with self.assertRaisesRegex(ValueError, "revision must be a non-negative integer"):
                service._cleanup_checkpoint_origin(loaded, inspect_only=True)
        finally:
            temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
