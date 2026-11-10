import copy
import unittest

from crescendo_engine.runner import (_checkpoint_checksum, resolve_design_config,
                                      validate_checkpoint_integrity)


class RunProfileCheckpointTests(unittest.TestCase):
    def checkpoint(self):
        return {"schema_version": "crescendo.checkpoint.v1", "manifest": {"continuation_id": "fixture-profile-continuation"},
                "spec": {"evaluator": "mock", "variables": [], "objectives": [], "constraints": []},
                "parameter_ids": [], "candidate_layout": ["id", "generation", "index", "status", "values", "metrics", "failure_class", "error", "started_at", "finished_at"],
                "candidates": [], "state": {"next_generation": 0, "active_plan": None,
                "generator": {"previous": [], "de_targets": []}, "selection_history": [],
                "checkpoint_revision": 0, "completed_candidates": 0,
                "failed_candidates": 0, "infrastructure_failed_candidates": 0}}

    def validate(self, checkpoint):
        checkpoint["payload_sha256"] = _checkpoint_checksum(checkpoint)
        return validate_checkpoint_integrity(checkpoint)

    def test_scheduler_change_rejected_even_with_valid_checksum(self):
        checkpoint = self.checkpoint()
        checkpoint["spec"]["command_scheduler"] = "batch"
        checkpoint["manifest"]["command_scheduler"] = "receipt"
        with self.assertRaisesRegex(ValueError, "command_scheduler identities disagree"):
            self.validate(checkpoint)

    def test_old_absent_scheduler_equates_to_auto(self):
        checkpoint = self.checkpoint()
        checkpoint["spec"]["command_scheduler"] = "auto"
        self.validate(checkpoint)

    def test_model_profile_change_rejected_even_with_valid_checksum(self):
        checkpoint = self.checkpoint()
        checkpoint["spec"]["simulator_model_profile_identity"] = {"fingerprint": "new-model"}
        checkpoint["manifest"]["simulator_model_profile_identity"] = {"fingerprint": "old-model"}
        with self.assertRaisesRegex(ValueError, "simulator_model_profile_identity identities disagree"):
            self.validate(checkpoint)

    def test_load_settings_preserves_explicit_wrapper_scheduler(self):
        document = {"schema_version": "crescendo.design_config.v1", "optimization": {
            "variables": [], "objectives": [], "constraints": [],
            "run": {"command_scheduler": "batch"}}}
        resolved = resolve_design_config(copy.deepcopy(document), {"parameters": [], "outputs": [], "setup": {}})
        self.assertEqual(resolved["spec"]["command_scheduler"], "batch")

    def test_preparation_measurement_budget_blank_is_unlimited_and_zero_rejected(self):
        from crescendo_engine.runner import _optional_candidate_timeout
        for value in (None, "", "  ", -1):
            self.assertEqual(_optional_candidate_timeout(value), -1)
        self.assertEqual(_optional_candidate_timeout("0.5"), 0.5)
        for value in (0, "0", True, -2, "nan"):
            with self.assertRaises(ValueError):
                _optional_candidate_timeout(value)

    def test_first_model_qualification_advances_static_anchor_only_forward(self):
        from crescendo_engine.runner import checkpoint_model_profile_transition, checkpoint_continuation_schema
        from crescendo_engine.server import _checkpoint_continuation_schema_equal
        before = self.checkpoint()
        after = copy.deepcopy(before)
        after["state"]["checkpoint_revision"] = 1
        after["spec"]["simulator_model_profile_identity"] = {"fingerprint": "model-a"}
        after["manifest"]["simulator_model_profile_identity"] = {"fingerprint": "model-a"}
        self.assertEqual(checkpoint_continuation_schema(before), checkpoint_continuation_schema(after))
        self.assertTrue(checkpoint_model_profile_transition(before, after))
        self.assertFalse(checkpoint_model_profile_transition(after, before))
        self.assertTrue(_checkpoint_continuation_schema_equal(before, after))
        dropped = copy.deepcopy(before)
        dropped["state"]["checkpoint_revision"] = 2
        self.assertFalse(_checkpoint_continuation_schema_equal(after, dropped))
        changed = copy.deepcopy(after)
        changed["state"]["checkpoint_revision"] = 2
        changed["spec"]["simulator_model_profile_identity"] = {"fingerprint": "model-b"}
        changed["manifest"]["simulator_model_profile_identity"] = {"fingerprint": "model-b"}
        self.assertFalse(_checkpoint_continuation_schema_equal(after, changed))


if __name__ == "__main__":
    unittest.main()
