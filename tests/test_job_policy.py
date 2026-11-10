import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.job_policy import (
    RECORD_SCHEMA, capture_setup_job_policy, job_policy_capture_definitions,
    job_policy_capture_skill, job_policy_fingerprint, job_policy_override_definitions,
    normalize_job_overrides, parse_policy_records, policy_from_records,
)
from crescendo_engine.maestro_context import SCHEMA, parse_launch_records


def record(tag, *values):
    return tag + "\t" + "\t".join("nil" if v is None else "t" if v is True else json.dumps(v) for v in values)


class JobPolicyTests(unittest.TestCase):
    def rows(self):
        return [
            ("P", ["global", None, True]),
            ("J", ["global", None, "name", "Shared"]),
            ("J", ["global", None, "distributionmethod", "Command"]),
            ("J", ["global", None, "maxjobs", "64"]),
            ("J", ["global", None, "jobsubmitcommand", 'batch-submit -R "cpu=6;mem=30000"']),
            ("J", ["global", None, "defaultmemoryvalue", "1000"]),
            ("P", ["test", "shared_test", True]),
            ("D", ["test", "shared_test"]),
            ("P", ["test", "independent_test", True]),
            ("J", ["test", "independent_test", "name", "Own"]),
            ("J", ["test", "independent_test", "distributionmethod", "Command"]),
            ("J", ["test", "independent_test", "maxjobs", "2"]),
        ]

    def test_capture_preserves_quotes_and_effective_policy_groups(self):
        policy = policy_from_records(self.rows())
        self.assertTrue(policy["available"])
        self.assertEqual(policy["tests"][0]["properties"], policy["global"]["properties"])
        self.assertIsNot(policy["tests"][0]["properties"], policy["global"]["properties"])
        self.assertEqual(policy["tests"][1]["properties"]["maxjobs"], "2")
        self.assertEqual(policy["global"]["properties"]["jobsubmitcommand"], 'batch-submit -R "cpu=6;mem=30000"')

    def test_unknown_is_never_invented_as_local_or_one(self):
        policy = policy_from_records([("P", ["global", None, None])])
        self.assertFalse(policy["available"])
        self.assertEqual(policy["global"]["properties"], {})
        self.assertTrue(policy["warnings"])

    def test_malformed_and_duplicate_evidence_is_rejected(self):
        for extra in [("J", ["global", None, "maxjobs", "1"]),
                      ("P", ["global", None, True]),
                      ("J", ["test", "shared_test", "maxjobs", "1"]),
                      ("J", ["global", None, "max-jobs", "1"]),
                      ("J", ["global", None, "jobsubmitcommand", "bad\ncommand"]),
                      ("D", ["global", None])]:
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                policy_from_records(self.rows() + [extra])

    def test_unavailable_global_cannot_certify_an_equal_test(self):
        with self.assertRaises(ValueError):
            policy_from_records([("P", ["global", None, None]), ("P", ["test", "test", True]), ("D", ["test", "test"])])

    def test_max_jobs_has_no_relation_to_population(self):
        for count in (1, 2, 64, 1024):
            self.assertEqual(normalize_job_overrides({"max_jobs": count}), {"max_jobs": count})
        for count in (0, -1, 1.0, "2", True, None):
            with self.subTest(count=count), self.assertRaises(ValueError):
                normalize_job_overrides({"max_jobs": count})

    def test_command_is_opaque_and_empty_is_explicit(self):
        value = 'batch-submit -A account -R "cpu=6;mem=30000" $(keep-literal) `also-literal`'
        self.assertEqual(normalize_job_overrides({"command": value}), {"command": value})
        self.assertEqual(normalize_job_overrides({"command": ""}), {"command": ""})
        self.assertEqual(normalize_job_overrides(None), {})
        for value in ("bad\ncommand", "bad\tcommand", "x" * 16385, None):
            with self.subTest(value=repr(value)[:30]), self.assertRaises(ValueError):
                normalize_job_overrides({"command": value})

    def test_identity_tracks_original_effective_settings_and_each_override(self):
        policy = policy_from_records(self.rows())
        unchanged = job_policy_fingerprint(policy)
        changed = copy.deepcopy(policy)
        changed["tests"][1]["properties"]["maxjobs"] = "3"
        self.assertNotEqual(unchanged, job_policy_fingerprint(changed))
        self.assertNotEqual(unchanged, job_policy_fingerprint(policy, {"max_jobs": 128}))
        self.assertEqual(unchanged, job_policy_fingerprint(dict(policy, probe_root="/temporary/read_2")))

    def test_unmodified_invoking_session_needs_no_ocean_query(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "maestro.sdb")
            setup = {"backend": "maestro", "setup_locator": {"path": path}}
            policy = policy_from_records(self.rows())
            config = {"_maestro_launch_context": {"source": {"setup_path": path}, "job_policy": policy}}
            with mock.patch("crescendo_engine.cadence.run_command") as run:
                captured = capture_setup_job_policy(setup, config)
            run.assert_not_called()
            self.assertEqual(captured, policy)
            self.assertIsNot(captured, policy)

    def test_missing_ocean_is_optional_and_undeclared_defaults_are_absent(self):
        result = capture_setup_job_policy({"backend": "maestro"}, {"cadence": {}})
        self.assertFalse(result["available"])
        self.assertEqual(result["global"]["properties"], {})

    def test_active_state_only_save_invalidates_invoking_job_and_storage_snapshot(self):
        from crescendo_engine.utils import sha256_file
        with tempfile.TemporaryDirectory() as root:
            path, state = os.path.join(root, "maestro.sdb"), os.path.join(root, "active.state")
            with open(path, "w", encoding="utf-8") as stream:
                stream.write("<setupdb/>")
            for transition in ("created", "changed", "removed"):
                with self.subTest(transition=transition):
                    if transition != "created":
                        with open(state, "w", encoding="utf-8") as stream:
                            stream.write("before")
                    source = {"setup_path": path, "setup_content_sha256": sha256_file(path),
                              "active_state_sha256": sha256_file(state) if os.path.isfile(state) else None}
                    if transition == "removed":
                        os.unlink(state)
                    else:
                        with open(state, "w", encoding="utf-8") as stream:
                            stream.write("after")
                    setup = {"backend": "maestro", "setup_locator": {"path": path}}
                    config = {"_maestro_launch_context": {"source": source,
                        "job_policy": policy_from_records(self.rows()),
                        "save_options": {"effective": {"simulation_root": "/stale/native/root"}}}}
                    captured = capture_setup_job_policy(setup, config)
                    self.assertFalse(captured["available"])
                    self.assertEqual(captured["source"], "saved_setup_native")
                    self.assertNotIn("save_options", setup)
                    if os.path.isfile(state):
                        os.unlink(state)

    def test_optional_probe_storage_failure_does_not_block_setup_inspection(self):
        with mock.patch("crescendo_engine.job_policy.ensure_dir", side_effect=OSError(122, "Disk quota exceeded")):
            result = capture_setup_job_policy({"backend": "maestro"}, {
                "cadence": {"ocean_bin": "/fixture/ocean"}, "runtime": {"run_root": "/fixture/root"},
            })
        self.assertFalse(result["available"])
        self.assertIn("122", result["warnings"][0])

    def test_native_probe_is_readonly_and_never_submits_jobs(self):
        skill = job_policy_capture_skill("Lib", "TB", "maestro", "/tmp/records", ["test"])
        self.assertIn('?mode "r"', skill)
        self.assertIn("maeCloseSession", skill)
        for action in ("maeSaveSetup", "maeRunSimulation", "system(", "ipcBeginProcess"):
            self.assertNotIn(action, skill)

    def test_named_policy_overrides_preserve_group_and_verify_properties(self):
        skill = job_policy_override_definitions({"command": 'batch-submit -R "cpu=6"', "max_jobs": 128})
        self.assertIn("name = original->name", skill)
        self.assertIn("newValue = get(readback key)", skill)
        self.assertIn("crescCopyEffectiveJobPolicy", skill)
        self.assertIn('original->distributionmethod = "Command"', skill)
        self.assertIn('original->maxjobs = "128"', skill)
        self.assertNotIn("maeRunSimulation", skill)

    def test_no_override_has_no_application_work(self):
        skill = job_policy_override_definitions()
        body = skill.split("procedure(crescApplyJobOverrides", 1)[1]
        self.assertNotIn("maeSetJobPolicy", body)
        self.assertNotIn("maeGetJobPolicy", body)

    def test_batch_qualifier_requires_complete_actual_evidence(self):
        skill = job_policy_override_definitions()
        qualifier = skill.split("procedure(crescBatchSourceUsesGlobalPolicy", 1)[1].split("procedure(", 1)[0]
        self.assertIn("errset(maeGetJobPolicy", qualifier)
        self.assertIn("globalResult && sourceResult", qualifier)
        self.assertIn("crescJobPolicyHasJobBudget(car(globalResult))", qualifier)
        self.assertIn("crescJobPolicyPropertiesEqual(car(globalResult) car(sourceResult) t)", qualifier)
        # Availability failures cannot be converted into inherited settings;
        # qualifiers are read-only and must run before candidate cloning.
        self.assertNotIn("sourceResult = globalResult", qualifier)
        self.assertNotIn("maeSetJobPolicy", qualifier)
        self.assertNotIn("maeCreateTest", qualifier)

    def test_batch_alias_requires_successful_public_detach_and_full_readback(self):
        skill = job_policy_override_definitions()
        copier = skill.split("procedure(crescCopyEffectiveJobPolicy", 1)[1].split("procedure(", 1)[0]
        self.assertIn("crescJobPolicyPropertiesEqual(original globalPolicy t)", copier)
        self.assertIn('crescCloneJobPolicy(sess copy(globalPolicy) targetTest "CRESCENDO_shared_global")', copier)
        self.assertIn('accepted = axlDetachJobPolicy(sess "ICRP" targetTest)', copier)
        self.assertIn("unless(accepted", copier)
        self.assertIn("unless(crescJobPolicyPropertiesEqual(globalPolicy readback t)", copier)
        self.assertNotIn("readback = globalPolicy", copier)
        self.assertNotIn("maeRunSimulation", copier)

    def test_compact_record_transport_preserves_large_shared_policy_tables(self):
        rows = self.rows()[:6]
        for index in range(4096):
            rows.extend([("P", ["test", "test_%s" % index, True]), ("D", ["test", "test_%s" % index])])
        raw = RECORD_SCHEMA + "\n" + "\n".join(record(tag, *values) for tag, values in rows)
        self.assertLess(len(raw), 1024 * 1024)
        policy = parse_policy_records(raw)
        self.assertEqual(len(policy["tests"]), 4096)
        self.assertTrue(all(row["properties"]["maxjobs"] == "64" for row in policy["tests"]))

    def test_context_transport_captures_policy_without_changing_other_identity(self):
        with tempfile.TemporaryDirectory() as root:
            view = os.path.join(root, "Lib", "TB", "maestro")
            os.makedirs(view)
            with open(os.path.join(view, "maestro.sdb"), "w") as stream:
                stream.write('<setupdb application="maestro"/>')
            raw = SCHEMA + "\n" + record("C", "Lib", "TB", "maestro", root, view, None, "/fixture/ocean") + "\n"
            raw += "\n".join(record(tag, *values) for tag, values in self.rows())
            context = parse_launch_records(raw)
            self.assertEqual(context["source"]["cell"], "TB")
            self.assertEqual(context["job_policy"]["source"], "invoking_session")
            self.assertEqual(context["job_policy"]["global"]["properties"]["maxjobs"], "64")
