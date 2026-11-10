from __future__ import print_function

import json
import os
import signal
import stat
import tempfile
import unittest
from unittest import mock

from crescendo_engine.cleanroom import (
    SCHEMA_MANIFEST,
    CleanroomError,
    _clean_environment,
    _cadence_runtime_preflight,
    _case_command,
    _cleanup_owned,
    _copy_relocated_tree,
    _identity_alive,
    _prepared_source_leak_view,
    _tree_fingerprint,
    _write_runtime_files,
    build_plan,
    main,
    normalize_manifest,
    validate_work_root,
)


def _restore_test_tree_owner_write(path):
    for current, directories, files in os.walk(path, topdown=True, followlinks=False):
        current_mode = stat.S_IMODE(os.stat(current).st_mode)
        os.chmod(current, current_mode | 0o700)
        for name in directories:
            candidate = os.path.join(current, name)
            if not os.path.islink(candidate):
                mode = stat.S_IMODE(os.stat(candidate).st_mode)
                os.chmod(candidate, mode | 0o700)
        for name in files:
            candidate = os.path.join(current, name)
            if not os.path.islink(candidate):
                mode = stat.S_IMODE(os.stat(candidate).st_mode)
                os.chmod(candidate, mode | 0o600)


class CleanroomFixture(object):
    def __init__(self, root):
        self.root = root
        self.design = os.path.join(root, "design")
        self.pdk = os.path.join(root, "pdk")
        self.install = os.path.join(root, "cadence", "share", "cdssetup")
        os.makedirs(os.path.join(self.design, "TB", "maestro"))
        os.makedirs(os.path.join(self.design, "TB", "adexl"))
        os.makedirs(os.path.join(self.pdk, "models"))
        os.makedirs(self.install)
        with open(os.path.join(self.design, "TB", "maestro", "maestro.sdb"), "w") as stream:
            stream.write("<setupdb><active><tests/></active></setupdb>\n")
        with open(os.path.join(self.design, "TB", "adexl", "data.sdb"), "w") as stream:
            stream.write('<setupdb application="ADE XL"><active><tests/></active></setupdb>\n')
        with open(os.path.join(self.pdk, "models", "model.scs"), "w") as stream:
            stream.write("simulator lang=spectre\n")
        with open(os.path.join(self.install, "cds.lib"), "w") as stream:
            stream.write("# installation\n")
        self.cds_lib = os.path.join(root, "cds.lib")
        with open(self.cds_lib, "w") as stream:
            stream.write("SOFTINCLUDE %s\n" % os.path.join(self.install, "cds.lib"))
            stream.write("DEFINE DESIGN %s\n" % self.design)
            stream.write("DEFINE DEVICE %s\n" % self.pdk)
        self.wheel = os.path.join(root, "optimizer.whl")
        with open(self.wheel, "wb") as stream:
            stream.write(b"wheel")
        self.manifest_path = os.path.join(root, "manifest.json")
        self.document = {
            "schema_version": SCHEMA_MANIFEST,
            "source_cds_lib": self.cds_lib,
            "cadence": {
                "search_roots": [os.path.join(root, "cadence")],
                "installation_cds_lib": os.path.join(self.install, "cds.lib"),
            },
            "design_libraries": [{
                "id": "design-a", "name": "DESIGN", "source_root": self.design,
                "pdk_ids": ["pdk-a"],
                "setups": [{
                    "id": "case-a", "backend": "maestro",
                    "relative_path": "TB/maestro/maestro.sdb",
                }],
            }],
            "pdks": [{
                "id": "pdk-a", "name": "Portable PDK", "source_root": self.pdk,
                "reference_libraries": ["DEVICE"],
                "technology_libraries": ["DEVICE"],
                "model_roots": ["models"],
            }],
        }
        with open(self.manifest_path, "w") as stream:
            json.dump(self.document, stream)

    def normalized(self):
        return normalize_manifest(self.document, self.manifest_path)


class ManifestSafetyTests(unittest.TestCase):
    def test_normalizes_dual_role_pdk_library_and_relative_model_root(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = CleanroomFixture(root)
            manifest = fixture.normalized()
            self.assertEqual(manifest["pdks"][0]["reference_libraries"], ["DEVICE"])
            self.assertEqual(manifest["pdks"][0]["technology_libraries"], ["DEVICE"])
            self.assertEqual(manifest["pdks"][0]["libraries"], [{"name": "DEVICE", "relative": "."}])
            self.assertEqual(manifest["pdks"][0]["model_roots"][0]["relative"], "models")

    def test_accepts_adexl_data_sdb(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = CleanroomFixture(root)
            fixture.document["design_libraries"][0]["setups"][0].update({
                "backend": "adexl", "relative_path": "TB/adexl/data.sdb",
            })
            manifest = normalize_manifest(fixture.document, fixture.manifest_path)
            self.assertEqual(manifest["design_libraries"][0]["setups"][0]["backend"], "adexl")

    def test_rejects_backend_specific_setup_filename_mismatch(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = CleanroomFixture(root)
            fixture.document["design_libraries"][0]["setups"][0].update({
                "backend": "adexl", "relative_path": "TB/maestro/maestro.sdb",
            })
            with self.assertRaisesRegex(CleanroomError, "must end in data.sdb"):
                normalize_manifest(fixture.document, fixture.manifest_path)

    def test_rejects_unknown_ade_backend(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = CleanroomFixture(root)
            fixture.document["design_libraries"][0]["setups"][0]["backend"] = "unknown"
            with self.assertRaisesRegex(CleanroomError, "unsupported ADE setup backend"):
                normalize_manifest(fixture.document, fixture.manifest_path)

    def test_rejects_traversal_setup_path(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = CleanroomFixture(root)
            fixture.document["design_libraries"][0]["setups"][0]["relative_path"] = "../maestro.sdb"
            with self.assertRaisesRegex(CleanroomError, "unsafe path component"):
                normalize_manifest(fixture.document, fixture.manifest_path)

    def test_rejects_pdk_library_outside_declared_source(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = CleanroomFixture(root)
            fixture.document["pdks"][0]["source_root"] = os.path.join(root, "other")
            os.makedirs(fixture.document["pdks"][0]["source_root"])
            with self.assertRaisesRegex(CleanroomError, "not mapped beneath"):
                normalize_manifest(fixture.document, fixture.manifest_path)


class WorkRootSafetyTests(unittest.TestCase):
    def test_requires_fresh_prefixed_root_under_bounded_parent(self):
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "source")
            os.makedirs(source)
            work = os.path.join(parent, "crescendo-cleanroom-new")
            self.assertEqual(validate_work_root(work, [source], [parent]), work)
            os.makedirs(work)
            with self.assertRaisesRegex(CleanroomError, "must not already exist"):
                validate_work_root(work, [source], [parent])

    def test_rejects_overlap_and_unprefixed_destination(self):
        with tempfile.TemporaryDirectory() as parent:
            source = os.path.join(parent, "source")
            os.makedirs(source)
            with self.assertRaisesRegex(CleanroomError, "overlaps protected source"):
                validate_work_root(os.path.join(source, "crescendo-cleanroom-new"), [source], [parent])
            with self.assertRaisesRegex(CleanroomError, "basename must start"):
                validate_work_root(os.path.join(parent, "unsafe"), [source], [parent])


class RelocationTests(unittest.TestCase):
    def test_prepare_leak_view_allows_only_expected_diagnostics_and_remap_source(self):
        source = "/protected/pdk"
        payload = {
            "setup": {"path": "/relocated/design/maestro.sdb"},
            "eda_context": {
                "external_environment_diagnostics": {"path_watch": [{"path": source}]},
                "pdk": {"contexts": [{"model_path_remaps": [{
                    "source": source, "destination": "/relocated/pdk",
                }]}]},
            },
        }
        sanitized = _prepared_source_leak_view(payload)
        self.assertNotIn(source, json.dumps(sanitized, sort_keys=True))
        payload["setup"]["path"] = source + "/design/maestro.sdb"
        sanitized = _prepared_source_leak_view(payload)
        self.assertIn(source, json.dumps(sanitized, sort_keys=True))

    @mock.patch("crescendo_engine.cleanroom.DEFAULT_WORK_PARENTS", (tempfile.gettempdir(),))
    def test_runtime_files_are_relocated_and_source_paths_are_not_in_cds_lib(self):
        with tempfile.TemporaryDirectory() as fixture_root, tempfile.TemporaryDirectory() as parent:
            fixture = CleanroomFixture(fixture_root)
            work = os.path.join(parent, "crescendo-cleanroom-runtime")
            plan = build_plan(fixture.normalized(), fixture.wheel, work)
            os.makedirs(work, 0o700)
            try:
                _write_runtime_files(plan)
                with open(plan["generated"]["cds_lib"], "r") as stream:
                    generated = stream.read()
                self.assertNotIn(fixture.design, generated)
                self.assertNotIn(fixture.pdk, generated)
                self.assertIn(os.path.join(work, "relocated", "designs", "design-a"), generated)
                self.assertIn(os.path.join(work, "relocated", "pdks", "pdk-a"), generated)
                relocated_file = os.path.join(work, "relocated", "pdks", "pdk-a", "models", "model.scs")
                self.assertEqual(stat.S_IMODE(os.stat(relocated_file).st_mode) & 0o222, 0)
                self.assertEqual(
                    stat.S_IMODE(os.stat(os.path.join(work, "relocated")).st_mode), 0o555,
                )
                with open(plan["generated"]["config"], "r") as stream:
                    config = json.load(stream)
                profile = config["cadence"]["pdk_profiles"][0]
                self.assertTrue(profile["package_root"].startswith(work + os.sep))
                self.assertTrue(profile["model_roots"][0].startswith(work + os.sep))
                self.assertEqual(profile["model_path_remaps"], [{
                    "source": fixture.pdk,
                    "destination": os.path.join(work, "relocated", "pdks", "pdk-a"),
                }])
            finally:
                _restore_test_tree_owner_write(work)

    def test_copy_rejects_external_symlink(self):
        with tempfile.TemporaryDirectory() as root:
            source = os.path.join(root, "source")
            destination = os.path.join(root, "destination")
            os.makedirs(source)
            outside = os.path.join(root, "outside")
            with open(outside, "w") as stream:
                stream.write("outside")
            os.symlink(outside, os.path.join(source, "escape"))
            with self.assertRaisesRegex(CleanroomError, "external symlink"):
                _copy_relocated_tree(source, destination)

    def test_tree_fingerprint_detects_content_change(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "file")
            with open(path, "w") as stream:
                stream.write("first")
            before = _tree_fingerprint(root)
            with open(path, "w") as stream:
                stream.write("other")
            after = _tree_fingerprint(root)
            self.assertNotEqual(before["sha256"], after["sha256"])


class EnvironmentAndCleanupTests(unittest.TestCase):
    @mock.patch("crescendo_engine.cleanroom.DEFAULT_WORK_PARENTS", (tempfile.gettempdir(),))
    def test_child_environment_is_allowlisted_not_parent_copy(self):
        with tempfile.TemporaryDirectory() as fixture_root, tempfile.TemporaryDirectory() as parent:
            fixture = CleanroomFixture(fixture_root)
            work = os.path.join(parent, "crescendo-cleanroom-env")
            plan = build_plan(fixture.normalized(), fixture.wheel, work)
            plan["generated"] = {"cds_lib": os.path.join(work, "input", "cds.lib")}
            with mock.patch.dict(os.environ, {
                    "PYTHONPATH": "/source/leak", "LD_LIBRARY_PATH": "/source/runtime",
            }, clear=False):
                environment = _clean_environment(plan, "token-a")
            self.assertNotIn("PYTHONPATH", environment)
            self.assertNotIn("LD_LIBRARY_PATH", environment)
            self.assertEqual(environment["CRESCENDO_CLEANROOM_TOKEN"], "token-a")
            self.assertEqual(environment["HOME"], os.path.join(work, "home"))

    def test_cadence_runtime_preflight_uses_discovered_root_relatives(self):
        with tempfile.TemporaryDirectory() as root:
            regular = os.path.join(root, "tools.lnx86", "dfII", "bin")
            bits64 = os.path.join(regular, "64bit")
            os.makedirs(bits64)
            for path in (os.path.join(regular, "cpDataDir"), os.path.join(bits64, "psf")):
                with open(path, "w") as stream:
                    stream.write("#!/bin/sh\nexit 0\n")
                os.chmod(path, 0o755)
            doctor = {"config": {"cadence": {"cadence_root": root}}}
            environment, result = _cadence_runtime_preflight(doctor, {"PATH": "/usr/bin:/bin"})
            self.assertTrue(result["ok"])
            self.assertEqual(result["path_additions"], [regular, bits64])
            self.assertTrue(environment["PATH"].startswith(regular + os.pathsep + bits64))
            self.assertEqual([row["name"] for row in result["tools"]], ["cpDataDir", "psf"])
            self.assertEqual(result["tools"][0]["path"], os.path.join(regular, "cpDataDir"))
            self.assertEqual(result["tools"][1]["path"], os.path.join(bits64, "psf"))

    def test_cadence_runtime_preflight_fails_closed_when_tool_missing(self):
        with tempfile.TemporaryDirectory() as root:
            regular = os.path.join(root, "tools.lnx86", "dfII", "bin")
            os.makedirs(regular)
            with open(os.path.join(regular, "cpDataDir"), "w") as stream:
                stream.write("#!/bin/sh\nexit 0\n")
            os.chmod(os.path.join(regular, "cpDataDir"), 0o755)
            doctor = {"config": {"cadence": {"cadence_root": root}}}
            with self.assertRaisesRegex(CleanroomError, "psf"):
                _cadence_runtime_preflight(doctor, {"PATH": "/usr/bin:/bin"})

    def test_cleanup_signals_only_exact_observed_identity(self):
        owned = {"pid": 41001, "start_ticks": 7001, "pgrp": 41001}
        foreign = {"pid": 41002, "start_ticks": 7002, "pgrp": 41002}
        signals = []

        def alive(identity):
            return identity == owned and not signals

        with mock.patch("crescendo_engine.cleanroom._token_processes", return_value=[owned]), \
                mock.patch("crescendo_engine.cleanroom._identity_alive", side_effect=alive), \
                mock.patch("crescendo_engine.cleanroom.os.kill", side_effect=lambda pid, sig: signals.append((pid, sig))), \
                mock.patch("crescendo_engine.cleanroom.time.sleep"):
            result = _cleanup_owned("token-a", [foreign], grace_seconds=0)
        self.assertEqual(signals, [(owned["pid"], signal.SIGTERM)])
        self.assertTrue(result["ok"])


class DryRunTests(unittest.TestCase):
    @mock.patch("crescendo_engine.cleanroom.DEFAULT_WORK_PARENTS", (tempfile.gettempdir(),))
    def test_plan_and_child_commands_preserve_each_setup_backend(self):
        with tempfile.TemporaryDirectory() as fixture_root, tempfile.TemporaryDirectory() as parent:
            fixture = CleanroomFixture(fixture_root)
            fixture.document["design_libraries"][0]["setups"].append({
                "id": "case-adexl", "backend": "adexl",
                "relative_path": "TB/adexl/data.sdb",
            })
            work = os.path.join(parent, "crescendo-cleanroom-backends")
            plan = build_plan(fixture.normalized(), fixture.wheel, work)
            plan["generated"] = {"config": os.path.join(work, "input", "optimizer.json")}
            self.assertEqual([row["backend"] for row in plan["cases"]], ["maestro", "adexl"])
            for case in plan["cases"]:
                inspect = _case_command(plan, case, "inspect")
                prepare = _case_command(plan, case, "prepare")
                self.assertEqual(inspect[-2:], ["--backend", case["backend"]])
                self.assertEqual(prepare[-2:], ["--backend", case["backend"]])

    @mock.patch("crescendo_engine.cleanroom.DEFAULT_WORK_PARENTS", (tempfile.gettempdir(),))
    def test_cli_defaults_to_dry_run_without_creating_work_root(self):
        with tempfile.TemporaryDirectory() as fixture_root, tempfile.TemporaryDirectory() as parent:
            fixture = CleanroomFixture(fixture_root)
            work = os.path.join(parent, "crescendo-cleanroom-dry")
            with mock.patch("builtins.print") as output:
                returncode = main([
                    fixture.manifest_path, "--wheel", fixture.wheel,
                    "--work-root", work,
                ])
            self.assertEqual(returncode, 0)
            self.assertFalse(os.path.exists(work))
            payload = json.loads(output.call_args[0][0])
            self.assertTrue(payload["dry_run"])
            self.assertEqual(payload["cases"][0]["backend"], "maestro")


if __name__ == "__main__":
    unittest.main()
