import copy
import os
import tempfile
import time
import unittest

from crescendo_engine.environment_identity import (
    check_external_eda_environment_metadata, collect_external_eda_environment,
    validate_external_eda_environment_identity,
)


class ExternalEdaIdentityTests(unittest.TestCase):
    def _fixture(self, root, moved=False):
        prefix = "moved" if moved else "original"
        base = os.path.join(root, prefix)
        os.makedirs(base)
        spectre_root = os.path.join(base, "spectre")
        engine = os.path.join(spectre_root, "tools.lnx86", "spectre", "bin", "64bit")
        os.makedirs(engine)
        launcher = os.path.join(base, "spectre-launcher")
        with open(launcher, "w") as stream:
            stream.write("#!/bin/sh\nexec spectre-real \"$@\"\n")
        os.chmod(launcher, 0o755)
        with open(os.path.join(engine, "spectre"), "wb") as stream:
            stream.write(b"ELF-spectre-engine")
        os.chmod(os.path.join(engine, "spectre"), 0o755)

        cds = os.path.join(base, "cds.lib")
        cds_child = os.path.join(base, "vendor.lib")
        with open(cds, "w") as stream:
            stream.write("INCLUDE vendor.lib\nDEFINE HH ./HH\n")
        with open(cds_child, "w") as stream:
            stream.write("DEFINE analogLib ./analogLib\n")
        cdsinit = os.path.join(base, ".cdsinit")
        startup = os.path.join(base, "startup.il")
        with open(cdsinit, "w") as stream:
            stream.write('load("startup.il")\n')
        with open(startup, "w") as stream:
            stream.write("procedure(test() t)\n")

        models = os.path.join(base, "models")
        os.makedirs(models)
        model = os.path.join(models, "root.scs")
        child = os.path.join(models, "child.scs")
        with open(model, "w") as stream:
            stream.write('include "child.scs" section=tt\n')
        with open(child, "w") as stream:
            stream.write("parameters vdd=1.8\n")

        pdk = os.path.join(base, "PDK")
        os.makedirs(pdk)
        with open(os.path.join(pdk, "tech.db"), "wb") as stream:
            stream.write(b"technology")

        maestro = os.path.join(base, "maestro")
        os.makedirs(maestro)
        sdb = os.path.join(maestro, "maestro.sdb")
        active = os.path.join(maestro, "active.state")
        with open(sdb, "w") as stream:
            stream.write(
                "<root><model><modelfile>%s</modelfile></model></root>" % model
            )
        with open(active, "w") as stream:
            stream.write("<root><field Name=\"includePath\">\"%s\"</field></root>" % models)
        setup = {"source": {"maestro_sdb": sdb, "active_state": active}}
        cadence = {
            "spectre_bin": launcher, "spectre_root": spectre_root,
            "cds_lib": cds, "pdk_dir": pdk,
            "startup_extensions": [startup],
        }
        pdk_context = {
            "pdk_name": "PDK", "pdk_dir": pdk, "source_cds_lib": cds,
        }
        return setup, cadence, pdk_context, child

    def test_identity_is_path_independent_and_content_bound(self):
        with tempfile.TemporaryDirectory() as root:
            first_setup, first_cadence, first_pdk, _first_child = self._fixture(root)
            moved_setup, moved_cadence, moved_pdk, moved_child = self._fixture(root, moved=True)
            first = collect_external_eda_environment(first_setup, first_cadence, first_pdk)
            moved = collect_external_eda_environment(moved_setup, moved_cadence, moved_pdk)
            self.assertEqual(first["identity"], moved["identity"])
            self.assertNotEqual(first["diagnostics"], moved["diagnostics"])
            with open(moved_child, "a") as stream:
                stream.write("parameters changed=1\n")
            changed = collect_external_eda_environment(moved_setup, moved_cadence, moved_pdk)
            self.assertNotEqual(
                first["identity"]["fingerprint"], changed["identity"]["fingerprint"],
            )

    def test_startup_file_absolute_paths_are_semantically_portable(self):
        with tempfile.TemporaryDirectory() as root:
            first_setup, first_cadence, first_pdk, _first_child = self._fixture(root)
            moved_setup, moved_cadence, moved_pdk, _moved_child = self._fixture(root, moved=True)
            for cadence in (first_cadence, moved_cadence):
                base = os.path.dirname(cadence["cds_lib"])
                with open(cadence["cds_lib"], "w") as stream:
                    stream.write("INCLUDE %s\nDEFINE HH %s\n" % (
                        os.path.join(base, "vendor.lib"), os.path.join(base, "HH"),
                    ))
                with open(os.path.join(base, ".cdsinit"), "w") as stream:
                    stream.write('load("%s")\n' % os.path.join(base, "startup.il"))
            first = collect_external_eda_environment(first_setup, first_cadence, first_pdk)
            moved = collect_external_eda_environment(moved_setup, moved_cadence, moved_pdk)
            self.assertEqual(first["identity"], moved["identity"])

    def test_identity_covers_required_external_artifacts(self):
        with tempfile.TemporaryDirectory() as root:
            setup, cadence, pdk, _child = self._fixture(root)
            submit_script = os.path.join(root, "submit_spectre.sh")
            with open(submit_script, "w") as stream:
                stream.write("#!/bin/sh\nexec spectre \"$@\"\n")
            os.chmod(submit_script, 0o755)
            result = collect_external_eda_environment(
                setup, cadence, pdk,
                submission={"submit_script": submit_script},
            )
            roles = [row["role"] for row in result["identity"]["artifacts"]]
            for role in (
                "simulator_executable", "simulator_engine", "source_cds_lib",
                "source_cds_lib_include", "startup_extension",
                "model_file", "model_dependency", "technology_file", "submit_script",
            ):
                self.assertIn(role, roles)
            self.assertFalse(result["identity"]["coverage"]["source_cdsinit"])
            self.assertEqual(result["identity"]["coverage"]["startup_extensions"], 1)
            serialized = repr(result["identity"])
            self.assertNotIn(root, serialized)

    def test_resolved_file_metadata_detects_changes_and_symlink_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            setup, cadence, pdk, child = self._fixture(root)
            result = collect_external_eda_environment(setup, cadence, pdk)
            unchanged = check_external_eda_environment_metadata(result["diagnostics"])
            self.assertTrue(unchanged["ok"])
            self.assertGreater(unchanged["checked_files"], 0)

            with open(child, "a") as stream:
                stream.write("parameters changed=1\n")
            changed = check_external_eda_environment_metadata(result["diagnostics"])
            self.assertFalse(changed["ok"])
            self.assertTrue(any(
                row.get("reason") in ("metadata_changed", "path_resolution_changed")
                for row in changed["changes"]
            ))

            target = os.path.join(root, "replacement.scs")
            with open(target, "w") as stream:
                stream.write("parameters replacement=1\n")
            os.unlink(child)
            os.symlink(target, child)
            unsafe = check_external_eda_environment_metadata(result["diagnostics"])
            self.assertTrue(unsafe["unsafe"])

    def test_same_length_rewrite_with_restored_mtime_is_detected_by_ctime(self):
        with tempfile.TemporaryDirectory() as root:
            setup, cadence, pdk, child = self._fixture(root)
            result = collect_external_eda_environment(setup, cadence, pdk)
            before = os.stat(child)
            with open(child, "r+b") as stream:
                content = stream.read()
                stream.seek(0)
                stream.write(content.replace(b"1.8", b"2.0"))
                stream.truncate()
            os.utime(child, ns=(
                int(getattr(before, "st_atime_ns", before.st_atime * 1000000000)),
                int(getattr(before, "st_mtime_ns", before.st_mtime * 1000000000)),
            ))
            observed = check_external_eda_environment_metadata(result["diagnostics"])
            self.assertFalse(observed["ok"])
            changed_rows = [
                row for row in observed["changes"]
                if row.get("path") == os.path.realpath(child)
            ]
            self.assertTrue(changed_rows)
            self.assertNotEqual(
                changed_rows[0]["before"]["target_lstat"]["ctime_ns"],
                changed_rows[0]["after"]["target_lstat"]["ctime_ns"],
            )

    def test_initial_symlink_repoint_is_detected_without_rejecting_initial_chain(self):
        with tempfile.TemporaryDirectory() as root:
            setup, cadence, pdk, child = self._fixture(root)
            first_target = child + ".first"
            second_target = child + ".second"
            os.rename(child, first_target)
            with open(second_target, "w") as stream:
                stream.write("parameters vdd=2.5\n")
            os.symlink(first_target, child)
            result = collect_external_eda_environment(setup, cadence, pdk)
            self.assertTrue(check_external_eda_environment_metadata(result["diagnostics"])["ok"])
            os.unlink(child)
            os.symlink(second_target, child)
            changed = check_external_eda_environment_metadata(result["diagnostics"])
            self.assertFalse(changed["ok"])
            self.assertFalse(changed["unsafe"])
            self.assertTrue(any(
                row.get("reason") == "path_resolution_changed"
                and row.get("before", {}).get("symlink_chain")
                for row in changed["changes"]
            ))

    def test_intermediate_symlink_chain_repoint_is_detected(self):
        with tempfile.TemporaryDirectory() as root:
            setup, cadence, pdk, child = self._fixture(root)
            first_target = child + ".first"
            second_target = child + ".second"
            middle = child + ".middle"
            os.rename(child, first_target)
            with open(second_target, "w") as stream:
                stream.write("parameters vdd=2.5\n")
            os.symlink(first_target, middle)
            os.symlink(middle, child)
            result = collect_external_eda_environment(setup, cadence, pdk)
            child_rows = [
                row for row in result["diagnostics"]["resolved_paths"]
                if row.get("reference_path") == child
            ]
            self.assertTrue(any(
                len(row.get("symlink_chain") or []) == 2 for row in child_rows
            ))
            os.unlink(middle)
            os.symlink(second_target, middle)
            changed = check_external_eda_environment_metadata(result["diagnostics"])
            self.assertFalse(changed["ok"])
            self.assertTrue(any(
                row.get("reason") == "path_resolution_changed"
                and len(row.get("before", {}).get("symlink_chain") or []) == 2
                for row in changed["changes"]
            ))

    def test_legacy_resolved_metadata_without_ctime_or_chain_remains_accepted(self):
        with tempfile.TemporaryDirectory() as root:
            setup, cadence, pdk, _child = self._fixture(root)
            diagnostics = copy.deepcopy(
                collect_external_eda_environment(setup, cadence, pdk)["diagnostics"]
            )
            diagnostics.pop("path_watch", None)
            for row in diagnostics["resolved_paths"]:
                row.pop("reference_path", None)
                row.pop("symlink_chain", None)
                row.pop("path_components", None)
                row["lstat"].pop("ctime_ns", None)
                row["lstat"].pop("directory", None)
            self.assertTrue(check_external_eda_environment_metadata(diagnostics)["ok"])

    def test_metadata_fast_path_for_many_files_stays_lightweight(self):
        with tempfile.TemporaryDirectory() as root:
            setup, cadence, pdk, child = self._fixture(root)
            model_root = os.path.join(os.path.dirname(child), "root.scs")
            with open(model_root, "a") as stream:
                for index in range(50):
                    dependency = os.path.join(
                        os.path.dirname(child), "dependency_%02d.scs" % index,
                    )
                    with open(dependency, "w") as dependency_stream:
                        dependency_stream.write("parameters value=%d\n" % index)
                    stream.write('include "%s"\n' % os.path.basename(dependency))
            diagnostics = collect_external_eda_environment(
                setup, cadence, pdk,
            )["diagnostics"]
            self.assertGreaterEqual(len(diagnostics["resolved_paths"]), 50)
            started = time.monotonic()
            iterations = 25
            for _index in range(iterations):
                self.assertTrue(
                    check_external_eda_environment_metadata(diagnostics)["ok"]
                )
            average_seconds = (time.monotonic() - started) / float(iterations)
            self.assertLess(average_seconds, 0.005)

    def test_unresolved_files_are_recorded_as_explainable_limitations(self):
        with tempfile.TemporaryDirectory() as root:
            setup, cadence, pdk, _child = self._fixture(root)
            broken = copy.deepcopy(setup)
            with open(broken["source"]["maestro_sdb"], "w") as stream:
                stream.write("<root><modelfile>/missing/model.scs</modelfile></root>")
            result = collect_external_eda_environment(broken, cadence, pdk)
            limitations = result["identity"]["limitations"]
            self.assertTrue(any(
                row["role"] == "model_file" and row["reason"] == "referenced_file_missing"
                for row in limitations
            ))
            self.assertTrue(all("/missing/model.scs" not in repr(row) for row in limitations))
            self.assertTrue(any(
                row.get("reference_basename") == "model.scs" for row in limitations
            ))
            self.assertTrue(any(
                row.get("reference") == "/missing/model.scs"
                for row in result["diagnostics"]["limitations"]
            ))

    def test_identity_payload_tampering_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            setup, cadence, pdk, _child = self._fixture(root)
            identity = collect_external_eda_environment(setup, cadence, pdk)["identity"]
            changed = copy.deepcopy(identity)
            changed["artifacts"][0]["size"] += 1
            with self.assertRaisesRegex(ValueError, "does not match"):
                validate_external_eda_environment_identity(changed)


if __name__ == "__main__":
    unittest.main()
