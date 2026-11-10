from __future__ import print_function

import hashlib
import os
import tempfile
import unittest

from crescendo_engine.model_paths import (
    ModelPathRemapError, discover_setup_path_references,
    remap_workspace_setup_paths, resolve_relocated_reference,
)


class ModelPathRemapTest(unittest.TestCase):
    @staticmethod
    def _digest(path):
        with open(path, "rb") as stream:
            return hashlib.sha256(stream.read()).hexdigest()

    def test_resolves_relocated_model_by_profile_root(self):
        with tempfile.TemporaryDirectory() as root:
            relocated = os.path.join(root, "pdk", "kit", "spectre", "device.scs")
            os.makedirs(os.path.dirname(relocated))
            with open(relocated, "w") as stream:
                stream.write("model")
            resolved = resolve_relocated_reference(
                "/old/vendor/kit/spectre/device.scs", [os.path.join(root, "pdk")],
            )
            self.assertEqual(resolved["path"], relocated)

    def test_ambiguous_relocated_model_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            for directory in ("a", "b"):
                path = os.path.join(root, directory, "device.scs")
                os.makedirs(os.path.dirname(path))
                with open(path, "w") as stream:
                    stream.write(directory)
            with self.assertRaisesRegex(ModelPathRemapError, "ambiguous"):
                resolve_relocated_reference("/old/device.scs", [root])

    def test_missing_relocated_model_reports_configuration_hint(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(ModelPathRemapError, "model_roots"):
                resolve_relocated_reference(
                    "/old/vendor/kit/spectre/missing.scs", [root],
                )

    def test_explicit_remap_overrides_an_existing_old_model(self):
        with tempfile.TemporaryDirectory() as root:
            old_root = os.path.join(root, "installed-old")
            new_root = os.path.join(root, "installed-new")
            relative = os.path.join("kit", "spectre", "device.scs")
            old_model = os.path.join(old_root, relative)
            new_model = os.path.join(new_root, relative)
            for path, text in ((old_model, "old"), (new_model, "new")):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w") as stream:
                    stream.write(text)
            resolved = resolve_relocated_reference(
                old_model, [old_root, new_root], remaps=[(old_root, new_root)],
            )
            self.assertEqual(resolved["path"], new_model)
            self.assertEqual(resolved["mode"], "explicit_remap")

    def test_rewrites_only_private_setup_copy(self):
        with tempfile.TemporaryDirectory() as root:
            source_reference = "/old/vendor/kit/spectre/device.scs"
            model = os.path.join(root, "pdk", "kit", "spectre", "device.scs")
            os.makedirs(os.path.dirname(model))
            with open(model, "w") as stream:
                stream.write("model")
            workspace_root = os.path.join(root, "workspace")
            setup_dir = os.path.join(workspace_root, "libraries", "D", "TB", "maestro")
            os.makedirs(setup_dir)
            source_setup = os.path.join(root, "source", "maestro.sdb")
            os.makedirs(os.path.dirname(source_setup))
            with open(source_setup, "w") as stream:
                stream.write(
                    '<Root><Field Name="modelFiles">"%s"</Field></Root>'
                    % source_reference
                )
            source_digest = self._digest(source_setup)
            setup_sdb = os.path.join(setup_dir, "maestro.sdb")
            with open(source_setup) as source, open(setup_sdb, "w") as stream:
                stream.write(source.read())
            workspace = {
                "root": workspace_root, "setup_dir": setup_dir,
                "setup_sdb": setup_sdb,
            }
            config = {"cadence": {"pdk_dir": os.path.join(root, "pdk")}}
            result = remap_workspace_setup_paths(workspace, config)
            self.assertEqual(result["changed_files"], [setup_sdb])
            with open(setup_sdb) as stream:
                self.assertIn(model, stream.read())
            self.assertEqual(self._digest(source_setup), source_digest)

    def test_active_pdk_profile_selects_between_same_named_models(self):
        with tempfile.TemporaryDirectory() as root:
            pdk_a = os.path.join(root, "pdk-a")
            pdk_b = os.path.join(root, "pdk-b")
            relative = os.path.join("kit", "spectre", "device.scs")
            for pdk_dir, text in ((pdk_a, "a"), (pdk_b, "b")):
                model = os.path.join(pdk_dir, relative)
                os.makedirs(os.path.dirname(model))
                with open(model, "w") as stream:
                    stream.write(text)
            workspace_root = os.path.join(root, "workspace")
            setup_dir = os.path.join(workspace_root, "libraries", "D", "TB", "maestro")
            os.makedirs(setup_dir)
            setup_sdb = os.path.join(setup_dir, "maestro.sdb")
            with open(setup_sdb, "w") as stream:
                stream.write(
                    '<Root><Field Name="modelFiles">"%s"</Field></Root>'
                    % os.path.join("/old/pdk", relative)
                )
            config = {"cadence": {"pdk_profiles": [{
                "name": "PDK-A", "reference_libraries": ["DEV_A"],
                "pdk_dir": pdk_a,
            }, {
                "name": "PDK-B", "reference_libraries": ["DEV_B"],
                "pdk_dir": pdk_b,
            }]}}
            result = remap_workspace_setup_paths(
                {"root": workspace_root, "setup_dir": setup_dir, "setup_sdb": setup_sdb},
                config, pdk_context={"candidate_libraries": ["DEV_B"]},
            )
            self.assertEqual(result["references"][0]["resolved"], os.path.join(pdk_b, relative))

    def test_discovers_include_directory_field(self):
        with tempfile.TemporaryDirectory() as root:
            setup_sdb = os.path.join(root, "maestro.sdb")
            with open(setup_sdb, "w") as stream:
                stream.write('<Root><Field Name="includePath">"/pdk/include"</Field></Root>')
            result = discover_setup_path_references(root, setup_sdb=setup_sdb)
            self.assertTrue(result["references"][0]["directory"])

    def test_private_project_models_are_valid_with_external_pdk_dirs(self):
        with tempfile.TemporaryDirectory() as root:
            private = os.path.join(root, "workspace")
            library = os.path.join(private, "libraries", "D")
            setup_dir = os.path.join(library, "SETUP", "maestro")
            os.makedirs(setup_dir)
            model = os.path.join(library, "kit", "startup.scs")
            os.makedirs(os.path.dirname(model))
            with open(model, "w") as stream:
                stream.write("ic OUTP=0.01\n")
            setup_sdb = os.path.join(setup_dir, "maestro.sdb")
            with open(setup_sdb, "w") as stream:
                stream.write('<Root><Field Name="modelFiles">"%s"</Field></Root>' % model)
            workspace = {"root": private, "setup_dir": setup_dir, "setup_sdb": setup_sdb,
                         "copied_libraries": {"D": library}}
            result = remap_workspace_setup_paths(workspace, {"cadence": {"pdk_dir": os.path.join(root, "pdk")}})
            self.assertEqual(result["references"][0]["resolved"], model)
            self.assertEqual(result["references"][0]["resolution"], "private_library")
            # A symlink cannot expand the trusted private model boundary.
            external = os.path.join(root, "external.scs")
            with open(external, "w") as stream:
                stream.write("external")
            os.remove(model)
            os.symlink(external, model)
            with self.assertRaises(ModelPathRemapError):
                remap_workspace_setup_paths(workspace, {"cadence": {"pdk_dir": os.path.join(root, "pdk")}})


if __name__ == "__main__":
    unittest.main()
