import json
import os
import shlex
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from crescendo_engine import resources
from crescendo_engine.maestro_plugin import install_maestro_menu


class MaestroPluginInstallTests(unittest.TestCase):
    def test_native_resources_resolve_from_source(self):
        for name in ("crescendo_launch", "crescendo_maestro_menu.il"):
            self.assertTrue(os.path.isfile(resources.cadence_resource_path(name)))
        with self.assertRaises(ValueError):
            resources.cadence_resource_path("../crescendo_launch")

    def test_installer_preserves_project_startup_and_quotes_paths(self):
        with tempfile.TemporaryDirectory(prefix="crescendo plugin ' ") as root:
            startup = os.path.join(root, ".cdsinit")
            original = b"; retained user startup\n"
            with open(startup, "wb") as stream:
                stream.write(original)
            result = install_maestro_menu(os.path.join(root, "crescendo menu.il"))
            self.assertFalse(result["startup_changed"])
            self.assertEqual(stat.S_IMODE(os.stat(result["loader"]).st_mode), 0o600)
            self.assertEqual(stat.S_IMODE(os.stat(result["launcher"]).st_mode), 0o700)
            with open(startup, "rb") as stream:
                self.assertEqual(stream.read(), original)
            with open(result["loader"], encoding="utf-8") as stream:
                loader = stream.read()
            self.assertIn(json.dumps(result["launcher"]), loader)
            self.assertIn(json.dumps(result["menu_resource"]), loader)
            with open(result["launcher"], encoding="utf-8") as stream:
                wrapper = stream.read()
            self.assertIn("${CRESCENDO_PREFERRED_PYTHON+x}", wrapper)

    def test_existing_loader_or_wrapper_is_not_overwritten(self):
        for existing in ("menu.il", "menu.crescendo_launch"):
            with tempfile.TemporaryDirectory() as root:
                path = os.path.join(root, existing)
                with open(path, "w") as stream:
                    stream.write("retained")
                with self.assertRaisesRegex(ValueError, "overwrite"):
                    install_maestro_menu(os.path.join(root, "menu.il"))
                with open(path) as stream:
                    self.assertEqual(stream.read(), "retained")
                self.assertEqual(os.listdir(root), [existing])

    def test_symlink_target_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as root:
            loader = os.path.join(root, "menu.il")
            os.symlink(os.path.join(root, "missing.il"), loader)
            with self.assertRaisesRegex(ValueError, "overwrite"):
                install_maestro_menu(loader)
            self.assertTrue(os.path.islink(loader))

    def test_explicit_missing_interpreter_fails_without_creating_outputs(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(ValueError, "unavailable"):
                install_maestro_menu(os.path.join(root, "menu.il"), python=os.path.join(root, "no-python"))
            self.assertEqual(os.listdir(root), [])

    def test_successful_non_python_executable_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(ValueError, "Python version"):
                install_maestro_menu(os.path.join(root, "menu.il"), python="/bin/true")
            self.assertEqual(os.listdir(root), [])

    def test_python_alias_is_preserved_for_virtual_environments(self):
        with tempfile.TemporaryDirectory() as root:
            alias = os.path.join(root, "python alias")
            os.symlink(sys.executable, alias)
            result = install_maestro_menu(os.path.join(root, "menu.il"), python=alias)
            self.assertEqual(result["python"], alias)

    def test_wrapper_preserves_inherited_python_and_argument_boundaries(self):
        with tempfile.TemporaryDirectory(prefix="crescendo quoted ' ") as root:
            native_launcher = os.path.join(root, "native launcher")
            capture = os.path.join(root, "captured.json")
            with open(native_launcher, "w") as stream:
                stream.write("#!/bin/bash\nexec %s -c %s \"$@\"\n" % (
                    shlex.quote(sys.executable),
                    shlex.quote(
                        "import json, os, sys; json.dump({'python': os.environ.get('CRESCENDO_PYTHON'), "
                        "'preferred': os.environ.get('CRESCENDO_PREFERRED_PYTHON'), "
                        "'args': sys.argv[1:]}, open(os.environ['CRESCENDO_CAPTURE'], 'w'))"
                    ),
                ))
            os.chmod(native_launcher, 0o700)
            def resource(name):
                return native_launcher if name == "crescendo_launch" else resources.cadence_resource_path(name)
            with mock.patch("crescendo_engine.maestro_plugin.cadence_resource_path", side_effect=resource):
                result = install_maestro_menu(os.path.join(root, "menu.il"))
            for inherited in (None, "", "invalid-python"):
                for preferred in (None, "", "earlier-preference"):
                    environment = dict(os.environ, CRESCENDO_CAPTURE=capture)
                    # The actual editor is allowed to have no usable PATH.
                    environment["PATH"] = ""
                    for name, value in (("CRESCENDO_PYTHON", inherited),
                                        ("CRESCENDO_PREFERRED_PYTHON", preferred)):
                        if value is None:
                            environment.pop(name, None)
                        else:
                            environment[name] = value
                    context = os.path.join(root, "context ' a b")
                    subprocess.check_call([result["launcher"], context], env=environment)
                    with open(capture) as stream:
                        captured = json.load(stream)
                    self.assertEqual(captured["python"], inherited)
                    self.assertEqual(captured["preferred"], sys.executable if preferred is None else preferred)
                    self.assertEqual(captured["args"], [context])

    def test_failed_second_output_rolls_back_only_its_new_wrapper(self):
        from crescendo_engine import maestro_plugin
        with tempfile.TemporaryDirectory() as root:
            original = maestro_plugin._create_private_file
            count = [0]
            def create(path, text, mode):
                count[0] += 1
                if count[0] == 2:
                    raise OSError("failed loader")
                return original(path, text, mode)
            with mock.patch.object(maestro_plugin, "_create_private_file", side_effect=create):
                with self.assertRaisesRegex(OSError, "failed loader"):
                    install_maestro_menu(os.path.join(root, "menu.il"))
            self.assertEqual(os.listdir(root), [])


if __name__ == "__main__":
    unittest.main()
