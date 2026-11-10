import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.config import default_run_root, load_config
from crescendo_engine.resources import guardian_script_path, submit_spectre_script_path, web_root


def _write_executable(path):
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
        os.makedirs(parent)
    with open(path, "w") as stream:
        stream.write("#!/bin/sh\nexit 0\n")
    os.chmod(path, 0o755)


class PortableRuntimePathsTest(unittest.TestCase):
    def test_default_run_root_is_project_local_even_with_xdg_state_home(self):
        with tempfile.TemporaryDirectory() as root:
            state_home = os.path.join(root, "state")
            user_config = os.path.join(root, "config", "user.json")
            environment = {
                "HOME": os.path.join(root, "home"),
                "CRESCENDO_USER_CONFIG": user_config,
                "PATH": os.environ.get("PATH", ""),
                "XDG_STATE_HOME": state_home,
            }
            with mock.patch.dict(os.environ, environment, clear=True):
                config = load_config(persist_discovery=False, validate_discovery=False)
            self.assertEqual(
                config["runtime"]["run_root"],
                os.path.join(os.path.realpath(os.getcwd()), ".crescendo", "runs"),
            )

    def test_default_run_root_never_uses_home_for_persistent_runs(self):
        environment = {"HOME": "/tmp/crescendo-portable-home"}
        self.assertEqual(
            default_run_root(environment),
            os.path.join(os.path.realpath(os.getcwd()), ".crescendo", "runs"),
        )

    def test_environment_run_root_and_submit_script_resolve_from_cwd(self):
        with tempfile.TemporaryDirectory() as root:
            user_config = os.path.join(root, "user.json")
            submit_script = os.path.join(root, "bin", "submit.sh")
            _write_executable(submit_script)
            environment = {
                "HOME": os.path.join(root, "home"),
                "CRESCENDO_RUN_ROOT": "relative-runs",
                "CRESCENDO_SPECTRE_SUBMIT_SCRIPT": os.path.join("bin", "submit.sh"),
                "CRESCENDO_USER_CONFIG": user_config,
                "PATH": os.environ.get("PATH", ""),
            }
            with mock.patch.dict(os.environ, environment, clear=True), mock.patch(
                    "crescendo_engine.config.os.getcwd", return_value=root):
                config = load_config(persist_discovery=False, validate_discovery=False)
            self.assertEqual(config["runtime"]["run_root"], os.path.join(root, "relative-runs"))
            self.assertEqual(config["runtime"]["spectre_submit_script"], submit_script)

    def test_explicit_relative_paths_resolve_from_config_directory(self):
        with tempfile.TemporaryDirectory() as root:
            config_path = os.path.join(root, "config", "optimizer.json")
            submit_script = os.path.join(root, "config", "bin", "submit.sh")
            _write_executable(submit_script)
            with open(config_path, "w") as stream:
                json.dump({
                    "runtime": {
                        "run_root": "runs",
                        "spectre_submit_script": os.path.join("bin", "submit.sh"),
                    },
                }, stream)
            environment = {
                "HOME": os.path.join(root, "home"),
                "CRESCENDO_USER_CONFIG": os.path.join(root, "user.json"),
                "PATH": os.environ.get("PATH", ""),
            }
            with mock.patch.dict(os.environ, environment, clear=True):
                config = load_config(
                    config_path, persist_discovery=False, validate_discovery=False,
                )
            self.assertEqual(config["runtime"]["run_root"], os.path.join(root, "config", "runs"))
            self.assertEqual(config["runtime"]["spectre_submit_script"], submit_script)

    def test_runtime_resources_are_resolved_without_project_root(self):
        self.assertTrue(os.path.isdir(web_root()))
        self.assertTrue(os.path.isfile(submit_spectre_script_path()))
        self.assertTrue(os.path.isfile(guardian_script_path()))


if __name__ == "__main__":
    unittest.main()
