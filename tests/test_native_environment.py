import copy
import json
import os
import tempfile
import runpy
import subprocess
import sys
import unittest
from unittest import mock

from crescendo_engine.cadence import child_environment, guardian_environment
from crescendo_engine.config import DEFAULTS, configure_eda, load_config
from crescendo_engine.maestro_context import parse_launch_records, bind_launch_config
from crescendo_engine.native_environment import (
    GUI_ENV_NAMES, HOME_NATIVE_ENV_NAMES, MODEL_ENV_NAMES, NATIVE_ENV_NAMES, NATIVE_ENV_SCHEMA, NATIVE_ENV_SNAPSHOT_VAR,
    clean_gui_environment, native_snapshot_warnings, parse_native_environment_snapshot,
    restore_native_environment, validate_native_values,
)
from crescendo_engine.persistent_controller import _safe_controller_environment
from crescendo_engine.persistent_maestro import _guardian_environment
from crescendo_engine.resources import guardian_script_path


def serialized(values):
    return json.dumps({"schema": NATIVE_ENV_SCHEMA, "values": values})


class NativeEnvironmentTests(unittest.TestCase):
    def values(self, state=None):
        return dict((name, state) for name in NATIVE_ENV_NAMES)

    def test_all_environment_states_restore_without_mutating_caller(self):
        caller = dict((name, "GUI_INJECTED") for name in NATIVE_ENV_NAMES)
        caller.update(DISPLAY=":owned", XAUTHORITY="authentication")
        before = dict(caller)
        for state in (None, "", "NATIVE_SOURCE"):
            with self.subTest(state=state):
                restored = restore_native_environment(caller, self.values(state))
                for name in NATIVE_ENV_NAMES:
                    if state is None:
                        self.assertNotIn(name, restored)
                    else:
                        self.assertEqual(restored[name], state)
                self.assertEqual(restored["DISPLAY"], ":owned")
                self.assertEqual(restored["XAUTHORITY"], "authentication")
                self.assertEqual(caller, before)

    def test_legacy_partial_snapshot_clears_unknown_gui_overrides_with_warning(self):
        caller = dict((name, "GUI_INJECTED") for name in NATIVE_ENV_NAMES)
        partial = {"LD_LIBRARY_PATH": "/native/lib", "PATH": "/native/bin"}
        restored = restore_native_environment(caller, partial)
        self.assertEqual(restored["LD_LIBRARY_PATH"], "/native/lib")
        for name in GUI_ENV_NAMES:
            if name != "LD_LIBRARY_PATH":
                self.assertNotIn(name, restored)
        self.assertEqual(restored["PATH"], "/native/bin")
        self.assertIn("Reopen Crescendo", native_snapshot_warnings(partial)[0])
        self.assertFalse(native_snapshot_warnings(self.values()))

    def test_second_launch_preserves_first_snapshot_over_gui_injection(self):
        values = self.values(None)
        values.update(PATH="/original/bin", LD_LIBRARY_PATH="", TK_LIBRARY="/native/tk")
        first = restore_native_environment({"DISPLAY": ":owned"}, values)
        second_caller = dict(first)
        second_caller.update(dict((name, "GUI_INJECTED") for name in GUI_ENV_NAMES))
        second = restore_native_environment(second_caller, values)
        self.assertEqual(second, first)
        self.assertEqual(values["LD_LIBRARY_PATH"], "")

    def test_model_environment_states_are_frozen_without_borrowing_gui_values(self):
        for state in (None, "", "/native/model root"):
            values = self.values(None)
            values.update({name: state for name in MODEL_ENV_NAMES})
            caller = {name: "/gui/model root" for name in MODEL_ENV_NAMES}
            with self.subTest(state=state):
                restored = restore_native_environment(caller, values)
                for name in MODEL_ENV_NAMES:
                    if state is None:
                        self.assertNotIn(name, restored)
                    else:
                        self.assertEqual(restored[name], state)

    def test_previous_complete_home_snapshot_remains_accepted_with_model_warning(self):
        values = {name: None for name in HOME_NATIVE_ENV_NAMES}
        self.assertEqual(parse_native_environment_snapshot(serialized(values)), values)
        warning = native_snapshot_warnings(values)[0]
        self.assertIn("PDK", warning)
        self.assertIn("CRESCENDO_ALPS_MODEL_DIR", warning)

    def test_snapshot_without_context_preserves_standalone_contract(self):
        caller = {"PATH": "/existing/bin", "LD_LIBRARY_PATH": "/existing/lib"}
        self.assertEqual(restore_native_environment(caller, None), caller)

    def test_private_snapshot_schema_and_values_are_strict(self):
        values = self.values()
        self.assertEqual(parse_native_environment_snapshot(serialized(values)), values)
        invalid = ["", "not-json", serialized({"PATH": "/partial"}),
                   json.dumps({"schema": "wrong", "values": values}),
                   json.dumps({"schema": NATIVE_ENV_SCHEMA, "values": values, "unexpected": True})]
        for raw in invalid:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_native_environment_snapshot(raw)
        for value in (3, True, [], "bad\nvalue"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_native_values({"PATH": value})
        with self.assertRaises(ValueError):
            validate_native_values({"UNSUPPORTED_ENV": "/unknown"})

    def test_python_controller_clears_overrides_then_adds_its_own_import_root(self):
        environ = dict((name, "GUI_INJECTED") for name in GUI_ENV_NAMES)
        environ.update(DISPLAY=":owned", PATH="/usr/bin")
        with mock.patch.dict(os.environ, environ, clear=True):
            child = _safe_controller_environment("/moved/project", 4, "/owned/liveness.lock")
            self.assertEqual(dict(os.environ), environ)
        self.assertEqual(child["PYTHONPATH"], "/moved/project")
        for name in GUI_ENV_NAMES:
            if name != "PYTHONPATH":
                self.assertNotIn(name, child)
        self.assertEqual(child["DISPLAY"], ":owned")

    def test_eda_child_uses_snapshot_and_explicit_tool_paths_without_gui_leakage(self):
        for state in (None, "", "/native/lib"):
            snapshot = self.values(None)
            snapshot.update(LD_LIBRARY_PATH=state, PATH="/native/bin")
            config = {"cadence": {"launch_environment": snapshot}}
            caller = dict((name, "GUI_INJECTED") for name in NATIVE_ENV_NAMES)
            with self.subTest(state=state), mock.patch.dict(os.environ, caller, clear=True):
                child = child_environment(config, "/private/home")
                self.assertEqual(dict(os.environ), caller)
            self.assertEqual(child["PATH"], "/native/bin")
            if state is None:
                self.assertNotIn("LD_LIBRARY_PATH", child)
            else:
                self.assertEqual(child["LD_LIBRARY_PATH"], state)
            for name in GUI_ENV_NAMES:
                if name != "LD_LIBRARY_PATH":
                    self.assertNotIn(name, child)
            self.assertNotIn("CDS_LIC_FILE", child)
            self.assertNotIn("LM_LICENSE_FILE", child)
        config["cadence"].update(extra_binary_paths=["/explicit/bin"], extra_library_paths=["/explicit/lib"])
        with mock.patch.dict(os.environ, caller, clear=True):
            child = child_environment(config, "/private/home")
        self.assertTrue(child["PATH"].startswith("/explicit/bin:"))
        self.assertTrue(child["LD_LIBRARY_PATH"].startswith("/explicit/lib:"))

    def test_configuration_discovery_and_children_share_the_captured_environment(self):
        with tempfile.TemporaryDirectory() as root:
            values = self.values(None)
            values.update(PATH="/native/bin", LD_LIBRARY_PATH="")
            caller = dict((name, "GUI_INJECTED") for name in GUI_ENV_NAMES)
            caller.update(PATH="/gui/bin", CRESCENDO_USER_CONFIG=os.path.join(root, "config.json"),
                          CRESCENDO_RUN_ROOT=os.path.join(root, "runtime"))
            caller[NATIVE_ENV_SNAPSHOT_VAR] = serialized(values)
            discovery = {"effective": {}, "sources": {}, "ok": False, "recommendations": []}
            with mock.patch.dict(os.environ, caller, clear=True), mock.patch(
                    "crescendo_engine.config.discover_eda_environment", return_value=discovery) as discover:
                config = load_config(persist_discovery=False, validate_discovery=False)
                used = discover.call_args[1]["environ"]
                self.assertEqual(used["PATH"], "/native/bin")
                self.assertEqual(used["LD_LIBRARY_PATH"], "")
                for name in GUI_ENV_NAMES:
                    if name != "LD_LIBRARY_PATH":
                        self.assertNotIn(name, used)
                self.assertEqual(config["cadence"]["launch_environment"], values)
                child = child_environment(config, root)
                self.assertEqual(child["LD_LIBRARY_PATH"], "")
                self.assertEqual(child["PATH"], "/native/bin")
                self.assertEqual(dict(os.environ), caller)
            self.assertFalse(os.path.exists(caller["CRESCENDO_USER_CONFIG"]))

    def test_all_python_guardian_bridges_restore_the_native_seven_variable_states(self):
        helper = runpy.run_path(guardian_script_path(), run_name="cr_guardian_test")
        self.assertEqual(tuple(helper["CHILD_ENVIRONMENT_KEYS"]), GUI_ENV_NAMES)
        for bridge in (guardian_environment, _guardian_environment):
            for value in (None, "", "/native/dependency"):
                native = {"PATH": os.defpath, "DISPLAY": ":owned"}
                if value is not None:
                    native.update(dict((name, value) for name in GUI_ENV_NAMES))
                parent = bridge(native)
                for name in GUI_ENV_NAMES:
                    self.assertNotIn(name, parent)
                with mock.patch.dict(os.environ, parent, clear=True):
                    child = helper["_child_environment"]()
                self.assertEqual(child, native)
                self.assertFalse(any(name.startswith("CRESCENDO_GUARDIAN_CHILD_") for name in child))

    def test_real_python_guardian_bootstrap_survives_native_pythonhome_and_restores_it(self):
        native = {"PATH": os.defpath, "PYTHONHOME": "/native/incompatible-python-home",
                  "PYTHONPATH": "/native/modules", "TCL_LIBRARY": "", "TK_LIBRARY": ""}
        safe = guardian_environment(native)
        code = ("import runpy,sys; helper=runpy.run_path(sys.argv[1],run_name='qa_guardian'); "
                "env=helper['_child_environment'](); "
                "assert env['PYTHONHOME']=='/native/incompatible-python-home'; "
                "assert env['TCL_LIBRARY']=='' and env['TK_LIBRARY']==''; print('native_restore_ok')")
        process = subprocess.run([sys.executable, "-c", code, guardian_script_path()],
                                 env=safe, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 universal_newlines=True, timeout=10)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(process.stdout.strip(), "native_restore_ok")

    def test_eda_settings_version_check_uses_the_same_native_snapshot(self):
        values = self.values(None)
        values.update(PATH="/native/bin", LD_LIBRARY_PATH="")
        caller = dict((name, "GUI_INJECTED") for name in GUI_ENV_NAMES)
        caller[NATIVE_ENV_SNAPSHOT_VAR] = serialized(values)
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(os.environ, caller, clear=True), mock.patch(
                "crescendo_engine.config.discover_eda_environment",
                return_value={"effective": {}, "ok": True}) as discover:
            configure_eda(path=os.path.join(root, "setup.json"), persist=False)
            used = discover.call_args[1]["environ"]
            self.assertEqual(used["PATH"], "/native/bin")
            self.assertEqual(used["LD_LIBRARY_PATH"], "")
            for name in GUI_ENV_NAMES:
                if name != "LD_LIBRARY_PATH":
                    self.assertNotIn(name, used)

    def test_native_menu_records_preserve_unset_empty_and_nonempty_values(self):
        with tempfile.TemporaryDirectory() as root:
            view = os.path.join(root, "LIB", "CELL", "maestro")
            os.makedirs(view)
            with open(os.path.join(view, "maestro.sdb"), "w") as stream:
                stream.write('<setupdb type="maestro"/>')
            def record(tag, *values):
                return tag + "\t" + "\t".join("nil" if value is None else json.dumps(value) for value in values)
            base = ["CRESCENDO_MAESTRO_CONTEXT_1", record("C", "LIB", "CELL", "maestro", root,
                    view, None, os.path.join(root, "bin", "ocean"))]
            for value in (None, "", "NATIVE_SOURCE"):
                rows = base + [record("R", value)]
                rows += [record("E", name, value) for name in NATIVE_ENV_NAMES if name != "LD_LIBRARY_PATH"]
                context = parse_launch_records("\n".join(rows) + "\n")
                self.assertEqual(context["runtime_env"], self.values(value))
                self.assertFalse(context["runtime_env_warnings"])
            legacy = parse_launch_records("\n".join(base + [record("R", "")]) + "\n")
            self.assertEqual(legacy["runtime_env"]["LD_LIBRARY_PATH"], "")
            self.assertTrue(legacy["runtime_env_warnings"])

    def test_malformed_explicit_snapshot_fails_instead_of_inheriting_gui_environment(self):
        with tempfile.TemporaryDirectory() as root, mock.patch.dict(os.environ, {
                NATIVE_ENV_SNAPSHOT_VAR: "invalid", "CRESCENDO_USER_CONFIG": os.path.join(root, "missing.json")}, clear=True):
            with self.assertRaisesRegex(ValueError, "snapshot"):
                load_config(persist_discovery=False, validate_discovery=False)


if __name__ == "__main__":
    unittest.main()
