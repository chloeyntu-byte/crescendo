"""Portability contracts for shell selection and staged, mocked GUI bootstrap.

These tests execute only owned shell/Python fixtures. Mocked Tk tests certify
stage separation, never a real visible application or native EDA completion.
"""
import contextlib
import io
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

from crescendo_engine import launcher
from crescendo_engine import __main__ as application_module
from crescendo_engine.native_environment import (
    GUI_ENV_NAMES, NATIVE_ENV_NAMES, NATIVE_ENV_SCHEMA, NATIVE_ENV_SNAPSHOT_VAR,
    parse_native_environment_snapshot,
)


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def snapshot(values=None):
    values = dict((name, None) for name in NATIVE_ENV_NAMES) if values is None else values
    return json.dumps({"schema": NATIVE_ENV_SCHEMA, "values": values})


def bridge(values):
    result = {}
    for name, value in values.items():
        result["CRESCENDO_LAUNCH_STATE_" + name] = "unset" if value is None else "set"
        if value is not None:
            result["CRESCENDO_LAUNCH_VALUE_" + name] = value
    return result


class LauncherBootstrapPortabilityTests(unittest.TestCase):
    def test_bridge_preserves_native_three_states_and_is_consumed_once(self):
        for value in (None, "", "native value"):
            values = dict((name, value) for name in NATIVE_ENV_NAMES)
            original = dict(bridge(values), DISPLAY=":owned", XAUTHORITY="owned-auth")
            original.update(dict((name, "gui injection") for name in GUI_ENV_NAMES))
            clean = launcher.bootstrap_environment(original)
            self.assertEqual(parse_native_environment_snapshot(clean[NATIVE_ENV_SNAPSHOT_VAR]), values)
            self.assertFalse(any(name in clean for name in GUI_ENV_NAMES))
            self.assertFalse(any(name.startswith(("CRESCENDO_LAUNCH_STATE_", "CRESCENDO_LAUNCH_VALUE_")) for name in clean))
            second = dict(clean)
            second.update(dict((name, "second gui injection") for name in GUI_ENV_NAMES))
            second.update(bridge(dict((name, "wrong second origin") for name in NATIVE_ENV_NAMES)))
            self.assertEqual(launcher.bootstrap_environment(second), clean)
            self.assertEqual(clean["DISPLAY"], ":owned")
            self.assertEqual(original["PYTHONHOME"], "gui injection")

    def test_invalid_existing_snapshot_never_recaptures_gui_environment(self):
        for invalid in ("", "broken", json.dumps({"schema": NATIVE_ENV_SCHEMA, "values": {}})):
            original = dict(bridge(dict((name, "new gui source") for name in NATIVE_ENV_NAMES)))
            original[NATIVE_ENV_SNAPSHOT_VAR] = invalid
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                launcher.bootstrap_environment(original)

    def test_argument_boundaries_and_relative_config_are_preserved(self):
        config = "settings ' $HOME file.json"
        arguments = launcher.gui_arguments(["--standalone", "--config", "explicit config.json"], {"CRESCENDO_CONFIG": config})
        self.assertEqual(arguments, ["--config", config, "--config", "explicit config.json", "gui"])
        with tempfile.TemporaryDirectory(prefix="crescendo cwd ' $") as root, mock.patch("os.getcwd", return_value=root):
            result = launcher.gui_arguments(["context ' $HOME file"], {"CRESCENDO_CONFIG": config})
        self.assertEqual(result[:3], ["--config", config, "gui"])
        self.assertEqual(result[-1], os.path.join(root, "context ' $HOME file"))

    def fake_tk(self, fail_stage=None):
        module = types.ModuleType("tkinter")
        module.ttk = types.ModuleType("tkinter.ttk")
        def tcl():
            if fail_stage == "tcl_init":
                raise RuntimeError("mock Tcl initialization failure")
            return mock.Mock(call=mock.Mock(return_value="8.mock"))
        def tk():
            if fail_stage == "tk_init":
                raise RuntimeError("mock display authentication failure")
            root = mock.Mock()
            root.tk.call.return_value = "8.mock"
            return root
        module.Tcl, module.Tk = tcl, tk
        return module

    def run_mocked_bootstrap(self, arguments, fail_stage=None, app_error=None):
        with tempfile.TemporaryDirectory() as root:
            log = os.path.join(root, "launch.log")
            with open(log, "w") as stream:
                stream.write("owned launcher log\n")
            os.chmod(log, 0o600)
            env = {NATIVE_ENV_SNAPSHOT_VAR: snapshot(), "CRESCENDO_LAUNCH_LOG": log}
            fake = self.fake_tk(fail_stage)
            output, error = io.StringIO(), io.StringIO()
            modules = {"tkinter": fake, "tkinter.ttk": fake.ttk}
            if fail_stage == "tk_import":
                modules = {"tkinter": None}
            with mock.patch.dict(os.environ, env, clear=True), mock.patch.dict(sys.modules, modules), mock.patch.object(
                    application_module, "main", return_value=0, side_effect=app_error) as app:
                with contextlib.redirect_stdout(output), contextlib.redirect_stderr(error):
                    status = launcher.main(arguments)
            with open(log) as stream:
                messages = stream.read()
            return status, output.getvalue(), error.getvalue(), messages, app.call_count

    def test_mocked_check_stops_before_application_and_reports_not_tested(self):
        status, output, error, messages, app_calls = self.run_mocked_bootstrap(["--check"])
        self.assertEqual(status, 0, error)
        report = json.loads(output)
        self.assertEqual(report["stages"]["application_init"], "NOT TESTED")
        self.assertEqual(app_calls, 0)
        self.assertNotIn("application_init PASS", messages)
        self.assertIn("tk_init PASS", messages)

    def test_failure_stage_distinguishes_tk_import_tcl_and_display(self):
        for stage in ("tk_import", "tcl_init", "tk_init"):
            with self.subTest(stage=stage):
                status, output, error, messages, app_calls = self.run_mocked_bootstrap(["--check"], fail_stage=stage)
                self.assertEqual(status, 1)
                self.assertEqual(json.loads(output)["failure"]["stage"], stage)
                self.assertEqual(app_calls, 0)
                self.assertNotIn("application_init PASS", messages)

    def test_tk_preflight_success_does_not_certify_application_constructor(self):
        status, output, error, messages, app_calls = self.run_mocked_bootstrap(
            ["--standalone"], app_error=RuntimeError("mock application failed"))
        self.assertEqual(status, 1)
        self.assertEqual(app_calls, 1)
        self.assertIn("application_init START", messages)
        self.assertIn("failed at application_init", messages)
        self.assertNotIn("application_init PASS", messages)

    def test_application_hook_is_after_constructor_and_before_mainloop(self):
        from crescendo_engine import gui
        root, service, app = mock.Mock(), mock.Mock(), mock.Mock()
        app.cleanup_confirmed = True
        order = []
        with mock.patch.object(gui.tk, "Tk", return_value=root), mock.patch.object(
                gui, "OptimizerGui", side_effect=lambda *args: order.append("constructor") or app), mock.patch.object(
                launcher, "record_application_initialized", side_effect=lambda: order.append("application_init")), mock.patch.object(
                gui.signal, "signal", return_value=object()):
            root.mainloop.side_effect = lambda: order.append("mainloop")
            self.assertEqual(gui.launch_gui({}, service=service), 0)
        self.assertEqual(order, ["constructor", "application_init", "mainloop"])
        with mock.patch.object(gui.tk, "Tk", return_value=root), mock.patch.object(
                gui, "OptimizerGui", side_effect=ValueError("owned constructor failure")), mock.patch.object(
                launcher, "record_application_initialized") as initialized:
            with self.assertRaises(ValueError):
                gui.launch_gui({}, service=service)
            initialized.assert_not_called()

    def test_successful_real_bootstrap_directory_is_removed_and_failure_is_sealed(self):
        from crescendo_engine.scratch import OWNER_FILE
        for fail in (False, True):
            with tempfile.TemporaryDirectory() as parent:
                directory = tempfile.mkdtemp(prefix="crescendo-launch.", dir=parent)
                log = os.path.join(directory, "launch.log")
                with open(log, "w") as stream:
                    stream.write("private bootstrap\n")
                os.chmod(log, 0o600)
                fake = self.fake_tk("tk_init" if fail else None)
                with mock.patch.dict(os.environ, {NATIVE_ENV_SNAPSHOT_VAR: snapshot(), "CRESCENDO_LAUNCH_LOG": log}, clear=True), mock.patch.dict(
                        sys.modules, {"tkinter": fake, "tkinter.ttk": fake.ttk}), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    status = launcher.main(["--check"])
                self.assertEqual(status, 1 if fail else 0)
                if fail:
                    with open(os.path.join(directory, OWNER_FILE)) as stream:
                        self.assertTrue(json.load(stream)["sealed"])
                    self.assertTrue(os.path.isfile(log))
                else:
                    self.assertFalse(os.path.exists(directory))

    def test_private_log_rejects_symlink_and_shared_permissions(self):
        with tempfile.TemporaryDirectory() as root:
            log = os.path.join(root, "log")
            with open(log, "w") as stream:
                stream.write("retained")
            os.chmod(log, 0o644)
            with mock.patch.dict(os.environ, {"CRESCENDO_LAUNCH_LOG": log}):
                with self.assertRaises(ValueError):
                    launcher.record_application_initialized()
            os.chmod(log, 0o600)
            link = os.path.join(root, "linked-log")
            os.symlink(log, link)
            with mock.patch.dict(os.environ, {"CRESCENDO_LAUNCH_LOG": link}):
                with self.assertRaises(OSError):
                    launcher.record_application_initialized()
            with open(log) as stream:
                self.assertEqual(stream.read(), "retained")


class ShellLauncherPortabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo launcher ' $ ")
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.cwd = os.path.join(self.root, "caller ' $ workspace")
        self.logs = os.path.join(self.root, "owned logs")
        os.makedirs(self.cwd)
        os.makedirs(self.logs)
        self.capture = os.path.join(self.root, "selected-python.json")
        self.python = os.path.join(self.cwd, "chosen python ' $ alias")
        program = (
            "#!" + sys.executable + "\n"
            "import json,os,subprocess,sys\n"
            "if '-c' in sys.argv or any(a.endswith('/python_runtime.py') for a in sys.argv[1:]):\n"
            "  os.execv(sys.executable,[sys.executable]+sys.argv[1:])\n"
            "json.dump({'args':sys.argv[1:],'cwd':os.getcwd(),'env':dict(os.environ),"
            "'invocation':sys.argv[0]},open(os.environ['CRESCENDO_TEST_CAPTURE'],'w'))\n"
        )
        with open(self.python, "w") as stream:
            stream.write(program)
        os.chmod(self.python, 0o700)

    def environment(self):
        result = {"PATH": os.defpath, "LC_ALL": "C", "PYTHONIOENCODING": "utf-8",
                  "TMPDIR": self.logs, "CRESCENDO_TEST_CAPTURE": self.capture,
                  "CRESCENDO_PYTHON": self.python, "DISPLAY": ":owned", "XAUTHORITY": "owned-auth"}
        return result

    def fixture(self, wheel=False):
        root = os.path.join(self.root, "wheel ' $ prefix" if wheel else "source ' $ prefix")
        native = os.path.join(root, "crescendo_engine", "resources", "cadence") if wheel else os.path.join(root, "cadence")
        package = os.path.join(root, "crescendo_engine")
        os.makedirs(native)
        os.makedirs(package, exist_ok=True)
        # Automatic discovery executes the real, bounded helper and probe.
        # The owned interpreter wrapper records only final application launch.
        for name in ("__init__.py", "runtime.py", "launcher.py", "native_environment.py", "python_runtime.py", "gui_runtime_dependencies.py", "scratch.py"):
            shutil.copy2(os.path.join(PROJECT_ROOT, "crescendo_engine", name), os.path.join(package, name))
        with open(os.path.join(package, "__main__.py"), "w") as stream:
            stream.write("# owned module placement fixture\n")
        launcher_file = os.path.join(native, "crescendo_launch")
        shutil.copy2(os.path.join(PROJECT_ROOT, "cadence", "crescendo_launch"), launcher_file)
        if not wheel:
            shutil.copy2(os.path.join(PROJECT_ROOT, "start.sh"), os.path.join(root, "start.sh"))
        return root, launcher_file

    def run_launcher(self, command, arguments, env=None):
        result = subprocess.run([command] + arguments, cwd=self.cwd, env=env or self.environment(),
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, timeout=15)
        captured = None
        if os.path.isfile(self.capture):
            with open(self.capture) as stream:
                captured = json.load(stream)
        logs = []
        for directory in os.listdir(self.logs):
            path = os.path.join(self.logs, directory, "launch.log")
            if os.path.isfile(path):
                with open(path) as stream:
                    logs.append((path, stream.read()))
        return result, captured, logs

    def test_source_and_wheel_layouts_keep_caller_cwd_and_select_exact_interpreter(self):
        for wheel in (False, True):
            root, command = self.fixture(wheel)
            env = self.environment()
            env["CRESCENDO_CONFIG"] = "config ' $ relative.json"
            result, captured, logs = self.run_launcher(command, ["--standalone", "--config", "cli settings.json"], env)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(captured["cwd"], self.cwd)
            self.assertEqual(captured["args"], ["-B", os.path.join(root, "crescendo_engine", "launcher.py"),
                                                "--standalone", "--config", "cli settings.json"])
            self.assertEqual(captured["env"]["CRESCENDO_CONFIG"], "config ' $ relative.json")
            self.assertEqual(captured["invocation"], self.python)
            self.assertTrue(logs)

    def test_source_entry_and_native_launcher_follow_external_symlinks(self):
        root, command = self.fixture()
        for target, arguments in ((os.path.join(root, "start.sh"), ["--config", "relative.json"]),
                                  (command, ["--standalone"])):
            link = os.path.join(self.cwd, "entry ' $ " + os.path.basename(target))
            os.symlink(target, link)
            result, captured, unused = self.run_launcher(link, arguments)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(captured["cwd"], self.cwd)
            self.assertEqual(captured["args"][1], os.path.join(root, "crescendo_engine", "launcher.py"))

    def test_native_context_path_keeps_apostrophes_dollars_and_argument_boundaries(self):
        unused, command = self.fixture()
        context = os.path.join(self.cwd, "context ' $HOME $(touch not-owned).txt")
        with open(context, "w") as stream:
            stream.write("owned context fixture")
        result, captured, unused = self.run_launcher(command, [context])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(captured["args"][-1], context)
        self.assertFalse(os.path.exists(os.path.join(self.cwd, "not-owned")))

    def test_explicit_empty_missing_nonpython_and_broken_alias_never_fall_back(self):
        unused, command = self.fixture()
        bad_alias = os.path.join(self.cwd, "broken python alias")
        os.symlink(os.path.join(self.cwd, "missing interpreter"), bad_alias)
        for selected in ("", "missing-crescendo-python-command", "/bin/true", bad_alias):
            with self.subTest(selected=selected):
                if os.path.exists(self.capture):
                    os.unlink(self.capture)
                env = self.environment()
                env["CRESCENDO_PYTHON"] = selected
                result, captured, logs = self.run_launcher(command, ["--check"], env)
                self.assertNotEqual(result.returncode, 0)
                self.assertIsNone(captured)
                self.assertTrue(logs)
                self.assertNotIn("application_init PASS", logs[-1][1])
                self.assertIn("Crescendo launch failed at", logs[-1][1])
                path = logs[-1][0]
                self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
                self.assertEqual(stat.S_IMODE(os.stat(os.path.dirname(path)).st_mode), 0o700)

    def test_unset_interpreter_uses_existing_path_choice_and_relative_alias_is_preserved(self):
        unused, command = self.fixture()
        binroot = os.path.join(self.cwd, "owned bin")
        os.makedirs(binroot)
        alias = os.path.join(binroot, "python3")
        os.symlink(self.python, alias)
        env = self.environment()
        env.pop("CRESCENDO_PYTHON")
        env["PATH"] = binroot
        result, captured, unused = self.run_launcher(command, ["--check"], env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(captured["invocation"], alias)
        env["CRESCENDO_PYTHON"] = "./" + os.path.basename(self.python)
        result, captured, unused = self.run_launcher(command, ["--check"], env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(captured["invocation"], env["CRESCENDO_PYTHON"])

    def test_automatic_discovery_skips_path_bootstrap_without_tk_and_uses_next_runtime(self):
        unused,command=self.fixture()
        first_bin=os.path.join(self.cwd,'first bin without Tk')
        second_bin=os.path.join(self.cwd,'second bin with Tk')
        os.makedirs(first_bin);os.makedirs(second_bin)
        first=os.path.join(first_bin,'python3')
        with open(first,'w') as stream:
            stream.write('#!'+sys.executable+'\nimport os,sys\n'
                         "if '-c' in sys.argv and 'crescendo.python_runtime_probe.v1' in sys.argv[sys.argv.index('-c')+1]:\n"
                         "  sys.modules['tkinter']=None\n"
                         "  exec(compile(sys.argv[sys.argv.index('-c')+1],'<owned-no-tk-probe>','exec'))\n"
                         "else: os.execv(sys.executable,[sys.executable]+sys.argv[1:])\n")
        os.chmod(first,0o700)
        second=os.path.join(second_bin,'python3')
        os.symlink(self.python,second)
        env=self.environment();env.pop('CRESCENDO_PYTHON')
        env['CRESCENDO_PREFERRED_PYTHON']=os.path.join(self.cwd,'missing installed preference')
        env['PATH']=os.pathsep.join((first_bin,second_bin))
        result,captured,logs=self.run_launcher(command,['--check'],env)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(captured['invocation'],second)
        self.assertEqual(captured['cwd'],self.cwd)
        messages=''.join(value for unused,value in logs)
        self.assertIn('tk_import',messages)
        self.assertNotIn('application_init PASS',messages)
        values=parse_native_environment_snapshot(launcher.bootstrap_environment(captured['env'])[NATIVE_ENV_SNAPSHOT_VAR])
        self.assertEqual(values['PATH'],env['PATH'])

    def test_shell_captures_all_three_native_states_before_clearing_gui_overrides(self):
        unused, command = self.fixture()
        for state in (None, "", "native-source"):
            with self.subTest(state=state):
                env = self.environment()
                for name in NATIVE_ENV_NAMES:
                    env.pop(name, None)
                    if state is not None:
                        env[name] = state
                result, captured, unused = self.run_launcher(command, ["--check"], env)
                self.assertEqual(result.returncode, 0, result.stderr)
                restored = launcher.bootstrap_environment(captured["env"])
                values = parse_native_environment_snapshot(restored[NATIVE_ENV_SNAPSHOT_VAR])
                self.assertEqual(values, dict((name, state) for name in NATIVE_ENV_NAMES))
                self.assertFalse(any(name in captured["env"] for name in GUI_ENV_NAMES))

    def test_shell_second_launch_keeps_snapshot_and_clears_new_gui_injection(self):
        unused, command = self.fixture()
        values = dict((name, None) for name in NATIVE_ENV_NAMES)
        values.update(PATH="/original/bin", LD_LIBRARY_PATH="", TK_LIBRARY="/original/tk")
        env = self.environment()
        env[NATIVE_ENV_SNAPSHOT_VAR] = snapshot(values)
        env.update(dict((name, "gui-new-source") for name in GUI_ENV_NAMES))
        result, captured, unused = self.run_launcher(command, ["--check"], env)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(parse_native_environment_snapshot(captured["env"][NATIVE_ENV_SNAPSHOT_VAR]), values)
        self.assertFalse(any(name in captured["env"] for name in GUI_ENV_NAMES))
        self.assertFalse(any(name.startswith("CRESCENDO_LAUNCH_STATE_") for name in captured["env"]))

    def test_invalid_snapshot_has_safe_early_log_without_native_values(self):
        unused, command = self.fixture()
        env = self.environment()
        env["CRESCENDO_PYTHON"] = sys.executable
        env[NATIVE_ENV_SNAPSHOT_VAR] = "invalid PRIVATE_LICENSE_VALUE"
        # Run the actual bootstrap; the invalid snapshot fails before Tk.
        result, captured, logs = self.run_launcher(os.path.join(PROJECT_ROOT, "cadence", "crescendo_launch"), ["--check"], env)
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(logs)
        all_messages = result.stdout + result.stderr + "".join(value for unused, value in logs)
        self.assertIn("failed at environment", all_messages)
        self.assertNotIn("PRIVATE_LICENSE_VALUE", all_messages)
        self.assertNotIn("application_init PASS", all_messages)


if __name__ == "__main__":
    unittest.main()
