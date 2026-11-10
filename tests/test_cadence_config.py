import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine.cadence import _version_probe, child_environment, detect_pdk_context, discover_eda_environment, run_command
from crescendo_engine.config import configure_eda, doctor, load_config, persist_eda_setup


def write_executable(path, output="version test"):
    directory = os.path.dirname(path)
    if not os.path.isdir(directory):
        os.makedirs(directory)
    with open(path, "w") as stream:
        stream.write("#!/bin/sh\necho '%s'\n" % output)
    os.chmod(path, 0o755)


def fake_install(root):
    cadence_root = os.path.join(root, "IC999")
    spectre_root = os.path.join(root, "SPECTRE999")
    virtuoso = os.path.join(cadence_root, "bin", "virtuoso")
    ocean = os.path.join(cadence_root, "tools.lnx86", "dfII", "bin", "ocean")
    spectre = os.path.join(spectre_root, "bin", "spectre")
    write_executable(virtuoso, "IC9.9")
    write_executable(ocean, "IC9.9")
    write_executable(spectre, "SPECTRE9.9")
    cds_lib = os.path.join(cadence_root, "share", "cdssetup", "cds.lib")
    os.makedirs(os.path.dirname(cds_lib))
    with open(cds_lib, "w") as stream:
        stream.write("# base\n")
    license_file = os.path.join(root, "license", "license.dat")
    os.makedirs(os.path.dirname(license_file))
    with open(license_file, "w") as stream:
        stream.write("SERVER test\n")
    return {
        "cadence_root": cadence_root,
        "spectre_root": spectre_root,
        "virtuoso_bin": virtuoso,
        "ocean_bin": ocean,
        "spectre_bin": spectre,
        "cds_lib": cds_lib,
        "license_file": license_file,
        "search_roots": [root],
    }


class CadenceDiscoveryTest(unittest.TestCase):
    def test_version_probe_preserves_argument_environment_and_nonzero_result(self):
        with tempfile.TemporaryDirectory(prefix="crescendo version '") as root:
            binary = os.path.join(root, "version wrapper")
            with open(binary, "w") as stream:
                stream.write('#!/bin/sh\nprintf "%s|%s" "$1" "$CRESCENDO_VERSION_FIXTURE"\n'
                             'printf "|stderr" >&2\nexit "${CRESCENDO_VERSION_EXIT:-0}"\n')
            os.chmod(binary, 0o700)
            for exit_code in (0, 7):
                env = {"PATH": "", "CRESCENDO_VERSION_FIXTURE": "isolated value",
                       "CRESCENDO_VERSION_EXIT": str(exit_code)}
                result = _version_probe("ocean_bin", binary, env)
                self.assertEqual(result["ok"], exit_code == 0)
                self.assertEqual(result["returncode"], exit_code)
                self.assertEqual(result["argv"], [binary, "-W"])
                self.assertEqual(result["version"], "-W|isolated value|stderr")

    def test_version_timeout_cleans_owned_term_resistant_pipe_children(self):
        # Both a live wrapper and an exited wrapper can leave a child holding
        # PIPE writers. A watchdog keeps this regression bounded on old code.
        with tempfile.TemporaryDirectory(prefix="crescendo-version-timeout-") as root:
            sentinel = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                        start_new_session=True)
            try:
                for parent_stays_alive in (True, False):
                    with self.subTest(parent_stays_alive=parent_stays_alive):
                        pid_path = os.path.join(root, "owned-pids.json")
                        binary = os.path.join(root, "version-wrapper")
                        child_code = "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)"
                        with open(binary, "w") as stream:
                            stream.write("#!" + sys.executable + "\nimport json,os,signal,subprocess,sys,time\n"
                                         "signal.signal(signal.SIGTERM,signal.SIG_IGN)\n"
                                         "child=subprocess.Popen([sys.executable,'-c',%r])\n"
                                         "with open(%r,'w') as stream: json.dump([os.getpid(),child.pid],stream)\n"
                                         "%s\n" % (child_code, pid_path,
                                              "time.sleep(60)" if parent_stays_alive else "sys.exit(0)"))
                        os.chmod(binary, 0o700)
                        def terminate_fixture():
                            try:
                                with open(pid_path) as stream:
                                    parent, child = json.load(stream)
                                if os.getpgid(child) == parent:
                                    os.killpg(parent, signal.SIGKILL)
                            except (OSError, ValueError):
                                pass
                        watchdog = threading.Timer(6, terminate_fixture)
                        watchdog.start()
                        started = time.monotonic()
                        try:
                            result = _version_probe("ocean_bin", binary, os.environ.copy(), timeout_seconds=0.5)
                        finally:
                            watchdog.cancel()
                            watchdog.join()
                            terminate_fixture()
                        self.assertLess(time.monotonic() - started, 4.0)
                        self.assertFalse(result["ok"])
                        self.assertTrue(result["timed_out"])
                        self.assertFalse(result["termination_failed"])
                        with open(pid_path) as stream:
                            owned = json.load(stream)
                        for pid in owned:
                            self.assertFalse(os.path.exists("/proc/%d" % pid))
                        self.assertIsNone(sentinel.poll())
            finally:
                sentinel.kill()
                sentinel.wait(timeout=3)

    def test_target_vendor_layouts_and_tcsh_pdk_are_discovered_independently(self):
        with tempfile.TemporaryDirectory(prefix="crescendo target server ' ") as root:
            manual = fake_install(root)
            spectre_root = os.path.join(root, "software", "cadence", "spectre", "18.1isr6")
            spectre = os.path.join(spectre_root, "tools.lnx86", "bin", "spectre")
            alps_root = os.path.join(root, "software", "empyrean", "alps", "2026.06.hf1")
            alps = os.path.join(alps_root, "bin", "alps")
            write_executable(spectre)
            write_executable(alps)
            pdk = os.path.join(root, "models", "c1x_plus_20251210")
            for simulator in ("spectre", "alps"):
                os.makedirs(os.path.join(pdk, simulator))
            overrides = dict((key, value) for key, value in manual.items() if key not in ("spectre_bin", "spectre_root"))
            overrides.update(search_roots=[os.path.dirname(spectre_root)], alps_search_roots=[os.path.dirname(alps_root)])
            result = discover_eda_environment(overrides, validate=False, environ={"PATH": "", "PDK": pdk, "SHELL": "/bin/tcsh"})
            self.assertTrue(result["ok"], result["missing"])
            self.assertEqual(result["effective"]["spectre_bin"], spectre)
            self.assertEqual(result["effective"]["alps_bin"], alps)
            for simulator in ("spectre", "alps"):
                self.assertEqual(result["effective"][simulator + "_model_dir"], os.path.join(pdk, simulator))
                self.assertEqual(result["sources"][simulator + "_model_dir"], "environment:PDK/" + simulator)

    def test_explicit_invalid_binary_cannot_borrow_a_root_or_path_tool(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(root)
            for key, command in (("virtuoso_bin", "virtuoso"), ("ocean_bin", "ocean"), ("spectre_bin", "spectre")):
                for source in ("manual", "environment"):
                    overrides = dict(manual)
                    environ = {"PATH": os.path.dirname(manual[key])}
                    missing = os.path.join(root, "missing", command)
                    if source == "manual":
                        overrides[key] = missing
                    else:
                        overrides.pop(key)
                        environ["CRESCENDO_" + command.upper() + "_BIN"] = missing
                    with self.subTest(key=key, source=source):
                        result = discover_eda_environment(overrides, validate=False, environ=environ)
                        self.assertIsNone(result["effective"][key])
                        self.assertIn(key, result["missing"])
                        self.assertEqual(result["sources"][key], "manual" if source == "manual" else "environment:CRESCENDO_" + command.upper() + "_BIN")

    def test_explicit_roots_select_their_binary_before_unrelated_path(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(os.path.join(root, "selected"))
            other = fake_install(os.path.join(root, "other"))
            overrides = dict((key, value) for key, value in manual.items() if not key.endswith("_bin"))
            result = discover_eda_environment(overrides, validate=False, environ={"PATH": ":".join(os.path.dirname(other[key]) for key in ("virtuoso_bin", "ocean_bin", "spectre_bin"))})
            self.assertTrue(result["ok"], result["missing"])
            for key in ("virtuoso_bin", "ocean_bin", "spectre_bin"):
                self.assertEqual(result["effective"][key], manual[key])

    def test_explicit_missing_model_directory_does_not_use_pdk_fallback(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(root)
            pdk = os.path.join(root, "pdk")
            os.makedirs(os.path.join(pdk, "spectre"))
            os.makedirs(os.path.join(pdk, "alps"))
            missing = os.path.join(root, "missing-models")
            for simulator in ("spectre", "alps"):
                overrides = dict(manual, **{simulator + "_model_dir": missing})
                result = discover_eda_environment(overrides, validate=False, environ={"PDK": pdk, "PATH": ""})
                self.assertEqual(result["effective"][simulator + "_model_dir"], missing)
                self.assertEqual(result["sources"][simulator + "_model_dir"], "manual")
                self.assertIn(simulator + "_model_dir", result["missing"])

    def test_pdk_expansion_uses_native_discovery_environment_and_only_existing_dirs(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(root)
            pdk = os.path.join(root, "native PDK")
            os.makedirs(os.path.join(pdk, "spectre"))
            with mock.patch.dict(os.environ, {"PDK": "/wrong/gui-pdk"}, clear=False):
                result = discover_eda_environment(dict(manual, spectre_model_dir="${PDK}/spectre"), validate=False,
                                                  environ={"PDK": pdk, "PATH": ""})
            self.assertEqual(result["effective"]["spectre_model_dir"], os.path.join(pdk, "spectre"))
            self.assertIsNone(result["effective"]["alps_model_dir"])
            self.assertNotIn("alps_model_dir", result["sources"])

    def test_pdk_auto_directory_must_be_readable_and_traversable(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(root)
            pdk = os.path.join(root, "pdk")
            spectre_dir, alps_dir = [os.path.join(pdk, name) for name in ("spectre", "alps")]
            os.makedirs(spectre_dir)
            os.makedirs(alps_dir)
            access = os.access
            with mock.patch("crescendo_engine.cadence.os.access", side_effect=lambda path, mode: False if path == alps_dir else access(path, mode)):
                automatic = discover_eda_environment(manual, validate=False, environ={"PDK": pdk, "PATH": ""})
                explicit = discover_eda_environment(dict(manual, alps_model_dir=alps_dir), validate=False,
                                                     environ={"PDK": pdk, "PATH": ""})
            self.assertEqual(automatic["effective"]["spectre_model_dir"], spectre_dir)
            self.assertIsNone(automatic["effective"]["alps_model_dir"])
            self.assertEqual(explicit["effective"]["alps_model_dir"], alps_dir)
            self.assertIn("alps_model_dir", explicit["missing"])

    def test_cancel_before_launch_does_not_start_process(self):
        with mock.patch("crescendo_engine.cadence.subprocess.Popen") as popen:
            result = run_command(
                ["virtuoso"], "/tmp", {}, 30, cancel_check=lambda: True,
            )
        popen.assert_not_called()
        self.assertTrue(result["cancelled"])
        self.assertFalse(result["timed_out"])

    def test_cancel_interrupts_startup_lock_wait(self):
        startup_lock = threading.Lock()
        startup_lock.acquire()
        cancelled = threading.Event()
        timer = threading.Timer(0.05, cancelled.set)
        timer.start()
        started = time.monotonic()
        try:
            with mock.patch("crescendo_engine.cadence.subprocess.Popen") as popen:
                result = run_command(
                    ["virtuoso"], "/tmp", {}, 30, startup_lock=startup_lock,
                    lock_timeout_seconds=5, cancel_check=cancelled.is_set,
                )
        finally:
            startup_lock.release()
            timer.join()
        popen.assert_not_called()
        self.assertTrue(result["cancelled"])
        self.assertLess(time.monotonic() - started, 1.0)

    def test_cancel_terminates_owned_process_group(self):
        with tempfile.TemporaryDirectory() as root:
            cancelled = threading.Event()
            timer = threading.Timer(0.05, cancelled.set)
            timer.start()
            started = time.monotonic()
            try:
                result = run_command(
                    ["/bin/sh", "-c", "trap '' TERM; sleep 30 & wait"],
                    root, os.environ.copy(), 30, cancel_check=cancelled.is_set,
                    termination_grace_seconds=0.05, termination_kill_seconds=1,
                )
            finally:
                timer.join()
        self.assertTrue(result["cancelled"])
        self.assertFalse(result["termination_failed"])
        self.assertNotEqual(result["returncode"], 0)
        self.assertLess(time.monotonic() - started, 2.0)

    def test_progress_timeout_terminates_stalled_process_group(self):
        with tempfile.TemporaryDirectory() as root:
            progress = os.path.join(root, "progress.log")
            result = run_command(
                ["/bin/sh", "-c", "sleep 5"], root, os.environ.copy(), 10,
                progress_path=progress, progress_timeout_seconds=0.1,
            )
        self.assertTrue(result["timed_out"])
        self.assertTrue(result["progress_timed_out"])

    def test_progress_watchdog_uses_separate_initial_allowance(self):
        with tempfile.TemporaryDirectory() as root:
            progress = os.path.join(root, "progress.log")
            result = run_command(
                ["/bin/sh", "-c", "sleep 0.15; echo begin > progress.log; sleep 0.04"],
                root, os.environ.copy(), 2,
                progress_path=progress, progress_timeout_seconds=0.1,
                progress_initial_timeout_seconds=0.4,
            )
        self.assertEqual(result["returncode"], 0)
        self.assertFalse(result["timed_out"])

    def test_startup_lock_wait_is_bounded_by_command_timeout(self):
        startup_lock = mock.Mock()
        startup_lock.acquire.return_value = False
        with mock.patch("crescendo_engine.cadence.subprocess.Popen") as popen:
            result = run_command(
                ["virtuoso"], "/tmp", {}, 30, startup_lock=startup_lock,
                startup_timeout_seconds=5,
            )
        startup_lock.acquire.assert_called_once_with(timeout=5.0)
        popen.assert_not_called()
        self.assertTrue(result["timed_out"])
        self.assertTrue(result["startup_timed_out"])

    def test_explicit_lock_timeout_is_independent_of_candidate_timeout(self):
        startup_lock = mock.Mock()
        startup_lock.acquire.return_value = False
        with mock.patch("crescendo_engine.cadence.subprocess.Popen") as popen:
            result = run_command(
                ["virtuoso"], "/tmp", {}, 30, startup_lock=startup_lock,
                startup_timeout_seconds=5, lock_timeout_seconds=1800,
            )
        startup_lock.acquire.assert_called_once_with(timeout=1800.0)
        popen.assert_not_called()
        self.assertGreaterEqual(result["lock_wait_seconds"], 0.0)

    def test_bounded_discovery_and_isolated_version_checks(self):
        with tempfile.TemporaryDirectory() as root:
            expected = fake_install(root)
            result = discover_eda_environment({"search_roots": [root]}, validate=True, environ={"PATH": ""})
            self.assertTrue(result["ok"])
            self.assertEqual(result["effective"]["cadence_root"], expected["cadence_root"])
            self.assertEqual(result["effective"]["spectre_root"], expected["spectre_root"])
            self.assertEqual([row["ok"] for row in result["version_checks"]], [True, True, True])
            self.assertEqual(os.environ.get("CDS_AUTO_64BIT"), None)

    def test_missing_setup_has_precise_recommendations(self):
        with tempfile.TemporaryDirectory() as root:
            result = discover_eda_environment({"search_roots": [root]}, validate=False, environ={"PATH": ""})
            self.assertFalse(result["ok"])
            self.assertIn("virtuoso_bin", result["missing"])
            self.assertIn("license_file", result["missing"])
            self.assertGreaterEqual(len(result["recommendations"]), 3)

    def test_child_environment_only_uses_discovered_paths(self):
        config = {"cadence": {"license_file": "5280@license", "extra_binary_paths": ["/eda/bin"], "extra_library_paths": ["/eda/lib"]}}
        with mock.patch.dict(os.environ, {"PATH": "/usr/bin", "LD_LIBRARY_PATH": "/usr/lib"}, clear=True):
            env = child_environment(config, "/tmp/crescendo-home")
        self.assertEqual(env["PATH"].split(":")[:2], ["/eda/bin", "/usr/bin"])
        self.assertEqual(env["LD_LIBRARY_PATH"].split(":")[:2], ["/eda/lib", "/usr/lib"])
        self.assertNotIn("/opt/eda/cadence/IC618", env["PATH"])
        self.assertEqual(env["CDS_LIC_FILE"], "5280@license")

    def test_detect_pdk_from_hierarchy_and_cds_lib(self):
        with tempfile.TemporaryDirectory() as root:
            library_root = os.path.join(root, "HH")
            pdk_dir = os.path.join(root, "pdks", "foundry")
            os.makedirs(library_root)
            os.makedirs(pdk_dir)
            with open(os.path.join(root, "cds.lib"), "w") as stream:
                stream.write("DEFINE foundry %s\n" % pdk_dir)
            setup = {"libraries": ["HH"], "source": {"library_root": library_root}}
            hierarchy = {"designs": [{"hierarchy": {"instances": [
                {"master_library": "foundry"}, {"master_library": "analogLib"},
            ]}}]}
            result = detect_pdk_context(setup, hierarchy)
            self.assertTrue(result["ok"])
            self.assertEqual(result["pdk_name"], "foundry")
            self.assertEqual(result["pdk_dir"], pdk_dir)

    def test_ambiguous_pdk_is_non_blocking_warning(self):
        setup = {"libraries": ["HH"], "source": {}}
        hierarchy = {"designs": [{"hierarchy": {"instances": [
            {"master_library": "pdk_a"}, {"master_library": "pdk_b"},
        ]}}]}
        result = detect_pdk_context(setup, hierarchy)
        self.assertFalse(result["ok"])
        self.assertFalse(result["blocking"])
        self.assertEqual(result["candidate_libraries"], ["pdk_a", "pdk_b"])


class UserConfigTest(unittest.TestCase):
    def test_manual_override_is_validated_and_atomically_persisted(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(os.path.join(root, "eda"))
            config_path = os.path.join(root, "user", "config.json")
            discovery, saved_path = configure_eda(manual, mode="manual", path=config_path, validate=True)
            self.assertTrue(discovery["ok"])
            self.assertEqual(saved_path, config_path)
            with open(config_path, "r") as stream:
                payload = json.load(stream)
            self.assertEqual(payload["eda_setup"]["mode"], "manual")
            self.assertEqual(payload["eda_setup"]["manual"]["cadence_root"], manual["cadence_root"])
            self.assertFalse(any(name.endswith(".tmp") for name in os.listdir(os.path.dirname(config_path))))

    def test_invalid_manual_override_is_not_saved(self):
        with tempfile.TemporaryDirectory() as root:
            config_path = os.path.join(root, "config.json")
            with self.assertRaisesRegex(ValueError, "EDA override validation failed"):
                configure_eda({"search_roots": [os.path.join(root, "missing")]}, mode="manual", path=config_path, validate=True)
            self.assertFalse(os.path.exists(config_path))

    def test_validated_eda_setup_can_be_resolved_without_persisting(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(os.path.join(root, "eda"))
            config_path = os.path.join(root, "user", "config.json")
            discovery, resolved_path = configure_eda(
                manual, mode="manual", path=config_path, validate=True,
                persist=False,
            )
            self.assertTrue(discovery["ok"])
            self.assertEqual(resolved_path, config_path)
            self.assertFalse(os.path.exists(config_path))

    @mock.patch("crescendo_engine.config.discover_eda_environment")
    def test_invalid_mode_is_rejected_before_discovery(self, discover):
        with self.assertRaisesRegex(ValueError, "mode must be auto or manual"):
            configure_eda({}, mode="automatic", persist=False)
        discover.assert_not_called()

    def test_load_config_uses_port_server_license_and_configurable_user_path(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(os.path.join(root, "eda"))
            manual["license_file"] = "5280@license-host"
            user_path = os.path.join(root, "custom", "user.json")
            persist_eda_setup("manual", manual, discover_eda_environment(manual, validate=False), user_path)
            with mock.patch.dict(os.environ, {"CRESCENDO_USER_CONFIG": user_path, "CRESCENDO_RUN_ROOT": os.path.join(root, "runs")}, clear=False):
                config = load_config(persist_discovery=False, validate_discovery=False)
            self.assertEqual(config["cadence"]["license_file"], "5280@license-host")
            self.assertEqual(config["cadence"]["setup_mode"], "manual")
            self.assertEqual(config["cadence"]["user_config_path"], user_path)
            self.assertEqual(
                config["cadence"]["_manual_overrides"]["license_file"],
                "5280@license-host",
            )

    def test_auto_discovery_values_are_not_recorded_as_manual_overrides(self):
        with tempfile.TemporaryDirectory() as root:
            user_path = os.path.join(root, "user.json")
            discovery = {
                "ok": True,
                "effective": {"virtuoso_bin": "/auto/bin/virtuoso"},
                "sources": {}, "checks": [], "version_checks": [],
                "missing": [], "recommendations": [], "compatibility": {"ok": True},
            }
            with mock.patch.dict(
                    os.environ,
                    {"CRESCENDO_USER_CONFIG": user_path, "CRESCENDO_RUN_ROOT": os.path.join(root, "runs")},
                    clear=False,
            ), mock.patch(
                    "crescendo_engine.config.discover_eda_environment",
                    return_value=discovery,
            ):
                config = load_config(persist_discovery=False, validate_discovery=False)
            self.assertEqual(config["cadence"]["virtuoso_bin"], "/auto/bin/virtuoso")
            self.assertEqual(config["cadence"]["_manual_overrides"], {})

    def test_relative_user_pdk_adapter_resolves_from_user_config_directory(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(os.path.join(root, "eda"))
            user_path = os.path.join(root, "user", "config.json")
            adapter = os.path.join(root, "user", "adapters", "pdk.il")
            os.makedirs(os.path.dirname(adapter))
            with open(adapter, "w") as stream:
                stream.write("procedure(crescPdkApplyParameter(inst name value) t)\n")
            manual["pdk_adapter"] = os.path.join("adapters", "pdk.il")
            persist_eda_setup("manual", manual, discover_eda_environment(manual, validate=False), user_path)
            with mock.patch.dict(os.environ, {"CRESCENDO_USER_CONFIG": user_path}, clear=False):
                config = load_config(persist_discovery=False, validate_discovery=False)
            self.assertEqual(config["cadence"]["pdk_adapter"], adapter)

    def test_relative_startup_extensions_resolve_from_config_directory(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(os.path.join(root, "eda"))
            explicit_path = os.path.join(root, "project.json")
            extension = os.path.join(root, "skill", "portable.il")
            os.makedirs(os.path.dirname(extension))
            with open(extension, "w") as stream:
                stream.write("procedure(crescPortableStartup() t)\n")
            payload = {"cadence": dict(manual, startup_extensions=["skill/portable.il"])}
            with open(explicit_path, "w") as stream:
                json.dump(payload, stream)
            user_path = os.path.join(root, "user.json")
            with mock.patch.dict(os.environ, {"CRESCENDO_USER_CONFIG": user_path}, clear=False):
                config = load_config(
                    explicit_path, persist_discovery=False, validate_discovery=False,
                )
            self.assertEqual(config["cadence"]["startup_extensions"], [extension])
            self.assertEqual(config["cadence"]["_manual_overrides"], {})
            self.assertEqual(config["cadence"]["_file_overrides"]["startup_extensions"], [extension])

    def test_relative_model_paths_and_profile_paths_resolve_from_config_directory(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(os.path.join(root, "eda"))
            explicit_path = os.path.join(root, "project.json")
            payload = {"cadence": dict(
                manual,
                pdk_name="DEVICE",
                pdk_dir="portable-pdk",
                model_roots=["portable-pdk/models"],
                model_path_remaps=[{
                    "source": "legacy-models",
                    "destination": "portable-pdk/models",
                }],
                pdk_profiles=[{
                    "id": "portable-pdk",
                    "name": "DEVICE",
                    "package_root": "portable-pdk",
                    "model_roots": ["portable-pdk/models"],
                    "model_path_remaps": {
                        "/vendor/models": "portable-pdk/models",
                    },
                    "startup_extensions": ["skill/pdk.il"],
                }],
            )}
            with open(explicit_path, "w") as stream:
                json.dump(payload, stream)
            user_path = os.path.join(root, "user.json")
            with mock.patch.dict(os.environ, {"CRESCENDO_USER_CONFIG": user_path}, clear=False):
                config = load_config(
                    explicit_path, persist_discovery=False, validate_discovery=False,
                )
            cadence = config["cadence"]
            self.assertEqual(cadence["pdk_dir"], os.path.join(root, "portable-pdk"))
            self.assertEqual(cadence["model_roots"], [
                os.path.join(root, "portable-pdk", "models"),
            ])
            self.assertEqual(cadence["model_path_remaps"], [{
                "source": os.path.join(root, "legacy-models"),
                "destination": os.path.join(root, "portable-pdk", "models"),
            }])
            profile = cadence["pdk_profiles"][0]
            self.assertEqual(profile["package_root"], os.path.join(root, "portable-pdk"))
            self.assertEqual(profile["model_roots"], [
                os.path.join(root, "portable-pdk", "models"),
            ])
            self.assertEqual(profile["startup_extensions"], [
                os.path.join(root, "skill", "pdk.il"),
            ])

    def test_submit_script_is_project_relative_and_included_in_doctor(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(os.path.join(root, "eda"))
            explicit_path = os.path.join(root, "project.json")
            submit_script = os.path.join(root, "bin", "submit.sh")
            pdk_adapter = os.path.join(root, "adapters", "pdk.il")
            write_executable(submit_script)
            os.makedirs(os.path.dirname(pdk_adapter))
            with open(pdk_adapter, "w") as stream:
                stream.write("procedure(crescPdkApplyParameter(inst name value) t)\n")
            with open(explicit_path, "w") as stream:
                json.dump({
                    "cadence": dict(manual, pdk_adapter="adapters/pdk.il"),
                    "runtime": {"run_root": "runs",
                                "spectre_submit_script": "bin/submit.sh"},
                }, stream)
            user_path = os.path.join(root, "user.json")
            with mock.patch.dict(os.environ, {"CRESCENDO_USER_CONFIG": user_path}, clear=False):
                config = load_config(explicit_path, persist_discovery=False, validate_discovery=False)
            self.assertEqual(config["runtime"]["spectre_submit_script"], submit_script)
            self.assertEqual(config["runtime"]["run_root"], os.path.join(root, "runs"))
            self.assertEqual(config["cadence"]["pdk_adapter"], pdk_adapter)
            checks = dict((row["name"], row) for row in doctor(config)["checks"])
            self.assertTrue(checks["spectre_submit_script"]["ok"])

    def test_explicit_legacy_cadence_values_imply_manual_mode(self):
        with tempfile.TemporaryDirectory() as root:
            manual = fake_install(os.path.join(root, "eda"))
            explicit_path = os.path.join(root, "project.json")
            with open(explicit_path, "w") as stream:
                json.dump({"cadence": manual, "runtime": {"run_root": os.path.join(root, "runs")}}, stream)
            user_path = os.path.join(root, "user.json")
            with mock.patch.dict(os.environ, {"CRESCENDO_USER_CONFIG": user_path}, clear=False):
                config = load_config(explicit_path, persist_discovery=False, validate_discovery=False)
            self.assertEqual(config["cadence"]["setup_mode"], "manual")
            self.assertEqual(config["cadence"]["virtuoso_bin"], manual["virtuoso_bin"])


if __name__ == "__main__":
    unittest.main()


class AlpsInstallationScopeDiscoveryTests(unittest.TestCase):
    def setup_installation(self, root, versions=('release_A',)):
        manual = fake_install(os.path.join(root,'cadence_fixture'))
        family = os.path.join(root,'vendor','alps')
        binaries = []
        for version in versions:
            binary = os.path.join(family,version,'bin','alps')
            write_executable(binary)
            binaries.append(binary)
        model_dir = os.path.join(root,'models','alps')
        os.makedirs(model_dir)
        manual.update(alps_search_roots=[family],alps_model_dir=model_dir)
        return manual,family,binaries

    def test_declared_family_version_and_bin_directories_locate_unique_launcher(self):
        for mode in ('family','version','bin'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as root:
                manual,family,binaries = self.setup_installation(root)
                version = os.path.dirname(os.path.dirname(binaries[0]))
                manual['alps_root'] = {'family':family,'version':version,'bin':os.path.dirname(binaries[0])}[mode]
                with mock.patch('subprocess.Popen',side_effect=AssertionError('no discovery process')):
                    result = discover_eda_environment(manual,validate=False,environ={'PATH':''})
                self.assertEqual(result['effective']['alps_bin'],binaries[0])
                self.assertEqual(result['effective']['alps_root'],version)
                check = next(row for row in result['checks'] if row['name'] == 'alps_bin')
                self.assertEqual(check['resolution']['status'],'selected')
                self.assertEqual(check['resolution']['selected'],binaries[0])

    def test_family_environment_root_is_scoped_and_multiple_versions_are_ambiguous(self):
        from crescendo_engine.simulators import simulator_readiness
        with tempfile.TemporaryDirectory() as root:
            manual,family,binaries = self.setup_installation(root,('release_A','release_B'))
            other = os.path.join(root,'other','bin','alps')
            write_executable(other)
            result = discover_eda_environment(manual,validate=False,environ={'PATH':os.path.dirname(other),'ALPS_HOME':family})
            self.assertIsNone(result['effective']['alps_bin'])
            check = next(row for row in result['checks'] if row['name'] == 'alps_bin')
            self.assertEqual(check['resolution']['status'],'ambiguous')
            self.assertEqual(set(check['resolution']['candidates']),set(binaries))
            cadence = dict(result['effective'],discovery=result,simulator='alps')
            ready = simulator_readiness({'cadence':cadence},{'simulator':'alps'})
            self.assertFalse(ready['ready'])
            self.assertIn('Multiple ALPS installations',ready['reason'])
            self.assertEqual(ready['alps_executable_discovery']['resolution']['status'],'ambiguous')

    def test_invalid_declared_binary_cannot_borrow_valid_root_launcher(self):
        from crescendo_engine.simulators import simulator_readiness
        with tempfile.TemporaryDirectory() as root:
            manual,family,binaries = self.setup_installation(root)
            bad = os.path.join(root,'bad_alps')
            write_executable(bad)
            os.chmod(bad,0o644)
            manual.update(alps_root=os.path.dirname(os.path.dirname(binaries[0])),alps_bin=bad)
            result = discover_eda_environment(manual,validate=False,environ={'PATH':os.path.dirname(binaries[0])})
            self.assertIsNone(result['effective']['alps_bin'])
            check = next(row for row in result['checks'] if row['name'] == 'alps_bin')
            self.assertEqual(check['resolution']['status'],'invalid_explicit_binary')
            self.assertEqual(check['resolution']['rejected'][0],{'path':bad,'status':'not_executable'})
            cadence = dict(result['effective'],discovery=result,simulator='alps')
            ready = simulator_readiness({'cadence':cadence},{'simulator':'alps'})
            self.assertFalse(ready['ready'])
            self.assertIn(bad,ready['reason'])
            self.assertIn('no root/PATH fallback',ready['reason'])
            from crescendo_engine.simulators import execution_contract, SimulatorConfigurationError
            with self.assertRaisesRegex(SimulatorConfigurationError,'no root/PATH fallback'):
                execution_contract({'cadence':cadence},'alps',default_executable=binaries[0])

    def test_unexecutable_bin_launcher_reports_exact_path_and_permission_reason(self):
        with tempfile.TemporaryDirectory() as root:
            manual,family,binaries = self.setup_installation(root)
            os.chmod(binaries[0],0o644)
            manual['alps_root'] = os.path.dirname(os.path.dirname(binaries[0]))
            result = discover_eda_environment(manual,validate=False,environ={'PATH':''})
            self.assertIsNone(result['effective']['alps_bin'])
            check = next(row for row in result['checks'] if row['name'] == 'alps_bin')
            self.assertIn(binaries[0],check['message'])
            self.assertIn('not_executable',check['message'])

    def test_automatic_family_search_does_not_choose_arbitrary_version(self):
        with tempfile.TemporaryDirectory() as root:
            manual,family,binaries = self.setup_installation(root,('release_A','release_B'))
            result = discover_eda_environment(manual,validate=False,environ={'PATH':''})
            self.assertIsNone(result['effective']['alps_bin'])
            check = next(row for row in result['checks'] if row['name'] == 'alps_bin')
            self.assertEqual(check['resolution']['status'],'ambiguous')
            self.assertEqual(len(check['resolution']['candidates']),2)

    def test_internal_alps_home_selects_only_same_version_launcher_and_keeps_declaration(self):
        with tempfile.TemporaryDirectory(prefix="crescendo ALPS home . ' ") as root:
            manual,family,binaries = self.setup_installation(root)
            version = os.path.dirname(os.path.dirname(binaries[0]))
            home = os.path.join(version,'tools','alps')
            os.makedirs(home)
            other = os.path.join(root,'other','bin','alps')
            write_executable(other)
            with mock.patch('subprocess.Popen',side_effect=AssertionError('no native discovery process')):
                result = discover_eda_environment(manual,validate=False,
                    environ={'PATH':os.path.dirname(other),'ALPS_HOME':home})
            self.assertEqual(result['effective']['alps_bin'],binaries[0])
            self.assertEqual(result['effective']['alps_root'],version)
            self.assertEqual(result['sources']['alps_root'],'environment:ALPS_HOME')
            check = next(row for row in result['checks'] if row['name'] == 'alps_bin')
            self.assertEqual(check['resolution']['declared_root'],home)
            self.assertEqual(check['resolution']['search_roots'],[home])
            self.assertEqual(check['resolution']['selected_root'],version)

    def test_internal_home_keeps_direct_bin_and_explicit_binary_priority(self):
        with tempfile.TemporaryDirectory() as root:
            manual,family,binaries = self.setup_installation(root)
            version = os.path.dirname(os.path.dirname(binaries[0]))
            home = os.path.join(version,'tools','alps')
            direct = os.path.join(home,'bin','alps')
            write_executable(direct)
            manual['alps_root'] = home
            result = discover_eda_environment(manual,validate=False,environ={'PATH':''})
            self.assertEqual(result['effective']['alps_bin'],direct)
            other = os.path.join(root,'manual','bin','alps')
            write_executable(other)
            manual['alps_bin'] = other
            result = discover_eda_environment(manual,validate=False,environ={'PATH':''})
            self.assertEqual(result['effective']['alps_bin'],other)
            os.chmod(other,0o644)
            result = discover_eda_environment(manual,validate=False,environ={'PATH':os.path.dirname(binaries[0])})
            self.assertIsNone(result['effective']['alps_bin'])
            check = next(row for row in result['checks'] if row['name'] == 'alps_bin')
            self.assertEqual(check['resolution']['status'],'invalid_explicit_binary')

    def test_internal_home_unusable_ancestor_never_borrows_other_version_or_path(self):
        with tempfile.TemporaryDirectory() as root:
            manual,family,binaries = self.setup_installation(root,('release_A','release_B'))
            home = os.path.join(os.path.dirname(os.path.dirname(binaries[0])),'tools','alps')
            os.makedirs(home)
            os.chmod(binaries[0],0o644)
            manual['alps_root'] = home
            result = discover_eda_environment(manual,validate=False,environ={'PATH':os.path.dirname(binaries[1])})
            self.assertIsNone(result['effective']['alps_bin'])
            check = next(row for row in result['checks'] if row['name'] == 'alps_bin')
            self.assertIn({'path':binaries[0],'status':'not_executable'},check['resolution']['rejected'])
            self.assertNotIn(binaries[1],check['resolution']['candidates'])
            self.assertEqual(check['resolution']['declared_root'],home)

    def test_internal_home_rule_is_exact_and_multiple_homes_stay_ambiguous(self):
        from crescendo_engine.cadence import _resolve_alps_installation
        with tempfile.TemporaryDirectory() as root:
            manual,family,binaries = self.setup_installation(root,('release_A','release_B'))
            homes=[]
            for binary in binaries:
                home=os.path.join(os.path.dirname(os.path.dirname(binary)),'tools','alps')
                os.makedirs(home)
                homes.append(home)
            selected,binary,evidence=_resolve_alps_installation(homes,'manual homes')
            self.assertIsNone(selected)
            self.assertIsNone(binary)
            self.assertEqual(evidence['status'],'ambiguous')
            self.assertEqual(set(evidence['candidates']),set(binaries))
            other_home=os.path.join(os.path.dirname(os.path.dirname(binaries[0])),'tools','other')
            os.makedirs(other_home)
            selected,binary,evidence=_resolve_alps_installation([other_home],'manual root')
            self.assertIsNone(binary)
            self.assertEqual(evidence['status'],'unavailable')
            for mode in ('file','broken_link'):
                version=os.path.join(family,'invalid_home_' + mode)
                write_executable(os.path.join(version,'bin','alps'))
                tools=os.path.join(version,'tools')
                os.makedirs(tools)
                home=os.path.join(tools,'alps')
                if mode == 'file':
                    with open(home,'w') as stream:
                        stream.write('not an ALPS home directory')
                else:
                    os.symlink(os.path.join(root,'missing_home'),home)
                with self.subTest(home_type=mode):
                    selected,binary,evidence=_resolve_alps_installation([home],'manual root')
                    self.assertIsNone(binary)
                    self.assertEqual(evidence['status'],'unavailable')

    def test_incomplete_bounded_search_does_not_claim_unique_candidate(self):
        from crescendo_engine.cadence import _resolve_alps_installation
        with tempfile.TemporaryDirectory() as root:
            manual,family,binaries = self.setup_installation(root)
            os.makedirs(os.path.join(family,'z_uninspected'))
            selected,binary,evidence = _resolve_alps_installation([family],'fixture declared root',limit=1)
            self.assertIsNone(selected)
            self.assertIsNone(binary)
            self.assertEqual(evidence['status'],'incomplete')
            self.assertTrue(evidence['search_incomplete'])
