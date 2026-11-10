import json
import os
import signal
import subprocess
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine.submission import (
    LocalProcessExecutor, SimulationSubmissionService, build_process_environment,
    discover_toolchain, execute_request,
)
from tools.submit_spectre import main as spectre_cli


def write_executable(path, body):
    directory = os.path.dirname(path)
    if not os.path.isdir(directory):
        os.makedirs(directory)
    with open(path, "w") as stream:
        stream.write("#!/bin/sh\nset -eu\n" + body + "\n")
    os.chmod(path, 0o755)


def write_json(path, value):
    with open(path, "w") as stream:
        json.dump(value, stream)


class SubmissionTest(unittest.TestCase):
    def test_production_submit_script_preserves_arguments_exit_code_and_timing(self):
        with tempfile.TemporaryDirectory() as root:
            simulator = os.path.join(root, "fake spectre")
            arguments = os.path.join(root, "arguments.txt")
            timing = os.path.join(root, "timing.log")
            write_executable(simulator, 'printf "%s\\n" "$@" > "$CRESCENDO_ARGUMENTS"; exit 7')
            script = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tools", "submit_spectre_job.sh")
            environment = dict(os.environ)
            environment.update({
                "CRESCENDO_ARGUMENTS": arguments,
                "CRESCENDO_SPECTRE_TIMEOUT_LOG": timing,
                "CRESCENDO_SIMULATION_TIMEOUT_SECONDS": "5",
            })
            returncode = subprocess.call([script, simulator, "input netlist.scs", "+escchars"], env=environment)
            self.assertEqual(returncode, 7)
            with open(arguments) as stream:
                self.assertEqual(stream.read().splitlines(), ["input netlist.scs", "+escchars"])
            with open(timing) as stream:
                rows = stream.read().splitlines()
            self.assertEqual(len(rows), 2)
            self.assertTrue(rows[0].startswith("START epoch="))
            self.assertTrue(rows[1].endswith("returncode=7"))

    def test_production_submit_script_retains_only_requested_spectre_log(self):
        with tempfile.TemporaryDirectory() as root:
            simulator = os.path.join(root, "fake-spectre")
            netlist_root = os.path.join(root, "run", "netlist")
            psf_root = os.path.join(root, "run", "psf")
            candidate_root = os.path.join(root, "candidate")
            os.makedirs(netlist_root)
            os.makedirs(psf_root)
            os.makedirs(candidate_root)
            write_executable(simulator, '''
log_path=
expect_log=0
for argument in "$@"; do
  if [ "$expect_log" -eq 1 ]; then log_path=$argument; expect_log=0; continue; fi
  if [ "$argument" = "+log" ]; then expect_log=1; fi
done
printf "spectre candidate log\\n" > "$log_path"
printf "raw data\\n" > ../psf/raw.bin
exit 9''')
            script = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "tools", "submit_spectre_job.sh",
            )
            environment = dict(os.environ)
            environment.update({
                "CRESCENDO_SPECTRE_TIMEOUT_LOG": os.path.join(root, "timing.log"),
                "CRESCENDO_SPECTRE_RETAIN_ROOT": candidate_root,
                "CRESCENDO_SIMULATION_TIMEOUT_SECONDS": "5",
            })
            returncode = subprocess.call(
                [script, simulator, "input.scs", "+log", "../psf/spectre.out"],
                cwd=netlist_root, env=environment,
            )
            self.assertEqual(returncode, 9)
            retained_logs = sorted(
                name for name in os.listdir(candidate_root)
                if name.startswith("retained_spectre_") and name.endswith(".log")
            )
            self.assertEqual(len(retained_logs), 1)
            with open(os.path.join(candidate_root, retained_logs[0])) as stream:
                self.assertEqual(stream.read(), "spectre candidate log\n")
            meta_name = retained_logs[0][:-4] + ".meta"
            with open(os.path.join(candidate_root, meta_name)) as stream:
                metadata = stream.read()
            self.assertIn("schema_version=crescendo.retained_spectre_log.v1", metadata)
            self.assertIn(
                "source_path=%s" % os.path.join(psf_root, "spectre.out"),
                metadata,
            )
            self.assertIn("returncode=9", metadata)
            self.assertNotIn("raw.bin", " ".join(os.listdir(candidate_root)))

    def test_production_submit_script_retains_partial_log_on_group_termination(self):
        with tempfile.TemporaryDirectory() as root:
            simulator = os.path.join(root, "fake-spectre")
            netlist_root = os.path.join(root, "run", "netlist")
            psf_root = os.path.join(root, "run", "psf")
            candidate_root = os.path.join(root, "candidate")
            ready_path = os.path.join(root, "ready")
            os.makedirs(netlist_root)
            os.makedirs(psf_root)
            os.makedirs(candidate_root)
            write_executable(simulator, '''
log_path=
expect_log=0
for argument in "$@"; do
  if [ "$expect_log" -eq 1 ]; then log_path=$argument; expect_log=0; continue; fi
  if [ "$argument" = "+log" ]; then expect_log=1; fi
done
printf "partial spectre log\\n" > "$log_path"
: > "$CRESCENDO_READY"
sleep 30''')
            script = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "tools", "submit_spectre_job.sh",
            )
            environment = dict(os.environ)
            environment.update({
                "CRESCENDO_READY": ready_path,
                "CRESCENDO_SPECTRE_TIMEOUT_LOG": os.path.join(root, "timing.log"),
                "CRESCENDO_SPECTRE_RETAIN_ROOT": candidate_root,
                "CRESCENDO_SIMULATION_TIMEOUT_SECONDS": "60",
            })
            process = subprocess.Popen(
                [script, simulator, "input.scs", "+log", "../psf/spectre.out"],
                cwd=netlist_root, env=environment, preexec_fn=os.setsid,
            )
            deadline = time.time() + 5
            while time.time() < deadline and not os.path.isfile(ready_path):
                time.sleep(0.01)
            self.assertTrue(os.path.isfile(ready_path))
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=5)
            retained_logs = [
                name for name in os.listdir(candidate_root)
                if name.startswith("retained_spectre_") and name.endswith(".log")
            ]
            self.assertEqual(len(retained_logs), 1)
            with open(os.path.join(candidate_root, retained_logs[0])) as stream:
                self.assertEqual(stream.read(), "partial spectre log\n")

    def test_production_submit_script_resolves_persistent_candidate_pointer(self):
        with tempfile.TemporaryDirectory() as root:
            simulator = os.path.join(root, "fake-spectre")
            run_root = os.path.join(root, "run")
            candidate_root = os.path.join(root, "candidate")
            pointer = os.path.join(root, "candidate-pointer.txt")
            os.makedirs(run_root)
            os.makedirs(candidate_root)
            with open(pointer, "w") as stream:
                stream.write(candidate_root + "\n")
            write_executable(simulator, 'printf "pointer log\\n" > spectre.out')
            script = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "tools", "submit_spectre_job.sh",
            )
            environment = dict(os.environ)
            environment.update({
                "CRESCENDO_SPECTRE_TIMEOUT_LOG": os.path.join(root, "timing.log"),
                "CRESCENDO_SPECTRE_RETAIN_POINTER": pointer,
                "CRESCENDO_SIMULATION_TIMEOUT_SECONDS": "5",
            })
            self.assertEqual(subprocess.call(
                [script, simulator, "input.scs", "+log=spectre.out"],
                cwd=run_root, env=environment,
            ), 0)
            retained = [
                name for name in os.listdir(candidate_root)
                if name.startswith("retained_spectre_") and name.endswith(".log")
            ]
            self.assertEqual(len(retained), 1)

    def test_toolchain_uses_manifest_relative_paths_and_request_precedence(self):
        with tempfile.TemporaryDirectory() as root:
            manifest = os.path.join(root, "toolchain.json")
            write_json(manifest, {
                "toolchain": {
                    "spectre_bin": "vendor/bin/spectre",
                    "ocean_bin": "vendor/bin/ocean",
                    "extra_library_paths": ["vendor/lib"],
                },
            })
            request_binary = os.path.join(root, "override", "spectre")
            result = discover_toolchain({
                "toolchain_manifest_path": manifest,
                "toolchain": {"spectre_bin": request_binary},
                "_base_dir": root,
            }, environ={"PATH": ""})
            self.assertEqual(result["toolchain"]["spectre_bin"], request_binary)
            self.assertEqual(result["toolchain"]["ocean_bin"], os.path.join(root, "vendor", "bin", "ocean"))
            self.assertEqual(result["toolchain"]["extra_library_paths"], [os.path.join(root, "vendor", "lib")])

    def test_child_environment_is_process_local(self):
        request = {"workdir": "/tmp/private", "environment": {"set": {"CRESCENDO_TEST": "yes"}, "unset": ["REMOVE"]}}
        source = {"PATH": "/usr/bin", "REMOVE": "value"}
        child = build_process_environment({
            "spectre_bin": "/eda/bin/spectre",
            "extra_binary_paths": ["/vendor/bin"],
            "extra_library_paths": ["/vendor/lib"],
            "license_file": "5280@server",
        }, request, environ=source)
        self.assertEqual(source, {"PATH": "/usr/bin", "REMOVE": "value"})
        self.assertEqual(child["PATH"].split(":")[:2], ["/vendor/bin", "/eda/bin"])
        self.assertNotIn("REMOVE", child)
        self.assertEqual(child["CRESCENDO_TEST"], "yes")
        self.assertEqual(child["CDS_LIC_FILE"], "5280@server")

    def test_local_spectre_is_plannable_but_disabled_by_default(self):
        with tempfile.TemporaryDirectory() as root:
            binary = os.path.join(root, "bin", "spectre")
            netlist = os.path.join(root, "input.scs")
            write_executable(binary, "exit 0")
            open(netlist, "w").close()
            request = {
                "backend": "local_spectre", "action": "plan", "workdir": root,
                "toolchain": {"spectre_bin": binary},
                "spectre": {"netlist": netlist, "output_format": "psfxl"},
            }
            planned = execute_request(request)
            self.assertEqual(planned["status"], "planned")
            self.assertFalse(planned["enabled"])
            self.assertIn("-format", planned["command"]["argv"])
            request["action"] = "run"
            refused = execute_request(request)
            self.assertEqual(refused["status"], "invalid")
            self.assertIn("disabled", refused["error"])

    def test_local_spectre_run_collects_normalized_result(self):
        with tempfile.TemporaryDirectory() as root:
            binary = os.path.join(root, "bin", "spectre")
            netlist = os.path.join(root, "input.scs")
            write_executable(binary, 'echo "spectre:$CRESCENDO_MARK"; echo "warning" >&2; exit 0')
            open(netlist, "w").close()
            result = execute_request({
                "backend": "local_spectre", "action": "run", "request_id": "candidate-1",
                "workdir": root, "run_dir": os.path.join(root, "run"), "timeout_seconds": 5,
                "backend_options": {"enabled": True},
                "toolchain": {"spectre_bin": binary},
                "environment": {"inherit": False, "set": {"CRESCENDO_MARK": "isolated"}},
                "spectre": {"netlist": netlist, "escchars": False, "use_64bit": False},
            }, service=SimulationSubmissionService(environ={"PATH": "/usr/bin:/bin"}))
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["returncode"], 0)
            self.assertIn("spectre:isolated", result["stdout_tail"])
            self.assertIn("warning", result["stderr_tail"])
            self.assertTrue(any(row["kind"] == "stdout" and row["exists"] for row in result["artifacts"]))

    def test_timeout_and_explicit_cancel_kill_process_group(self):
        with tempfile.TemporaryDirectory() as root:
            binary = os.path.join(root, "bin", "spectre")
            netlist = os.path.join(root, "input.scs")
            write_executable(binary, "sleep 30")
            open(netlist, "w").close()
            base = {
                "backend": "local_spectre", "workdir": root,
                "backend_options": {"enabled": True},
                "toolchain": {"spectre_bin": binary},
                "spectre": {"netlist": netlist, "escchars": False, "use_64bit": False},
            }
            fast_executor = LocalProcessExecutor(poll_interval_seconds=0.01, terminate_grace_seconds=0.05)
            service = SimulationSubmissionService(executor=fast_executor, environ={"PATH": "/usr/bin:/bin"})
            timed = dict(base, request_id="timeout", timeout_seconds=0.08)
            result = service.run(timed)
            self.assertEqual(result["status"], "timed_out")
            self.assertTrue(result["timed_out"])

            cancellable = dict(base, request_id="cancel", timeout_seconds=10)
            handle = service.submit(cancellable)
            time.sleep(0.03)
            cancelled = service.cancel(handle)
            self.assertEqual(cancelled["status"], "cancelled")
            self.assertTrue(cancelled["cancelled"])
            self.assertIsNotNone(cancelled["returncode"])

    def test_maestro_backend_wraps_ocean_script(self):
        with tempfile.TemporaryDirectory() as root:
            ocean = os.path.join(root, "bin", "ocean")
            script = os.path.join(root, "run.ocn")
            write_executable(ocean, 'echo "ocean:$HOME:$*"')
            open(script, "w").close()
            result = execute_request({
                "backend": "maestro", "action": "run", "request_id": "maestro-1",
                "workdir": root, "run_dir": os.path.join(root, "run"),
                "toolchain": {"ocean_bin": ocean},
                "maestro": {"ocean_script": script},
            }, service=SimulationSubmissionService(environ={"PATH": "/usr/bin:/bin"}))
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["command"]["argv"], [ocean, "-nograph", "-restore", script])
            self.assertIn("ocean:%s:-nograph -restore" % root, result["stdout_tail"])

    def test_adexl_backend_wraps_virtuoso_skill_driver(self):
        with tempfile.TemporaryDirectory() as root:
            virtuoso = os.path.join(root, "bin", "virtuoso")
            script = os.path.join(root, "run_adexl.il")
            write_executable(virtuoso, 'echo "virtuoso:$HOME:$*"')
            open(script, "w").close()
            result = execute_request({
                "backend": "adexl", "action": "run", "request_id": "adexl-1",
                "workdir": root, "run_dir": os.path.join(root, "run"),
                "toolchain": {"virtuoso_bin": virtuoso},
                "adexl": {"skill_script": script},
            }, service=SimulationSubmissionService(environ={"PATH": "/usr/bin:/bin"}))
            self.assertEqual(result["status"], "completed")
            self.assertEqual(
                result["command"]["argv"],
                [virtuoso, "-nograph", "-restore", script],
            )
            self.assertIn("virtuoso:%s:-nograph -restore" % root, result["stdout_tail"])

    def test_probe_reports_selected_and_all_backends_without_running_them(self):
        with tempfile.TemporaryDirectory() as root:
            ocean = os.path.join(root, "bin", "ocean")
            spectre = os.path.join(root, "bin", "spectre")
            write_executable(ocean, "exit 99")
            write_executable(spectre, "exit 99")
            result = execute_request({
                "backend": "local_spectre", "action": "probe", "workdir": root,
                "toolchain": {"ocean_bin": ocean, "spectre_bin": spectre},
            })
            self.assertEqual(result["status"], "available")
            self.assertFalse(result["selected"]["enabled"])
            self.assertEqual(
                [row["backend"] for row in result["backends"]],
                ["adexl", "local_spectre", "maestro"],
            )

    def test_spectre_cli_reads_and_writes_json(self):
        with tempfile.TemporaryDirectory() as root:
            binary = os.path.join(root, "bin", "spectre")
            netlist = os.path.join(root, "input.scs")
            request_path = os.path.join(root, "request.json")
            output_path = os.path.join(root, "result.json")
            write_executable(binary, "exit 0")
            open(netlist, "w").close()
            write_json(request_path, {
                "schema_version": "crescendo.simulation_request.v1",
                "action": "plan", "workdir": ".",
                "toolchain": {"spectre_bin": "bin/spectre"},
                "spectre": {"netlist": "input.scs"},
            })
            with mock.patch("sys.stdout"):
                returncode = spectre_cli(["--request", request_path, "--output", output_path])
            self.assertEqual(returncode, 0)
            with open(output_path, "r") as stream:
                result = json.load(stream)
            self.assertEqual(result["status"], "planned")
            self.assertEqual(result["backend"], "local_spectre")


if __name__ == "__main__":
    unittest.main()
