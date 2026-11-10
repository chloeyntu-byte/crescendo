"""Focused GUI dependency repair checks using existing Python and private ELF fixtures.

The delegates and dlopen graph exercise real loader failures. They are not a
new Python/Tk installation or evidence of acceptance on a second machine.
"""
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from crescendo_engine import gui_runtime_dependencies as dependencies
from crescendo_engine import python_runtime as runtime
from crescendo_engine.native_environment import GUI_ENV_NAMES


class GuiRuntimeDependenciesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc"):
            raise unittest.SkipTest("Private ELF fixtures require an existing gcc; nothing is installed")

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="crescendo GUI's ELF fixtures ")
        self.root = Path(self.temporary.name)
        self.prefix = self.root / "Python installation's prefix"
        self.candidate = self.prefix / "bin/python3"
        self.extension = self.prefix / "lib/python3.11/lib-dynload/_tkinter.so"
        self.environment = {key: value for key, value in os.environ.items()
                            if key not in GUI_ENV_NAMES and not key.startswith("CRESCENDO_")}
        self.metadata = dict(prefix=str(self.prefix), base_prefix=str(self.prefix),
                             exec_prefix=str(self.prefix), tkinter_file=str(self.extension),
                             libdirs=[str(self.prefix / "lib")],
                             tcl_version="8.6", tk_version="8.6")

    def tearDown(self):
        self.temporary.cleanup()

    def compile(self, target, source, soname=None, links=()):
        target = Path(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        source_file = target.with_name(target.name + ".c")
        source_file.write_text(source, encoding="utf-8")
        command = ["gcc", "-x", "c", str(source_file), "-o", str(target)]
        if soname:
            command += ["-shared", "-fPIC", "-Wl,-soname," + soname]
        if links:
            command += ["-x", "none", "-Wl,--no-as-needed"] + [str(path) for path in links]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        target.chmod(0o755)
        source_file.unlink()
        return target

    def delegate(self):
        library = self.compile(self.prefix / "lib/tcl-tk/lib/libcr_gui_runtime_fixture.so",
                               "int cr_runtime_fixture(void){return 1;}",
                               "libcr_gui_runtime_fixture.so")
        source = '''#include <unistd.h>
extern int cr_runtime_fixture(void);
int main(int argc,char **argv){
 if(cr_runtime_fixture()!=1)return 89;
 argv[0]=REAL_PYTHON;
 execv(REAL_PYTHON,argv);
 return 90;
}'''.replace("REAL_PYTHON", json.dumps(sys.executable))
        return self.compile(self.candidate, source, links=[library]), library

    def test_real_missing_loader_dependency_recovers_existing_python_and_tcl(self):
        # Only the launcher ELF is a fixture; the successful probe imports and
        # initializes the current interpreter's real Tcl/Tk installation.
        real = runtime._probe_once(sys.executable, self.environment, 10)
        if real["state"] != "eligible":
            self.skipTest("The test interpreter needs an existing working Tk installation")
        candidate, library = self.delegate()
        before = runtime._probe_once(str(candidate), self.environment, 10)
        self.assertEqual(before["state"], "rejected")
        self.assertIn("libcr_gui_runtime_fixture.so", before.get("error", ""))
        self.assertNotEqual(before.get("returncode"), 0)
        original = dict(self.environment)
        result = runtime.probe_candidate(str(candidate), self.environment, timeout=15)
        self.assertEqual(result["state"], "eligible", result)
        self.assertTrue(result.get("tcl_version"))
        if not result["display_unavailable"]:
            self.assertEqual(result["tk_init"], "PASS")
        self.assertEqual(result["gui_overlays"], {"LD_LIBRARY_PATH": str(library.parent)})
        self.assertEqual(self.environment, original)
        after = runtime._probe_once(str(candidate), dict(self.environment, **result["gui_overlays"]), 10)
        self.assertEqual(after["state"], "eligible", after)

    def graph(self):
        self.compile(self.candidate, "int main(void){return 0;}")
        leaf = self.compile(self.prefix / "lib/libcr_gui_runtime_leaf_fixture.so",
                            "int cr_leaf(void){return 1;}", "libcr_gui_runtime_leaf_fixture.so")
        tcl = self.compile(self.prefix / "lib/tcl8.6/libtcl8.6_cr_fixture.so",
                           "extern int cr_leaf(void);int cr_tcl(void){return cr_leaf();}",
                           "libtcl8.6_cr_fixture.so", [leaf])
        tk = self.compile(self.prefix / "lib/tcl-tk/lib/libtk8.6_cr_fixture.so",
                          "extern int cr_tcl(void);int cr_tk(void){return cr_tcl();}",
                          "libtk8.6_cr_fixture.so", [tcl])
        self.compile(self.extension,
                     "extern int cr_tk(void);int cr_extension(void){return cr_tk();}",
                     "_tkinter.so", [tk])
        return leaf, tcl, tk

    def loader_probe(self, outcomes):
        def probe(candidate, environment, timeout):
            process = subprocess.run([sys.executable, "-I", "-B", "-c",
                                      "import ctypes,sys;ctypes.CDLL(sys.argv[1]);print('loaded')",
                                      str(self.extension)], env=environment,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
            outcomes.append((process, dict(environment)))
            return dict(path=candidate, state="eligible" if process.returncode == 0 else "rejected",
                        stage="ready" if process.returncode == 0 else "tk_import",
                        error=process.stderr.decode("utf-8", "replace"),
                        metadata=dict(self.metadata), tk_init="NOT TESTED", display_unavailable=True)
        return probe

    def test_real_dlopen_transitive_dependencies_restore_only_needed_directories(self):
        leaf, tcl, tk = self.graph()
        unrelated = self.root / "unrelated Cadence libraries"
        unrelated.mkdir()
        environment = dict(self.environment, LD_LIBRARY_PATH=str(unrelated) + ":" + str(tk.parent),
                           TCL_LIBRARY="/invalid/inherited/tcl", TK_LIBRARY="/invalid/inherited/tk")
        original = dict(environment)
        outcomes = []
        with patch.object(runtime, "_probe_once", side_effect=self.loader_probe(outcomes)):
            result = runtime.probe_candidate(str(self.candidate), environment, timeout=15)
        self.assertEqual(result["state"], "eligible", result)
        self.assertGreaterEqual(len(outcomes), 4)
        self.assertIn(tk.name, outcomes[0][0].stderr.decode())
        self.assertIn(tcl.name, outcomes[1][0].stderr.decode())
        self.assertIn(leaf.name, outcomes[2][0].stderr.decode())
        self.assertEqual(outcomes[-1][0].returncode, 0)
        self.assertEqual(set(result["gui_overlays"]["LD_LIBRARY_PATH"].split(os.pathsep)),
                         {str(leaf.parent), str(tcl.parent), str(tk.parent)})
        for unused, probe_environment in outcomes:
            self.assertNotIn(str(unrelated), probe_environment.get("LD_LIBRARY_PATH", "").split(os.pathsep))
            self.assertNotIn("TCL_LIBRARY", probe_environment)
            self.assertNotIn("TK_LIBRARY", probe_environment)
        self.assertEqual(environment, original)

    def test_real_loader_retains_soname_symlink_directory(self):
        candidate, library = self.delegate()
        target = self.root / "separate versioned library" / (library.name + ".1.2")
        target.parent.mkdir()
        library.rename(target)
        library.symlink_to(target)
        before = runtime._probe_once(str(candidate), self.environment, 5)
        self.assertIn(library.name, before.get("error", ""))
        result = runtime.probe_candidate(str(candidate), self.environment, timeout=10)
        self.assertEqual(result["state"], "eligible", result)
        self.assertEqual(result["gui_overlays"]["LD_LIBRARY_PATH"], str(library.parent))

    def matching_library(self, soname="libtk8.6_cr_fixture.so"):
        self.compile(self.candidate, "int main(void){return 0;}")
        filename = self.compile(self.prefix / "lib/libtk8.6_cr_fixture.so",
                                "int cr_library(void){return 1;}", soname)
        return filename, dependencies._elf_info(self.candidate)

    def test_elf_architecture_mismatch_is_rejected(self):
        filename, reference = self.matching_library()
        raw = bytearray(filename.read_bytes())
        raw[18:20] = struct.pack("<H", 40)  # ARM ELF machine, versus current x86_64.
        filename.write_bytes(raw)
        diagnostics = []
        selected = dependencies._select_library(filename.name, [str(filename.parent)], reference,
                                                self.metadata, diagnostics)
        self.assertIsNone(selected)
        self.assertIn("architecture mismatch", "\n".join(diagnostics))

    def test_wrong_soname_is_rejected(self):
        filename, reference = self.matching_library("libcr_wrong_soname.so")
        diagnostics = []
        selected = dependencies._select_library(filename.name, [str(filename.parent)], reference,
                                                self.metadata, diagnostics)
        self.assertIsNone(selected)
        self.assertIn("SONAME mismatch", "\n".join(diagnostics))

    def test_tkinter_abi_mismatch_is_rejected(self):
        filename, reference = self.matching_library()
        diagnostics = []
        metadata = dict(self.metadata, tk_version="8.5")
        selected = dependencies._select_library(filename.name, [str(filename.parent)], reference,
                                                metadata, diagnostics)
        self.assertIsNone(selected)
        self.assertIn("_tkinter Tcl/Tk ABI mismatch", "\n".join(diagnostics))

    def test_loader_hints_ignore_relative_empty_and_unrelated_qt_paths(self):
        self.compile(self.candidate, "int main(void){return 0;}")
        qt = self.root / "Cadence/Qt5/lib"
        hint = self.root / "matching lookup clue"
        qt.mkdir(parents=True)
        hint.mkdir()
        environment = {"CRESCENDO_DEPENDENCY_LOADER_HINT": ":.:relative:" + str(qt) + ":" + str(hint) + ":"}
        original = dict(environment)
        directories = dependencies._candidate_directories(str(self.candidate), self.metadata,
                                                           environment, [])
        self.assertIn(str(hint), directories)
        self.assertNotIn(str(qt), directories)
        self.assertTrue(all(os.path.isabs(directory) for directory in directories))
        self.assertEqual(environment, original)

    def test_tcl_and_tk_script_directories_must_match_runtime_version(self):
        for kind, script, package in (("tcl", "init.tcl", "Tcl"), ("tk", "tk.tcl", "Tk")):
            with self.subTest(kind=kind):
                wrong = self.root / (kind + "8.5")
                correct = self.root / (kind + "8.6")
                unknown = self.root / (kind + "-unknown")
                for directory, text in ((wrong, "package provide %s 8.5\n" % package),
                                        (correct, "package require -exact %s 8.6.13\n" % package),
                                        (unknown, "# no identifiable ABI\n")):
                    directory.mkdir()
                    (directory / script).write_text(text, encoding="utf-8")
                diagnostics = []
                selected = dependencies._data_candidates(kind, [str(wrong), str(unknown), str(correct)],
                                                           self.metadata, set(), diagnostics)
                self.assertEqual(selected, str(correct))
                self.assertIn("ABI mismatch", "\n".join(diagnostics))
                self.assertIn("unidentifiable ABI", "\n".join(diagnostics))
                self.assertIsNone(dependencies._data_candidates(kind, [str(correct)], self.metadata,
                                                                {str(correct)}, []))

    def test_non_dependency_failure_never_injects_native_loader_hints(self):
        environment = dict(self.environment, LD_LIBRARY_PATH="/native/cadence/libs",
                           LD_PRELOAD="/native/cadence/preload.so")
        original = dict(environment)
        rejected = dict(state="rejected", stage="tk_import", error="No module named '_tkinter'",
                        tk_init="NOT TESTED", display_unavailable=True)
        with patch.object(runtime, "_probe_once", return_value=rejected) as probe:
            result = runtime.probe_candidate(str(self.candidate), environment, timeout=5)
        self.assertEqual(result["state"], "rejected")
        self.assertEqual(result["gui_overlays"], {})
        self.assertEqual(probe.call_count, 1)
        self.assertNotIn("LD_LIBRARY_PATH", probe.call_args[0][1])
        self.assertNotIn("LD_PRELOAD", probe.call_args[0][1])
        self.assertEqual(environment, original)


if __name__ == "__main__":
    unittest.main()
