from __future__ import print_function

import atexit
import contextlib
import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from crescendo_engine import __version__
from crescendo_engine.__main__ import main
from crescendo_engine.ade_setup import (
    build_setup_reference, inspect_setup, normalize_setup_reference,
    resolve_setup_locator,
)
from crescendo_engine.maestro import inspect_maestro


FIXTURE_ROOT = os.path.join(os.path.dirname(__file__), "fixtures")
# The shipped fixture has a text extension; the engine needs the Maestro name,
# so copy it into a scratch directory.
_MAESTRO_DIR = tempfile.mkdtemp(prefix="crescendo-maestro-fixture-")
atexit.register(shutil.rmtree, _MAESTRO_DIR, True)
MAESTRO_FIXTURE = os.path.join(_MAESTRO_DIR, "maestro.sdb")
shutil.copyfile(os.path.join(FIXTURE_ROOT, "maestro.sdb.xml"), MAESTRO_FIXTURE)
shutil.copyfile(os.path.join(FIXTURE_ROOT, "active.state.xml"), os.path.join(_MAESTRO_DIR, "active.state"))
ADEXL_XML_FIXTURE = os.path.join(FIXTURE_ROOT, "adexl_setup.xml")


class AdeSetupInspectionTest(unittest.TestCase):
    def setUp(self):
        self.temporary_root = tempfile.TemporaryDirectory()
        self.adexl_fixture = os.path.join(self.temporary_root.name, "library", "cell", "custom_view_name")
        os.makedirs(self.adexl_fixture)
        shutil.copyfile(ADEXL_XML_FIXTURE, os.path.join(self.adexl_fixture, "data.sdb"))

    def tearDown(self):
        self.temporary_root.cleanup()

    def test_inspects_maestro_through_unified_schema(self):
        setup = inspect_setup(MAESTRO_FIXTURE)
        self.assertEqual(setup["schema_version"], "crescendo.ade_setup.v1")
        self.assertEqual(setup["backend"], "maestro")
        self.assertEqual(setup["setup_locator"]["path"], MAESTRO_FIXTURE)
        self.assertEqual(setup["source"]["setup_db"], MAESTRO_FIXTURE)
        self.assertEqual(setup["source"]["maestro_sdb"], MAESTRO_FIXTURE)
        self.assertEqual(setup["output_names"], ["bandwidth", "gain"])
        self.assertEqual(setup["variables"][0]["name"], "BIAS")

    def test_inspects_adexl_without_hardcoded_view_name(self):
        setup = inspect_setup(self.adexl_fixture)
        self.assertEqual(setup["backend"], "adexl")
        self.assertEqual(setup["setup_locator"]["view_name"], "custom_view_name")
        self.assertEqual(setup["source"]["data_sdb"], os.path.join(self.adexl_fixture, "data.sdb"))
        self.assertEqual(setup["libraries"], ["SMOKE_LIB"])
        self.assertEqual(setup["cells"], ["TB_OPA"])
        self.assertEqual(setup["analysis_names"], ["ac"])
        self.assertEqual(setup["output_names"], ["bandwidth", "gain"])
        self.assertEqual(setup["tests"][0]["outputs"][0]["expression"], 'db20(VF("/out"))')
        self.assertEqual(setup["variables"], [
            {"name": "VDD", "value": "1.8", "scope": "test", "test": "opa_ac"},
            {"name": "temperature", "value": "125", "scope": "corner", "corner": "SS"},
        ])

    def test_saved_terminal_uses_signal_name_instead_of_state_slot(self):
        state_path = os.path.join(self.adexl_fixture, "active.state")
        with open(state_path, "w") as stream:
            stream.write('''<?xml version="1.0"?>
<state><test Name="opa_ac"><field Name="outputList_7" Type="defstruct">
<field Name="name" Type="list">nil</field>
<field Name="expression" Type="list">nil</field>
<field Name="signal" Type="string">"/VOUT"</field>
<field Name="type" Type="symbol">terminal</field>
<field Name="save" Type="symbol">t</field>
</field></test></state>\n''')
        setup = inspect_setup(self.adexl_fixture)
        saved = [row for row in setup["outputs"] if row.get("signal")]
        self.assertEqual(saved[0]["name"], "/VOUT")
        self.assertEqual(saved[0]["signal"], "/VOUT")
        self.assertNotIn("outputList_7", setup["output_names"])

    def test_backend_override_rejects_mismatch(self):
        with self.assertRaisesRegex(ValueError, "backend mismatch"):
            inspect_setup(self.adexl_fixture, backend="maestro")

    def test_ambiguous_directory_requires_backend(self):
        with tempfile.TemporaryDirectory() as root:
            for name, marker in (("maestro.sdb", "maestro"), ("data.sdb", "ADE XL")):
                with open(os.path.join(root, name), "w") as stream:
                    stream.write('<?xml version="1.0"?><setupdb>%s<active/></setupdb>' % marker)
            with self.assertRaisesRegex(ValueError, "ambiguous"):
                resolve_setup_locator(root)
            self.assertEqual(resolve_setup_locator(root, backend="adexl")["backend"], "adexl")

    def test_rejects_non_setupdb_xml(self):
        with tempfile.NamedTemporaryFile(suffix=".sdb", mode="w") as stream:
            stream.write("<notSetupDB />")
            stream.flush()
            with self.assertRaisesRegex(ValueError, "root must be setupdb"):
                inspect_setup(stream.name, backend="adexl")

    def test_explicit_backend_accepts_noncanonical_setupdb_name(self):
        setup_path = os.path.join(self.temporary_root.name, "portable_setup.xml")
        shutil.copyfile(ADEXL_XML_FIXTURE, setup_path)
        self.assertEqual(inspect_setup(setup_path, backend="adexl")["backend"], "adexl")

    def test_reference_helpers_accept_new_and_legacy_shapes(self):
        setup = inspect_setup(self.adexl_fixture)
        reference = build_setup_reference(setup)
        self.assertEqual(reference["backend"], "adexl")
        self.assertEqual(reference["path"], os.path.join(self.adexl_fixture, "data.sdb"))
        legacy = normalize_setup_reference({"maestro_path": MAESTRO_FIXTURE})
        self.assertEqual(legacy["backend"], "maestro")
        self.assertEqual(legacy["path"], MAESTRO_FIXTURE)
        self.assertEqual(normalize_setup_reference(os.path.join("relative", "data.sdb"))["backend"], "adexl")

    def test_maestro_wrapper_preserves_legacy_schema(self):
        setup = inspect_maestro(MAESTRO_FIXTURE)
        self.assertEqual(setup["schema_version"], "crescendo.maestro_setup.v1")
        self.assertEqual(setup["backend"], "maestro")

    def test_cli_inspect_setup(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            returncode = main(["inspect-setup", self.adexl_fixture])
        self.assertEqual(returncode, 0)
        self.assertIn('"backend": "adexl"', output.getvalue())


class CliInformationTests(unittest.TestCase):
    def _information_output(self, arguments):
        output = io.StringIO()
        with contextlib.ExitStack() as stack:
            boundaries = [stack.enter_context(mock.patch(name, side_effect=AssertionError(
                "CLI information must exit before EDA discovery"))) for name in (
                    "crescendo_engine.__main__.load_config",
                    "crescendo_engine.config.discover_eda_environment",
                    "crescendo_engine.cadence.discover_eda_environment",
                )]
            stack.enter_context(contextlib.redirect_stdout(output))
            with self.assertRaises(SystemExit) as exit_status:
                main(arguments)
            self.assertEqual(exit_status.exception.code, 0)
            for boundary in boundaries:
                boundary.assert_not_called()
        return output.getvalue()

    def test_version_matches_package_before_loading_a_config(self):
        self.assertEqual(self._information_output([
            "--config", "/missing/unused-config.json", "--version",
        ]), __version__ + "\n")

    def test_help_exits_before_loading_config_or_discovering_eda(self):
        help_text = self._information_output(["--help"])
        self.assertIn("--version", help_text)
        self.assertIn("Standalone Cadence ADE circuit optimizer", help_text)

    def test_subcommand_help_exits_before_discovering_eda(self):
        self.assertIn("doctor", self._information_output(["doctor", "--help"]))

    def test_real_module_information_entry_works_without_eda_or_runtime_writes(self):
        source = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with tempfile.TemporaryDirectory(prefix="crescendo cli information '") as root:
            environment = dict(os.environ, PATH="", DISPLAY="", PYTHONPATH=source,
                               PYTHONDONTWRITEBYTECODE="1",
                               CRESCENDO_USER_CONFIG=os.path.join(root, "missing-config.json"),
                               CRESCENDO_RUN_ROOT=os.path.join(root, "unused-runs"))
            for arguments in (["--version"], ["--help"], ["doctor", "--help"]):
                with self.subTest(arguments=arguments):
                    result = subprocess.run(
                        [sys.executable, "-B", "-m", "crescendo_engine"] + arguments,
                        cwd=root, env=environment, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, universal_newlines=True, timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stderr, "")
                    if arguments == ["--version"]:
                        self.assertEqual(result.stdout, __version__ + "\n")
                    else:
                        self.assertIn("usage:", result.stdout)
                    self.assertEqual(os.listdir(root), [])


if __name__ == "__main__":
    unittest.main()
