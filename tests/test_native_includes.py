"""Target server include contracts; these fixtures execute no EDA solver."""
import copy
import hashlib
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.native_includes import (
    NativeIncludeError, expand_native_path, freeze_native_includes, saved_model_file_rows,
)
from crescendo_engine.netlist_providers import prepare_maestro_netlists
from crescendo_engine.private_netlist import materialize_candidate, prepare_private_netlist, validate_manifest
from tests.test_netlist_providers import NumericMaestroProviderTest


class NativeIncludeResolutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo PDK ' with spaces ")
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.cwd = os.path.join(self.root, "native netlist")
        self.pdk = os.path.join(self.root, "models")
        os.makedirs(self.cwd)
        for simulator in ("spectre", "alps"):
            os.makedirs(os.path.join(self.pdk, simulator))
        self.model = self.write(os.path.join(self.pdk, "spectre", "mos.scs"), "// Spectre TT\n")
        self.alps_model = self.write(os.path.join(self.pdk, "alps", "mos.scs"), "// ALPS TT\n")
        self.config = {"cadence": {"spectre_model_dir": os.path.join(self.pdk, "spectre"),
                                    "alps_model_dir": os.path.join(self.pdk, "alps")}}

    def write(self, path, text):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as stream:
            stream.write(text)
        return path

    def task(self, include):
        return {"native_cwd": self.cwd, "argv": ["spectre", "input.scs"] + include}

    def freeze(self, text, include=None, **kwargs):
        with mock.patch.dict(os.environ, {"PDK": self.pdk}, clear=False):
            return freeze_native_includes(text, self.task(include or []), self.config, "spectre", **kwargs)

    def test_tcsh_exported_PDK_is_expanded_in_include_and_both_I_forms(self):
        for reference in ("$PDK/spectre/mos.scs", "${PDK}/spectre/mos.scs", "mos.scs"):
            for include in (["-I", "${PDK}/spectre"], ["-I$PDK/spectre"]):
                with self.subTest(reference=reference, include=include):
                    text, argv, proof = self.freeze('include "' + reference + '" section=TT\n', include)
                    self.assertEqual(text, 'include "' + self.model + '" section=TT\n')
                    self.assertNotIn("$PDK", " ".join(argv))
                    self.assertNotIn("${PDK}", " ".join(argv))
                    self.assertEqual(proof["references"][0]["path"], self.model)
                    self.assertFalse(proof["model_format_equivalence_claimed"])

    def test_exact_native_cwd_precedes_ordered_I_even_with_duplicate_basenames(self):
        native = self.write(os.path.join(self.cwd, "mos.scs"), "native first\n")
        text, unused, proof = self.freeze('include "mos.scs"\n', ["-I", os.path.dirname(self.model)])
        self.assertEqual(proof["references"][0]["path"], native)
        os.unlink(native)
        first = self.write(os.path.join(self.root, "first", "nested", "mos.scs"), "first I\n")
        second = self.write(os.path.join(self.root, "second", "nested", "mos.scs"), "second I\n")
        text, unused, proof = self.freeze('include "nested/mos.scs"\n', ["-I", os.path.dirname(os.path.dirname(first)), "-I" + os.path.dirname(os.path.dirname(second))])
        self.assertEqual(proof["references"][0]["path"], first)

    def test_relative_I_roots_are_bound_to_capture_cwd(self):
        relative = self.write(os.path.join(self.cwd, "model dir", "exact.scs"), "relative I\n")
        text, argv, proof = self.freeze('include "exact.scs"\n', ["-I", "model dir"])
        self.assertIn(os.path.join(self.cwd, "model dir"), argv)
        self.assertEqual(proof["references"][0]["path"], relative)

    def test_missing_reference_is_not_guessed_by_basename_or_another_solver(self):
        self.write(os.path.join(self.pdk, "spectre", "nested", "only.scs"), "wrong suffix\n")
        self.write(os.path.join(self.pdk, "alps", "alps_only.scs"), "different format\n")
        for name in ("only.scs", "alps_only.scs"):
            with self.subTest(name=name):
                with self.assertRaisesRegex(NativeIncludeError, "captured cwd/-I"):
                    self.freeze('include "' + name + '"\n')

    def test_undefined_variable_and_empty_I_fail_without_shell_or_search(self):
        for text, include in (('include "${CRESCENDO_UNSET_PDK}/mos.scs"\n', []), ('include "mos.scs"\n', ["-I"])):
            with self.subTest(text=text, include=include):
                with mock.patch("subprocess.Popen", side_effect=AssertionError("Shell must not run")):
                    with self.assertRaises(NativeIncludeError):
                        self.freeze(text, include)

    def test_parameter_include_remains_candidate_local_only_in_symbolic_frontend(self):
        text, unused, proof = self.freeze('include "cr_candidate_parameters.scs"\n', allow_candidate_parameters=True)
        self.assertEqual(text, 'include "cr_candidate_parameters.scs"\n')
        self.assertEqual(proof["references"], [])
        with self.assertRaises(NativeIncludeError):
            self.freeze(text)

    def test_environment_substitution_is_single_pass_and_other_values_stay_private(self):
        environment = {"PDK": "dir with $not_expanded", "CDS_LIC_FILE": "private-license"}
        self.assertEqual(expand_native_path("${PDK}/spectre", environment), "dir with $not_expanded/spectre")
        before = copy.deepcopy(environment)
        expand_native_path("$PDK/alps", environment)
        self.assertEqual(environment, before)

    def test_saved_model_files_literal_pairs_preserve_original_sections_and_hash_filename(self):
        result = saved_model_file_rows('(("toplevel.scs" "TOP_TT_RFTYP") ("# real file.scs" "Pre_Sim"))')
        self.assertEqual([(row['reference'], row['section'], row['enabled']) for row in result],
                         [('toplevel.scs', 'TOP_TT_RFTYP', True), ('# real file.scs', 'Pre_Sim', True)])

    def test_documented_cdsenv_disabled_record_does_not_strip_hash_in_enabled_names(self):
        result = saved_model_file_rows('toplevel.scs;TOP_TT_RFTYP #;disabled.scs;OFF hash#part.scs;Pre_Sim')
        self.assertEqual([(row['reference'], row['section'], row['enabled']) for row in result],
                         [('toplevel.scs', 'TOP_TT_RFTYP', True), ('disabled.scs', 'OFF', False),
                          ('hash#part.scs', 'Pre_Sim', True)])

    def test_hash_space_saved_filename_is_retained_and_native_missing_include_still_fails(self):
        self.assertEqual(saved_model_file_rows('"# toplevel.scs"')[0]['reference'], '# toplevel.scs')
        with self.assertRaisesRegex(NativeIncludeError, '# toplevel.scs'):
            self.freeze('include "# toplevel.scs" section=TOP_TT_RFTYP\n')

    def test_unsupported_saved_model_expressions_are_never_evaluated(self):
        for text in ('list(list("mos.scs" "TT"))', '((strcat("$PDK" "/mos.scs") "TT"))',
                     '(("mos.scs" "TT") extra)', '(("mos.scs" "TT" nil))',
                     'quote(("mos.scs" "TT"))', "'(modelFiles)", "'(modelFiles)'", 't',
                     "'((\"/pdk/mos.scs\" \"TT\"))'"):
            with self.subTest(text=text), mock.patch('subprocess.Popen', side_effect=AssertionError('No shell')):
                with self.assertRaises(ValueError):
                    saved_model_file_rows(text)

    def test_quoted_model_filename_parentheses_remain_literal(self):
        result = saved_model_file_rows('"model(part#1).scs"')
        self.assertEqual(result[0]['reference'], 'model(part#1).scs')


class BothNativeFrontendIncludeTests(unittest.TestCase):
    def test_numeric_and_symbolic_frontends_freeze_same_PDK_search(self):
        fixture = NumericMaestroProviderTest()
        with tempfile.TemporaryDirectory(prefix="crescendo two frontend ' ") as root:
            prepared, parameters, config, spec, old_model = fixture.fixtures(root)
            prepared["setup"]["tests"][0].update(library="PRIVATE", cell="TB", view="schematic")
            pdk = os.path.join(root, "target PDK")
            model_dir = os.path.join(pdk, "spectre")
            os.makedirs(model_dir)
            model = os.path.join(model_dir, "mos.scs")
            with open(model, "w") as stream:
                stream.write("// target native model\n")
            data_file = os.path.join(model_dir, "ind.s10p")
            with open(data_file, "w") as stream:
                stream.write("native data input\n")
            parameters[0]["numeric_value"] = 2e-6
            parameters[0].update(name="r", owner_library="PRIVATE", owner_cell="TB", owner_view="schematic",
                hierarchy_path="/R0", owner_occurrence_path="/", design_library="PRIVATE", design_cell="TB",
                design_view="schematic")
            config["cadence"]["spectre_model_dir"] = model_dir
            base_capture = fixture.native_capture(old_model)
            def capture(workspace, selected, values, symbols, seeds, active_config, phase_root, **kwargs):
                # Reuse native metadata and vary one assignment across real
                # preparation phases, so the symbolic mapping gate also runs.
                result = base_capture(workspace, selected, values, {}, {}, active_config, phase_root,
                                      symbolic=False, verify_numeric_application=True)
                task = result["tasks"][0]
                with open(task["netlist_path"]) as stream:
                    text = stream.read().replace('include "' + old_model + '"', 'include "mos.scs"')
                text += ('parameters INPUT_FILE="ind.s10p" UNUSED="historical-missing-file"\n'
                         'N0 (out 0) nport file="ind.s10p"\n'
                         'N1 (out 0) nport file=INPUT_FILE\n')
                with open(task["netlist_path"], "w") as stream:
                    stream.write(text)
                with open(task["netlist_path"], "rb") as stream:
                    task["template_files"][0]["sha256"] = hashlib.sha256(stream.read()).hexdigest()
                task["argv"].extend(["-I${PDK}/spectre"])
                result["patch_plan"] = {"parameters": []}
                result["export_snapshot"] = {}
                result["applied_values"] = dict((key, value if isinstance(value, (int, float)) else 2e-6) for key, value in values.items())
                return result
            with mock.patch.dict(os.environ, {"PDK": pdk}, clear=False), \
                    mock.patch("crescendo_engine.netlist_providers.build_workspace", side_effect=fixture.workspace), \
                    mock.patch("crescendo_engine.private_netlist.build_workspace", side_effect=fixture.workspace), \
                    mock.patch("crescendo_engine.netlist_providers._capture_phase", side_effect=capture), \
                    mock.patch("crescendo_engine.private_netlist._capture_phase", side_effect=capture), \
                    mock.patch("crescendo_engine.netlist_providers.verify_sources_unchanged", return_value={"ok": True}), \
                    mock.patch("crescendo_engine.private_netlist.verify_sources_unchanged", return_value={"ok": True}):
                numeric = prepare_maestro_netlists(prepared, parameters, {"p": 2e-6}, config,
                    os.path.join(root, "numeric-source"), "c1", spec=spec)
                symbolic = prepare_private_netlist(prepared, parameters, config,
                    os.path.join(root, "symbolic-source"), spec=spec)
                candidate = materialize_candidate(symbolic, {"p": 2.1e-6}, os.path.join(root, "candidate"), "c1")
            for manifest in (numeric, symbolic, candidate):
                self.assertTrue(validate_manifest(manifest))
                task = manifest["tasks"][0]
                self.assertEqual(task["include_resolution"]["references"][0]["path"], model)
                with open(task["netlist_path"]) as stream:
                    text = stream.read()
                self.assertIn('include "' + model + '" section=TT', text)
                self.assertIn('N0 (out 0) nport file="' + data_file + '"', text)
                self.assertIn('N1 (out 0) nport file=INPUT_FILE', text)
                self.assertEqual(task['native_file_inputs']['resolved_input_count'], 1)
                self.assertEqual(task['native_file_inputs']['resolved_inputs'][0]['execution_path'], data_file)
                self.assertEqual(task['native_file_inputs']['test'], 'tb')
                self.assertEqual(task['native_file_inputs']['deferred_count'], 1)
                self.assertNotIn("${PDK}", " ".join(task["argv"]))
                self.assertIn("-I" + model_dir, task["argv"])
            with open(candidate['tasks'][0]['netlist_path']) as stream:
                self.assertIn('include "cr_candidate_parameters.scs"', stream.read())
            self.assertTrue(os.path.isfile(os.path.join(candidate['tasks'][0]['cwd'], 'cr_candidate_parameters.scs')))


if __name__ == "__main__":
    unittest.main()
