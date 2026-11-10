import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.cadence import discover_eda_environment
from crescendo_engine.native_environment import NATIVE_ENV_NAMES
from crescendo_engine.simulator_profiles import (
    apply_source_profile_defaults, discover_simulator_profiles, model_directory,
    setup_model_references,
)


class SourceModelDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo model roots ' ")
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        for name in ("spectre", "alps", "work", "view"):
            os.makedirs(os.path.join(self.root, name))
        self.spectre = os.path.join(self.root, "spectre")
        self.alps = os.path.join(self.root, "alps")
        for name, section in (("spectre", "TT"), ("alps", "TT_ALPS")):
            with open(os.path.join(self.root, name, "toplevel.scs"), "w") as stream:
                stream.write("section %s\nmodel nmos bsim4\nendsection %s\n" % (section, section))
        self.xml = os.path.join(self.root, "view", "maestro.sdb")
        self.setup = {"source": {"setup_db": self.xml},
            "tests": [{"name": "test", "simulator": "spectre"}],
            "model_groups": [{"name": "g", "models": [{"file": "toplevel.scs", "section": "TT"}]}]}
        self.config = {"cadence": {"project_dir": os.path.join(self.root, "work"),
                                  "launch_environment": {key: None for key in NATIVE_ENV_NAMES}}}
        self.save_include_path(self.spectre)

    def save_include_path(self, path):
        from xml.sax.saxutils import escape
        with open(self.xml, "w") as stream:
            stream.write('<root><field Name="includePath">%s</field><modelfile>"toplevel.scs"</modelfile></root>' %
                         escape(json.dumps(path)))

    def test_relative_model_group_uses_saved_include_path_before_resolution(self):
        expected = [{"file": os.path.join(self.spectre, "toplevel.scs"), "section": "TT"}]
        self.assertEqual(setup_model_references(self.setup, self.config), expected)
        before = copy.deepcopy(self.config)
        derived = apply_source_profile_defaults(self.config, self.setup)
        self.assertEqual(derived["cadence"]["spectre_model_dir"], self.spectre)
        self.assertEqual(derived["cadence"]["alps_model_dir"], self.alps)
        self.assertEqual(self.config, before)
        self.assertEqual(derived["cadence"]["discovery"]["effective"]["alps_model_dir"], self.alps)
        self.assertEqual(derived["cadence"]["discovery"]["sources"]["spectre_model_dir"], "source_model_root")
        self.assertFalse(derived["cadence"]["_model_discovery"]["model_sections_qualified"])

    def test_relative_model_is_resolved_against_cadence_project_directory(self):
        self.save_include_path("")
        self.setup["model_groups"][0]["models"][0]["file"] = "../spectre/toplevel.scs"
        self.assertEqual(setup_model_references(self.setup, self.config)[0]["file"],
                         os.path.join(self.spectre, "toplevel.scs"))

    def test_relative_include_path_is_based_on_native_workdir(self):
        self.save_include_path("../spectre")
        self.assertEqual(setup_model_references(self.setup, self.config)[0]["file"],
                         os.path.join(self.spectre, "toplevel.scs"))

    def test_native_context_workdir_is_used_when_cadence_project_dir_is_absent(self):
        self.config["cadence"].pop("project_dir")
        self.config["_maestro_launch_context"] = {"source": {"work_dir": os.path.join(self.root, "work")}}
        self.save_include_path("../spectre")
        self.assertEqual(setup_model_references(self.setup, self.config)[0]["file"],
                         os.path.join(self.spectre, "toplevel.scs"))

    def test_saved_include_order_wins_over_fallback_model_directory(self):
        fallback = os.path.join(self.root, "fallback", "spectre")
        os.makedirs(fallback)
        with open(os.path.join(fallback, "toplevel.scs"), "w") as stream:
            stream.write("// a different fallback model\n")
        self.config["cadence"]["spectre_model_dir"] = fallback
        self.assertEqual(setup_model_references(self.setup, self.config)[0]["file"],
                         os.path.join(self.spectre, "toplevel.scs"))

    def test_literal_xml_fallback_does_not_include_quote_characters_in_filename(self):
        self.setup.pop("model_groups")
        self.assertEqual(setup_model_references(self.setup, self.config),
                         [{"file": os.path.join(self.spectre, "toplevel.scs"), "section": None}])

    def test_unresolved_model_remains_blocked_even_when_include_root_is_found(self):
        self.setup["model_groups"][0]["models"][0]["file"] = "missing.scs"
        derived = apply_source_profile_defaults(self.config, self.setup)
        self.assertEqual(derived["cadence"]["spectre_model_dir"], self.spectre)
        self.assertEqual(derived["cadence"]["_model_dir_origins"]["spectre"], "saved_setup_include_path")
        self.assertEqual(len(derived["cadence"]["_model_discovery"]["failures"]), 1)
        with self.assertRaisesRegex(ValueError, "missing.scs"):
            setup_model_references(self.setup, self.config)
        discovered = discover_simulator_profiles(self.config, self.setup)
        self.assertFalse(discovered["profiles"]["spectre"]["ready"])
        self.assertEqual(discovered["profiles"]["spectre"]["status"], "blocked")

    def test_configured_pdk_dir_direct_children_are_discovered_without_scanning(self):
        config = {"cadence": {"pdk_dir": self.root}}
        with mock.patch("os.walk", side_effect=AssertionError("PDK must not be scanned")), mock.patch(
                "subprocess.Popen", side_effect=AssertionError("No native probe")):
            discovery = discover_eda_environment(config["cadence"], validate=False, environ={"PATH": ""})
        self.assertEqual(discovery["effective"]["spectre_model_dir"], self.spectre)
        self.assertEqual(discovery["effective"]["alps_model_dir"], self.alps)
        self.assertEqual(discovery["sources"]["alps_model_dir"], "configured_pdk_dir")

    def test_multiple_supplied_model_roots_are_ambiguous_and_not_silently_chosen(self):
        other = os.path.join(self.root, "other")
        os.makedirs(os.path.join(other, "spectre"))
        cadence = {"model_roots": [self.root, other]}
        self.assertEqual(model_directory(cadence, "spectre"), (None, "ambiguous_configured_model_roots"))

    def test_explicit_bad_solver_model_directory_never_falls_back(self):
        missing = os.path.join(self.root, "missing")
        self.config["cadence"].update(spectre_model_dir=missing, _manual_overrides={"spectre_model_dir": missing})
        derived = apply_source_profile_defaults(self.config, self.setup)
        self.assertEqual(derived["cadence"]["spectre_model_dir"], missing)
        self.assertEqual(derived["cadence"]["_manual_overrides"], {"spectre_model_dir": missing})

    def test_captured_pdk_wins_over_ambient_gui_environment(self):
        self.config["cadence"]["launch_environment"]["PDK"] = self.root
        self.save_include_path("$PDK/spectre")
        with mock.patch.dict(os.environ, {"PDK": "/different/gui/pdk"}, clear=False):
            derived = apply_source_profile_defaults(self.config, self.setup)
        self.assertEqual(derived["cadence"]["spectre_model_dir"], self.spectre)
        # The captured PDK expands the saved includePath; the resolved current
        # source file then supplies directory evidence ahead of location hints.
        self.assertEqual(derived["cadence"]["_model_dir_origins"]["spectre"], "source_model_root")

    def test_frozen_unset_or_empty_pdk_does_not_borrow_ambient_model_paths(self):
        self.setup = {}
        for value in (None, ""):
            self.config["cadence"]["launch_environment"]["PDK"] = value
            with self.subTest(value=value), mock.patch.dict(os.environ, {"PDK": self.root}, clear=False):
                derived = apply_source_profile_defaults(self.config, self.setup)
            self.assertIsNone(derived["cadence"].get("spectre_model_dir"))

    def test_invalid_model_override_env_does_not_fall_back_to_pdk(self):
        missing = os.path.join(self.root, "missing")
        selected = model_directory({}, "alps", environ={"PDK": self.root, "CRESCENDO_ALPS_MODEL_DIR": missing})
        self.assertEqual(selected, (missing, "environment:CRESCENDO_ALPS_MODEL_DIR"))

    def _native_saved_model_files(self, value, extra=''):
        from xml.sax.saxutils import escape
        self.setup.pop('model_groups', None)
        self.setup['tests'] = [{'name': 'C', 'simulator': 'spectre', 'enabled': True}]
        with open(self.xml, 'w') as stream:
            stream.write('<setupdb><active/>' + extra + '</setupdb>')
        state = os.path.join(self.root, 'view', 'active.state')
        with open(state, 'w') as stream:
            stream.write('<state><test Name="C"><field Name="includePath">"$PDK/spectre"</field>'
                         '<field Name="modelFiles">' + escape(value) + '</field></test></state>')
        self.setup['source']['active_state'] = state
        self.config['cadence']['launch_environment']['PDK'] = self.root
        binary = os.path.join(self.root, 'fixture_solver')
        with open(binary, 'w') as stream:
            stream.write('#!/bin/sh\nexit 0\n')
        os.chmod(binary, 0o700)
        self.config['cadence'].update(spectre_bin=binary, alps_bin=binary)

    def test_actual_relative_model_section_from_active_state_qualifies_both_pdk_profiles(self):
        for solver in ('spectre', 'alps'):
            with open(os.path.join(self.root, solver, 'toplevel.scs'), 'w') as stream:
                stream.write('section TOP_TT_RFTYP\nmodel nmos bsim4\nendsection TOP_TT_RFTYP\n')
        self._native_saved_model_files('(("toplevel.scs" "TOP_TT_RFTYP"))')
        before = copy.deepcopy(self.config)
        with mock.patch('subprocess.Popen', side_effect=AssertionError('No native execution')):
            result = discover_simulator_profiles(self.config, self.setup)
            from crescendo_engine.native_includes import freeze_native_includes
            from crescendo_engine.simulators import execution_contract, rewrite_model_references
            selected = apply_source_profile_defaults(self.config, self.setup)
            text, argv, include_proof = freeze_native_includes(
                'include "toplevel.scs" section=TOP_TT_RFTYP\n',
                {'native_cwd': self.config['cadence']['project_dir'], 'argv': ['spectre', 'input.scs', '-I$PDK/spectre']},
                selected, 'spectre')
            self.assertEqual(text, 'include "' + os.path.join(self.spectre, 'toplevel.scs') + '" section=TOP_TT_RFTYP\n')
            self.assertEqual(include_proof['include_dirs'], [self.spectre])
            contract = execution_contract(selected, 'spectre',
                [{'file': include_proof['references'][0]['path'], 'section': 'TOP_TT_RFTYP'}], spec={'simulator': 'alps'})
            self.assertEqual(rewrite_model_references(text, contract),
                'include "' + os.path.join(self.alps, 'toplevel.scs') + '" section=TOP_TT_RFTYP\n')
        for solver in ('spectre', 'alps'):
            profile = result['profiles'][solver]
            self.assertTrue(profile['ready'], profile)
            self.assertEqual(profile['model_dir'], os.path.join(self.root, solver))
            self.assertEqual(profile['model_evidence'][0]['source_section'], 'TOP_TT_RFTYP')
            self.assertEqual(profile['model_evidence'][0]['section'], 'TOP_TT_RFTYP')
        self.assertEqual(self.config, before)

    def test_uninterpreted_hash_space_saved_metadata_defers_without_false_profile_block(self):
        self._native_saved_model_files('(("# toplevel.scs" "TOP_TT_RFTYP"))')
        result = discover_simulator_profiles(self.config, self.setup)
        for profile in result['profiles'].values():
            self.assertEqual(profile['status'], 'unqualified')
            self.assertFalse(profile['ready'])
            deferred = profile['deferred_model_metadata'][0]
            self.assertEqual(deferred['reference'], '# toplevel.scs')
            self.assertEqual(deferred['source_file'], self.setup['source']['active_state'])
        self.assertFalse(result['model_discovery']['static_references_complete'])

    def test_real_hash_space_model_filename_is_resolved_exactly(self):
        filename = '# toplevel.scs'
        for solver in ('spectre', 'alps'):
            with open(os.path.join(self.root, solver, filename), 'w') as stream:
                stream.write('section TOP_TT_RFTYP\nmodel nmos bsim4\nendsection TOP_TT_RFTYP\n')
        self._native_saved_model_files('(("# toplevel.scs" "TOP_TT_RFTYP"))')
        references = setup_model_references(self.setup, self.config)
        self.assertEqual(references, [{'file': os.path.join(self.spectre, filename), 'section': 'TOP_TT_RFTYP'}])
        self.assertTrue(discover_simulator_profiles(self.config, self.setup)['profiles']['alps']['ready'])

    def test_unknown_model_expression_defers_but_known_missing_model_remains_blocked(self):
        self._native_saved_model_files('list(list("toplevel.scs" "TT"))')
        self.assertTrue(all(row['status'] == 'unqualified' for row in
                            discover_simulator_profiles(self.config, self.setup)['profiles'].values()))
        self._native_saved_model_files('(("missing.scs" "TT"))')
        self.assertTrue(all(row['status'] == 'blocked' for row in
                            discover_simulator_profiles(self.config, self.setup)['profiles'].values()))

    def test_documented_disabled_model_string_is_not_required(self):
        self._native_saved_model_files('toplevel.scs;TT #;disabled_missing.scs;OFF')
        references = setup_model_references(self.setup, self.config)
        self.assertEqual(references, [{'file': os.path.join(self.spectre, 'toplevel.scs'), 'section': 'TT'}])
        derived = apply_source_profile_defaults(self.config, self.setup)
        self.assertEqual(derived['cadence']['_model_discovery']['disabled_model_references'][0]['reference'], 'disabled_missing.scs')

    def test_history_and_disabled_test_model_records_do_not_enter_current_fallback(self):
        self._native_saved_model_files('(("toplevel.scs" "TT"))',
            '<history><field Name="modelFiles">"old_missing.scs"</field></history>')
        state = self.setup['source']['active_state']
        with open(state) as stream:
            original = stream.read()
        with open(state, 'w') as stream:
            stream.write(original.replace('</state>', '<test Name="off" enabled="0"><field Name="modelFiles">"off_missing.scs"</field></test>'
                '<disabledtests><test Name="C"><field Name="modelFiles">"list_missing.scs"</field></test></disabledtests></state>'))
        self.assertEqual(setup_model_references(self.setup, self.config),
                         [{'file': os.path.join(self.spectre, 'toplevel.scs'), 'section': 'TT'}])

    def test_explicit_current_test_selection_is_not_replaced_by_saved_disabled_list(self):
        self._native_saved_model_files('(("toplevel.scs" "TT"))')
        state = self.setup['source']['active_state']
        with open(state) as stream:
            original = stream.read()
        with open(state, 'w') as stream:
            stream.write(original.replace('<test Name="C">', '<test Name="C" enabled="0">').replace('</state>',
                '<disabledtests><test Name="C"/></disabledtests></state>'))
        self.assertTrue(self.setup['tests'][0]['enabled'])
        self.assertEqual(setup_model_references(self.setup, self.config),
                         [{'file': os.path.join(self.spectre, 'toplevel.scs'), 'section': 'TT'}])

    def test_non_string_model_group_reference_stays_a_bounded_configuration_failure(self):
        self.setup['model_groups'][0]['models'][0]['file'] = True
        result = discover_simulator_profiles(self.config, self.setup)
        for profile in result['profiles'].values():
            self.assertEqual(profile['status'], 'blocked')
            self.assertFalse(profile['ready'])
        derived = apply_source_profile_defaults(self.config, self.setup)
        failure = derived['cadence']['_model_discovery']['failures'][0]
        self.assertEqual(failure['error_type'], 'NativeIncludeError')
        with self.assertRaises(ValueError):
            setup_model_references(self.setup, self.config)


    def test_partial_saved_metadata_does_not_run_an_automatic_model_bridge(self):
        self._native_saved_model_files('(("toplevel.scs" "TT") ("# pending.scs" "TT"))')
        with open(os.path.join(self.spectre, 'toplevel.scs'), 'w') as stream:
            stream.write('section TT\nunknown_vendor_control option=1\nendsection TT\n')
        from crescendo_engine.simulator_profiles import ModelProfileError
        with mock.patch('crescendo_engine.simulator_profiles.automatic_model_mapping',
                        side_effect=ModelProfileError('No readable model/subckt interface')) as bridge:
            result = discover_simulator_profiles(self.config, self.setup)
        bridge.assert_not_called()
        for solver, profile in result['profiles'].items():
            self.assertEqual(profile['status'], 'unqualified')
            self.assertEqual(profile['model_dir'], os.path.join(self.root, solver))
            self.assertEqual(profile['capture_source_model_dir'], self.spectre)
            self.assertEqual(profile['source_model_references'][0]['file'], os.path.join(self.spectre, 'toplevel.scs'))
            self.assertEqual(profile['target_model_references'], [])
            self.assertEqual(profile['resolved_model_map'], {})
            self.assertEqual(profile['reason_code'], 'native_model_capture_required')
            self.assertEqual(profile['qualification_stage'], 'saved_model_metadata')
            self.assertTrue(profile['next_action'])

    def test_partial_saved_metadata_does_not_hide_invalid_solver_or_model_root(self):
        self._native_saved_model_files('(("# pending.scs" "TT"))')
        missing = os.path.join(self.root, 'missing')
        for key in ('alps_bin', 'alps_model_dir'):
            config = copy.deepcopy(self.config)
            config['cadence'][key] = missing
            with self.subTest(key=key):
                profile = discover_simulator_profiles(config, self.setup)['profiles']['alps']
                self.assertEqual(profile['status'], 'blocked')
                self.assertFalse(profile['ready'])

    def test_complete_unreadable_interface_keeps_source_and_target_locations_distinct(self):
        self._native_saved_model_files('(("toplevel.scs" "TT"))')
        with open(os.path.join(self.spectre, 'toplevel.scs'), 'w') as stream:
            stream.write('section TT\nunknown_vendor_control option=1\nendsection TT\n')
        profile = discover_simulator_profiles(self.config, self.setup)['profiles']['alps']
        self.assertEqual(profile['status'], 'blocked')
        self.assertEqual(profile['model_dir'], self.alps)
        self.assertEqual(profile['capture_source_model_dir'], self.spectre)
        self.assertEqual(profile['source_model_references'][0]['file'], os.path.join(self.spectre, 'toplevel.scs'))
        self.assertEqual(profile['target_model_references'], [])
        self.assertEqual(profile['reason_code'], 'model_interface_unavailable')
        self.assertTrue(profile['next_action'])

    def test_explicit_native_references_still_enforce_interface_when_saved_metadata_is_partial(self):
        self._native_saved_model_files('(("# pending.scs" "TT"))')
        source = os.path.join(self.spectre, 'toplevel.scs')
        with open(source, 'w') as stream:
            stream.write('section TT\nunknown_vendor_control option=1\nendsection TT\n')
        profile = discover_simulator_profiles(self.config, self.setup,
            model_references=[{'file': source, 'section': 'TT'}])['profiles']['alps']
        self.assertEqual(profile['status'], 'blocked')
        self.assertFalse(profile['ready'])
        self.assertEqual(profile['reason_code'], 'model_interface_unavailable')

    def _saved_new_pdk_with_cached_auto_roots(self, promoted=False):
        self._native_saved_model_files('(("toplevel.scs" "TT"))')
        current = os.path.join(self.root, 'saved_new_pdk')
        for solver, section in (('spectre', 'TT'), ('alps', 'TT_ALPS')):
            directory = os.path.join(current, solver)
            os.makedirs(directory)
            with open(os.path.join(directory, 'toplevel.scs'), 'w') as stream:
                stream.write('section ' + section + '\nmodel nmos bsim4\nendsection ' + section + '\n')
        state = self.setup['source']['active_state']
        with open(state) as stream:
            text = stream.read()
        with open(state, 'w') as stream:
            stream.write(text.replace('$PDK/spectre', os.path.join(current, 'spectre')))
        cadence = self.config['cadence']
        cadence.update(_manual_overrides={}, _file_overrides={})
        effective = {solver + '_model_dir': os.path.join(self.root, solver) for solver in ('spectre', 'alps')}
        cadence.update(effective)
        cadence['discovery'] = {'effective': effective,
            'sources': {key: 'manual' if promoted else 'environment:PDK/' + key.split('_', 1)[0]
                        for key in effective}}
        return current

    def test_current_saved_include_path_supersedes_cached_automatic_pdk_dirs(self):
        current = self._saved_new_pdk_with_cached_auto_roots()
        before = copy.deepcopy(self.config)
        result = discover_simulator_profiles(self.config, self.setup)
        for solver, profile in result['profiles'].items():
            self.assertTrue(profile['ready'], profile)
            self.assertEqual(profile['model_dir'], os.path.join(current, solver))
            self.assertEqual(profile['capture_source_model_dir'], os.path.join(current, 'spectre'))
        self.assertEqual(self.config, before)

    def test_discovery_effective_values_accidentally_labeled_manual_are_not_user_choices(self):
        current = self._saved_new_pdk_with_cached_auto_roots(promoted=True)
        result = discover_simulator_profiles(self.config, self.setup)
        self.assertEqual(result['profiles']['alps']['model_dir'], os.path.join(current, 'alps'))
        self.assertTrue(result['profiles']['alps']['ready'])

    def test_real_manual_file_and_model_environment_choices_stay_authoritative(self):
        current = self._saved_new_pdk_with_cached_auto_roots()
        refs = [{'file': os.path.join(current, 'spectre', 'toplevel.scs'), 'section': 'TT'}]
        for declaration in ('_manual_overrides', '_file_overrides'):
            cadence = copy.deepcopy(self.config['cadence'])
            cadence[declaration]['alps_model_dir'] = self.alps
            with self.subTest(declaration=declaration):
                self.assertEqual(model_directory(cadence, 'alps', references=refs)[0], self.alps)
                self.assertEqual(model_directory(cadence, 'alps', references=refs)[1],
                                 'manual' if declaration == '_manual_overrides' else 'configuration file')
        cadence = copy.deepcopy(self.config['cadence'])
        cadence['_manual_overrides']['alps_model_dir'] = self.alps
        cadence['_file_overrides']['alps_model_dir'] = os.path.join(self.root, 'file-choice-missing')
        self.assertEqual(model_directory(cadence, 'alps', references=refs)[0], cadence['_file_overrides']['alps_model_dir'])
        missing = os.path.join(self.root, 'env-choice-missing')
        self.assertEqual(model_directory(self.config['cadence'], 'alps', references=refs,
            environ={'PDK': self.root, 'CRESCENDO_ALPS_MODEL_DIR': missing}),
            (missing, 'environment:CRESCENDO_ALPS_MODEL_DIR'))
        cadence = copy.deepcopy(self.config['cadence'])
        cadence['discovery']['sources']['alps_model_dir'] = 'manual'
        cadence['launch_environment']['CRESCENDO_ALPS_MODEL_DIR'] = missing
        self.assertEqual(model_directory(cadence, 'alps', references=refs),
                         (missing, 'environment:CRESCENDO_ALPS_MODEL_DIR'))

    def test_runtime_contract_uses_current_native_model_evidence_over_cached_auto_roots(self):
        from crescendo_engine.simulators import execution_contract
        current = self._saved_new_pdk_with_cached_auto_roots()
        source = os.path.join(current, 'spectre', 'toplevel.scs')
        contract = execution_contract(self.config, 'spectre', [{'file': source, 'section': 'TT'}], spec={'simulator': 'alps'})
        self.assertEqual(contract['model_dir'], os.path.join(current, 'alps'))
        self.assertEqual(contract['models'][0]['target']['path'], os.path.join(current, 'alps', 'toplevel.scs'))

    def test_saved_include_path_without_model_points_is_not_mixed_with_old_fallback_evidence(self):
        current = self._saved_new_pdk_with_cached_auto_roots()
        state = self.setup['source']['active_state']
        with open(state) as stream:
            text = stream.read()
        with open(state, 'w') as stream:
            stream.write(text.replace('((&quot;toplevel.scs&quot; &quot;TT&quot;))', 'list(modelFiles)')
                         .replace('(("toplevel.scs" "TT"))', 'list(modelFiles)'))
        result = discover_simulator_profiles(self.config, self.setup)
        for solver, profile in result['profiles'].items():
            self.assertEqual(profile['status'], 'unqualified')
            self.assertEqual(profile['model_dir'], os.path.join(current, solver))
        self.assertEqual(result['model_discovery']['saved_include_directories'], [os.path.join(current, 'spectre')])

    def test_without_current_model_evidence_automatic_paths_remain_location_hints(self):
        self._saved_new_pdk_with_cached_auto_roots()
        selected = model_directory(self.config['cadence'], 'alps')
        self.assertEqual(selected, (self.alps, 'environment:PDK/alps'))

if __name__ == "__main__":
    unittest.main()
