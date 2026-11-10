"""Paired solver profiles: no EDA processes or numerical-equivalence claims."""
import copy
import hashlib
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import config, simulator_profiles as profiles, simulators


class SimulatorProfileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo paired PDK ' ")
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.spectre = os.path.join(self.root, "spectre")
        self.alps = os.path.join(self.root, "alps")
        os.makedirs(self.spectre)
        os.makedirs(self.alps)
        self.bin = self.write("solver", "#!/bin/sh\nexit 0\n")
        os.chmod(self.bin, 0o700)
        self.source = self.write("spectre/mos.scs", "section TT\nmodel nmos bsim4 vth0=0.4\nendsection TT\nsection FF\nmodel nmos bsim4 vth0=0.3\nendsection FF\n")
        self.target = self.write("alps/mos.scs", "section TT_ALPS\nmodel nmos bsim4 vth0=0.42\nendsection TT_ALPS\nsection FF_ALPS\nmodel nmos bsim4 vth0=0.32\nendsection FF_ALPS\n")
        self.config = {"cadence": {"spectre_bin": self.bin, "alps_bin": self.bin,
            "spectre_model_dir": self.spectre, "alps_model_dir": self.alps,
            "simulator": "spectre", "simulator_result_reader": "spectre"}}

    def write(self, relative, text):
        path = os.path.join(self.root, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as stream:
            stream.write(text)
        return path

    def contract(self, section="TT", **changes):
        selected = copy.deepcopy(self.config)
        selected["cadence"].update(changes)
        return simulators.execution_contract(selected, "spectre", [{"file": self.source, "section": section}], spec={"simulator": "alps"})

    def test_both_profiles_are_loaded_once_without_executing_tools(self):
        before = copy.deepcopy(self.config)
        with mock.patch("subprocess.Popen", side_effect=AssertionError("No vendor probe allowed")):
            result = profiles.discover_simulator_profiles(self.config, model_references=[{"file": self.source, "section": "TT"}])
        self.assertEqual(set(result["profiles"]), {"spectre", "alps"})
        self.assertTrue(all(row["ready"] for row in result["profiles"].values()))
        self.assertEqual(result["profiles"]["alps"]["resolved_model_map"][self.source]["sections"], {"TT": "TT_ALPS"})
        self.assertEqual(self.config, before)

    def test_no_native_points_is_visible_unqualified_even_when_tools_exist(self):
        result = profiles.discover_simulator_profiles(self.config)
        self.assertTrue(all(row["status"] == "unqualified" and not row["ready"] for row in result["profiles"].values()))

    def test_service_eda_display_projects_once_and_reads_each_new_request(self):
        from crescendo_engine.server import OptimizerService
        service = OptimizerService.__new__(OptimizerService)
        service.config = copy.deepcopy(self.config)
        setup = {"tests": [{"name": "test", "simulator": "spectre"}],
                 "model_groups": [{"name": "ttgroup", "models": [{"file": self.source, "section": "TT"}]}]}
        with mock.patch.object(profiles, "_setup_model_locations", wraps=profiles._setup_model_locations) as read:
            first = service.eda_setup(setup)
            self.assertEqual(read.call_count, 1)
            self.assertTrue(first["simulator_profiles"]["profiles"]["alps"]["ready"])
            # Fresh file contents must be observed, even though the same setup
            # object and pathname are used by the next display request.
            self.write("spectre/mos.scs", "section TT\nmodel changed_nmos bsim4 vth0=0.4\nendsection TT\n")
            second = service.eda_setup(setup)
            self.assertEqual(read.call_count, 2)
            self.assertFalse(second["simulator_profiles"]["profiles"]["alps"]["ready"])
        self.assertEqual(service.config, self.config)

    def test_service_projection_keeps_capture_adapter_and_empty_setup_rules(self):
        from crescendo_engine.server import OptimizerService
        service = OptimizerService.__new__(OptimizerService)
        service.config = self.config
        for setup in (None, {}, {"tests": [{"simulator": "alps"}]}):
            with self.subTest(setup=setup):
                with mock.patch.object(profiles, "_setup_model_locations", wraps=profiles._setup_model_locations) as read:
                    actual = service.eda_setup(setup)["simulator_profiles"]
                    self.assertEqual(read.call_count, 1)
                expected = profiles.discover_simulator_profiles(
                    profiles.apply_source_profile_defaults(self.config, setup), setup)
                self.assertEqual(actual, expected)

    def test_public_profile_discovery_still_projects_and_honors_explicit_references(self):
        setup = {"tests": [{"simulator": "spectre"}],
                 "model_groups": [{"models": [{"file": self.source, "section": "TT"}]}]}
        for explicit in (None, [], [{"file": self.source, "section": "FF"}]):
            with self.subTest(explicit=explicit):
                with mock.patch.object(profiles, "_setup_model_locations", wraps=profiles._setup_model_locations) as read:
                    result = profiles.discover_simulator_profiles(self.config, setup, model_references=explicit)
                    self.assertEqual(read.call_count, 1)
                expected = [{"file": self.source, "section": "TT"}] if explicit is None else explicit
                self.assertEqual(result["profiles"]["alps"]["source_model_references"], expected)

    def test_relative_mirror_requires_matching_interface_and_unique_section(self):
        contract = self.contract()
        row = contract["models"][0]
        self.assertEqual(row["section"], "TT_ALPS")
        self.assertEqual(row["mapping_origin"], "automatic_verified_relative_interface")
        self.assertNotEqual(row["source"]["sha256"], row["target"]["sha256"])
        self.assertFalse(contract["model_equivalence_claimed"])
        self.assertEqual(row["mapping_evidence"]["search_mode"], "exact_relative_counterpart")

    def test_model_bundle_accepts_exact_parameter_only_auxiliary_sections(self):
        for path in (self.source, self.target):
            with open(path, 'a') as stream:
                stream.write('section Pre_Sim\nparameters pre_option=1\nendsection Pre_Sim\n'
                             'section Noise_Typical\n.param noise_option=2\nendsection Noise_Typical\n')
        references = [{'file': self.source, 'section': name} for name in ('TT', 'Pre_Sim', 'Noise_Typical')]
        contract = simulators.execution_contract(self.config, 'spectre', references, spec={'simulator': 'alps'})
        self.assertEqual([row['mapping_evidence']['interface_kind'] for row in contract['models']],
                         ['model_subckt', 'parameter_only', 'parameter_only'])
        self.assertEqual(contract['models'][1]['mapping_evidence']['parameter_names'], ['pre_option'])
        self.assertFalse(contract['model_equivalence_claimed'])
        text = ''.join('include "' + self.source + '" section=' + name + '\n'
                       for name in ('TT', 'Pre_Sim', 'Noise_Typical'))
        rewritten = simulators.rewrite_model_references(text, contract)
        self.assertIn('section=TT_ALPS', rewritten)
        self.assertIn('section=Pre_Sim', rewritten)
        self.assertIn('section=Noise_Typical', rewritten)
        self.assertNotIn(self.source, rewritten)

    def test_auxiliary_sections_alone_do_not_replace_a_device_model_interface(self):
        for path in (self.source, self.target):
            with open(path, 'a') as stream:
                stream.write('section Pre_Sim\nparameters setting=1\nendsection Pre_Sim\n')
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, 'alone cannot qualify'):
            self.contract('Pre_Sim')

    def test_auxiliary_section_mapping_rejects_values_names_empty_and_control_syntax(self):
        with open(self.source, 'a') as stream:
            stream.write('section Pre_Sim\nparameters setting=1\nendsection Pre_Sim\n')
        for content in ('parameters setting=2', 'parameters renamed=1', '',
                        'parameters setting=1\nunknown_control option=1', 'pragma protect'):
            self.write('alps/mos.scs', 'section Pre_Sim\n' + content + '\nendsection Pre_Sim\n')
            with self.subTest(content=content), self.assertRaises(simulators.SimulatorConfigurationError):
                self.contract('Pre_Sim')

    def test_auxiliary_dependency_assignment_order_is_part_of_literal_evidence(self):
        with open(self.source, 'a') as stream:
            stream.write('section Pre_Sim\nparameters setting=1\ninclude "aux.scs"\nendsection Pre_Sim\n')
        self.write('spectre/aux.scs', 'parameters setting=2\n')
        self.write('alps/aux.scs', 'parameters setting=2\n')
        self.write('alps/mos.scs', 'section Pre_Sim\ninclude "aux.scs"\nparameters setting=1\nendsection Pre_Sim\n')
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, 'interfaces differ'):
            self.contract('Pre_Sim')

    def test_auxiliary_mapping_never_searches_for_a_different_filename(self):
        with open(self.source, 'a') as stream:
            stream.write('section Pre_Sim\nparameters setting=1\nendsection Pre_Sim\n')
        self.write('alps/other.scs', 'section Pre_Sim\nparameters setting=1\nendsection Pre_Sim\n')
        with mock.patch('os.walk', side_effect=AssertionError('Do not guess auxiliary models')):
            with self.assertRaisesRegex(simulators.SimulatorConfigurationError, 'source=.*section=Pre_Sim.*target='):
                self.contract('Pre_Sim')

    def test_same_filename_without_exported_interface_is_rejected(self):
        self.write("alps/mos.scs", "section TT\nparameters only=1\nendsection TT\n")
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "unqualified"):
            self.contract()

    def test_model_type_and_exported_name_mismatch_are_rejected(self):
        for declaration in ("model pmos bsim4", "model nmos bsim3"):
            self.write("alps/mos.scs", "section TT\n" + declaration + "\nendsection TT\n")
            with self.subTest(declaration=declaration):
                with self.assertRaises(simulators.SimulatorConfigurationError):
                    self.contract()

    def test_corner_identity_cannot_be_inferred_from_model_names_alone(self):
        self.write("alps/mos.scs", "section SS\nmodel nmos bsim4\nendsection SS\n")
        with self.assertRaises(simulators.SimulatorConfigurationError):
            self.contract()

    def test_two_normalized_corner_sections_are_ambiguous(self):
        self.write("alps/mos.scs", "section TT\nmodel nmos bsim4\nendsection TT\nsection TT_ALPS\nmodel nmos bsim4\nendsection TT_ALPS\n")
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "ambiguous"):
            self.contract()

    def test_unique_different_filename_uses_bounded_interface_search(self):
        os.rename(self.target, os.path.join(self.alps, "different_vendor_name.scs"))
        row = self.contract()["models"][0]
        self.assertTrue(row["target"]["path"].endswith("different_vendor_name.scs"))
        self.assertEqual(row["mapping_evidence"]["search_mode"], "unique_bounded_interface_candidate")

    def test_two_matching_different_filenames_do_not_guess(self):
        os.rename(self.target, os.path.join(self.alps, "first.scs"))
        self.write("alps/second.scs", "section TT\nmodel nmos bsim4\nendsection TT\n")
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "ambiguous"):
            self.contract()

    def test_explicit_mapping_wins_and_can_name_arbitrary_corner(self):
        self.write("alps/mos.scs", "// vendor encrypted model; native solver qualification required\n")
        row = self.contract(alps_model_file_map={self.source: {"path": self.target, "section": "Corner_Custom"}})["models"][0]
        self.assertEqual(row["section"], "Corner_Custom")
        self.assertEqual(row["mapping_origin"], "manual")

    def test_one_manual_file_can_map_multiple_corner_sections(self):
        mapping = {self.source: {"path": self.target, "sections": {"TT": "TT_ALPS", "FF": "FF_ALPS"}}}
        self.assertEqual(self.contract("TT", alps_model_file_map=mapping)["models"][0]["section"], "TT_ALPS")
        self.assertEqual(self.contract("FF", alps_model_file_map=mapping)["models"][0]["section"], "FF_ALPS")
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "no target"):
            self.contract("SS", alps_model_file_map=mapping)

    def test_invalid_section_mapping_is_not_ignored(self):
        for section in (None, "TT;danger", [], 8):
            with self.subTest(section=section):
                with self.assertRaises(simulators.SimulatorConfigurationError):
                    self.contract(alps_model_file_map={self.source: {"path": self.target, "sections": {"TT": section}}})

    def test_independent_reader_switch_does_not_mutate_other_profile(self):
        self.config["cadence"].update(alps_result_reader="alps", spectre_result_reader="spectre")
        result = profiles.discover_simulator_profiles(self.config, model_references=[{"file": self.source, "section": "TT"}])
        self.assertEqual(result["profiles"]["spectre"]["result_reader"], "spectre")
        self.assertEqual(result["profiles"]["alps"]["result_reader"], "alps")
        self.assertEqual(self.contract()["result_reader"], "alps")

    def test_invalid_explicit_reader_never_uses_the_common_reader(self):
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "reader"):
            self.contract(alps_result_reader="other")

    def test_source_sibling_directory_can_be_inferred_but_explicit_bad_path_cannot(self):
        self.config["cadence"].pop("alps_model_dir")
        contract = self.contract()
        self.assertEqual(contract["model_dir"], self.alps)
        self.assertEqual(contract["model_dir_origin"], "source_pdk_sibling")
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "unavailable"):
            self.contract(alps_model_dir=os.path.join(self.root, "missing"))

    def test_inferred_source_cannot_auto_pair_to_unrelated_pdk_dir(self):
        self.config["cadence"].pop("spectre_model_dir")
        other = os.path.join(self.root, "other_pdk", "alps")
        self.write("other_pdk/alps/mos.scs", "section TT_ALPS\nmodel nmos bsim4\nendsection TT_ALPS\n")
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "unrelated"):
            self.contract(alps_model_dir=other)

    def test_nested_dependency_interface_and_frozen_bytes_are_verified(self):
        self.write("spectre/mos.scs", 'section TT\ninclude "nested.scs"\nendsection TT\n')
        self.write("alps/mos.scs", 'section TT_ALPS\ninclude "nested.scs"\nendsection TT_ALPS\n')
        self.write("spectre/nested.scs", "model nmos bsim4 vth0=.4\n")
        nested = self.write("alps/nested.scs", "model nmos bsim4 vth0=.42\n")
        contract = self.contract()
        self.assertEqual(len(contract["models"][0]["mapping_evidence"]["target_dependencies"]), 2)
        with open(nested, "a") as stream:
            stream.write("// content changed\n")
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "dependency content changed"):
            simulators.validate_execution_contract(contract)

    def test_nested_model_escape_and_cycle_are_blocked(self):
        for target in ('include "../outside.scs"', 'include "mos.scs" section=TT_ALPS'):
            self.write("outside.scs", "model nmos bsim4\n")
            self.write("alps/mos.scs", "section TT_ALPS\n" + target + "\nendsection TT_ALPS\n")
            with self.subTest(target=target):
                with self.assertRaises(simulators.SimulatorConfigurationError):
                    self.contract()

    def test_subckt_port_order_and_parameter_interface_must_match(self):
        self.write("spectre/mos.scs", "section TT\nsubckt nmos (d g s b)\nparameters width=1u length=.1u\nends nmos\nendsection TT\n")
        for ports, parameters in (("d s g b", "width=2u length=.2u"), ("d g s b", "w=2u l=.2u")):
            self.write("alps/mos.scs", "section TT_ALPS\nsubckt nmos (" + ports + ")\nparameters " + parameters + "\nends nmos\nendsection TT_ALPS\n")
            with self.subTest(ports=ports, parameters=parameters):
                with self.assertRaises(simulators.SimulatorConfigurationError):
                    self.contract()
        self.write("alps/mos.scs", "section TT_ALPS\nsubckt nmos (d g s b)\nparameters width=2u length=.2u\nends nmos\nendsection TT_ALPS\n")
        self.assertEqual(self.contract()["models"][0]["section"], "TT_ALPS")

    def test_spice_lib_model_interface_is_qualified_with_unique_section(self):
        self.write("spectre/mos.scs", ".lib TT\n.model nmos NMOS level=54\n.endl TT\n")
        self.write("alps/mos.scs", ".lib TT_ALPS\n.model nmos NMOS level=54\n.endl TT_ALPS\n")
        self.assertEqual(self.contract()["models"][0]["section"], "TT_ALPS")

    def test_encrypted_auto_model_is_blocked_without_claiming_model_equivalence(self):
        self.write("alps/mos.scs", "pragma protect\nsection TT\nmodel nmos bsim4\nendsection TT\n")
        result = profiles.discover_simulator_profiles(self.config, model_references=[{"file": self.source, "section": "TT"}])
        self.assertEqual(result["profiles"]["alps"]["status"], "blocked")
        self.assertFalse(result["profiles"]["alps"]["model_equivalence_claimed"])

    def test_search_bound_does_not_claim_unique_partial_result(self):
        os.rename(self.target, os.path.join(self.alps, "matching.scs"))
        for index in range(129):
            self.write("alps/other_%03d.scs" % index, "// no model\n")
        with self.assertRaisesRegex(simulators.SimulatorConfigurationError, "bound"):
            self.contract()

    def test_one_pass_rewrite_preserves_circuit_and_avoids_section_chaining(self):
        self.config["cadence"]["spectre_model_file_map"] = {self.source: {"path": self.source, "sections": {"TT": "FF", "FF": "TT"}}}
        contract = simulators.execution_contract(self.config, "spectre", [{"file": self.source, "section": section} for section in ("TT", "FF")])
        prefix = "simulator lang=spectre\nparameters width=2u cornerVar=8\n"
        suffix = "// include text stays untouched\nX0 (d g s b) nmos w=width\nac ac start=1 stop=1G\n"
        original = prefix + 'include "%s" section=TT\ninclude "%s" section=FF\n' % (self.source, self.source) + suffix
        rewritten = simulators.rewrite_model_references(original, contract)
        self.assertEqual(rewritten, prefix + 'include "%s" section=FF\ninclude "%s" section=TT\n' % (self.source, self.source) + suffix)

    def test_setup_groups_freeze_both_model_directories_without_editing_config(self):
        self.config["cadence"].pop("alps_model_dir")
        self.config["cadence"].pop("spectre_model_dir")
        before = copy.deepcopy(self.config)
        setup = {"tests": [{"name": "test", "simulator": "spectre"}],
                 "model_groups": [{"name": "ttgroup", "models": [{"file": self.source, "section": "TT"}]}]}
        frozen = profiles.apply_source_profile_defaults(self.config, setup)
        self.assertEqual(frozen["cadence"]["spectre_model_dir"], self.spectre)
        self.assertEqual(frozen["cadence"]["alps_model_dir"], self.alps)
        self.assertEqual(self.config, before)
        self.assertTrue(profiles.discover_simulator_profiles(self.config, setup)["profiles"]["alps"]["ready"])

    def test_alps_native_addon_cannot_bypass_the_qualified_source_adapter(self):
        result = profiles.discover_simulator_profiles(self.config, {"tests": [{"simulator": "alps"}]})
        self.assertTrue(all(row["status"] == "blocked" for row in result["profiles"].values()))

    def test_per_solver_reader_and_sections_are_persistable_manual_settings(self):
        for key in ("spectre_result_reader", "alps_result_reader"):
            self.assertIn(key, config.MANUAL_KEYS)
        values = {"alps_model_file_map": {"spectre/mos.scs": {"path": "alps/mos.scs", "sections": {"TT": "TT_ALPS"}}}}
        normalized = config._resolve_file_overrides(values, self.root)
        self.assertEqual(normalized["alps_model_file_map"][self.source]["sections"], {"TT": "TT_ALPS"})

    def test_run_model_guard_rejects_new_candidate_target_content(self):
        spec = {}
        first = {"tasks": [{"test": "tb", "simulator_contract": self.contract()}]}
        before = profiles.require_frozen_model_profile(spec, first)
        self.assertEqual(before, profiles.require_frozen_model_profile(spec, first))
        self.write("alps/mos.scs", "section TT_ALPS\nmodel nmos bsim4 vth0=.9\nendsection TT_ALPS\n")
        current = {"tasks": [{"test": "tb", "simulator_contract": self.contract()}]}
        with self.assertRaisesRegex(profiles.ModelProfileError, "start a fresh"):
            profiles.require_frozen_model_profile(spec, current)
        self.assertEqual(spec["simulator_model_profile_identity"], before)

    def test_resume_model_guard_is_path_independent_but_reader_and_solver_bound(self):
        spec = {}
        first = {"tasks": [{"test": "tb", "simulator_contract": self.contract()}]}
        profiles.require_frozen_model_profile(spec, first)
        relocated = copy.deepcopy(first)
        relocated["tasks"][0]["simulator_contract"]["model_dir_origin"] = "different diagnostic origin"
        relocated["tasks"][0]["simulator_contract"]["fingerprint"] = simulators._fingerprint(relocated["tasks"][0]["simulator_contract"])
        self.assertEqual(profiles.require_frozen_model_profile(spec, relocated), spec["simulator_model_profile_identity"])
        changed = {"tasks": [{"test": "tb", "simulator_contract": self.contract(alps_result_reader="alps")}]}
        with self.assertRaises(profiles.ModelProfileError):
            profiles.require_frozen_model_profile(spec, changed)

    def test_frozen_profile_compacts_repeated_points_without_dropping_corner_sections(self):
        tt, ff = self.contract("TT"), self.contract("FF")
        result = profiles.frozen_model_profile_identity({"tasks": [{"test": "tb", "simulator_contract": contract} for contract in (tt, tt, ff)]})
        self.assertEqual(len(result["profiles"]), 2)
        self.assertEqual({row["models"][0]["section"] for row in result["profiles"]}, {"TT_ALPS", "FF_ALPS"})

    def test_frozen_profile_survives_checkpoint_json_roundtrip(self):
        spec, manifest = {}, {"tasks": [{"test": "tb", "simulator_contract": self.contract()}]}
        profiles.require_frozen_model_profile(spec, manifest)
        saved = json.loads(json.dumps(spec))
        self.assertEqual(profiles.require_frozen_model_profile(saved, manifest), spec["simulator_model_profile_identity"])


class SharedFrontendProfileTests(unittest.TestCase):
    def test_both_frontends_adapt_same_corner_models_and_capture_timeout_overrides(self):
        from crescendo_engine.netlist_providers import prepare_maestro_netlists
        from crescendo_engine.private_netlist import prepare_private_netlist, materialize_candidate, validate_manifest
        from tests.test_netlist_providers import NumericMaestroProviderTest
        fixture = NumericMaestroProviderTest()
        with tempfile.TemporaryDirectory(prefix="crescendo two profiled sources ' ") as root:
            prepared, parameters, active_config, spec, unused_model = fixture.fixtures(root)
            prepared["setup"]["tests"][0].update(library="PRIVATE", cell="TB", view="schematic")
            parameters[0]["numeric_value"] = 2e-6
            parameters[0].update(name="r", owner_library="PRIVATE", owner_cell="TB", owner_view="schematic",
                hierarchy_path="/R0", owner_occurrence_path="/", design_library="PRIVATE", design_cell="TB",
                design_view="schematic")
            source_directory, target_directory = os.path.join(root, "models", "spectre"), os.path.join(root, "models", "alps")
            os.makedirs(source_directory)
            os.makedirs(target_directory)
            source_model, target_model = os.path.join(source_directory, "model.scs"), os.path.join(target_directory, "model.scs")
            with open(source_model, "w") as stream:
                stream.write("section TT\nmodel nmos bsim4 vth0=.4\nendsection TT\n")
            with open(target_model, "w") as stream:
                stream.write("section TT_ALPS\nmodel nmos bsim4 vth0=.42\nendsection TT_ALPS\n")
            active_config["cadence"].update(alps_bin=active_config["cadence"]["spectre_bin"],
                spectre_model_dir=source_directory, alps_model_dir=target_directory, alps_result_reader="alps")
            spec.update(simulator="alps", maestro_job_overrides={"timeouts": {"run_seconds": 120, "linger_seconds": 300}})
            base_capture, capture_configs = fixture.native_capture(source_model), []
            def capture(workspace, selected, values, symbols, seeds, cfg, phase_root, **kwargs):
                capture_configs.append(copy.deepcopy(cfg))
                result = base_capture(workspace, selected, values, {}, {}, cfg, phase_root,
                    symbolic=False, verify_numeric_application=True)
                task = result["tasks"][0]
                with open(task["netlist_path"], "rb") as stream:
                    task["template_files"][0]["sha256"] = hashlib.sha256(stream.read()).hexdigest()
                result["patch_plan"], result["export_snapshot"] = {"parameters": []}, {}
                result["applied_values"] = {key: value if isinstance(value, (int, float)) else 2e-6 for key, value in values.items()}
                return result
            with mock.patch("crescendo_engine.netlist_providers.build_workspace", side_effect=fixture.workspace), \
                    mock.patch("crescendo_engine.private_netlist.build_workspace", side_effect=fixture.workspace), \
                    mock.patch("crescendo_engine.netlist_providers._capture_phase", side_effect=capture), \
                    mock.patch("crescendo_engine.private_netlist._capture_phase", side_effect=capture), \
                    mock.patch("crescendo_engine.netlist_providers.verify_sources_unchanged", return_value={"ok": True}), \
                    mock.patch("crescendo_engine.private_netlist.verify_sources_unchanged", return_value={"ok": True}):
                numeric = prepare_maestro_netlists(prepared, parameters, {"p": 2e-6}, active_config,
                    os.path.join(root, "numeric-source"), "c1", spec=spec)
                symbolic = prepare_private_netlist(prepared, parameters, active_config,
                    os.path.join(root, "symbolic-source"), spec=spec)
                candidate = materialize_candidate(symbolic, {"p": 2.1e-6}, os.path.join(root, "candidate"), "c1")
            for manifest in (numeric, symbolic, candidate):
                self.assertTrue(validate_manifest(manifest))
                task = manifest["tasks"][0]
                self.assertEqual(task["capture_source_simulator"], "spectre")
                self.assertEqual(task["execution_simulator"], "alps")
                self.assertEqual(task["simulator_contract"]["result_reader"], "alps")
                self.assertEqual(task["execution_model_identity"][0]["mapping_origin"], "automatic_verified_relative_interface")
                with open(task["netlist_path"]) as stream:
                    self.assertIn('include "' + target_model + '" section=TT_ALPS', stream.read())
                self.assertIn("-o", task["argv"])
                self.assertNotIn("-raw", task["argv"])
            self.assertTrue(capture_configs)
            self.assertTrue(all(row["_capture_job_overrides"]["timeouts"] == {"run_seconds": 120, "linger_seconds": 300} for row in capture_configs))
            self.assertNotIn("_capture_job_overrides", active_config)


if __name__ == "__main__":
    unittest.main()
