import copy
import os
import unittest

import tests.test_setup_baseline as baseline_tests
from crescendo_engine.ade_setup import inspect_setup
from crescendo_engine.setup_baseline import verify_setup_baseline, load_bound_setup
from crescendo_engine.suite_updates import update_saved_sections


class SelectiveSuiteUpdateTests(baseline_tests.SetupBaselineTests):
    def test_job_policy_preserves_other_tests_and_matches_snapshot(self):
        import json
        self.prepared['job_policy']['tests'] = [{'name': 'C', 'properties': {'maxjobs': '1'}}, {'name': 'OTHER', 'properties': {'maxjobs': '2'}}]
        self.prepared['job_policy']['run_options'] = {'JobLimit': [['C', 1], ['OTHER', 2]]}
        self.freeze()
        self.prepared['setup']['selected_test'] = 'C'
        self.prepared['_incoming_job_policy'] = {'available': True, 'global': {'properties': {'maxjobs': '9'}}, 'tests': [{'name': 'C', 'properties': {'maxjobs': '4'}}, {'name': 'OTHER', 'properties': {'maxjobs': '8'}}], 'run_options': {'JobLimit': [['C', 4], ['OTHER', 8]]}}
        setup, _ = update_saved_sections(self.prepared, self.config, ['job_setup'])
        policy = setup['baseline_job_policy']
        self.assertEqual({row['name']: row['properties']['maxjobs'] for row in policy['tests']}, {'C': '4', 'OTHER': '2'})
        self.assertEqual(dict(policy['run_options']['JobLimit']), {'C': 4, 'OTHER': 2})
        with open(os.path.join(setup['baseline']['root'], 'setup.json')) as stream:
            self.assertEqual(json.load(stream)['job_policy'], policy)

    def test_unavailable_job_policy_does_not_block_other_sections(self):
        import json
        self.xml = self.xml.replace('<tests>', '<jobpolicyname>old</jobpolicyname><tests>')
        self.write(self.path, self.xml)
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        self.write(self.path, self.xml.replace('<jobpolicyname>old</jobpolicyname>', '<jobpolicyname>new</jobpolicyname>').replace('<value>3</value>', '<value>9</value>'))
        self.prepared['_incoming_job_policy'] = {'available': False}
        setup, plan = update_saved_sections(self.prepared, self.config, ['job_setup', 'global_variables'])
        self.assertEqual(setup['global_variables'][0]['value'], '9')
        self.assertEqual(setup['job_policy_references']['global'], 'old')
        self.assertEqual(plan['skipped_sections'], ['job_setup'])
        self.assertTrue(plan['warnings'])
        with open(os.path.join(setup['baseline']['root'], 'setup.json')) as stream:
            self.assertEqual(json.load(stream)['job_policy'], self.prepared['job_policy'])

    def test_unavailable_job_policy_alone_reuses_old_baseline(self):
        self.xml = self.xml.replace('<tests>', '<jobpolicyname>old</jobpolicyname><tests>')
        self.write(self.path, self.xml)
        self.prepared['setup'] = inspect_setup(self.path)
        baseline = self.freeze()
        self.write(self.path, self.xml.replace('<jobpolicyname>old</jobpolicyname>', '<jobpolicyname>new</jobpolicyname>'))
        self.prepared['_incoming_job_policy'] = {'available': False}
        setup, plan = update_saved_sections(self.prepared, self.config, ['job_setup'])
        self.assertEqual(setup['baseline']['baseline_id'], baseline['baseline_id'])
        self.assertTrue(plan['no_changes'])
        self.assertTrue(plan['warnings'])

    def test_available_job_policy_does_not_override_unselected_save_options(self):
        import json
        self.prepared['setup']['save_options'] = {'available': True, 'source': 'old'}
        self.prepared['job_policy']['save_options'] = {'available': True, 'source': 'old'}
        self.freeze()
        self.prepared['_incoming_job_policy'] = {'available': True, 'save_options': {'available': True, 'source': 'new'}, 'global': {'properties': {'maxjobs': '4'}}}
        setup, _ = update_saved_sections(self.prepared, self.config, ['job_setup'])
        with open(os.path.join(setup['baseline']['root'], 'setup.json')) as stream:
            policy = json.load(stream)['job_policy']
        self.assertEqual(policy['global']['properties']['maxjobs'], '4')
        self.assertEqual(policy['save_options']['source'], 'old')

    def test_unchanged_xml_reuses_baseline_despite_formatting(self):
        baseline = self.freeze()
        self.write(self.path, self.xml.replace('><', '>\n  <'))
        parent = os.path.join(self.config['runtime']['run_root'], 'setup_baselines')
        before = set(os.listdir(parent))
        setup, plan = update_saved_sections(self.prepared, self.config, ['global_variables'])
        self.assertEqual(setup['baseline']['baseline_id'], baseline['baseline_id'])
        self.assertTrue(plan['no_changes'])
        self.assertFalse(plan['native_readback_required'])
        self.assertEqual(set(os.listdir(parent)), before)

    def test_source_hash_identifies_mixed_baseline_not_latest_source(self):
        from crescendo_engine.utils import sha256_file
        baseline = self.freeze()
        self.write(self.path, self.xml.replace('<value>3</value>', '<value>9</value>').replace('FN_gm*2', 'FN_gm*8'))
        setup, _ = update_saved_sections(self.prepared, self.config, ['global_variables'])
        self.assertEqual(setup['source']['setup_db'], self.path)
        self.assertEqual(setup['source']['sha256'], sha256_file(setup['baseline']['setup_path']))
        self.assertNotEqual(setup['source']['sha256'], sha256_file(self.path))
        self.assertNotEqual(setup['source']['sha256'], sha256_file(baseline['setup_path']))
        self.assertEqual(setup['tests'][0]['variables'][0]['value'], 'FN_gm*2')

    def test_selected_native_sections_and_old_baseline_remain_intact(self):
        self.xml = self.xml.replace('</test>', '<outputs><output Name="out"><expr>old</expr></output></outputs></test>')
        self.write(self.path, self.xml)
        self.write(self.state, '<statedb><Test Name="C"><component Name="analyses"><analysis Name="ac"><field Name="enable">t</field></analysis></component><component Name="outputs"><field Name="outputList_0"><field Name="name">"out"</field><field Name="expression">old</field></field></component><component Name="other">keep</component></Test></statedb>')
        self.prepared['setup'] = inspect_setup(self.path)
        old = self.freeze()
        newxml = self.xml.replace('<value>3</value>', '<value>9</value>').replace('FN_gm*2', 'FN_gm*7').replace('<expr>old</expr>', '<expr>new</expr>')
        self.write(self.path, newxml)
        self.write(self.state, '<statedb><Test Name="C"><component Name="analyses"><analysis Name="tran"><field Name="enable">t</field></analysis></component><component Name="outputs"><field Name="outputList_0"><field Name="name">"out"</field><field Name="expression">new</field></field></component><component Name="other">changed</component></Test></statedb>')
        setup, plan = update_saved_sections(self.prepared, self.config, ['global_variables', 'outputs'])
        self.assertEqual(setup['global_variables'][0]['value'], '9')
        self.assertEqual(setup['tests'][0]['variables'][0]['value'], 'FN_gm*2')
        self.assertEqual(setup['tests'][0]['outputs'][0]['expression'], 'new')
        self.assertEqual(setup['tests'][0]['analyses'][0]['name'], 'ac')
        with open(os.path.join(os.path.dirname(setup['baseline']['setup_path']), 'active.state')) as stream:
            self.assertIn('>keep<', stream.read())
        self.assertTrue(plan['refresh_outputs'])
        self.assertEqual(verify_setup_baseline(self.prepared['setup']), old)
        self.config['_setup_baseline']['baseline'] = setup['baseline']
        self.assertEqual(load_bound_setup(self.path, self.config)['global_variables'][0]['value'], '9')

    def test_design_updates_both_files_preserving_simulator(self):
        self.write(self.state, '<statedb><Test Name="C"><component Name="adeInfo"><field Name="designInfo">("design" "TB" "schematic" "spectre")</field></component></Test></statedb>')
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        self.write(self.path, self.xml.replace('<value>TB</value>', '<value>TB2</value>'))
        self.write(self.state, '<statedb><Test Name="C"><component Name="adeInfo"><field Name="designInfo">("design" "TB2" "schematic" "other")</field></component></Test></statedb>')
        setup, plan = update_saved_sections(self.prepared, self.config, ['test_design'])
        self.assertEqual(setup['tests'][0]['cell'], 'TB2')
        with open(os.path.join(os.path.dirname(setup['baseline']['setup_path']), 'active.state')) as stream:
            self.assertIn('("design" "TB2" "schematic" "spectre")', stream.read())
        self.assertTrue(plan['requires_hierarchy'])

    def test_selected_test_ignores_unrelated_new_test_and_preserves_unselected(self):
        self.xml = self.xml.replace('</tests>', '<test Name="OLD"><vars><var Name="a"><value>old</value></var></vars></test></tests>')
        self.write(self.path, self.xml)
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        self.prepared['setup']['selected_test'] = 'C'
        self.write(self.path, self.xml.replace('FN_gm*2', '1:2:9').replace('Name="OLD"', 'Name="NEW"'))
        setup, plan = update_saved_sections(self.prepared, self.config, ['local_variables'])
        rows = {test['name']: test for test in setup['tests']}
        self.assertEqual(rows['C']['variables'][0]['value'], '1:2:9')
        self.assertEqual(rows['OLD']['variables'][0]['value'], 'old')
        self.assertNotIn('NEW', rows)

    def test_other_settings_update_keeps_explicit_sections(self):
        self.xml = self.xml.replace('</test>', '<modelfiles><model>old</model></modelfiles></test>')
        self.write(self.path, self.xml)
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        self.write(self.path, self.xml.replace('<model>old</model>', '<model>new</model>').replace('FN_gm*2', 'unselected'))
        setup, _ = update_saved_sections(self.prepared, self.config, ['other_settings'])
        with open(setup['baseline']['setup_path']) as stream:
            self.assertIn('<model>new</model>', stream.read())
        self.assertEqual(setup['tests'][0]['variables'][0]['value'], 'FN_gm*2')

    def test_corner_masks_preserve_other_test_and_empty_vars(self):
        self.xml = self.xml.replace('<tests>', '<corners><corner Name="fast"><disabledtests><test Name="OLD"/></disabledtests></corner></corners><tests>').replace('</tests>', '<test Name="OLD"/></tests>')
        self.write(self.path, self.xml)
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        self.prepared['setup']['selected_test'] = 'C'
        source = self.xml.replace('<test Name="OLD"/></disabledtests>', '<test Name="C"/></disabledtests>').replace('<vars><var Name="local"><value>FN_gm*2</value></var></vars>', '<vars/>')
        self.write(self.path, source)
        setup, _ = update_saved_sections(self.prepared, self.config, ['corners', 'local_variables'])
        self.assertEqual(setup['tests'][0]['variables'], [])
        self.assertEqual(setup['test_corner_masks']['C']['disabled_corners'], ['fast'])
        self.assertEqual(setup['test_corner_masks']['OLD']['disabled_corners'], ['fast'])

    def test_corner_add_and_remove_preserves_unselected_test(self):
        self.xml = self.xml.replace('<tests>', '<corners><corner Name="old"><disabledtests><test Name="OLD"/></disabledtests></corner></corners><tests>').replace('</tests>', '<test Name="OLD"/></tests>')
        self.write(self.path, self.xml)
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        self.prepared['setup']['selected_test'] = 'C'
        self.write(self.path, self.xml.replace('<corner Name="old"><disabledtests><test Name="OLD"/></disabledtests></corner>', '<corner Name="new"><disabledtests><test Name="OLD"/></disabledtests></corner>'))
        setup, _ = update_saved_sections(self.prepared, self.config, ['corners'])
        self.assertEqual(setup['test_corner_masks']['C']['disabled_corners'], ['old'])
        self.assertEqual(setup['test_corner_masks']['OLD']['disabled_corners'], ['new', 'old'])

    def test_corner_refreshes_referenced_model_group_only(self):
        self.xml = self.xml.replace('<tests>', '<modelgroups><modelgroup Name="g"><models><model Name="m"><modelfile>old</modelfile></model></models></modelgroup><modelgroup Name="keep"><models><model Name="k"><modelfile>keep</modelfile></model></models></modelgroup></modelgroups><corners><corner Name="fast"><modelgroupname>("g")</modelgroupname></corner></corners><tests>')
        self.write(self.path, self.xml)
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        self.write(self.path, self.xml.replace('<modelfile>old</modelfile>', '<modelfile>new</modelfile>').replace('<modelfile>keep</modelfile>', '<modelfile>wrong</modelfile>'))
        setup, _ = update_saved_sections(self.prepared, self.config, ['corners'])
        groups = {row['name']: row for row in setup['model_groups']}
        self.assertEqual(groups['g']['models'][0]['file'], 'new')
        self.assertEqual(groups['keep']['models'][0]['file'], 'keep')

    def test_other_settings_switch_view_change_invalidates_hierarchy(self):
        self.write(self.state, '<statedb><Test Name="C"><component Name="environmentOptions"><field Name="switchViewList">("schematic")</field></component></Test></statedb>')
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        self.write(self.state, '<statedb><Test Name="C"><component Name="environmentOptions"><field Name="switchViewList">("extracted" "schematic")</field></component></Test></statedb>')
        _, plan = update_saved_sections(self.prepared, self.config, ['other_settings'])
        self.assertTrue(plan['requires_hierarchy'])

    def test_real_fixture_xml_mixed_update(self):
        import shutil
        fixture = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fixtures')
        for source, name in (('maestro.sdb.xml', 'maestro.sdb'), ('active.state.xml', 'active.state')):
            shutil.copyfile(os.path.join(fixture, source), os.path.join(self.view, name))
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        with open(self.state) as stream:
            state = stream.read()
        self.write(self.state, state.replace('"1"', '"2"').replace('value(mag(v("/out")) 1)', 'value(mag(v("/out")) 2)'))
        setup, _ = update_saved_sections(self.prepared, self.config, ['analyses', 'outputs'])
        self.assertEqual(setup['tests'][0]['analyses'][0]['fields']['start'], '2')
        self.assertIn(' 2)', setup['tests'][0]['outputs'][0]['expression'])

    def test_test_enabled_only_does_not_invalidate_hierarchy(self):
        self.write(self.path, self.xml)
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        self.write(self.path, self.xml.replace('<test Name="C">', '<test Name="C" enabled="0">'))
        _, plan = update_saved_sections(self.prepared, self.config, ['test_settings'])
        self.assertFalse(plan['requires_hierarchy'])

    def test_test_settings_sync_enabled_simulator_and_state(self):
        self.xml = self.xml.replace('</tooloptions>', '<option Name="sim"><value>spectre</value></option></tooloptions>')
        self.write(self.path, self.xml)
        self.write(self.state, '<statedb><Test Name="C"><component Name="adeInfo"><field Name="designInfo">("design" "TB" "schematic" "spectre")</field></component></Test></statedb>')
        self.prepared['setup'] = inspect_setup(self.path)
        self.freeze()
        self.prepared['setup']['selected_test'] = 'C'
        self.write(self.path, self.xml.replace('<value>spectre</value>', '<value>aps</value>').replace('<tests>', '<disabledtests><test Name="C"/></disabledtests><tests>'))
        self.write(self.state, '<statedb><Test Name="C"><component Name="adeInfo"><field Name="designInfo">("different" "other" "view" "aps")</field></component></Test></statedb>')
        setup, _ = update_saved_sections(self.prepared, self.config, ['test_settings'])
        self.assertFalse(setup['tests'][0]['enabled'])
        self.assertEqual(setup['tests'][0]['simulator'], 'aps')
        with open(os.path.join(os.path.dirname(setup['baseline']['setup_path']), 'active.state')) as stream:
            self.assertIn('("design" "TB" "schematic" "aps")', stream.read())

    def test_reject_changed_test_catalogue_without_publishing(self):
        self.freeze()
        self.write(self.path, self.xml.replace('Name="C"', 'Name="D"'))
        parent = os.path.join(self.config['runtime']['run_root'], 'setup_baselines')
        before = set(os.listdir(parent))
        with self.assertRaisesRegex(ValueError, 'Selected Test missing'):
            update_saved_sections(self.prepared, self.config, ['global_variables'])
        self.assertEqual(set(os.listdir(parent)), before)

    def test_save_options_persist_or_warn_when_unavailable(self):
        import json
        self.prepared['setup']['save_options'] = {'available': True, 'source': 'old'}
        self.freeze()
        self.prepared['_incoming_save_options'] = {'available': False}
        setup, plan = update_saved_sections(self.prepared, self.config, ['save_options'])
        self.assertEqual(setup['save_options']['source'], 'old')
        self.assertTrue(plan['warnings'])
        self.prepared['_incoming_save_options'] = {'available': True, 'source': 'saved_setup_native', 'raw': {'save_dir': '/new'}}
        setup, plan = update_saved_sections(self.prepared, self.config, ['save_options'])
        self.assertEqual(setup['save_options']['source'], 'saved_setup_native')
        with open(os.path.join(setup['baseline']['root'], 'setup.json')) as stream:
            snapshot = json.load(stream)
        self.assertEqual(snapshot['job_policy']['save_options']['raw']['save_dir'], '/new')
        self.assertEqual(plan['warnings'], [])

if __name__ == '__main__':
    unittest.main()
