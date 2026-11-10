import copy
import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET
from unittest import mock

from crescendo_engine.single_test import project_setup, native_selection, native_selection_lines, project_native_files
from crescendo_engine.server import OptimizerService
from tests.test_setup_baseline import SetupBaselineTests


class SingleTestBackendTests(unittest.TestCase):
    def setUp(self):
        self.fixture = SetupBaselineTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.service = OptimizerService(copy.deepcopy(self.fixture.config))

    def test_catalogue_is_read_only_and_projection_does_not_scan_other_test(self):
        setup = copy.deepcopy(self.fixture.prepared['setup'])
        second = copy.deepcopy(setup['tests'][0])
        second.update(name='invalid', library='missing', enabled=False)
        setup['tests'].append(second)
        with mock.patch('crescendo_engine.test_catalogue.load_test_catalogue', return_value=setup), \
             mock.patch('crescendo_engine.hierarchy.probe_hierarchy', side_effect=AssertionError('unexpected scan')):
            token, preview = self.service.inspect_tests(self.fixture.path)
        self.assertEqual([r['name'] for r in preview['tests']], ['C', 'invalid'])
        with mock.patch.object(self.service, '_inspect_cells_registered', return_value=('cell', {})) as scan:
            self.service.inspect_test_cells(token, 'invalid')
        self.assertEqual(scan.call_args[1]['selected_test'], 'invalid')
        projected = project_setup(setup, 'invalid')
        self.assertEqual([r['name'] for r in projected['tests']], ['invalid'])
        self.assertTrue(projected['tests'][0]['enabled'])
        self.assertFalse(projected['_source_tests'][1]['enabled'])
        lines = '\n'.join(native_selection_lines(native_selection(projected)))
        self.assertNotIn('maeGetAnalysis("C"', lines)
        self.assertNotIn('maeSetSetup(?tests list("C")', lines)
        self.assertIn('maeSetSetup(?tests list("invalid") ?enabled t', lines)

    def test_projected_baseline_preserves_full_source_identity(self):
        from crescendo_engine.setup_baseline import freeze_setup_baseline
        setup = project_setup(self.fixture.prepared['setup'], 'C')
        prepared = copy.deepcopy(self.fixture.prepared)
        prepared['setup'] = setup
        self.assertTrue(freeze_setup_baseline(prepared, self.fixture.config))
        self.assertEqual(setup['selected_test'], 'C')
        self.assertEqual(len(setup['_source_tests']), 1)

    def test_restored_test_selection_survives_source_enabled_changes(self):
        from crescendo_engine.unified_configuration import _match_execution_selection
        old = {'tests': {'A': {'enabled': True, 'analyses': {}}}}
        current = {'tests': {'A': {'enabled': False, 'analyses': {}}}}
        result = _match_execution_selection({'tests': ['A']}, old, current, [])
        self.assertEqual(result['tests'], ['A'])

    def test_live_binding_checks_only_selected_test_after_catalogue(self):
        setup = copy.deepcopy(self.fixture.prepared['setup'])
        other = copy.deepcopy(setup['tests'][0])
        other['name'] = 'other'
        setup['tests'].append(other)
        self.service.config['_maestro_launch_context'] = {'live_tests': [
            {'name': 'C', 'library': 'design', 'cell': 'TB', 'view': 'schematic'},
            {'name': 'other', 'library': 'changed', 'cell': 'other', 'view': 'schematic'}]}
        self.service._validate_live_test_bindings(setup)
        self.service._validate_live_test_bindings(project_setup(setup, 'C'))
        with self.assertRaisesRegex(ValueError, 'differs from Maestro test other'):
            self.service._validate_live_test_bindings(project_setup(setup, 'other'))

    def test_old_multi_or_unqualified_prepared_rejected(self):
        with self.assertRaisesRegex(ValueError, 'qualified single Test'):
            self.service._require_single_test(self.fixture.prepared)
        prepared = copy.deepcopy(self.fixture.prepared)
        prepared['setup'] = project_setup(prepared['setup'], 'C')
        self.assertEqual(self.service._require_single_test(prepared), 'C')
        prepared['setup']['tests'].append(copy.deepcopy(prepared['setup']['tests'][0]))
        with self.assertRaisesRegex(ValueError, 'qualified single Test'):
            self.service._require_single_test(prepared)

    def test_native_projection_known_paths_case_namespace_and_mode(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'maestro.sdb')
            state = os.path.join(root, 'active.state')
            receipt = os.path.join(root, 'projection.json')
            with open(path, 'w') as stream:
                stream.write('<n:setupdb xmlns:n="urn:x"><n:active><n:Tests>'
                    '<n:Test name="A"/><n:Test Name="B"/></n:Tests>'
                    '<n:disabledTests><n:Test name="B"/></n:disabledTests>'
                    '<n:corners><n:corner Name="keep" Enabled="1"/><n:corner Name="off" Enabled="no"/></n:corners>'
                    '<n:vars><n:var Name="v"><n:dependenttests><n:dependenttest Name="A"/>'
                    '<n:dependenttest name="B"/></n:dependenttests></n:var></n:vars>'
                    '<n:unknown><n:disabledTests><n:Test Name="B"/></n:disabledTests></n:unknown>'
                    '</n:active></n:setupdb>')
            with open(state, 'w') as stream:
                stream.write('<statedb><Test Name="A"/><Test name="B"/><Other><Test Name="B"/></Other></statedb>')
            os.chmod(path, 0o640)
            project_native_files(path, 'A', receipt, selected_corners=['keep'])
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o640)
            root_xml = ET.parse(path).getroot()
            from crescendo_engine.ade_setup import _child, _children, _name
            active = _child(root_xml, 'active')
            self.assertEqual([_name(x) for x in _children(_child(active, 'tests'), 'test')], ['A'])
            self.assertEqual([_name(x) for x in _children(_child(active, 'corners'), 'corner')], ['keep'])
            self.assertEqual(_name(_children(_child(_child(active, 'unknown'), 'disabledtests'), 'test')[0]), 'B')
            with open(receipt) as stream:
                manifest = json.load(stream)
            self.assertEqual(manifest['selected_corners'], ['keep'])
            self.assertTrue(all(row.get('node_sha256') for row in manifest['removed']))
