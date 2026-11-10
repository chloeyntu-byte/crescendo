import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.cadence import detect_pdk_context
from crescendo_engine.maestro_context import selected_setup_config


class OptionalPdkDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.paths = dict((name, os.path.join(self.root, name)) for name in ('DUT', 'PDK', 'OTHER'))
        for path in self.paths.values():
            os.makedirs(path)
        self.cds = os.path.join(self.root, 'cds.lib')
        self.write_mapping(bad_include=True)
        self.setup = {'libraries': ['DUT'], 'source': {'library_root': self.paths['DUT']}}
        self.hierarchy = {'schema_version': 'crescendo.hierarchy.v1', 'read_only': True, 'designs': [{
            'design': {'library': 'DUT', 'cell': 'TB', 'view': 'schematic'},
            'command': {'returncode': 0, 'timed_out': False},
            'hierarchy': {'cells': [{'hierarchy_path': '/', 'library': 'DUT', 'cell': 'TB', 'view': 'schematic'}],
                          'instances': [{'hierarchy_path': '/M0', 'master_library': 'PDK', 'master_cell': 'mos'}],
                          'errors': []},
        }]}
        self.config = {'cds_lib': self.cds, 'source_cds_lib_override': self.cds,
                       '_native_library_mappings': copy.deepcopy(self.paths)}

    def write_mapping(self, bad_include):
        with open(self.cds, 'w') as stream:
            if bad_include:
                stream.write('INCLUDE ./unrelated-uninstalled-pdk/cds.lib\n')
            for name, path in self.paths.items():
                stream.write('DEFINE %s %s\n' % (name, path))

    def detect(self):
        return detect_pdk_context(self.setup, self.hierarchy, self.config)

    def test_unrelated_include_is_warning_after_complete_native_mapping_proof(self):
        result = self.detect()
        self.assertFalse(result['blocking'])
        self.assertTrue(result['discovery_degraded'])
        self.assertEqual(result['contexts'][0]['adapter_selection']['adapter_id'], 'generic-cdf')
        self.assertEqual(result['classification_warnings'][0]['code'], 'include_missing')
        self.assertEqual(result['library_map']['native_verified_definitions'],
                         dict((name, self.paths[name]) for name in ('DUT','PDK')))
        self.assertEqual(result['library_map']['errors'][0]['code'], 'include_missing')

    def test_without_actual_loaded_mapping_errors_remain_blocking(self):
        self.config.pop('_native_library_mappings')
        self.assertTrue(self.detect()['blocking'])

    def test_missing_actual_required_mapping_remains_blocking(self):
        self.config['_native_library_mappings'].pop('PDK')
        result = self.detect()
        self.assertTrue(result['blocking'])
        self.assertNotIn('discovery_degraded', result)

    def test_stale_native_mapping_must_not_replace_actual_source_namespace(self):
        self.config['_native_library_mappings']['PDK'] = self.paths['OTHER']
        self.assertTrue(self.detect()['blocking'])

    def test_missing_actual_directory_remains_blocking(self):
        os.rmdir(self.paths['PDK'])
        self.assertTrue(self.detect()['blocking'])

    def test_failed_native_probe_cannot_authorize_discovery_degradation(self):
        self.hierarchy['designs'][0]['command']['returncode'] = 1
        self.assertTrue(self.detect()['blocking'])

    def test_incomplete_hierarchy_cannot_authorize_discovery_degradation(self):
        self.hierarchy['designs'][0]['hierarchy']['errors'] = [
            {'kind':'ERROR', 'hierarchy_path':'/missing', 'values':[]},
        ]
        self.assertTrue(self.detect()['blocking'])

    def test_no_command_evidence_cannot_authorize_discovery_degradation(self):
        self.hierarchy['designs'][0].pop('command')
        self.assertTrue(self.detect()['blocking'])

    def test_technology_mapping_is_required_even_if_devices_have_known_master(self):
        self.hierarchy['designs'][0]['hierarchy']['technology_bindings'] = [
            {'library':'DUT', 'technology_library':'OTHER'},
        ]
        self.config['_native_library_mappings'].pop('OTHER')
        self.assertTrue(self.detect()['blocking'])

    def test_ambiguous_profiles_excluded_by_original_index_without_losing_unique_profile(self):
        self.write_mapping(bad_include=False)
        self.hierarchy['designs'][0]['hierarchy']['instances'].append(
            {'hierarchy_path':'/X0', 'master_library':'OTHER', 'master_cell':'device'})
        self.config['pdk_profiles'] = [
            {'id':'one','reference_libraries':['PDK'],'model_roots':[self.paths['PDK']]},
            {'id':'two','reference_libraries':['PDK'],'model_roots':[self.paths['OTHER']]},
            {'id':'unique','reference_libraries':['OTHER'],'model_roots':[self.paths['OTHER']]},
        ]
        result = self.detect()
        self.assertFalse(result['blocking'])
        self.assertTrue(result['discovery_degraded'])
        self.assertEqual(result['excluded_profile_ids'], ['one','two'])
        self.assertEqual(result['excluded_profile_indices'], [0,1])
        generic = next(row for row in result['contexts'] if 'PDK' in row['reference_libraries'])
        self.assertIsNone(generic['profile_id'])
        self.assertEqual(generic['model_roots'], [])
        self.assertEqual(generic['adapter_selection']['adapter_id'], 'generic-cdf')
        unique = next(row for row in result['contexts'] if row['profile_id'] == 'unique')
        self.assertEqual(unique['model_roots'], [self.paths['OTHER']])

    def test_healthy_legacy_discovery_does_not_change_payload_shape(self):
        self.write_mapping(bad_include=False)
        result = self.detect()
        self.assertFalse(result['blocking'])
        self.assertNotIn('discovery_degraded', result)
        self.assertNotIn('native_verified_definitions', result['library_map'])

    def test_missing_cds_file_cannot_be_replaced_with_an_old_capture(self):
        os.unlink(self.cds)
        self.assertTrue(self.detect()['blocking'])


class NativeMappingSelectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.loaded = os.path.join(self.root, 'loaded-library')
        self.other = os.path.join(self.root, 'other-library')
        os.makedirs(self.loaded)
        os.makedirs(self.other)
        self.cds = os.path.join(self.root, 'cds.lib')
        with open(self.cds, 'w') as stream:
            stream.write('DEFINE Loaded %s\nDEFINE Other %s\n' % (self.loaded,self.other))
        self.native = {'Loaded': self.loaded}
        self.origin = {'cadence': {'_native_library_mappings': copy.deepcopy(self.native),
                                  'launch_environment': {}, 'discovery': {}},
                       '_maestro_launch_context': {'libraries':copy.deepcopy(self.native),
                                                   'source': {'cds_lib':self.cds}},
                       'runtime': {'run_root':os.path.join(self.root,'runs')}}

    def select(self, library, path, current=None):
        locator = {'path':os.path.join(path,'TB','maestro','maestro.sdb'), 'backend':'maestro',
                   'library_root':path,'library_name':library,'cell_name':'TB','view_name':'maestro',
                   'view_dir':os.path.join(path,'TB','maestro')}
        with mock.patch('crescendo_engine.ade_setup.inspect_setup', return_value={'tests':[{'library':library}]}), \
                mock.patch('crescendo_engine.cadence.nearest_cds_lib', return_value=self.cds):
            return selected_setup_config(self.origin, current or self.origin, locator)

    def test_browse_another_workspace_drops_old_native_mapping(self):
        selected = self.select('Other', self.other)
        self.assertNotIn('_native_library_mappings', selected['cadence'])
        self.assertEqual(self.origin['cadence']['_native_library_mappings'], self.native)

    def test_selecting_another_saved_setup_in_loaded_library_retains_actual_map(self):
        selected = self.select('Loaded', self.loaded)
        self.assertEqual(selected['cadence']['_native_library_mappings'], self.native)
        selected['cadence']['_native_library_mappings'].clear()
        self.assertEqual(self.origin['cadence']['_native_library_mappings'], self.native)

    def test_explicit_cds_override_drops_old_native_map_even_for_matching_root(self):
        current = copy.deepcopy(self.origin)
        current['cadence']['_manual_overrides'] = {'cds_lib':self.cds}
        selected = self.select('Loaded', self.loaded, current)
        self.assertNotIn('_native_library_mappings', selected['cadence'])


if __name__ == '__main__':
    unittest.main()
