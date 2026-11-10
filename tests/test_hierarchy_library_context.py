"""Saved Maestro and test design may belong to different logical libraries."""
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine import hierarchy
from crescendo_engine.workspace import parse_cds_defines


class HierarchyLibraryContextTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='crescendo hierarchy mapping ')
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.setup_root = os.path.join(self.root, 'setup physical library')
        self.design_root = os.path.join(self.root, 'separate design library')
        os.makedirs(self.setup_root)
        os.makedirs(self.design_root)
        self.source_cds = os.path.join(self.root, 'cds.lib')
        self.config = {'cadence': {'virtuoso_bin': 'fixture-no-EDA-launch'},
                       'runtime': {'run_root': os.path.join(self.root, 'runtime')}}
        self.setup = {'source': {'library_root': self.setup_root},
                      'setup_locator': {'library_name': 'SetupLib'},
                      'tests': [{'name': 'tb', 'library': 'DesignLib', 'cell': 'TOP', 'view': 'schematic'}]}

    def mapping(self, path, definitions):
        with open(path, 'w', encoding='utf-8') as stream:
            for library, root in definitions.items():
                stream.write('DEFINE %s "%s"\n' % (library, root))

    def probe(self, expected_root):
        design = self.setup['tests'][0]
        def command(argv, cwd, **kwargs):
            self.assertEqual(argv[0], 'fixture-no-EDA-launch')
            mapping = parse_cds_defines(os.path.join(cwd, 'cds.lib'))
            self.assertEqual(mapping[design['library']], expected_root)
            self.assertNotEqual(mapping.get('DesignLib'), self.setup_root)
            with open(os.path.join(cwd, 'hierarchy.records'), 'w', encoding='utf-8') as stream:
                stream.write('CELL\t"/"\t"%s"\t"TOP"\t"schematic"\t0\n' % design['library'])
            return {'returncode': 0, 'fixture_only': True}
        with mock.patch.object(hierarchy, 'child_environment', return_value={'PATH': os.defpath}), \
                mock.patch.object(hierarchy, 'run_command', side_effect=command) as launch:
            result = hierarchy.probe_hierarchy(self.setup, self.config, cells_only=True)
        launch.assert_called_once()
        return result['designs'][0]['context']

    def test_nearest_setup_mapping_resolves_cross_library_test_without_overwrite(self):
        self.mapping(self.source_cds, {'SetupLib': self.setup_root, 'DesignLib': self.design_root})
        self.config['cadence']['cds_lib'] = self.source_cds
        context = self.probe(self.design_root)
        self.assertEqual(context['source_cds_lib'], self.source_cds)
        with open(context['local_cds_lib'], encoding='utf-8') as stream:
            self.assertIn('DEFINE DesignLib "%s"' % self.design_root, stream.read())

    def test_source_include_preserves_actual_precedence_over_config_softinclude(self):
        configured = os.path.join(self.root, 'installation.cds.lib')
        other = os.path.join(self.root, 'other design root')
        os.makedirs(other)
        self.mapping(configured, {'DesignLib': other})
        self.mapping(self.source_cds, {'SetupLib': self.setup_root, 'DesignLib': self.design_root})
        self.config['cadence']['cds_lib'] = configured
        self.assertEqual(self.probe(self.design_root)['source_cds_lib'], self.source_cds)

    def test_config_mapping_resolves_foreign_design_when_source_has_no_cds_lib(self):
        configured = os.path.join(self.root, 'configured.cds.lib')
        self.mapping(configured, {'DesignLib': self.design_root})
        self.config['cadence']['cds_lib'] = configured
        self.assertEqual(self.probe(self.design_root)['source_cds_lib'], configured)

    def test_invoking_native_override_remains_authoritative(self):
        override = os.path.join(self.root, 'native-mapping.cds.lib')
        self.mapping(self.source_cds, {'DesignLib': self.setup_root})
        self.mapping(override, {'DesignLib': self.design_root})
        self.config['cadence']['source_cds_lib_override'] = override
        self.assertEqual(self.probe(self.design_root)['source_cds_lib'], override)

    def test_same_logical_library_can_fallback_to_its_setup_root(self):
        self.setup['tests'][0]['library'] = 'SetupLib'
        context = self.probe(self.setup_root)
        self.assertIsNone(context['source_cds_lib'])

    def test_missing_cross_library_mapping_fails_before_any_native_launch(self):
        with mock.patch.object(hierarchy, 'run_command') as launch:
            with self.assertRaisesRegex(ValueError, 'different library'):
                hierarchy.probe_hierarchy(self.setup, self.config, cells_only=True)
        launch.assert_not_called()
        self.assertFalse(any('cds.lib' in files for _directory, _children, files in os.walk(self.config['runtime']['run_root'])))

    def test_unavailable_mapped_root_does_not_fallback_even_for_same_library(self):
        self.setup['tests'][0]['library'] = 'SetupLib'
        self.mapping(self.source_cds, {'SetupLib': os.path.join(self.root, 'missing')})
        with mock.patch.object(hierarchy, 'run_command') as launch:
            with self.assertRaisesRegex(ValueError, 'mapping is unavailable'):
                hierarchy.probe_hierarchy(self.setup, self.config, cells_only=True)
        launch.assert_not_called()
