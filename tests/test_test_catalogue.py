import copy
import os
import unittest
from unittest import mock

from crescendo_engine.test_catalogue import load_test_catalogue
from crescendo_engine.server import OptimizerService
from tests import test_setup_baseline as baseline_fixture


class TestCatalogueTests(unittest.TestCase):
    def setUp(self):
        self.fixture = baseline_fixture.SetupBaselineTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_headers_do_not_parse_analysis_output_or_device_metadata(self):
        f = self.fixture
        rows = ''.join('<test enabled="%s">T%d<tooloptions><option>lib<value>missing%d</value></option>'
                       '<option>cell<value>C</value></option></tooloptions>'
                       '<vars><var>X<value>unread_expression</value></var></vars></test>' %
                       ('1' if index % 2 else '0', index, index) for index in range(1000))
        f.write(f.path, '<setupdb><active><tests>' + rows + '</tests></active></setupdb>')
        service = OptimizerService(f.config)
        with mock.patch('crescendo_engine.ade_setup._state_details', side_effect=AssertionError('analysis read')), \
             mock.patch('crescendo_engine.hierarchy.probe_hierarchy', side_effect=AssertionError('hierarchy read')), \
             mock.patch('crescendo_engine.job_policy.capture_setup_job_policy', side_effect=AssertionError('native job read')), \
             mock.patch.object(service, '_prepare_setup_private_session', side_effect=AssertionError('private copy')):
            token, preview = service.inspect_tests(f.path)
        self.assertTrue(preview['setup']['metadata_only'])
        self.assertEqual(len(preview['tests']), 1000)
        self.assertEqual(preview['tests'][999]['library'], 'missing999')
        self.assertFalse(preview['tests'][0]['enabled'])
        self.assertTrue(preview['tests'][1]['enabled'])
        self.assertTrue(all('analyses' not in test and 'variables' not in test for test in preview['tests']))
        f.write(f.state, '<state><changed/></state>')
        with self.assertRaisesRegex(ValueError, 'changed since Test inspection'):
            service.inspect_test_cells(token, 'T1')

    def test_frozen_catalogue_keeps_origin_and_does_not_adopt_new_saved_values(self):
        f = self.fixture
        f.freeze()
        f.write(f.path, f.xml.replace('Name="C"', 'Name="NEW"'))
        catalogue = load_test_catalogue(f.path, f.config)
        self.assertEqual([row['name'] for row in catalogue['tests']], ['C'])
        self.assertEqual(catalogue['source']['setup_db'], f.path)
        self.assertTrue(catalogue.get('baseline'))

    def test_concurrent_save_cannot_pair_old_headers_with_new_hash(self):
        from crescendo_engine import test_catalogue
        f = self.fixture
        parse = test_catalogue._setupdb_root
        def changing(path):
            result = parse(path)
            f.write(f.path, f.xml.replace('Name="C"', 'Name="NEW"'))
            return result
        with mock.patch.object(test_catalogue, '_setupdb_root', side_effect=changing):
            with self.assertRaisesRegex(ValueError, 'changed while reading Tests'):
                load_test_catalogue(f.path, f.config)


class SingleTestHttpTests(unittest.TestCase):
    def test_staged_requests_keep_their_tokens_and_test_identity(self):
        from crescendo_engine.server import make_handler
        service = mock.Mock()
        service.inspect_tests.return_value = ('test-token', {'tests': [{'name': 'B'}]})
        service.inspect_test_cells.return_value = ('cell-token', {'cell_scopes': []})
        service.prepare_cells.return_value = ('ready-token', {'parameters': []})
        service.prepare.return_value = ('legacy-token', {})
        handler = object.__new__(make_handler(service))
        cases = [('/api/inspect-tests', {'path': '/setup', 'backend': 'maestro'}, 'test_inspection_id', 'test-token'),
                 ('/api/inspect-test-cells', {'test_inspection_id': 'test-token', 'test_name': 'B'}, 'inspection_id', 'cell-token'),
                 ('/api/prepare-cells', {'inspection_id': 'cell-token', 'scope_ids': ['root']}, 'prepared_id', 'ready-token'),
                 ('/api/prepare', {'path': '/setup', 'selected_test': 'B'}, 'prepared_id', 'legacy-token')]
        for path, body, key, expected in cases:
            handler.path = path
            handler._body = lambda body=body: body
            handler._json = mock.Mock()
            handler.do_POST()
            args = handler._json.call_args[0]
            self.assertEqual(args[0], 200)
            self.assertEqual(args[1][key], expected)
        service.inspect_test_cells.assert_called_once_with('test-token', 'B', max_depth=32)
        service.prepare_cells.assert_called_once_with('cell-token', ['root'])
        self.assertEqual(service.prepare.call_args[1]['selected_test'], 'B')
