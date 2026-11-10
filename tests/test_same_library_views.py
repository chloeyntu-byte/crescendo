from __future__ import print_function
import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from tests.test_workspace_source_reference import fixture
from crescendo_engine.workspace import (build_workspace, build_occurrence_clone_plan, source_design_identity,
    verify_sources_unchanged, _schematic_parameter_rows, _spectre_sections, _netlist_instance,
    private_library_is_owned)
from crescendo_engine.config_binding import private_config_plan, config_binding_lines
from crescendo_engine.same_library_views import (cleanup_same_library_views, MARKER, register_view,
                                                  owns_view, config_designs_for_same_library)
from crescendo_engine.persistent_maestro import _clone_lines, _candidate_rows, _pristine_cell_plan
from crescendo_engine.maestro_batch import prepare_snapshot_batch, snapshot_cleanup_lines


class SameLibraryViewsTest(unittest.TestCase):
    def test_plain_custom_schematic_top_keeps_exact_view_and_requires_actual_type(self):
        design = dict(library='A', cell='TB', view='schematic_FTdiff')
        cell = dict(design, hierarchy_path='/', view_type='schematic')
        hierarchy = {'designs': [{'design': design, 'hierarchy': {'cells': [cell], 'instances': []}}]}
        selected = [dict(design_library='A', design_cell='TB', design_view='schematic_FTdiff')]
        plan = config_designs_for_same_library(hierarchy, selected)
        self.assertEqual(plan[0]['resolved_top'], design)
        self.assertEqual(plan[0]['view'], 'schematic_FTdiff')
        for view_type in (None, 'maskLayout', 'veriloga'):
            cell['view_type'] = view_type
            with self.assertRaisesRegex(ValueError, 'observed schematic implementation'):
                config_designs_for_same_library(hierarchy, selected)

    def build(self, root, config_design=False):
        setup, hierarchy, parameter, config, sources, mapping = fixture(root, config_design)
        config['cadence']['workspace_copy_strategy'] = 'same_library_views'
        with open(setup['source']['maestro_sdb'], 'w') as stream:
            stream.write('<Maestro><Active/></Maestro>')
        # Real hierarchy carries the plain top and every owner occurrence.
        hierarchy['designs'][0]['hierarchy']['cells'][0].update(view='schematic', hierarchy_path='/')
        hierarchy['designs'][0]['hierarchy']['cells'][2].update(view='schematic', hierarchy_path='/I0')
        hierarchy['designs'][0]['hierarchy']['instances'] = [copy.deepcopy(parameter['occurrence_chain'][0])]
        with mock.patch('crescendo_engine.workspace.resolve_copy_libraries', return_value=('A', mapping, sources)):
            identity = source_design_identity(setup, hierarchy, config)
            workspace = build_workspace(setup, hierarchy, config,
                config['runtime']['run_root'] + '/worker/workspace', parameters=[parameter], expected_source_identity=identity)
        return workspace, parameter, setup, hierarchy, config, sources, mapping

    def create_owned(self, record):
        os.makedirs(record['path'])
        with open(os.path.join(record['path'], MARKER), 'w') as stream:
            json.dump({'token': record['token'], 'target': record['target']}, stream)
        with open(os.path.join(record['path'], 'sch.oa'), 'w') as stream:
            stream.write('owned schematic')

    def test_plain_design_only_maestro_is_copied_private_config_keeps_source_top(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, sources, _ = self.build(root)
            self.assertEqual(w['copy_policy'], 'same_library_views')
            self.assertEqual(w['copy_views']['A'], {})
            self.assertEqual(w['copy_views']['B'], {})
            clones = build_occurrence_clone_plan(w, [p], {'p': 2e-6})
            plan = private_config_plan(w, clones)[0]
            self.assertEqual(plan['top'], ('A', 'TB', 'schematic'))
            self.assertEqual(plan['config'][:2], ('A', 'TB'))
            self.assertTrue(plan['config'][2].startswith('cr_s_'))
            self.assertTrue(plan['generated_config'])
            self.assertEqual(plan['bindings'][0]['target'][:2], ('B', 'shared'))
            self.assertEqual(plan['bindings'][0]['target'][2], clones[0]['target_view'])
            self.assertFalse(os.path.exists(os.path.join(w['root'], 'libraries')))
            self.assertTrue(w['setup_dir'].startswith(sources['A'] + '/TB/cr_s_'))
            self.assertTrue(os.path.isfile(w['setup_sdb']))

    def test_existing_config_only_config_and_maestro_copy_not_actual_top(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, _, _ = self.build(root, True)
            self.assertEqual(w['copy_views']['A'], {})
            clone = build_occurrence_clone_plan(w, [p], {'p': 2e-6})[0]
            lines, plan = config_binding_lines(w, [clone], root + '/records')
            self.assertEqual(plan[0]['top'], ('A', 'TB', 'schematic'))
            text = '\n'.join(lines)
            self.assertIn('hdbBind(crescConfigPath "B" "shared" "symbol" "I0" t t nil)', text)
            self.assertNotIn('cr_s_', ' '.join(plan[0]['bindings'][0].get('verification_master') or ()))

    def test_original_library_is_not_owned_and_setup_requires_exact_marker(self):
        with tempfile.TemporaryDirectory() as root:
            w, _, _, _, _, sources, _ = self.build(root)
            self.assertFalse(private_library_is_owned(w, 'A'))
            target = (w['primary_library'], w['setup_cell'], w['setup_view'])
            self.assertTrue(owns_view(w, target, role='maestro', require_marker=True))
            self.assertFalse(owns_view(w, ('A', 'TB', 'maestro'), role='maestro', require_marker=True))
            self.assertFalse(owns_view(w, target, role='config', require_marker=True))
            marker = os.path.join(w['setup_dir'], MARKER)
            with open(marker, 'w') as stream:
                json.dump({'token': 'foreign', 'target': list(target)}, stream)
            self.assertFalse(owns_view(w, target, role='maestro', require_marker=True))

    def test_run_name_is_visible_without_losing_token_or_exact_key(self):
        with tempfile.TemporaryDirectory() as root:
            w, _, _, _, _, _, _ = self.build(root)
            w['run_display_name'] = 'VCO Trial 123'
            a = register_view(w, ('B', 'shared', 'schematic'), ('/I0',), role='working')
            b = register_view(w, ('B', 'shared', 'schematic'), ('/I1',), role='working')
            self.assertIn('VCO_Trial_123_working', a['target'][2])
            self.assertTrue(a['target'][2].startswith('cr_s_' + w['same_library_token'][:8] + '_'))
            self.assertNotEqual(a['target'][2], b['target'][2])

    def test_multiple_parameters_in_owner_share_exact_view_and_never_copy_ancestor(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, _, _ = self.build(root)
            ancestor = dict(p['occurrence_chain'][0], name='TOP', hierarchy_path='/TOP')
            leaf = dict(p['occurrence_chain'][0], hierarchy_path='/TOP/I0', parent_path='/TOP')
            p.update(owner_occurrence_path='/TOP/I0', hierarchy_path='/TOP/I0/M0', occurrence_chain=[ancestor, leaf])
            q = dict(p, parameter_id='q', name='l')
            clones = build_occurrence_clone_plan(w, [p, q], {'p': 2e-6, 'q': 1e-6})
            self.assertEqual(len(clones), 1)
            self.assertEqual(clones[0]['clone_cell'], 'shared')
            # _clone_lines must receive workspace to select the same-view writer.
            text = '\n'.join(_clone_lines(clones, root + '/records', dict(w, config_designs=[])))
            self.assertEqual(text.count('dbCopyCellView'), 1)
            self.assertNotIn('parentCv', text)
            self.assertNotIn('cdfCopyCDF', text)

    def test_same_cell_different_occurrences_get_distinct_view_values(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, _, _ = self.build(root)
            q = copy.deepcopy(p)
            q.update(parameter_id='q', owner_occurrence_path='/I1', hierarchy_path='/I1/M0')
            q['occurrence_chain'][0].update(name='I1', hierarchy_path='/I1')
            clones = build_occurrence_clone_plan(w, [p, q], {'p': 2e-6, 'q': 4e-6})
            lookup = {(c['design_library'], c['design_cell'], c['design_view'], c['occurrence_path']): c for c in clones}
            rows = _candidate_rows(w, [p, q], {'p': 2e-6, 'q': 4e-6}, lookup)
            self.assertEqual(len(clones), 2)
            self.assertNotEqual(rows[0]['view'], rows[1]['view'])
            self.assertEqual([(r['library'], r['cell']) for r in rows], [('B', 'shared'), ('B', 'shared')])

    def test_top_owner_uses_view_bound_at_config_top(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, _, _ = self.build(root)
            p.update(owner_library='A', owner_cell='TB', owner_occurrence_path='/', hierarchy_path='/M0', occurrence_chain=[])
            clones, rows = _schematic_parameter_rows(w, [p], {'p': 2e-6})
            self.assertEqual(rows[0]['view'], clones[0]['target_view'])
            self.assertEqual(private_config_plan(w, clones)[0]['top'], ('A', 'TB', clones[0]['target_view']))

    def test_owned_addition_and_export_do_not_drift_actual_source_views(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, setup, hierarchy, config, sources, mapping = self.build(root)
            before = w['source_identity']
            row = build_occurrence_clone_plan(w, [p], {'p': 2e-6})[0]['owned_view']
            self.create_owned(row)
            os.makedirs(os.path.join(sources['B'], 'shared', 'schematic_any_user_export'))
            with mock.patch('crescendo_engine.workspace.resolve_copy_libraries', return_value=('A', mapping, sources)):
                self.assertEqual(before, source_design_identity(setup, hierarchy, config))
            self.assertTrue(verify_sources_unchanged(w)['ok'])
            with open(os.path.join(sources['B'], 'shared', 'schematic', 'sch.oa'), 'w') as stream:
                stream.write('user drift')
            self.assertFalse(verify_sources_unchanged(w)['ok'])

    def test_original_cell_cdf_metadata_drift_is_detected(self):
        with tempfile.TemporaryDirectory() as root:
            w, _, _, _, _, sources, _ = self.build(root)
            with open(os.path.join(sources['B'], 'shared', 'data.dm'), 'w') as stream:
                stream.write('CDF drift')
            self.assertFalse(verify_sources_unchanged(w)['ok'])

    def test_opaque_veriloga_actual_view_drift_is_detected(self):
        with tempfile.TemporaryDirectory() as root:
            w, _, _, _, _, sources, _ = self.build(root)
            with open(os.path.join(sources['B'], 'opaque', 'veriloga', 'sch.oa'), 'w') as stream:
                stream.write('opaque model changed')
            self.assertFalse(verify_sources_unchanged(w)['ok'])

    def test_cleanup_requires_proof_and_exact_marker_never_deletes_source_cell(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, sources, _ = self.build(root)
            row = build_occurrence_clone_plan(w, [p], {'p': 2e-6})[0]['owned_view']
            self.create_owned(row)
            self.assertFalse(cleanup_same_library_views(w)['cleanup_proven'])
            self.assertTrue(os.path.isdir(row['path']))
            self.assertTrue(cleanup_same_library_views(w, True)['cleanup_proven'])
            self.assertFalse(os.path.exists(row['path']))
            self.assertTrue(os.path.isfile(os.path.join(sources['B'], 'shared', 'schematic', 'sch.oa')))
            self.assertTrue(os.path.isdir(os.path.join(sources['B'], 'shared', 'layout')))

    def test_unknown_marker_or_lock_is_retained(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, _, _ = self.build(root)
            row = build_occurrence_clone_plan(w, [p], {'p': 2e-6})[0]['owned_view']
            self.create_owned(row)
            with open(os.path.join(row['path'], MARKER), 'w') as stream:
                json.dump({'token': 'other', 'target': row['target']}, stream)
            self.assertFalse(cleanup_same_library_views(w, True)['cleanup_proven'])
            self.assertTrue(os.path.isdir(row['path']))

    def test_legacy_strategy_stays_explicit(self):
        with tempfile.TemporaryDirectory() as root:
            setup, h, p, c, sources, mapping = fixture(root)
            c['cadence']['workspace_copy_strategy'] = 'private_library'
            with mock.patch('crescendo_engine.workspace.resolve_copy_libraries', return_value=('A', mapping, sources)):
                w = build_workspace(setup, h, c, c['runtime']['run_root'] + '/workspace', parameters=[p])
            self.assertEqual(w['copy_policy'], 'source_reference')
            self.assertNotEqual(build_occurrence_clone_plan(w, [p], {'p': 2e-6})[0]['clone_library'], 'B')

    def test_netlist_same_cell_two_views_require_exact_view(self):
        with tempfile.TemporaryDirectory() as root:
            path = root + '/netlist'
            with open(path, 'w') as stream:
                stream.write('// Library name: B\n// Cell name: shared\n// View name: schematic\nM0 (a b) mos w=1u\n'
                             '// Library name: B\n// Cell name: shared\n// View name: cr_s_owned\nM0 (a b) mos w=4u\n')
            sections = _spectre_sections(path)
            self.assertEqual(_netlist_instance(sections, 'B', 'shared', 'M0', 'schematic')[0]['w'], ['1u'])
            self.assertEqual(_netlist_instance(sections, 'B', 'shared', 'M0', 'cr_s_owned')[0]['w'], ['4u'])
            with self.assertRaises(Exception):
                _netlist_instance(sections, 'B', 'shared', 'M0')

    def test_pristine_capture_stays_in_private_library(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, _, _ = self.build(root)
            clones = build_occurrence_clone_plan(w, [p], {'p': 2e-6})
            rows = _candidate_rows(w, [p], {'p': 2e-6}, {('A', 'TB', 'schematic', '/I0'): clones[0]})
            pristine = _pristine_cell_plan(rows, 'epoch', w)[0]
            self.assertEqual(pristine['pristine_library'], 'B')
            self.assertEqual(pristine['pristine_cell'], 'shared')
            self.assertNotEqual(pristine['pristine_view'], pristine['view'])
            self.assertEqual(pristine['owned_view']['role'], 'pristine')

    def test_parallel_batch_changes_only_leaf_views_and_private_config_cells(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, config, _, _ = self.build(root)
            clones = build_occurrence_clone_plan(w, [p], {'p': 2e-6})
            self.create_owned(clones[0]['owned_view'])
            class Worker(object):
                pass
            worker = Worker()
            worker.workspace, worker.parameters, worker.clone_plan = w, [p], clones
            worker.epoch, worker.worker_index = '12345678abc', 0
            worker.worker_root = config['runtime']['run_root'] + '/worker'
            worker._adapter_path = lambda: None
            run_root = config['runtime']['run_root']
            requested = [{'candidate_id': 'c%d' % index, 'candidate_root': run_root + '/candidates/c%d' % index,
                          'values': {'p': value}} for index, value in enumerate((2e-6, 4e-6))]
            manifest = prepare_snapshot_batch(worker, requested, run_root + '/batches/one',
                {'candidate_id': 'batch', 'sequence': 1, 'epoch': worker.epoch, 'nonce': 'n'})
            manifest['worker_epoch'] = worker.epoch
            self.assertEqual(len(manifest['owned_views']), 4)
            self.assertNotEqual(manifest['owned_views'][0]['target'][2], manifest['owned_views'][1]['target'][2])
            self.assertEqual(manifest['owned_cells'], [])
            self.assertEqual(sum(row['target'][:2] == ['B', 'shared'] for row in manifest['owned_views']), 2)
            self.assertEqual(sum(row['target'][:2] == ['A', 'TB'] for row in manifest['owned_views']), 2)
            self.assertNotIn('parentCv', '\n'.join(manifest['snapshot_lines']))
            cleanup = '\n'.join(snapshot_cleanup_lines(manifest))
            self.assertIn('ddGetObj("B" "shared" "cr_s_', cleanup)
            self.assertNotIn('ddGetObj("B" "shared")', cleanup)
            views = [row['patch_plan']['parameters']['parameters'][0]['view'] for row in manifest['candidates']]
            self.assertNotEqual(views[0], views[1])


if __name__ == '__main__':
    unittest.main()
