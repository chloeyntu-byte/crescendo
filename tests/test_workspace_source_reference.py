from __future__ import print_function

import copy
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.config_binding import config_binding_lines, config_clone_cdf_lines, private_config_plan
from crescendo_engine.storage_preflight import estimate_workspace_copy_bytes
from crescendo_engine.workspace import (
    build_workspace, build_occurrence_clone_plan, parse_cds_defines,
    private_design_identity, private_library_name, source_design_identity,
    verify_sources_unchanged, workspace_test_design_lines, WorkspaceBuildFailure,
)
from crescendo_engine.persistent_maestro import _candidate_rows, _clone_lines


def fixture(root, config_design=False):
    sources = dict((lib, os.path.join(root, 'source_' + lib)) for lib in ('A', 'B'))
    for library, cells in (('A', {'TB': ['maestro', 'schematic', 'layout'], 'digital': ['schematic']}),
                           ('B', {'shared': ['schematic', 'symbol', 'layout'], 'opaque': ['veriloga']})):
        os.makedirs(sources[library])
        with open(os.path.join(sources[library], '.oalib'), 'w') as f:
            f.write('<Library DMSystem="oaDMFileSys"/>')
        for cell, views in cells.items():
            os.makedirs(os.path.join(sources[library], cell))
            with open(os.path.join(sources[library], cell, 'data.dm'), 'w') as f:
                f.write('cell metadata')
            for view in views:
                folder = os.path.join(sources[library], cell, view)
                os.makedirs(folder)
                with open(os.path.join(folder, 'maestro.sdb' if view == 'maestro' else 'sch.oa'), 'w') as f:
                    f.write(('layout bytes ' * 50000) if view == 'layout' else (library + cell + view))
    setup = {'backend': 'maestro', 'source': {'library_root': sources['A'],
             'maestro_sdb': os.path.join(sources['A'], 'TB', 'maestro', 'maestro.sdb')},
             'tests': [{'name': 'test', 'library': 'A', 'cell': 'TB', 'view': 'schematic'}]}
    hierarchy = {'designs': [{'design': ('A', 'TB', 'schematic'), 'hierarchy': {'cells': [
        {'library': 'A', 'cell': 'TB'}, {'library': 'A', 'cell': 'digital'},
        {'library': 'B', 'cell': 'shared'}, {'library': 'B', 'cell': 'opaque'}]}}]}
    parameter = {'parameter_id': 'p', 'backend': 'schematic', 'design_library': 'A',
                 'design_cell': 'TB', 'design_view': 'schematic', 'owner_library': 'B',
                 'owner_cell': 'shared', 'owner_occurrence_path': '/I0', 'hierarchy_path': '/I0/M0',
                 'name': 'w', 'cdf_type': 'string', 'current_value': '1u',
                 'occurrence_chain': [{'hierarchy_path': '/I0', 'parent_path': '/', 'name': 'I0',
                                     'master_library': 'B', 'master_cell': 'shared',
                                     'master_view': 'symbol', 'master_view_type': 'schematicSymbol'}]}
    if config_design:
        os.makedirs(os.path.join(sources['A'], 'TB', 'config'))
        with open(os.path.join(sources['A'], 'TB', 'config', 'config.oa'), 'w') as f:
            f.write('config source')
        binding = dict(parameter['occurrence_chain'][0], owner_library='A', owner_cell='TB',
                       owner_view='schematic', resolved_library='B', resolved_cell='shared',
                       resolved_view='schematic', resolved_view_type='schematic', scan_traversable=True,
                       stop=False, fixed_library=True, fixed_cell=True, fixed_view=False)
        opaque = dict(binding, name='VA', hierarchy_path='/VA', master_cell='opaque',
                      resolved_cell='opaque', resolved_view='veriloga', scan_traversable=False, stop=True)
        design = {'library': 'A', 'cell': 'TB', 'view': 'config',
                  'resolved_top': {'library': 'A', 'cell': 'TB', 'view': 'schematic'},
                  'config_bindings': [binding, opaque],
                  'config_dependencies': [{'library': 'A', 'cell': 'TB', 'view': 'config'}]}
        hierarchy['designs'][0].update(design=('A', 'TB', 'config'), **design)
        parameter.update(design_view='config', resolved_top=design['resolved_top'], occurrence_chain=[binding])
        setup['tests'][0]['view'] = 'config'
    mapping = os.path.join(root, 'cds.lib')
    with open(mapping, 'w') as f:
        for library, path in sources.items():
            f.write('DEFINE %s %s\n' % (library, path))
    config = {'cadence': {'source_cds_lib_override': mapping, 'workspace_copy_strategy': 'private_library'}, 'runtime': {'run_root': os.path.join(root, 'runs')}}
    return setup, hierarchy, parameter, config, sources, mapping


class WorkspaceSourceReferenceTest(unittest.TestCase):
    def build(self, root, config_design=False, parameters=True):
        setup, hierarchy, parameter, config, sources, mapping = fixture(root, config_design)
        with mock.patch('crescendo_engine.workspace.resolve_copy_libraries', return_value=('A', mapping, sources)):
            workspace = build_workspace(setup, hierarchy, config, config['runtime']['run_root'] + '/workers/one/workspace',
                                        parameters=[parameter] if parameters else [])
        return workspace, parameter, setup, hierarchy, config, sources

    def test_only_mutable_views_copy_unselected_cells_and_layout_are_source_references(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, sources = self.build(root)
            self.assertTrue(os.path.isfile(os.path.join(w['copied_libraries']['A'], 'TB', 'schematic', 'sch.oa')))
            for library, cell in [('A', 'digital'), ('B', 'shared'), ('B', 'opaque')]:
                self.assertFalse(os.path.exists(os.path.join(w['copied_libraries'][library], cell)))
                self.assertTrue(os.path.isdir(os.path.join(sources[library], cell)))
            self.assertFalse(os.path.exists(os.path.join(w['copied_libraries']['A'], 'TB', 'layout')))
            self.assertEqual(w['schema_version'], 'crescendo.workspace.v1')
            self.assertEqual(w['copy_dependency_cells']['B'], [])
            self.assertIn('shared', w['source_dependency_cells']['B'])
            self.assertNotEqual(w['source_identity']['fingerprint'], w['copied_source_identity']['fingerprint'])
            self.assertEqual(w['materialized_source_identity'], w['copied_source_identity'])

    def test_private_writes_do_not_write_through_to_readonly_source_or_share_inode(self):
        with tempfile.TemporaryDirectory() as root:
            setup, hierarchy, parameter, config, sources, mapping = fixture(root)
            source = os.path.join(sources['A'], 'TB', 'schematic', 'sch.oa')
            os.chmod(source, 0o444)
            with open(source, 'rb') as f:
                original = f.read()
            with mock.patch('crescendo_engine.workspace.resolve_copy_libraries', return_value=('A', mapping, sources)):
                w = build_workspace(setup, hierarchy, config, config['runtime']['run_root'] + '/workers/one/workspace', parameters=[parameter])
            private = os.path.join(w['copied_libraries']['A'], 'TB', 'schematic', 'sch.oa')
            self.assertFalse(os.path.islink(private))
            self.assertNotEqual(os.stat(source).st_ino, os.stat(private).st_ino)
            with open(private, 'w') as f:
                f.write('private candidate change')
            with open(source, 'rb') as f:
                self.assertEqual(f.read(), original)
            self.assertEqual(os.stat(source).st_mode & 0o777, 0o444)
            self.assertTrue(verify_sources_unchanged(w)['ok'])

    def test_source_lib_defines_preserved_and_private_targets_distinct(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, sources = self.build(root)
            mapping = parse_cds_defines(os.path.join(w['root'], 'cds.lib'))
            for library in sources:
                self.assertEqual(mapping[library], sources[library])
                self.assertEqual(mapping[private_library_name(w, library)], w['copied_libraries'][library])
            self.assertNotEqual(w['primary_library'], 'A')
            lines = '\n'.join(workspace_test_design_lines(w))
            self.assertIn('list("lib" "%s")' % private_library_name(w, 'A'), lines)
            self.assertIn('axlSetTestToolArgs', lines)

    def test_native_nested_clone_reads_source_and_writes_only_private_libraries(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, _ = self.build(root)
            clones = build_occurrence_clone_plan(w, [p], {'p': 2e-6})
            row = clones[0]
            self.assertEqual(row['source_library'], 'B')
            self.assertEqual(row['clone_library'], private_library_name(w, 'B'))
            self.assertEqual(row['parent_library'], private_library_name(w, 'A'))
            script = '\n'.join(_clone_lines(clones, root + '/records', w))
            self.assertIn('"B" "shared" "schematic" "schematic" "r"', script)
            self.assertNotIn('"A" "TB" "schematic" "schematic" "a"', script)
            self.assertIn('cdfCopyCDF', script)
            lookup = {('A', 'TB', 'schematic', '/I0'): row}
            rows = _candidate_rows(w, [p], {'p': 2e-6}, lookup)
            self.assertEqual(rows[0]['library'], private_library_name(w, 'B'))
            self.assertEqual(rows[0]['netlist_design_library'], private_library_name(w, 'A'))

    def test_config_top_and_occurrence_are_private_opaque_unmodified_binding_keeps_source(self):
        with tempfile.TemporaryDirectory() as root:
            w, p, _, _, _, _ = self.build(root, config_design=True)
            clones = build_occurrence_clone_plan(w, [p], {'p': 2e-6})
            plan = private_config_plan(w, clones)[0]
            self.assertEqual(plan['config'][0], private_library_name(w, 'A'))
            self.assertEqual(plan['top'][0], private_library_name(w, 'A'))
            unchanged = next(row for row in plan['bindings'] if row['name'] == 'VA')
            self.assertEqual(unchanged['target'], ('B', 'opaque', 'veriloga'))
            script = '\n'.join(config_binding_lines(w, clones, root + '/config.records')[0])
            self.assertNotIn('hdbOpen("A" "TB" "config" "w")', script)
            self.assertIn('hdbIsAtStopPoint', script)

    def test_external_source_change_still_detected_without_materializing_cell(self):
        with tempfile.TemporaryDirectory() as root:
            w, _, _, _, _, sources = self.build(root)
            self.assertTrue(verify_sources_unchanged(w)['ok'])
            path = os.path.join(sources['A'], 'digital', 'schematic', 'sch.oa')
            with open(path, 'w') as f:
                f.write('changed same source dependency')
            self.assertFalse(verify_sources_unchanged(w)['ok'])

    def test_empty_selection_copies_setup_only(self):
        with tempfile.TemporaryDirectory() as root:
            w, _, _, _, _, _ = self.build(root, parameters=False)
            self.assertTrue(os.path.isdir(os.path.join(w['copied_libraries']['A'], 'TB', 'maestro')))
            self.assertFalse(os.path.exists(os.path.join(w['copied_libraries']['A'], 'TB', 'schematic')))
            self.assertEqual(private_design_identity(w, ('A', 'TB', 'schematic')), ('A', 'TB', 'schematic'))

    def test_storage_estimate_tracks_actual_mutable_views(self):
        with tempfile.TemporaryDirectory() as root:
            setup, hierarchy, parameter, config, sources, mapping = fixture(root)
            with mock.patch('crescendo_engine.storage_preflight.resolve_copy_libraries', return_value=('A', mapping, sources)):
                old = estimate_workspace_copy_bytes(setup, hierarchy, config)
                new = estimate_workspace_copy_bytes(setup, hierarchy, config, parameters=[parameter])
            self.assertGreater(old, 1000000)
            self.assertLess(new, 1000)

    def test_batch_snapshot_uses_private_root_and_branches_with_source_opaque_binding(self):
        from types import SimpleNamespace
        from crescendo_engine.maestro_batch import prepare_snapshot_batch
        for use_config in (False, True):
            with tempfile.TemporaryDirectory() as root:
                w, p, _, _, _, _ = self.build(root, config_design=use_config)
                w['setup'] = {'tests': copy.deepcopy(w['setup']['tests']), 'corners': []}
                clones = build_occurrence_clone_plan(w, [p], {'p': 2e-6})
                worker = SimpleNamespace(workspace=w, parameters=[p], clone_plan=clones,
                                         epoch='epoch', worker_index=0, worker_root=root,
                                         config={'runtime': {}}, _adapter_path=lambda: None)
                identity = {'epoch': 'epoch', 'sequence': 1, 'nonce': 'nonce', 'candidate_id': 'batch'}
                result = prepare_snapshot_batch(worker, [{'candidate_id': 'c0', 'values': {'p': 2e-6},
                                                'candidate_root': os.path.join(root, 'candidates', 'c0')}],
                                                os.path.join(root, 'batch'), identity)
                candidate = result['candidates'][0]
                native_test = result['batch_tests'][0]
                self.assertEqual(native_test['design'][0], private_library_name(w, 'A'))
                self.assertEqual(candidate['clone_plan'][0]['clone_library'], private_library_name(w, 'B'))
                script = '\n'.join(result['snapshot_lines'])
                self.assertNotIn('"A" "TB" "schematic" "schematic" "a"', script)
                self.assertIn('"%s" "TB" "schematic" "schematic" "r"' % private_library_name(w, 'A'), script)
                if use_config:
                    plan = candidate['config_binding_plan'][0]
                    opaque = next(row for row in plan['bindings'] if row['name'] == 'VA')
                    self.assertEqual(opaque['target'], ('B', 'opaque', 'veriloga'))

    def test_opaque_instance_source_is_fingerprinted_even_without_parameter_cell_scan(self):
        with tempfile.TemporaryDirectory() as root:
            setup, hierarchy, parameter, config, sources, mapping = fixture(root)
            body = hierarchy['designs'][0]['hierarchy']
            body['cells'] = [row for row in body['cells'] if row['cell'] != 'opaque']
            body['instances'] = [{'master_library': 'B', 'master_cell': 'opaque', 'master_view': 'symbol', 'has_schematic': False}]
            with mock.patch('crescendo_engine.workspace.resolve_copy_libraries', return_value=('A', mapping, sources)):
                w = build_workspace(setup, hierarchy, config, config['runtime']['run_root'] + '/workers/one/workspace', parameters=[parameter])
            self.assertFalse(os.path.exists(os.path.join(w['copied_libraries']['B'], 'opaque')))
            with open(os.path.join(sources['B'], 'opaque', 'veriloga', 'sch.oa'), 'w') as f:
                f.write('different implementation')
            self.assertFalse(verify_sources_unchanged(w)['ok'])

    def test_ade_xl_binds_private_design_before_candidate_and_closes_on_binding_error(self):
        from tests.test_adexl_runner import AdeXLRunnerTest
        from crescendo_engine.adexl_runner import run_adexl
        with tempfile.TemporaryDirectory() as root:
            w, config = AdeXLRunnerTest()._workspace_and_config(root)
            w['copy_policy'] = 'source_reference'
            w['private_library_names'] = {'DESIGN': 'PRIVATE'}
            w['primary_library'] = 'PRIVATE'
            w['setup']['tests'] = [{'name': 't', 'library': 'DESIGN', 'cell': 'DUT', 'view': 'schematic'}]
            w['private_design_identities'] = [{'source': ['DESIGN', 'DUT', 'schematic'], 'target': ['PRIVATE', 'DUT', 'schematic']}]

            def no_simulation(_argv, **kwargs):
                os.close(kwargs['guardian_launch_lock_fd'])
                return {'returncode': 1, 'timed_out': False, 'elapsed_seconds': 0.0}

            candidate = os.path.join(root, 'candidate')
            with mock.patch('crescendo_engine.adexl_runner.run_command', side_effect=no_simulation):
                run_adexl(w, config, candidate)
            runtime = os.path.join(candidate, 'adexl_runtime')
            with open(os.path.join(runtime, 'adexl_driver.il')) as f:
                driver = f.read()
            with open(os.path.join(runtime, 'adexl_private_binding.il')) as f:
                binding = f.read()
            self.assertLess(driver.index('adexl_startup.il'), driver.index('adexl_private_binding.il'))
            self.assertLess(driver.index('adexl_private_binding.il'), driver.index('adexl_candidate.il'))
            self.assertIn('mainSDB = crescAxlMainSetupDB', binding)
            self.assertIn('list("lib" "PRIVATE")', binding)
            self.assertIn('CRESCENDO_ADEXL_PRIVATE_BINDING_FAILED', binding)
            self.assertIn('crescAxlSafeClose()', binding)
            self.assertIn('exit(1)', binding)

    def test_missing_selected_top_view_refuses_before_native_creation(self):
        with tempfile.TemporaryDirectory() as root:
            setup, hierarchy, parameter, config, sources, mapping = fixture(root)
            parameter['design_view'] = 'not_a_view'
            with mock.patch('crescendo_engine.workspace.resolve_copy_libraries', return_value=('A', mapping, sources)):
                with self.assertRaises(WorkspaceBuildFailure) as ctx:
                    build_workspace(setup, hierarchy, config, config['runtime']['run_root'] + '/workers/one/workspace', parameters=[parameter])
            self.assertIn('required mutable source cellview is unavailable', str(ctx.exception))
            self.assertTrue(ctx.exception.cleanup['ok'])


if __name__ == '__main__':
    unittest.main()
