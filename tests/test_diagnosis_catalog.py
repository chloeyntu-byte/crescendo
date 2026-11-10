# -*- coding: utf-8 -*-
"""Presentation and retention fixtures only; never start native EDA tools."""
import ast
import json
import os
import tempfile
import unittest
from unittest import mock
from crescendo_engine.diagnosis_catalog import (CATALOG, describe_check, format_section, diagnostic_run, read_section)
from crescendo_engine.environment_diagnostics import retained_artifact_index
from crescendo_engine.environment_test import _report_context, EnvironmentTestManager
from crescendo_engine.netlist_execution import NetlistExecutionContext
from crescendo_engine.netlist_optimizer import PrivateNetlistOptimizer
from crescendo_engine import runner
from tests.test_netlist_optimizer import fixtures


def marker():
    return {'environment_test': {'schema_version': 'crescendo.environment_test.run.v1', 'test_id': 'owned-test', 'simulator': 'spectre'},
            'simulator': 'spectre', 'algorithm': 'random', 'generations': 1, 'population': 1,
            'algorithm_parameters': {'population': 1}, 'evaluator': 'real'}


class DiagnosisCatalogTests(unittest.TestCase):
    def test_native_run_error_and_later_history_error_remain_distinct(self):
        from crescendo_engine.environment_test import _native_failure_observations
        from crescendo_engine.diagnosis_catalog import diagnostic_code
        run = {'message': 'ERROR (ASSEMBLER-2404)', 'console_source': 'ocean.stdout',
               'console_line': 10, 'native_run_boundary': {'stage': 'Run', 'marker_line': 8}}
        history = {'message': '*Error* Crescendo native capture history unavailable',
                   'console_source': 'ocean.stdout', 'console_line': 40}
        primary = {'failure_stage': 'native_capture_skill_error', 'message': run['message'],
                   'capture_diagnostics': {'skill': {'first_error': run, 'errors': [run, history],
                       'native_run_evidence': [{'source': 'ocean.stdout', 'line': 38,
                                                'stage': 'HistoryLookup', 'event': 'BEGIN'}]}}}
        observations = _native_failure_observations(primary)
        self.assertEqual([row[0] for row in observations], ['native.run_window', 'native.history_lookup'])
        self.assertEqual(observations[0][1]['error'], run)
        self.assertEqual(primary['capture_diagnostics']['skill']['first_error'], run)
        self.assertEqual(diagnostic_code('execution', 'FAIL', primary), 'CRESCENDO-A402C4336E')
        self.assertEqual(diagnostic_code('native.history_lookup', 'FAIL'), 'CRESCENDO-4AC0515DE9')
        context = {'first_cause': primary}
        rendered = describe_check({'id': 'execution', 'status': 'FAIL'}, context)
        self.assertIn('ASSEMBLER-2404', rendered['summary'])
        self.assertIn('不代表跨流时间顺序', rendered['explanation'])

    def test_history_marker_alone_never_invents_failure(self):
        from crescendo_engine.environment_test import _native_failure_observations
        primary = {'capture_diagnostics': {'skill': {'errors': [], 'native_run_evidence': [
            {'source': 'ocean.stdout', 'line': 3, 'stage': 'HistoryName', 'event': 'BEGIN'}]}}}
        self.assertEqual(_native_failure_observations(primary), [])

    def test_native_worker_error_is_visible_without_parent_ocean_cdf_marker(self):
        worker = {'schema_version': 'crescendo.native_worker_diagnostics.v1', 'ok': False,
                  'logs': [{'path': '/owned/workspace/logs_root/logs0/Job0.log', 'fresh': True,
                      'ownership_proven': True, 'error_lines': [{'line': 123, 'text': '*Error* callback int("FN_gm")'}]}]}
        context = {'case_id': 'spectre.maestro_netlist', 'run_root': '/owned/run',
                   'first_cause': {'failure_stage': 'native_run_parameter_error', 'message': 'Native worker reported a parameter error'},
                   'native_worker_diagnostics': [worker]}
        check = {'id': 'execution', 'check_id': 'execution', 'status': 'FAIL', 'details': {}}
        result = describe_check(check, context)
        self.assertIn('Job0.log:123', result['explanation'])
        self.assertIn('FN_gm', result['explanation'])
        self.assertEqual(result['section_id'], 'netlist')
        self.assertIn('/owned/workspace/logs_root/logs0/Job0.log', [row['path'] for row in result['evidence']])
        worker['logs'][0]['fresh'] = False
        result = describe_check(check, context)
        self.assertNotIn('Job0.log:123', result['explanation'])

    def test_expected_wrapper_message_stays_auxiliary_after_parent_reader_failure(self):
        worker = {'schema_version': 'crescendo.native_worker_diagnostics.v1', 'ok': True,
            'parent_capture_failed': True, 'logs': [], 'expected_wrapper_aftermath_samples': [
                {'console_source': '/owned/logs_user/logs0/Job0.log', 'line': 123,
                 'message': 'ERROR (ADE-3036): No simulator run log', 'code': 'ADE-3036',
                 'stage': 'capture_wrapper_aftermath', 'primary_failure': False}]}
        context = {'case_id': 'spectre.maestro_netlist', 'run_root': '/owned/run',
            'first_cause': {'failure_stage': 'native_capture_skill_error', 'message': '*Error* lineread/read'},
            'native_worker_diagnostics': [worker],
            'records': [{'observation': 'diagnostic.native_worker_diagnostics',
                         'payload': {'native_worker_diagnostics': worker}}]}
        check = {'id': 'execution', 'check_id': 'execution', 'status': 'FAIL', 'details': {}}
        result = describe_check(check, context)
        self.assertIn('Job0.log:123', result['explanation'])
        self.assertIn('辅助证据，不作为根因', result['explanation'])
        self.assertIn('lineread/read', result['summary'])
        compact = format_section([check], context, compact=True)
        self.assertEqual(compact.count('ERROR (ADE-3036): No simulator run log'), 1)
        self.assertIn('"primary_failure": false', compact)
        worker['parent_capture_failed'] = False
        self.assertNotIn('ADE-3036', describe_check(check, context)['explanation'])

    def test_reader_trace_input_and_unverified_identity_survive_compact_once(self):
        from crescendo_engine.environment_diagnostics import _intern_structures, _semantic_artifact
        reader = {'state': 'preceding_unreturned_call', 'console_source': 'ocean.stdout',
            'trace_line': 91, 'error_line': 92, 'function': 'lineread', 'stack_depth': 4,
            'raw_trace_entry': 'TRACE_READER_INPUT_' + 'x' * 300, 'truncated': False,
            'argument_abbreviated': False, 'argument_completeness': 'printed_line_only',
            'qualification': 'diagnostic_only'}
        first = {'message': '*Error* lineread/read', 'native_reader_call': reader}
        artifact = _intern_structures(_semantic_artifact({'capture_only': True,
            'skill': {'ok': False, 'errors': [first], 'first_error': first}}))
        context = {'case_id': 'spectre.maestro_netlist',
            'first_cause': {'failure_stage': 'native_capture_skill_error', 'message': first['message']},
            'records': [{'observation': 'diagnostic.artifact',
                'payload': {'path': 'numeric/capture_diagnostics.json', 'data': artifact}},
                {'observation': 'diagnostic.identity_coverage', 'payload': {'status': 'NOT_VERIFIED',
                    'reason': 'native_capture_failed_before_point_qualification', 'unverified_count': None}}]}
        compact = format_section([{'id': 'execution', 'status': 'FAIL'}], context, compact=True)
        self.assertEqual(compact.count('"native_reader_call":'), 1)
        self.assertEqual(compact.count(reader['raw_trace_entry']), 1)
        self.assertIn('"argument_completeness": "printed_line_only"', compact)
        self.assertIn('native_capture_failed_before_point_qualification', compact)
        self.assertIn('"unverified_count": null', compact)

    def test_compact_report_keeps_one_context_and_copy_restores_native_cause(self):
        case = 'alps.maestro_netlist'
        primary = {'failure_stage': 'native_capture_skill_error', 'message': 'int("FN_gm") native callback error'}
        context = {'case_id': case, 'test_id': 'owned', 'run_root': '/owned/run', 'first_cause': primary,
                   'netlists': [{'path': '/owned/run/point/netlist/input.scs'}],
                   'records': [{'observation': 'diagnostic.producer_failure',
                       'payload': {'path': 'preparation_failure.json', 'data': {'error': 'EXACT_NATIVE_ERROR'}}}]}
        failed = {'id': case + '.execution', 'check_id': 'execution', 'status': 'FAIL',
                  'simulator': 'alps', 'engine': 'maestro_netlist', 'details': {'first_cause': primary}}
        missing = {'id': case + '.execution.not_reached', 'check_id': 'execution.not_reached',
                   'status': 'NOT_TESTED', 'simulator': 'alps', 'engine': 'maestro_netlist',
                   'details': {'stages': ['completion_evidence', 'metrics']}}
        seen = set()
        first = format_section([failed], context, compact=True, evidence_seen=seen)
        later = format_section([missing], context, compact=True, evidence_seen=seen)
        report = 'run_context ' + json.dumps({'scope': case, 'context': {
            key: value for key, value in context.items() if key != 'records'}}) + '\n' + first + later
        self.assertEqual(report.count('EXACT_NATIVE_ERROR'), 1)
        self.assertNotIn('"presentation"', report)
        self.assertNotIn('"first_cause":', first + later)
        self.assertIn('blocked_by', later)
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, 'environment_test.log')
            with open(path, 'w', encoding='utf-8') as stream:
                stream.write(report)
            copied = read_section(path, case, 'request')
        self.assertIn('FN_gm', copied)
        self.assertIn('EXACT_NATIVE_ERROR', copied)
        self.assertIn('/owned/run/point/netlist/input.scs', copied)

    def test_native_run_points_display_actual_dpar_and_scs_without_oa_claim(self):
        check = {'id': 'parameter_application', 'check_id': 'parameter_application', 'status': 'PASS',
                 'details': {'proof_kind': 'native_run_device_parameters', 'ok': True,
                     'private_oa_backannotation_proven': False, 'parameters': [{'parameter_id': 'nfin', 'requested': 5,
                         'effective': None, 'points': [{'test': 'tb', 'corner': 'TT', 'point_id': 7,
                             'requested': 5, 'effective': 'DPAR_0', 'dpar_status': 'resolved_from_run',
                             'scs_path': '/owned/current point/netlist/input.scs'}]}]}}
        result = describe_check(check)
        self.assertIn('DPAR_0', result['explanation'])
        self.assertIn('Corner=TT', result['explanation'])
        self.assertIn('不代表私有 schematic 已回写', result['explanation'])
        self.assertIn('/owned/current point/netlist/input.scs', [row['path'] for row in result['evidence']])

    def test_every_named_component_function_exists_in_actual_source(self):
        package = os.path.dirname(__import__('crescendo_engine').__file__)
        for prefix, section, title, filename, function in CATALOG:
            with self.subTest(prefix=prefix):
                with open(os.path.join(package, filename), encoding='utf-8') as stream:
                    tree = ast.parse(stream.read())
                names = function.split('.')
                node = tree
                for name in names:
                    node = next((item for item in node.body if isinstance(item, (ast.FunctionDef,ast.ClassDef)) and item.name == name), None)
                    self.assertIsNotNone(node, function)

    def test_same_section_missing_proof_explains_preceding_blocker(self):
        check = {'id': 'alps.maestro_netlist.source_guard', 'check_id': 'source_guard',
                 'simulator': 'alps', 'engine': 'maestro_netlist', 'status': 'NOT_TESTED',
                 'message': 'No native evidence available for this stage'}
        context = {'first_cause': {'failure_stage': 'native_user_scripts', 'message': 'Unknown active pre-run hook'}}
        result = describe_check(check, context)
        self.assertEqual(result['group_id'], 'alps.maestro_netlist')
        self.assertEqual(result['section_id'], 'netlist')
        self.assertEqual(result['blocked_by']['failure_stage'], 'native_user_scripts')
        self.assertIn('Unknown active pre-run hook', result['explanation'])
        self.assertNotIn('No native evidence', result['summary'])

    def test_error_file_without_cwd_is_reported_literal(self):
        check = {'id': 'execution', 'status': 'FAIL', 'details': {'reported_file': 'indirect.il', 'reported_line': 7}}
        result = describe_check(check, {'run_root': '/owned/run'})
        self.assertEqual(result['evidence'][0]['path'], 'indirect.il')
        self.assertEqual(result['evidence'][0]['attribution'], 'reported_literal')

    def test_runtime_probe_is_global_environment_even_when_spectre_log_hosts_it(self):
        p = describe_check({'id': 'runtime.summary', 'simulator': 'spectre', 'status': 'PASS'})
        self.assertEqual(p['group_id'], 'environment')

    def test_problem_section_contains_first_cause_and_minimum_prior_context(self):
        primary = {'failure_stage': 'native_user_scripts', 'message': 'preRunScript blocked'}
        context = {'case_id': 'alps.private_netlist', 'test_id': 'fresh', 'first_cause': primary,
                   'run_root': '/owned/run', 'request_context': {'simulator': 'alps'},
                   'netlists': [], 'artifacts': [{'path': '/owned/run/ocean.stderr'}],
                   'records': [{'observation': 'diagnostic.producer_failure',
                       'payload': {'path': 'preparation_failure.json', 'data': {'detail': primary, 'script_checks': ['exact marker']}}}]}
        checks = [{'id': 'alps.private_netlist.execution', 'check_id': 'execution', 'status': 'FAIL',
                   'simulator': 'alps', 'engine': 'private_netlist', 'details': {'first_cause': primary}}]
        text = format_section(checks,context)
        self.assertIn('SECTION BEGIN alps.private_netlist/netlist',text)
        self.assertIn('exact marker',text)
        self.assertIn('"simulator": "alps"',text)
        self.assertIn('/owned/run/ocean.stderr',text)
        self.assertIn('SECTION END alps.private_netlist/netlist',text)

    def test_cached_identity_and_current_state_are_safe_self_contained_context(self):
        row = {'test_id':'current','package_identity':{'application_version':'1.2.0','source_tree_sha256':'code-hash',
            'delivery_manifest':{'metadata':{'version':'1.2.0','source_commit':'commit-hash','tag':'v1.2.0'}},
            'license_file':'never-copy'},
            'prepared':{'setup':{'source':{'setup_db':'/work/TB/maestro/maestro.sdb','active_state':'/work/TB/maestro/active.state',
                'launch_environment':{'LICENSE':'never-copy'}},'setup_locator':{'library_name':'L','cell_name':'TB','view_name':'maestro'}}},
            'config':{'cadence':{'license_file':'never-copy','launch_environment':{'TOKEN':'never-copy'}}},
            'simulator_logs':{'spectre':'/report/Spectre/environment_test.log','alps':'/report/ALPS/environment_test.log'}}
        with mock.patch('crescendo_engine.environment_test.package_identity',side_effect=AssertionError('must not reprobe')):
            contexts = {scope:_report_context(row,scope) for scope in ('environment','spectre','alps')}
        self.assertEqual(contexts['environment']['log_path'],'/report/Spectre/environment_test.log')
        self.assertEqual(contexts['alps']['log_path'],'/report/ALPS/environment_test.log')
        context = contexts['alps']
        context['first_cause'] = {'failure_stage':'measurement_preflight','message':'missing_output_expression:main::gain'}
        text = format_section([{'id':'execution','check_id':'execution','status':'FAIL'}],context)
        self.assertIn('code-hash',text)
        self.assertIn('commit-hash',text)
        self.assertIn('/work/TB/maestro/active.state',text)
        self.assertIn('"library": "L"',text)
        self.assertNotIn('never-copy',text)
        self.assertNotIn('launch_environment',text)
        self.assertIn('用户的表达式为空',text)

    def test_known_capture_stages_explain_before_solver_without_changing_raw_error(self):
        for stage in ('native_corner_coverage','native_capture_point_coverage'):
            check = {'id':'execution','check_id':'execution','status':'FAIL','message':'raw exact native error',
                     'details':{'first_cause':{'failure_stage':stage,'message':'raw exact native error'}}}
            p = describe_check(check)
            self.assertIn('网表',p['summary'])
            self.assertIn('raw exact native error',p['explanation'])
            self.assertEqual(check['message'],'raw exact native error')

    def test_corner_coverage_routes_to_netlist_section_and_real_capture_function(self):
        for stage in ('native_corner_coverage','native_capture_point_coverage'):
            check = {'id':'spectre.private_netlist.execution','check_id':'execution','status':'FAIL',
                'details':{'first_cause':{'failure_stage':stage,'message':'original coverage error','diagnostic_path':'/owned/corner_identity.json'}}}
            p = describe_check(check)
            self.assertEqual(p['section_id'],'netlist')
            self.assertEqual(p['component']['file'],'crescendo_engine/private_netlist.py')
            self.assertEqual(p['component']['function'],'_capture_phase')
            self.assertEqual(check['details']['first_cause']['diagnostic_path'],'/owned/corner_identity.json')

    def test_other_measurement_preflight_failures_keep_their_actual_reason(self):
        for reason in ('cross_test_output:C::gain','outputscript:C::gain'):
            check = {'id':'execution','check_id':'execution','status':'FAIL',
                'details':{'first_cause':{'failure_stage':'measurement_preflight','message':reason}}}
            p = describe_check(check)
            self.assertEqual(p['summary'],reason)
            self.assertIn(reason,p['explanation'])
            self.assertNotIn('用户的表达式为空',p['explanation'])
            self.assertNotEqual(p['component']['function'],'qualify_expressions')

    def test_missing_expression_uses_real_measurement_qualifier_and_preserves_original(self):
        for message_key in ('message','error'):
            primary = {'failure_stage':'measurement_preflight',message_key:'missing_output_expression:C::gain',
                       'diagnostic_path':'/owned/execution_diagnostics.json'}
            p = describe_check({'id':'execution','check_id':'execution','status':'FAIL','details':{'first_cause':primary}})
            self.assertIn('用户的表达式为空',p['explanation'])
            self.assertEqual(p['component']['file'],'crescendo_engine/netlist_measurement.py')
            self.assertEqual(p['component']['function'],'qualify_expressions')
            self.assertEqual(primary[message_key],'missing_output_expression:C::gain')
            package = os.path.dirname(__import__('crescendo_engine').__file__)
            with open(os.path.join(package,'netlist_measurement.py'),encoding='utf-8') as stream:
                names = [node.name for node in ast.parse(stream.read()).body if isinstance(node,ast.FunctionDef)]
            self.assertIn(p['component']['function'],names)

    def test_request_gate_explanation_uses_actual_ui_error_and_recovery_without_native_claim(self):
        cause = {"failure_stage":"diagnostic_request","message":"准备检查未通过",
                 "blocking_reason_labels":["当前所选模块尚未通过准备检查"],
                 "ui_prepared_state":{"error":"raw selection mismatch"},
                 "recovery_actions":["完成当前所选模块的准备后再诊断。"]}
        check = {"id":"alps.maestro_netlist.execution","check_id":"execution","status":"BLOCKED",
                 "details":{"first_cause":cause}}
        p = describe_check(check)
        self.assertEqual(p["title"],"开始仿真前的准备检查")
        self.assertEqual(p["component"]["function"],"EnvironmentTestManager._run")
        self.assertIn("raw selection mismatch",p["explanation"])
        self.assertIn("完成当前所选模块",p["explanation"])
        self.assertEqual(p["section_id"],"request")

    def test_success_is_count_hash_and_scalar_summary_not_raw_nested_evidence(self):
        payload_marker = 'verbose-proof-' * 1000
        text = format_section([{'id': 'parameter_application', 'check_id': 'parameter_application', 'status': 'PASS',
                'details': {'ok': True, 'parameters': [{'trace': payload_marker}]}}])
        self.assertIn('parameters_sha256',text)
        self.assertIn('parameters_count',text)
        self.assertNotIn(payload_marker,text)

    def test_artifact_index_distinguishes_no_file_from_generated_file(self):
        with tempfile.TemporaryDirectory() as root:
            before = retained_artifact_index(root)
            self.assertEqual(before['netlists'],[])
            with open(os.path.join(root,'input.scs'),'w') as stream:
                stream.write('include "toplevel.scs" section=TOP_TT_RFTYP\n')
            after = retained_artifact_index(root)
            self.assertEqual(after['netlists'][0]['path'],os.path.join(root,'input.scs'))
            self.assertEqual(after['netlists'][0]['status'],'generated')
            self.assertTrue(after['netlists'][0]['exists'])

    def test_artifact_index_does_not_follow_external_link(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as external:
            with open(os.path.join(external,'input.scs'),'w') as stream:
                stream.write('external')
            os.symlink(external,os.path.join(root,'linked'))
            self.assertEqual(retained_artifact_index(root)['netlists'],[])

    def test_only_valid_bounded_diagnostic_marker_enables_retention(self):
        self.assertTrue(diagnostic_run(marker()))
        self.assertFalse(diagnostic_run({}))
        invalid = marker(); invalid['environment_test']['unexpected'] = True
        self.assertFalse(diagnostic_run(invalid))
        invalid = marker(); invalid['population'] = 2
        self.assertFalse(diagnostic_run(invalid))

    def test_read_latest_complete_exact_section_without_other_case(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root,'environment_test.log')
            old = '=== SECTION BEGIN alps.private_netlist/netlist ===\nold\n=== SECTION END alps.private_netlist/netlist ===\n'
            new = old.replace('old','new')
            other = old.replace('alps.private_netlist','spectre.maestro_netlist').replace('old','other')
            with open(path,'w') as stream:
                stream.write(old+other+new+'=== SECTION BEGIN alps.private_netlist/netlist ===\nincomplete\n')
            self.assertEqual(read_section(path,'alps.private_netlist','netlist'),new)
            self.assertNotIn('other',read_section(path,'alps.private_netlist','netlist'))
            self.assertIsNone(read_section(path,'alps.maestro_netlist','netlist'))

    def test_read_section_rejects_links_and_oversize_not_truncated_success(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root,'report.log')
            with open(path,'w') as stream:
                stream.write('=== SECTION BEGIN environment/report ===\nproof\n=== SECTION END environment/report ===\n')
            self.assertIsNone(read_section(path,'environment','report',max_bytes=20))
            os.symlink(path,path+'.link')
            self.assertIsNone(read_section(path+'.link','environment','report'))
            os.link(path,path+'.hard')
            self.assertIsNone(read_section(path,'environment','report'))

    def test_read_section_returns_none_for_incomplete_section(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root,'report.log')
            with open(path,'w') as stream:
                stream.write('=== SECTION BEGIN environment/report ===\nnot yet complete\n')
            self.assertIsNone(read_section(path,'environment','report'))

    def _cleanup(self, root, spec, candidate_id='g000_c0000'):
        candidate = os.path.join(root,'candidates',candidate_id)
        task_root = os.path.join(candidate,'tasks','main_1')
        for name in ('psf','netlist','measurement'):
            os.makedirs(os.path.join(task_root,name))
        source = os.path.join(task_root,'netlist','input.scs')
        with open(source,'w') as stream:
            stream.write('include "local.scs"\n')
        with open(os.path.join(task_root,'netlist','local.scs'),'w') as stream:
            stream.write('parameters p=2\n')
        context = NetlistExecutionContext.__new__(NetlistExecutionContext)
        context.spec,context.owned_run_root,context.progress_callback = spec,root,None
        task = {'task_id':'main_1','psf_dir':os.path.join(task_root,'psf'), 'spectre_log':os.path.join(task_root,'psf','spectre.out'),
                'test':'main','corner':'tt','point_id':1}
        result = context._cleanup_measured_point_unlocked({'root':candidate,'candidate_id':candidate_id},task,{'revision':1})
        return source,result

    def test_diagnostic_netlist_and_local_include_survive_point_payload_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            source,result = self._cleanup(root,marker())
            self.assertTrue(result['artifact_cleanup_complete'])
            self.assertTrue(os.path.isfile(source))
            snapshot = os.path.join(root,'private_netlist','diagnostic_netlists','g000_c0000','main_1')
            self.assertTrue(os.path.isfile(os.path.join(snapshot,'input.scs')))
            self.assertTrue(os.path.isfile(os.path.join(snapshot,'local.scs')))
            self.assertEqual(result['retention_policy'],'diagnostic_netlist_retained')
            self.assertNotIn('netlist',result['removed'])
            self.assertIn('psf',result['removed'])

    def test_snapshot_rejects_each_linked_parent_and_leaf_without_external_writes(self):
        prefixes = [('private_netlist',),('private_netlist','diagnostic_netlists'),
                    ('private_netlist','diagnostic_netlists','g000_c0000'),
                    ('private_netlist','diagnostic_netlists','g000_c0000','main_1')]
        for components in prefixes:
            with self.subTest(components=components), tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as external:
                parent = os.path.join(root,*components[:-1])
                os.makedirs(parent,exist_ok=True)
                os.symlink(external,os.path.join(root,*components))
                source,result = self._cleanup(root,marker())
                self.assertTrue(os.path.isfile(source))
                self.assertFalse(result['artifact_cleanup_complete'])
                self.assertTrue(result['errors'])
                self.assertEqual(os.listdir(external),[])

    def test_snapshot_rejects_noncanonical_candidate_component_and_keeps_source(self):
        with tempfile.TemporaryDirectory() as root:
            source,result = self._cleanup(root,marker(),candidate_id='g000/escape')
            self.assertTrue(os.path.isfile(source))
            self.assertFalse(result['artifact_cleanup_complete'])
            self.assertIn('identity is unsafe',result['errors'][0]['error'])
            self.assertFalse(os.path.exists(os.path.join(root,'private_netlist')))

    def test_standalone_failure_section_discloses_cache_index_and_collection_omissions(self):
        manager = EnvironmentTestManager(None)
        row = {'config':{},'prepared':{},'engine_outcomes':{},'selected_simulator':'spectre',
               'simulator':'spectre','case_id':'spectre.maestro_netlist','cancel':None,
               'case_contexts':{'spectre.maestro_netlist':{'report_omissions':'ordinary observations limited'}}}
        collection = {'observation':'diagnostic.collection','payload':{'status':'PARTIAL',
                      'omitted_or_truncated':[{'path':'capture.il','reason':'read limit'}]}}
        with mock.patch('crescendo_engine.environment_test.retained_artifact_index',create=True):
            # The index helper is imported inside _collect_inline.
            with mock.patch('crescendo_engine.environment_diagnostics.retained_artifact_index',return_value={'index_complete':False,'retention_status':'index_partial_original_files_retained'}), \
                 mock.patch('crescendo_engine.environment_test.collect_run_diagnostics',return_value=[collection]), \
                 mock.patch.object(manager,'_log'),mock.patch.object(manager,'_check'):
                manager._collect_inline(row,'maestro_netlist','/owned/run',{},True)
        context = row['case_contexts']['spectre.maestro_netlist']
        body = format_section([{'id':'execution','check_id':'execution','status':'FAIL'}],context)
        self.assertIn('ordinary observations limited',body)
        self.assertIn('"index_complete": false',body)
        self.assertIn('"collection_status": "PARTIAL"',body)
        self.assertIn('read limit',body)

    def test_normal_optimization_still_removes_point_netlist_payload(self):
        with tempfile.TemporaryDirectory() as root:
            source,result = self._cleanup(root,{})
            self.assertFalse(os.path.exists(source))
            self.assertIn('netlist',result['removed'])
            self.assertEqual(result['retained'],[])

    def test_retaining_capture_never_bypasses_native_termination_and_view_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            prepared,config,spec = fixtures(root,population=1)
            spec.update(marker())
            adapter = PrivateNetlistOptimizer(root,spec,prepared,prepared['parameters'],config,runner._failure_metrics)
            manifest = {'root':os.path.join(root,'templates'),'source_guard':{'ok':True}}
            with mock.patch('crescendo_engine.netlist_optimizer.reconcile_run_preparations',return_value={'cleanup_proven':True}), \
                 mock.patch('crescendo_engine.netlist_optimizer.reconcile_preparation',return_value={'cleanup_proven':False}) as cleanup, \
                 mock.patch('crescendo_engine.netlist_optimizer.NetlistExecutionContext'), \
                 mock.patch('crescendo_engine.netlist_optimizer.prepare_private_netlist',return_value=manifest), \
                 mock.patch('crescendo_engine.netlist_optimizer.compact_preparation_artifacts') as compact, \
                 mock.patch.object(adapter,'_progress'):
                with self.assertRaisesRegex(RuntimeError,'termination/view cleanup is unproven'):
                    adapter._prepare_once()
            cleanup.assert_called_once_with(manifest['root'],request_cancel=False)
            compact.assert_not_called()
            self.assertIsNotNone(adapter.prepare_error)

    def test_diagnostic_preparation_keeps_capture_but_normal_path_still_compacts(self):
        for diagnostic in (False,True):
            with tempfile.TemporaryDirectory() as root:
                prepared,config,spec = fixtures(root,population=1)
                if diagnostic:
                    spec.update(marker())
                adapter = PrivateNetlistOptimizer(root,spec,prepared,prepared['parameters'],config,runner._failure_metrics)
                manifest = {'root':os.path.join(root,'templates'),'source_guard':{'ok':True}}
                with mock.patch('crescendo_engine.netlist_optimizer.reconcile_run_preparations',return_value={'cleanup_proven':True}) as reconcile, \
                     mock.patch('crescendo_engine.netlist_optimizer.reconcile_preparation',return_value={'cleanup_proven':True}) as preparation_cleanup, \
                     mock.patch('crescendo_engine.netlist_optimizer.NetlistExecutionContext'), \
                     mock.patch('crescendo_engine.netlist_optimizer.prepare_private_netlist',return_value=manifest), \
                     mock.patch('crescendo_engine.netlist_optimizer.compact_preparation_artifacts',return_value={'ok':True}) as compact, \
                     mock.patch.object(adapter,'_progress') as progress:
                    adapter._prepare_once()
                self.assertTrue(reconcile.called)
                self.assertEqual(compact.call_count,0 if diagnostic else 1)
                self.assertEqual(preparation_cleanup.call_count,1 if diagnostic else 0)
                if diagnostic:
                    event = next(call[0][0] for call in progress.call_args_list if call[0][0].get('stage') == 'native_preparation_artifacts')
                    self.assertEqual(event['artifact_cleanup']['retention_policy'],'diagnostic_capture_retained')
