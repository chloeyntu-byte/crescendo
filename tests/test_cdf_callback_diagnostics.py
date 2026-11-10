from __future__ import print_function

import ast
import copy
import os
import re
import tempfile
import unittest

from crescendo_engine.cdf_callback_diagnostics import (
    MAX_EXPRESSION_CHARS, MAX_EXPRESSION_FIELDS, MAX_SCAN_FIELDS,
    callback_failure_lines,
)
from crescendo_engine.persistent_maestro import _candidate_plans, _patch_lines
from crescendo_engine.workspace import (
    ParameterApplicationFailure, _skill_quote, bounded_skill_body_lines,
    validate_schematic_application, write_schematic_patch,
)
from crescendo_engine.range_validation import _endpoint_rows


class CdfCallbackDiagnosticsTest(unittest.TestCase):
    def parameter(self, name, cdf_type='string'):
        return {'parameter_id': name + '_id', 'owner_library': 'D',
                'owner_cell': 'TB', 'design_library': 'D', 'design_cell': 'TB',
                'design_view': 'schematic', 'hierarchy_path': '/M0',
                'owner_occurrence_path': '/', 'name': name, 'cdf_type': cdf_type,
                'current_value': '6'}

    def render(self, root, parameters=None, values=None, custom=False):
        parameters = parameters or [self.parameter('nfin')]
        values = values or {'nfin_id': 5}
        path = os.path.join(root, 'patch.il')
        adapter = None
        if custom:
            adapter = os.path.join(root, 'custom.il')
            with open(adapter, 'w') as stream:
                stream.write('; Never executed in this offline test.\n')
        plan = write_schematic_patch(
            {'copied_libraries': {'D': root}}, parameters, values, path,
            pdk_adapter=adapter or 'generic-cdf', emit_clones=False)
        self.assertEqual(plan['callback_value_proof_required'], not custom)
        with open(path) as stream:
            regular = stream.read()
        persistent = '\n'.join(_patch_lines(
            copy.deepcopy(plan['parameters']), os.path.join(root, 'resident.records'), adapter))
        return regular, persistent

    def test_cdf_default_selected_values_exist_before_first_callback(self):
        parameters = [self.parameter('nfin'), self.parameter('w')]
        parameters[1]['current_value'] = '1u'
        with tempfile.TemporaryDirectory() as root:
            for script in self.render(root, parameters, {'nfin_id': 5, 'w_id': 2e-6}):
                before_callbacks = script.split('cdfResult = errset(', 1)[0]
                # Replay just the generated OA property assignments. Starting
                # without nfin reproduces a CDF-default-only selected field.
                properties = {'w': ('string', '1u'), 'nf': ('string', 'FN_gm')}
                assignments = re.findall(
                    r'(?m)^(when\(existingProp )?dbReplaceProp\(inst "([^"]+)" "([^"]+)" ("[^"]*"|[-+.0-9eE]+)\)\)?$',
                    before_callbacks)
                for conditional, name, kind, value in assignments:
                    if not conditional or name in properties:
                        properties[name] = (kind, ast.literal_eval(value))
                self.assertEqual(properties['nfin'], ('string', '5'))
                self.assertEqual(properties['w'], ('string', '2u'))
                self.assertEqual(properties['nf'], ('string', 'FN_gm'))
                self.assertEqual(len(assignments), 2)

    def test_integer_selected_type_and_custom_adapter_ownership(self):
        with tempfile.TemporaryDirectory() as root:
            for script in self.render(root, [self.parameter('nfin', 'int')]):
                self.assertIn('\ndbReplaceProp(inst "nfin" "int" 5)\n', script)
                self.assertNotIn('dbReplaceProp(inst "nf"', script)
            for script in self.render(root, custom=True):
                self.assertIn('when(existingProp dbReplaceProp(inst "nfin" "string" "5"))', script)
                self.assertIn('crescPdkApplyParameter(inst "nfin" "5")', script)
                self.assertNotIn('CRESCENDO_CDF_CALLBACK_INPUT', script)
                self.assertNotIn('crescDiagRemaining', script)
                self.assertNotIn('evalstring(callbackExpr)', script)
                self.assertNotIn('CDF_CALLBACK_EFFECTIVE', script)
                self.assertNotIn('crescCallbackEffectiveValue', script)
                self.assertNotIn('crescCallbackDefaultValue', script)

    def test_both_generators_share_failure_only_evidence_and_symbolic_boundary(self):
        with tempfile.TemporaryDirectory() as root:
            blocks = []
            for script in self.render(root):
                bounded_skill_body_lines(script.splitlines())
                start = script.index('when(callbackExpr && callbackExpr != ""')
                end = script.index("when(isCallable('cdfUpdateInstParam)", start)
                block = script[start:end]
                blocks.append(block)
                callback = block.index('callbackResult = errset(evalstring(callbackExpr) t)')
                failure = block.index('unless(callbackResult', callback)
                scan = block.index('crescDiagRemaining = cdfgData~>parameters')
                marker = block.index('CRESCENDO_CDF_CALLBACK_INPUT')
                original_error = block.index('CRESCENDO_ADAPTER_ERROR:cdf_callback_incompatible')
                self.assertLess(callback, failure)
                self.assertLess(failure, scan)
                self.assertLess(scan, marker)
                self.assertLess(marker, original_error)
                self.assertEqual(block.count('evalstring('), 1)
                self.assertIn('crescCallbackExtra = errset(', block)
                self.assertIn('"capture_stage" "callback_failure"', block)
                self.assertIn('!member(crescDiagName crescDiagSelected)', block)
                self.assertNotIn('dbReplaceProp', block)
                self.assertNotIn('formInitProc(', block)
                symbolic = re.sub(r'(?m)^\s*when\(callbackExpr && callbackExpr != ""\s*$.*?^\s*\)\s*$',
                                  '', script, flags=re.S | re.M)
                self.assertNotIn('CRESCENDO_CDF_CALLBACK_INPUT', symbolic)
                self.assertNotIn('crescDiagRemaining', symbolic)
                self.assertNotIn('evalstring(callbackExpr)', symbolic)
                self.assertNotIn('CDF_CALLBACK_EFFECTIVE', symbolic)
                self.assertNotIn('crescCallbackEffectiveValue', symbolic)
                self.assertNotIn('crescCallbackDefaultValue', symbolic)
                self.assertNotIn('crescCallbackFreshCdf', symbolic)
                bounded_skill_body_lines(symbolic.splitlines())
            self.assertEqual(blocks[0], blocks[1])

    def test_callback_proof_reads_each_selected_row_before_next_callback(self):
        with tempfile.TemporaryDirectory() as root:
            parameters = [self.parameter('nfin'), self.parameter('w')]
            for script in self.render(root, parameters, {'nfin_id': 5, 'w_id': 2e-6}):
                self.assertEqual(script.count('CDF_CALLBACK_EFFECTIVE'), 2)
                for name in ('nfin', 'w'):
                    start = script.index('cdfParam = get(cdfgData stringToSymbol("%s"))' % name)
                    default = script.index("crescCallbackDefaultValue = get(cdfParam 'defValue)", start)
                    callback = script.index('callbackResult = errset(evalstring(callbackExpr) t)', default)
                    capture = script.index('crescCallbackEffectiveValue = cdfParam~>value', start)
                    update = script.index('cdfUpdateInstParam(inst)', capture)
                    refreshed = script.index('crescCallbackFreshCdf = cdfGetInstCDF(inst)', update)
                    oa = script.index('crescCallbackOaProp = dbFindProp(inst "%s")' % name, refreshed)
                    record = script.index('CDF_CALLBACK_EFFECTIVE', oa)
                    self.assertLess(default, callback)
                    self.assertLess(callback, capture)
                    self.assertLess(capture, update)
                    self.assertLess(update, refreshed)
                    self.assertLess(refreshed, oa)
                    self.assertLess(oa, record)
                    next_read = script.find('cdfResult = errset(', record)
                    if next_read != -1:
                        self.assertLess(record, next_read)
                bounded_skill_body_lines(script.splitlines())
                symbolic = re.sub(r'(?m)^\s*when\(callbackExpr && callbackExpr != ""\s*$.*?^\s*\)\s*$',
                                  '', script, flags=re.S | re.M)
                self.assertNotIn('CDF_CALLBACK_EFFECTIVE', symbolic)
                self.assertNotIn('crescCallbackEffectiveValue', symbolic)
                self.assertNotIn('crescCallbackDefaultValue', symbolic)
                bounded_skill_body_lines(symbolic.splitlines())

    def application_records(self, root, proof='', param=None, required=True):
        path = os.path.join(root, 'callback-proof.records')
        with open(path, 'w') as stream:
            stream.write(proof)
            stream.write(param or 'PARAM\tnfin_id\t"1"\t"2"\t"2"\t"executed"\t"callback()"\n')
        plan = {'verification_path': path, 'clones': [],
                'parameters': [{'parameter_id': 'nfin_id'}]}
        if required is not None:
            plan['callback_value_proof_required'] = required
        return plan

    def test_callback_proof_rejects_lost_clamp_or_missing_oa_even_for_range(self):
        with tempfile.TemporaryDirectory() as root:
            for immediate, refreshed, present, oa in (
                    ('"2"', '"1"', 't', '"1"'),
                    ('"2"', '"2"', 't', '"1"'),
                    ('"2"', '"2"', 'nil', 'nil')):
                proof = 'CDF_CALLBACK_EFFECTIVE\tnfin_id\t%s\t%s\t%s\t%s\tnil\tnil\n' % (
                    immediate, refreshed, present, oa)
                plan = self.application_records(root, proof)
                for allow in (False, True):
                    with self.assertRaises(ParameterApplicationFailure) as raised:
                        validate_schematic_application(plan, allow_effective_adjustment=allow)
                    self.assertIn('did not persist', str(raised.exception))
                    self.assertEqual(raised.exception.evidence['status'], 'mismatch')

    def test_callback_proof_accepts_persisted_clamp_for_range_but_strict_rejects(self):
        with tempfile.TemporaryDirectory() as root:
            plan = self.application_records(root, 'CDF_CALLBACK_EFFECTIVE\tnfin_id\t2\t"2"\tt\t2\tnil\tnil\n')
            result = validate_schematic_application(plan, allow_effective_adjustment=True)
            proof = result['parameters'][0]['callback_value_proof']
            self.assertEqual(proof['status'], 'verified')
            self.assertEqual(proof['scope'], 'selected_parameter_after_callback')
            rows = _endpoint_rows([{'parameter_id': 'nfin_id', 'backend': 'schematic'}],
                                  'minimum', {'nfin_id': '1'}, {'schematic_verification': result})
            self.assertEqual(rows[0]['status'], 'pdk_adjusted')
            with self.assertRaises(ParameterApplicationFailure):
                validate_schematic_application(plan)

    def test_callback_proof_accepts_missing_oa_only_at_numeric_native_default(self):
        with tempfile.TemporaryDirectory() as root:
            proof = 'CDF_CALLBACK_EFFECTIVE\tnfin_id\t2\t"2"\tnil\tnil\tt\t"2"\n'
            for requested in ('1', '2'):
                param = 'PARAM\tnfin_id\t"%s"\t"2"\t"2"\t"executed"\t"callback()"\n' % requested
                plan = self.application_records(root, proof, param)
                result = validate_schematic_application(plan, allow_effective_adjustment=True)
                verified = result['parameters'][0]['callback_value_proof']
                self.assertEqual(verified['storage_evidence'], 'native_cdf_default')
                self.assertEqual(verified['native_default_value'], '2')
                if requested == '1':
                    with self.assertRaises(ParameterApplicationFailure):
                        validate_schematic_application(plan)
                else:
                    self.assertTrue(validate_schematic_application(plan)['ok'])
            physical = 'CDF_CALLBACK_EFFECTIVE\tnfin_id\t"2u"\t2e-6\tnil\tnil\tt\t"2.0u"\n'
            param = 'PARAM\tnfin_id\t"2u"\t"2u"\t"2u"\t"executed"\t"callback()"\n'
            self.assertTrue(validate_schematic_application(self.application_records(root, physical, param))['ok'])

    def test_callback_proof_default_cannot_hide_unknown_or_explicit_oa_conflict(self):
        with tempfile.TemporaryDirectory() as root:
            for immediate, refreshed, present, oa, default_present, default in (
                    ('"2"', '"2"', 'nil', 'nil', 't', '"3"'),
                    ('"2"', '"2"', 'nil', 'nil', 'nil', 'nil'),
                    ('"FN_gm"', '"FN_gm"', 'nil', 'nil', 't', '"FN_gm"'),
                    ('"2"', '"2"', 'nil', 'nil', 't', 'nil'),
                    ('"2"', '"2"', 't', '"1"', 't', '"2"'),
                    ('"2"', '"1"', 'nil', 'nil', 't', '"2"')):
                proof = 'CDF_CALLBACK_EFFECTIVE\tnfin_id\t%s\t%s\t%s\t%s\t%s\t%s\n' % (
                    immediate, refreshed, present, oa, default_present, default)
                plan = self.application_records(root, proof)
                for allow in (False, True):
                    with self.assertRaises(ParameterApplicationFailure) as raised:
                        validate_schematic_application(plan, allow_effective_adjustment=allow)
                    self.assertEqual(raised.exception.evidence['status'], 'mismatch')

    def test_callback_proof_requires_one_complete_record_for_new_generic_plans(self):
        valid = 'CDF_CALLBACK_EFFECTIVE\tnfin_id\t"2"\t"2"\tt\t"2"\tnil\tnil\n'
        with tempfile.TemporaryDirectory() as root:
            for proof in ('', valid + valid, 'CDF_CALLBACK_EFFECTIVE\tnfin_id\t"2"\n'):
                plan = self.application_records(root, proof)
                for allow in (False, True):
                    with self.assertRaises(ParameterApplicationFailure) as raised:
                        validate_schematic_application(plan, allow_effective_adjustment=allow)
                    self.assertIn('proof is missing or not unique', str(raised.exception))

    def test_callback_proof_accepts_physical_equivalence_and_preserves_legacy_adapter(self):
        with tempfile.TemporaryDirectory() as root:
            param = 'PARAM\tnfin_id\t"3u"\t"3u"\t"3u"\t"executed"\t"callback()"\n'
            proof = 'CDF_CALLBACK_EFFECTIVE\tnfin_id\t"3u"\t3e-6\tt\t"3.0u"\tnil\tnil\n'
            plan = self.application_records(root, proof, param)
            self.assertEqual(validate_schematic_application(plan)['parameters'][0]['callback_value_proof']['status'], 'verified')
            for required in (None, False):
                old = self.application_records(root, '', param, required=required)
                self.assertNotIn('callback_value_proof', validate_schematic_application(old)['parameters'][0])
            for status in ('not_defined', 'not_invoked_for_symbolic_export'):
                inactive = self.application_records(root, '', param.replace('"executed"', '"%s"' % status))
                self.assertNotIn('callback_value_proof', validate_schematic_application(inactive)['parameters'][0])

    def test_persistent_generic_and_adapter_plans_set_explicit_proof_requirement(self):
        for required in (True, False):
            plans = _candidate_plans([], '/unused', required)
            self.assertIs(plans['parameters']['callback_value_proof_required'], required)
            self.assertNotIn('callback_value_proof_required', plans['baseline'])
            self.assertNotIn('callback_value_proof_required', plans['verify'])

    def test_evidence_bounds_preserve_legacy_keys_without_dumping_cdf_objects(self):
        row = {'property': 'nfin', 'parameter_id': 'nfin_id', 'prop_type': 'string'}
        block = '\n'.join(callback_failure_lines(
            row, '"5"', 'D', 'TB', 'private', 'M0', ['nfin'], _skill_quote))
        for key in ('schema_version', 'parameter_id', 'instance_lcv', 'hierarchy_path',
                    'property', 'property_type', 'requested_value', 'db_property_present',
                    'db_property_value', 'cdf_value', 'callback', 'expression_fields',
                    'expression_fields_truncated', 'expression_fields_scanned',
                    'remaining_cdf_fields_uninspected', 'form_init_proc', 'done_proc'):
            self.assertIn('"' + key + '"', block)
        for key in ('callback_truncated', 'selected_parameter_names_truncated',
                    'selected_cdf_metadata_truncated', 'name_truncated',
                    'metadata_truncated', 'value_truncated',
                    'form_init_proc_truncated', 'done_proc_truncated'):
            self.assertIn('"' + key + '"', block)
        self.assertIn('crescDiagScanned < %d' % MAX_SCAN_FIELDS, block)
        self.assertIn('crescDiagCount < %d' % MAX_EXPRESSION_FIELDS, block)
        self.assertIn('crescDiagChars + crescDiagCost <= %d' % MAX_EXPRESSION_CHARS, block)
        self.assertIn('substring(crescDiagScalar 1 192)', block)
        self.assertIn('list("value_truncated"', block)
        self.assertNotIn('%L\\n" cdfgData', block)
        self.assertNotIn('evalstring(crescDiag', block)
        self.assertNotIn('cdfGetInstCDF', block)

    def test_long_selected_names_and_observed_fin_clamp_keep_explicit_boundaries(self):
        row = {'property': 'nfin', 'parameter_id': 'nfin_id', 'prop_type': 'string'}
        block = '\n'.join(callback_failure_lines(
            row, '"5"', 'D', 'TB', 'private', 'M0', ['nfin', 'x' * 80], _skill_quote))
        self.assertIn('list("selected_parameter_names_truncated" t)', block)
        with tempfile.TemporaryDirectory() as root:
            records = os.path.join(root, 'records')
            with open(records, 'w') as stream:
                stream.write('PARAM\tnfin_id\t"1"\t"2"\t"2"\t"executed"\t"C710_coremos_CB"\n')
            plan = {'verification_path': records, 'clones': [],
                    'parameters': [{'parameter_id': 'nfin_id'}]}
            with self.assertRaises(ParameterApplicationFailure):
                validate_schematic_application(plan)
            result = validate_schematic_application(plan, allow_effective_adjustment=True)
            rows = _endpoint_rows([{'parameter_id': 'nfin_id', 'backend': 'schematic'}],
                                  'minimum', {'nfin_id': '1'}, {'schematic_verification': result})
            self.assertEqual(rows[0]['status'], 'pdk_adjusted')
            self.assertEqual(rows[0]['requested'], '1')
            self.assertEqual(rows[0]['effective'], '2')


if __name__ == '__main__':
    unittest.main()
