# -*- coding: utf-8 -*-
"""Small 1020 failure fixtures; no native tools or environment probes."""
import os
import tempfile
import unittest

from crescendo_engine.native_diagnostics import generated_guard_context, skill_command_diagnostics
from crescendo_engine.environment_diagnostics import collect_run_diagnostics
from crescendo_engine.diagnosis_catalog import describe_check, brief_success


class Native1020EvidenceTests(unittest.TestCase):
    def test_cadence_policy_error_precedes_guard_and_has_real_console_line(self):
        with tempfile.TemporaryDirectory() as root:
            script = os.path.join(root, 'capture.il')
            with open(script, 'w') as stream:
                stream.write('session=maeOpenSetup("lib" "tb" "maestro")\n'
                    'unless(maeSetTestEnvVar("test" "controlMode" "batch") '
                    'error("Crescendo native test batch mode rejected\\n"))\nexit(0)\n')
            with open(os.path.join(root, 'ocean.stdout'), 'w') as stream:
                stream.write('Loading simui.cxt\n'
                    'ERROR (ASSEMBLER-1944): Could not set job policy ADEXL_Scheduler_short.\n'
                    'ERROR (ASSEMBLER-1944): Could not set job policy ADEXL_Scheduler_SClass_normal.\n'
                    '*Error* Crescendo native test batch mode rejected\n')
            result = skill_command_diagnostics({'returncode': 1}, script,
                'CRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE', root)
            self.assertEqual(result['first_error']['native_code'], 'ASSEMBLER-1944')
            self.assertEqual(result['first_error']['console_line'], 2)
            self.assertEqual(result['terminal_error']['console_line'], 4)
            self.assertEqual(len(result['errors']), 3)
            association = result['terminal_error']['generated_guard_context']
            self.assertFalse(association['native_stack_location'])
            self.assertEqual(association['matching_lines'], [2])
            self.assertIn('maeSetTestEnvVar', association['source_context'][1]['text'])
            self.assertNotIn('reported_line', result['terminal_error'])
            self.assertFalse(result['ok'])
            records = collect_run_diagnostics(root, 'maestro_netlist', stop_proven=True)
            context = next(row['payload']['context'] for row in records
                if row['observation'] == 'diagnostic.script_context')
            self.assertEqual(context['generated_guard_associations'][0]['matching_lines'], [2])

    def test_guard_is_not_guessed_when_literal_repeated(self):
        context = generated_guard_context('error("Crescendo failed\\n")\nerror("Crescendo failed\\n")', '*Error* Crescendo failed')
        self.assertEqual(context['match_count'], 2)
        self.assertEqual(context['context_status'], 'guard_not_unique_or_absent')
        self.assertNotIn('source_context', context)
        self.assertIsNone(generated_guard_context('error("vendor failure")', '*Error* vendor failure'))

    def test_existing_5031_exclusion_does_not_hide_3036_without_invocation_proof(self):
        with tempfile.TemporaryDirectory() as root:
            result = skill_command_diagnostics({'returncode': 0, 'stdout_tail':
                'ERROR (ASSEMBLER-3036): Failed after wrapper exit\n'
                'ERROR (ASSEMBLER-5031): Simulator failed\nCRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE'},
                os.path.join(root, 'capture.il'), 'CRESCENDO_PRIVATE_NETLIST_CAPTURE_DONE', root)
            self.assertFalse(result['ok'])
            self.assertEqual(result['first_error']['native_code'], 'ASSEMBLER-3036')
            self.assertEqual(result['capture_wrapper_failure_observations'], 1)

    def test_no_worker_logs_and_no_scs_are_labelled_without_success_claim(self):
        worker = {'schema_version': 'crescendo.native_worker_diagnostics.v1', 'ok': True, 'logs': []}
        context = {'case_id': 'spectre.maestro_netlist', 'native_worker_diagnostics': [worker],
            'retention_status': 'no_generated_scs_observed_check_capture_evidence', 'netlists': []}
        result = describe_check({'id': 'files', 'status': 'PASS', 'message': 'Retained evidence'}, context)
        self.assertIn('尚未生成 SCS', result['title'])
        result = describe_check({'id': 'execution', 'status': 'FAIL', 'message': 'Native capture failed'}, context)
        self.assertIn('尚无原生 worker 日志', result['explanation'])
        self.assertIn('not proven', brief_success(worker)['worker_evidence_status'])


if __name__ == '__main__':
    unittest.main()
