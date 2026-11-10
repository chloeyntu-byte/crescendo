import copy
import unittest
from unittest import mock
from crescendo_engine import runner
from crescendo_engine.netlist_execution import execution_policy, NetlistExecutionError
from tests import test_runner as fixture


class ExecutionSelectionRuntimeTests(unittest.TestCase):
    def test_checkpoint_serializes_and_binds_selection(self):
        selection={'tests':['A'],'analyses':{'A':['ac']}}
        with mock.patch.object(runner,'_generator_state',return_value={}):
            document=runner._checkpoint_document({'variables':[],'execution_selection':selection},{},None,[],[],0,None,{})
        self.assertEqual(document['manifest']['execution_selection'],selection)
        self.assertEqual(document['payload_sha256'],runner._checkpoint_checksum(document))
        checkpoint=fixture._minimal_checkpoint()
        checkpoint['spec']['execution_selection']=selection
        checkpoint['manifest']={'execution_selection':copy.deepcopy(selection)}
        runner.validate_checkpoint_integrity(checkpoint)
        checkpoint['manifest']['execution_selection']['analyses']['A']=['dc']
        with self.assertRaisesRegex(ValueError,'execution_selection identities disagree'):
            runner.validate_checkpoint_integrity(checkpoint)

    def test_unselected_test_policy_does_not_block_selected_test(self):
        prepared={'setup':{'tests':[{'name':'A','enabled':True},{'name':'B','enabled':False}]},
            'job_policy':{'global':{'properties':{'maxjobs':2}},'tests':[
                {'name':'A','properties':{'distributionmethod':'local'}},
                {'name':'B','properties':{'distributionmethod':'unsupported_site_backend'}}]}}
        policy=execution_policy(prepared,{'execution_selection':{'tests':['A']}},{'runtime':{}})
        self.assertEqual(set(policy['tests']),{'A'})
        with self.assertRaises(NetlistExecutionError):
            execution_policy(prepared,{'execution_selection':{'tests':['B']}},{'runtime':{}})
        self.assertFalse(prepared['setup']['tests'][1]['enabled'])

    def test_unknown_selection_rejected_before_execution_capacity_created(self):
        with self.assertRaisesRegex(ValueError,'Unknown Tests'):
            execution_policy({'setup':{'tests':[{'name':'A'}]}},
                             {'execution_selection':{'tests':['not_here']}},{'runtime':{}})

    def test_frozen_manifest_cannot_be_reused_with_different_test_or_analysis(self):
        from crescendo_engine.netlist_execution import validate_execution_selection
        from crescendo_engine.test_selection import selection_plan
        setup={'tests':[{'name':'A','analyses':[{'name':'ac','enabled':True},{'name':'dc','enabled':False}]},
                        {'name':'B','enabled':False,'analyses':[{'name':'dc'}]}]}
        ac=selection_plan(setup,{'execution_selection':{'tests':['A'],'analyses':{'A':['ac']}}})
        dc=selection_plan(setup,{'execution_selection':{'tests':['A'],'analyses':{'A':['dc']}}})
        manifest={'execution_selection':ac,'tasks':[{'test':'A'}]}
        validate_execution_selection(manifest,ac)
        with self.assertRaisesRegex(NetlistExecutionError,'analyses differ'):validate_execution_selection(manifest,dc)
        b=selection_plan(setup,{'execution_selection':{'tests':['B']}})
        with self.assertRaisesRegex(NetlistExecutionError,'Tests differ'):validate_execution_selection(manifest,b)
        with self.assertRaisesRegex(NetlistExecutionError,'unavailable'):validate_execution_selection({},ac)
        manifest['tasks'].append({'test':'B'})
        with self.assertRaisesRegex(NetlistExecutionError,'unselected'):validate_execution_selection(manifest,ac)
        validate_execution_selection({},None)
