import copy
import hashlib
from unittest import mock
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest

from crescendo_engine.job_policy import (command_job_policy, effective_job_policy, job_policy_application_skill,
    job_policy_capture_definitions, job_policy_observation_skill, policy_from_records, native_job_policy_assertion_skill,
    prepare_job_command_observers, verify_effective_job_policy, capture_setup_job_policy,
    _qualify_saved_job_policy)
from crescendo_engine.maestro_context import parse_launch_records, SCHEMA

ROOT = os.path.dirname(os.path.dirname(__file__))


def policy():
    props = {"name": "Maestro Default", "distributionmethod": "Command",
        "jobsubmitcommand": 'batch-submit -A this-session -R "cpu=6;mem=20000"', "maxjobs": "50",
        "defaultmemoryvalue": "1000"}
    return {"available": True, "source": "invoking_session", "global": {"available": True, "properties": props},
        "tests": [{"name": "source-test", "available": True, "properties": copy.deepcopy(props)}],
        "run_options": {"available": True, "RunIn": "Parallel", "DivideJobs": "Equally", "JobLimit": [["ICRP", 0]]}}


class MaestroDispatchPolicyTests(unittest.TestCase):
    def test_known_saved_name_mismatch_cannot_start_or_escape_with_max_jobs_only(self):
        observed = policy()
        qualified = _qualify_saved_job_policy(observed, {'global':'Saved Current Cluster Policy'})
        for overrides in (None, {}, {'max_jobs':7}, {'command':''}, {'command':'  ','max_jobs':7}):
            with self.subTest(overrides=overrides), self.assertRaisesRegex(ValueError, "expected 'Saved Current Cluster Policy', actual 'Maestro Default'.*Local fallback is blocked"):
                effective_job_policy(qualified,overrides)
        with self.assertRaisesRegex(ValueError,'Saved JobPolicy name mismatch'):
            job_policy_application_skill(qualified,{'max_jobs':7})

    def test_known_mismatch_allows_explicit_command_and_generic_unknown_stays_optional(self):
        from crescendo_engine.job_policy import unavailable_job_policy
        observed = policy()
        qualified = _qualify_saved_job_policy(observed, {'global':'Saved Current Cluster Policy'})
        overrides = {'command':'current_submit -q requested','max_jobs':3}
        expected = effective_job_policy(qualified,overrides)
        self.assertEqual(expected['global']['properties']['jobsubmitcommand'],overrides['command'])
        self.assertEqual(expected['global']['properties']['distributionmethod'],'Command')
        self.assertEqual(expected['name_reference_validation'],qualified['name_reference_validation'])
        self.assertIn('current_submit -q requested',job_policy_application_skill(qualified,overrides))
        unknown = unavailable_job_policy('saved_setup_native','Optional native policy query unavailable')
        self.assertFalse(effective_job_policy(unknown)['available'])
        self.assertEqual(effective_job_policy(unknown,{'max_jobs':3})['global']['properties'],{'maxjobs':'3'})

    def test_uncaptured_source_with_explicit_command_observes_and_verifies_startup(self):
        import test_persistent_maestro as fixture
        from crescendo_engine.job_policy import unavailable_job_policy
        with tempfile.TemporaryDirectory() as root:
            worker, factory, unused_clock = fixture.PersistentMaestroTest()._worker(root)
            worker.config['runtime']['maestro_source_job_policy'] = unavailable_job_policy('saved_setup_native', 'Saved named policy unresolved')
            overrides = {'command':'current_machine_submit -R "cpu=2;mem=4000"','max_jobs':4}
            worker.config['runtime']['maestro_job_overrides'] = overrides
            original_on_load = factory.on_load
            def native_readback(process, path):
                original_on_load(process,path)
                if os.path.basename(path) == 'bootstrap.il':
                    wrappers = worker.job_observation.get('wrappers') or []
                    command = wrappers[0]['effective_command'] if wrappers else overrides['command']
                    rows = ['CRESCENDO_JOB_POLICY_1', 'P\t"global"\tnil\tt']
                    for key,value in {'name':'Existing private native policy','distributionmethod':'Command','jobsubmitcommand':command,'maxjobs':'4'}.items():
                        rows.append('J\t"global"\tnil\t%s\t%s' % (json.dumps(key),json.dumps(value)))
                    rows.extend(['P\t"test"\t"T"\tt','D\t"test"\t"T"'])
                    with open(worker.job_policy_readback_path,'w') as stream:
                        stream.write('\n'.join(rows)+'\n')
            factory.on_load = native_readback
            try:
                with mock.patch.object(worker,'_log_event') as events:
                    worker.start()
                wrappers = worker.job_observation['wrappers']
                self.assertEqual(len(wrappers),1)
                self.assertEqual(wrappers[0]['original_command'],overrides['command'])
                self.assertEqual(worker.config['runtime']['maestro_submission_log_path'],wrappers[0]['submission_log'])
                with open(os.path.join(worker.control_root,'effective_job_policy.json')) as stream:
                    proof = json.load(stream)
                self.assertTrue(proof['ok'])
                self.assertEqual(proof['source'],'saved_setup_native')
                self.assertEqual(proof['overrides'],overrides)
                self.assertEqual(proof['global']['properties']['jobsubmitcommand'],wrappers[0]['effective_command'])
                self.assertTrue(any(call[0][0]=='maestro_effective_job_policy' for call in events.call_args_list))
            finally:
                worker._close_stdin()
                worker._close_streams()

    def test_uncaptured_explicit_fields_still_reject_native_local_readback(self):
        from crescendo_engine.job_policy import unavailable_job_policy
        expected = effective_job_policy(unavailable_job_policy('saved_setup_native'),{'command':'current_submit -q fast','max_jobs':2})
        actual = {'global':{'available':True,'properties':{'distributionmethod':'Local','jobsubmitcommand':'','maxjobs':'2'}},'tests':[]}
        proof = verify_effective_job_policy(actual,expected)
        self.assertFalse(proof['ok'])
        self.assertEqual(proof['source'],'saved_setup_native')
        self.assertEqual(proof['overrides'],expected['overrides'])
        self.assertEqual(set(row['field'] for row in proof['differences']),{'distributionmethod','jobsubmitcommand'})

    def test_saved_xml_policy_references_use_only_current_active_setup(self):
        from crescendo_engine.ade_setup import inspect_setup
        with tempfile.TemporaryDirectory() as root:
            view = os.path.join(root, 'library', 'TB', 'maestro')
            os.makedirs(view)
            path = os.path.join(view, 'maestro.sdb')
            with open(path, 'w') as stream:
                stream.write('<setupdb type="maestro"><active><jobpolicyname>Current Native Policy</jobpolicyname><tests><test>test_a<jobpolicyname>Dedicated Native Policy</jobpolicyname></test><test>test_b<jobpolicyname></jobpolicyname></test></tests></active><history><jobpolicyname>Old History Policy</jobpolicyname></history></setupdb>')
            references = inspect_setup(path)['job_policy_references']
            self.assertEqual(references['global'], 'Current Native Policy')
            self.assertEqual(references['tests'], [{'test':'test_a','policy_name':'Dedicated Native Policy'}])

    def test_saved_named_policy_query_rejects_isolated_local_default(self):
        observed = policy()
        observed['global']['properties'].update(name='Maestro Default', distributionmethod='Local', jobsubmitcommand=None, maxjobs='1')
        qualified = _qualify_saved_job_policy(observed, {'global':'Saved Cluster Policy'})
        self.assertFalse(qualified['available'])
        self.assertEqual(qualified['global']['properties'], {})
        details = qualified['name_reference_validation']
        self.assertFalse(details['local_default_accepted'])
        self.assertEqual(details['differences'], [{'scope':'global','test':None,'expected':'Saved Cluster Policy','actual':'Maestro Default'}])
        self.assertEqual(details['observed_policy']['global']['properties']['distributionmethod'],'Local')
        overridden = effective_job_policy(qualified, {'command':'actual-current-submit -q short','max_jobs':4})
        self.assertEqual(overridden['global']['properties']['distributionmethod'],'Command')
        self.assertEqual(overridden['global']['properties']['jobsubmitcommand'],'actual-current-submit -q short')

    def test_saved_named_policy_and_explicit_default_are_accepted_only_when_matching(self):
        observed = policy()
        self.assertIs(_qualify_saved_job_policy(observed, {'global':'Maestro Default','tests':[{'test':'source-test','policy_name':'Maestro Default'}]}), observed)
        observed['global']['properties']['name'] = 'Saved Cluster Policy'
        self.assertIs(_qualify_saved_job_policy(observed, {'global':'Saved Cluster Policy'}), observed)
        missing = _qualify_saved_job_policy(observed, {'tests':[{'test':'missing-test','policy_name':'Dedicated Policy'}]})
        self.assertFalse(missing['available'])
        self.assertEqual(missing['name_reference_validation']['differences'][0]['actual'], None)

    def test_invoking_capture_does_not_replace_current_policy_with_saved_name(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'maestro.sdb')
            current = policy()
            setup = {'backend':'maestro','setup_locator':{'path':path},'job_policy_references':{'global':'Different Saved Policy'}}
            config = {'_maestro_launch_context':{'source':{'setup_path':path},'job_policy':current}}
            with mock.patch('crescendo_engine.cadence.run_command') as run:
                self.assertEqual(capture_setup_job_policy(setup,config), current)
            run.assert_not_called()

    def test_saved_capture_applies_name_qualification_to_actual_query_records(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, 'library')
            os.makedirs(library)
            path = os.path.join(library,'TB','maestro','maestro.sdb')
            setup = {'backend':'maestro','setup_locator':{'path':path,'library_root':library,'library_name':'Lib','cell_name':'TB','view_name':'maestro'},'source':{'library_root':library},'job_policy_references':{'global':'Saved Native Policy'}}
            config = {'cadence':{'ocean_bin':'/fixture/ocean'},'runtime':{'hierarchy_probe_root':root}}
            records = 'CRESCENDO_JOB_POLICY_1\nP\t"global"\tnil\tt\nJ\t"global"\tnil\t"name"\t"Maestro Default"\nJ\t"global"\tnil\t"distributionmethod"\t"Local"\nJ\t"global"\tnil\t"maxjobs"\t"1"\n'
            def query(*args, **kwargs):
                with open(os.path.join(kwargs['cwd'],'job_policy.records'),'w') as stream:
                    stream.write(records)
                return {'returncode':0}
            with mock.patch('crescendo_engine.cadence.run_command',side_effect=query) as run:
                captured = capture_setup_job_policy(setup,config)
            self.assertEqual(run.call_count,1)
            self.assertFalse(captured['available'])
            self.assertEqual(captured['name_reference_validation']['differences'][0]['expected'],'Saved Native Policy')

    def test_captured_invocation_applies_without_explicit_overrides(self):
        script = job_policy_application_skill(policy(), output_path="/owned/readback.records", tests=["source-test"])
        self.assertIn('crescCloneJobPolicy(session list(', script)
        self.assertIn('batch-submit -A this-session', script)
        self.assertIn('axlSetRunDistributeOptions', script)
        self.assertIn('stringToSymbol("Parallel")', script)
        self.assertIn('get(crescRunOptionsReadback \'RunIn)', script)
        self.assertIn('crescWriteJobPolicyRecords', script)
        self.assertNotIn('maeRunSimulation', script)

    def test_saved_setup_change_does_not_replay_stale_invoking_command(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'maestro.sdb')
            with open(path, 'w') as stream:
                stream.write('new current setup')
            context = {'source': {'setup_path':path, 'setup_content_sha256':hashlib.sha256(b'old setup').hexdigest()}, 'job_policy':policy()}
            # No native query binary is configured here: this must stay unknown
            # rather than reporting the old command as current session evidence.
            result = capture_setup_job_policy({'backend':'maestro','setup_locator':{'path':path}}, {'_maestro_launch_context':context,'cadence':{}})
            self.assertFalse(result['available'])
            self.assertNotEqual(result.get('source'),'invoking_session')
            self.assertNotIn('jobsubmitcommand',result['global']['properties'])

    def test_override_matches_current_invocation_not_old_command(self):
        expected = effective_job_policy(policy(), {"command": 'newsub -q new', "max_jobs": 3})
        self.assertEqual(expected['global']['properties']['jobsubmitcommand'], 'newsub -q new')
        self.assertEqual(expected['tests'][0]['properties']['maxjobs'], '3')
        self.assertEqual(policy()['global']['properties']['maxjobs'], '50')
        actual = copy.deepcopy(expected)
        actual['global']['properties']['distributionmethod'] = 'Local'
        proof = verify_effective_job_policy(actual, expected)
        self.assertFalse(proof['ok'])
        self.assertEqual(proof['differences'][0]['field'], 'distributionmethod')

    def test_large_policy_tables_use_complete_bounded_argument_blocks(self):
        from crescendo_engine.job_policy import job_policy_capture_skill, job_policy_readback_skill
        names = ["source_%d" % index for index in range(4096)]
        script = job_policy_capture_skill("Lib","TB","maestro","/owned/policy.records",names)
        self.assertIn("crescCapturePolicyTests = reverse(crescCapturePolicyTests)",script)
        self.assertIn('"source_4095"',script)
        self.assertNotIn("list(%s)" % " ".join(json.dumps(name) for name in names),script)
        readback = job_policy_readback_skill('/owned/policy.records',tests=names)
        self.assertIn("crescReadbackPolicyTests = reverse(crescReadbackPolicyTests)",readback)
        self.assertIn('"source_4095"',readback)

    def test_extra_native_policy_properties_are_copied_only_when_different(self):
        script = job_policy_application_skill(policy())
        self.assertIn("axlSetJobPolicyProperty(name symbolToString(key) oldValue)", script)
        self.assertIn("!equal(oldValue get(readback key))", script)
        self.assertIn("crescRequireJobPolicyMatch(original readback)", script)

    def test_partial_or_missing_command_is_not_silent_local(self):
        original = policy()
        del original['global']['properties']['jobsubmitcommand']
        with self.assertRaises(ValueError):
            effective_job_policy(original)
        actual = copy.deepcopy(policy())
        actual['global']['available'] = False
        self.assertFalse(verify_effective_job_policy(actual, policy())['ok'])

    def test_run_distribution_transports_complete_limits_and_public_api(self):
        value = policy_from_records([('P',['global',None,True]),
            ('U',['RunIn',True,'Parallel']), ('U',['DivideJobs',True,'Equally']),
            ('U',['JobLimit:ICRP',True,50]), ('U',['JobLimit:SIMON',True,0])])
        self.assertEqual(value['run_options']['JobLimit'], [['ICRP',50],['SIMON',0]])
        with self.assertRaises(ValueError):
            policy_from_records([('U',['RunIn',True,'Parallel']),('U',['RunIn',True,'Series'])])
        captured = job_policy_capture_definitions()
        self.assertIn('axlGetRunDistributeOptions', captured)
        self.assertNotIn('_axlGetParallelRun', captured)

    def test_batch_alias_effective_command_and_parallel_are_checked(self):
        source = policy()
        actual = copy.deepcopy(source)
        actual['tests'][0]['name'] = 'crescendo-candidate-test'
        wrapper = {'test': None, 'original_command': source['global']['properties']['jobsubmitcommand'],
                   'effective_command':'/owned/observer.sh'}
        actual['global']['properties']['jobsubmitcommand'] = wrapper['effective_command']
        actual['tests'][0]['properties']['jobsubmitcommand'] = wrapper['effective_command']
        proof = verify_effective_job_policy(actual, source, {'crescendo-candidate-test':'source-test'}, [wrapper], True)
        self.assertTrue(proof['ok'])
        actual['run_options']['RunIn'] = 'Series'
        self.assertFalse(verify_effective_job_policy(actual, source, {'crescendo-candidate-test':'source-test'}, [wrapper], True)['ok'])

    def test_effective_readback_rejects_missing_alias_and_distribution_limits(self):
        expected = policy()
        actual = copy.deepcopy(expected)
        actual['tests'] = []
        proof = verify_effective_job_policy(actual, expected, {'candidate0':'source-test'})
        self.assertFalse(proof['ok'])
        self.assertEqual(proof['differences'][0]['field'],'test_policy_missing')
        actual = copy.deepcopy(expected)
        actual['run_options']['JobLimit'] = [['ICRP',1]]
        self.assertFalse(verify_effective_job_policy(actual,expected)['ok'])

    def test_final_native_assertion_precedes_submission_and_covers_every_alias(self):
        expected = effective_job_policy(policy(), {"max_jobs": 3})
        script = native_job_policy_assertion_skill(expected, test_map={"candidate0":"source-test", "candidate1":"source-test"}, force_parallel=True)
        self.assertIn('?testName "candidate0"', script)
        self.assertIn('?testName "candidate1"', script)
        self.assertIn("crescRequireJobPolicyMatch", script)
        self.assertIn("final effective Run Options", script)
        self.assertNotIn("maeRunSimulation", script)
        with self.assertRaises(ValueError):
            native_job_policy_assertion_skill(expected,test_map={"candidate0":"missing-test"})

    def test_complex_command_keeps_native_semantics_instead_of_invented_execution(self):
        original = policy()
        for scope in [original['global']] + original['tests']:
            scope['properties']['jobsubmitcommand'] = 'my_submit "$QUEUE" | logger'
        with tempfile.TemporaryDirectory() as root:
            result = prepare_job_command_observers(original, {}, root, root+'/submit.jsonl')
        self.assertFalse(result['wrappers'])
        self.assertTrue(result['unavailable'][0]['native_command_preserved'])

    def test_observers_do_not_assume_unnamed_scopes_share_a_policy(self):
        for global_name, test_name in ((None, None), ('', ''), (None, 'known'), ('known', None)):
            with self.subTest(global_name=global_name, test_name=test_name), tempfile.TemporaryDirectory() as root:
                expected = policy()
                expected['global']['properties']['name'] = global_name
                expected['tests'][0]['properties']['name'] = test_name
                wrappers = prepare_job_command_observers(expected, {}, root, root+'/submit.jsonl')['wrappers']
                self.assertEqual([row['test'] for row in wrappers], [None, 'source-test'])
                self.assertEqual(wrappers[0]['effective_command'], wrappers[1]['effective_command'])
                skill = job_policy_observation_skill(wrappers)
                self.assertIn('?testName "source-test"', skill)
                actual = copy.deepcopy(expected)
                actual['global']['properties']['jobsubmitcommand'] = wrappers[0]['effective_command']
                self.assertFalse(verify_effective_job_policy(actual, expected, command_wrappers=wrappers)['ok'])
                actual['tests'][0]['properties']['jobsubmitcommand'] = wrappers[1]['effective_command']
                self.assertTrue(verify_effective_job_policy(actual, expected, command_wrappers=wrappers)['ok'])

    def test_observers_bind_each_scope_even_with_same_known_name(self):
        with tempfile.TemporaryDirectory() as root:
            expected = policy()
            wrappers = prepare_job_command_observers(expected, {}, root, root+'/submit.jsonl')['wrappers']
            self.assertEqual([row['test'] for row in wrappers], [None, 'source-test'])
            self.assertEqual(wrappers[0]['observer_script'], wrappers[1]['observer_script'])
            skill = job_policy_observation_skill(wrappers, test_map={'candidate0': 'source-test', 'candidate1': 'source-test'})
            self.assertIn('?testName "candidate0"', skill)
            self.assertIn('?testName "candidate1"', skill)
            # Native readback already carrying the observer makes the generated
            # conditional skip cloning, including genuinely inherited policies.
            self.assertIn('unless(equal(crescObservedPolicy->jobsubmitcommand ', skill)
            actual = copy.deepcopy(expected)
            actual['global']['properties']['jobsubmitcommand'] = wrappers[0]['effective_command']
            self.assertFalse(verify_effective_job_policy(actual, expected, command_wrappers=wrappers)['ok'])
            actual['tests'][0]['properties']['jobsubmitcommand'] = wrappers[1]['effective_command']
            self.assertTrue(verify_effective_job_policy(actual, expected, command_wrappers=wrappers)['ok'])

    def test_requested_command_without_captured_names_binds_test_scope(self):
        expected = command_job_policy({}, {'command': 'site_submit -q requested', 'max_jobs': 3}, ['source-test'])
        with tempfile.TemporaryDirectory() as root:
            wrappers = prepare_job_command_observers(expected, {}, root, root+'/submit.jsonl')['wrappers']
            self.assertEqual([row['test'] for row in wrappers], [None, 'source-test'])
            self.assertEqual(wrappers[0]['observer_script'], wrappers[1]['observer_script'])

    def test_actual_command_observer_preserves_argv_native_env_and_return(self):
        with tempfile.TemporaryDirectory() as root:
            submit = root+'/native_submit.py'
            output = root+'/actual.json'
            with open(submit,'w') as stream:
                stream.write('import json,os,sys\njson.dump({"argv":sys.argv[1:],"ld":os.environ.get("LD_LIBRARY_PATH"),"tk":os.environ.get("TK_LIBRARY")},open(sys.argv[1],"w"))\nprint("Job <7391> submitted")\nprint("scheduler detail",file=sys.stderr)\nsys.exit(7)\n')
            command = '%s %s %s -R "cpu=6;mem=20000"' % (sys.executable, submit, output)
            original = policy()
            for scope in [original['global']] + original['tests']:
                scope['properties']['jobsubmitcommand'] = command
            result = prepare_job_command_observers(original,{},root,root+'/submit.jsonl')
            wrapper = result['wrappers'][0]
            self.assertEqual(len(result['wrappers']),2)
            self.assertEqual(result['wrappers'][0]['observer_script'], result['wrappers'][1]['observer_script'])
            environment = dict(os.environ,LD_LIBRARY_PATH='/native/necessary/lib',TK_LIBRARY='')
            process = subprocess.Popen([wrapper['observer_script'],'native candidate script'],env=environment,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout,stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode,7)
            with open(output) as stream:
                actual = json.load(stream)
            self.assertEqual(actual['argv'][1:], ['-R','cpu=6;mem=20000','native candidate script'])
            self.assertEqual(actual['ld'],'/native/necessary/lib')
            self.assertEqual(actual['tk'],'')
            with open(root+'/submit.jsonl') as stream:
                events = [json.loads(line) for line in stream]
            self.assertEqual(events[0]['event'],'submit_start')
            self.assertEqual(events[-1]['event'],'submit_end')
            self.assertEqual(events[-1]['returncode'],7)
            self.assertEqual(events[-1]['job_ids'],['7391'])
            self.assertIn(b'Job <7391> submitted',stdout)
            self.assertIn(b'scheduler detail',stderr)
            self.assertEqual(events[0]['argv'],wrapper['submit_argv']+['native candidate script'])


if __name__ == '__main__':
    unittest.main()
