"""Versioned storage, durable evidence, audit and recovery boundaries."""
import copy
import base64
import zlib
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from crescendo_engine import run_data_store as storage
from crescendo_engine.run_data_store import RunDataReader, RunDataStore, RunDataError, dataset_from_reference, export_dataset
from crescendo_engine.run_data_v2 import Decoder, digest
from tests.test_run_data_store import row, count_disk_reads


class RunDataV2Tests(unittest.TestCase):
    def store(self, root, version=2):
        return RunDataStore(os.path.join(root, 'data'), {'spec': {}}, format_version=version)

    def documents(self, path):
        with open(path) as stream:
            return [json.loads(line) for line in stream]

    def rewrite(self, path, docs):
        previous = None
        with open(path, 'wb') as stream:
            for index, doc in enumerate(docs, 1):
                doc = dict(doc, revision=index, previous_sha256=previous)
                doc.pop('sha256', None)
                doc['sha256'] = digest(doc)
                previous = doc['sha256']
                stream.write(storage._encoded(doc) + b'\n')

    def test_version_matrix_and_original_v1_bytes_survive_reopen(self):
        for version in (1, 2):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as root:
                writer = self.store(root, version)
                writer.commit_candidates([row(status='completed')])
                reference = writer.reference()
                writer.close()
                with open(reference['path'], 'rb') as stream:
                    before = stream.read()
                reopened = RunDataStore(reference['path'])
                self.assertEqual(reopened.reference(), reference)
                reopened.close()
                with open(reference['path'], 'rb') as stream:
                    self.assertEqual(stream.read(), before)
                self.assertEqual(dataset_from_reference(reference)['schema_version'],
                                 'crescendo.optimization_dataset.v%d' % version)
                wrong = dict(reference, schema_version='crescendo.optimization_dataset.v%d' % (3-version))
                with self.assertRaises(RunDataError):
                    dataset_from_reference(wrong)
                with self.assertRaises(RunDataError):
                    # This is the v1 reader's exact accepted schema contract.
                    first = self.documents(reference['path'])[0]
                    first['schema_version'] = 'crescendo.optimization_record.v99'
                    RunDataReader(reference['path'])._apply(first)

    def test_dedup_delta_alias_projection_portable_and_gc(self):
        with tempfile.TemporaryDirectory() as root:
            writer = self.store(root)
            point = {'candidate_id': 'g000_c0000', 'task_id': 'main_TT_1', 'test': 'main',
                     'corner': 'TT', 'point_id': 1, 'token': 'a'*32, 'candidate_completed': False,
                     'scheduler_observation': {'metadata': 'x'*8000},
                     'terminal_receipt': {'token': 'a'*32, 'returncode': 0},
                     'effective_terminal_receipt': {'token': 'a'*32, 'returncode': 0}}
            writer.commit_candidates([row()])
            writer.append_event('point', point)
            self.assertEqual(dataset_from_reference(writer.reference())['candidates'][0]['status'], 'planned')
            pipeline_point = {key: point[key] for key in ('task_id','test','corner','point_id','scheduler_observation')}
            pipeline_point.update(raw_terminal_receipt=point['terminal_receipt'], terminal_receipt=point['effective_terminal_receipt'])
            pipeline = {'schema_version': 'crescendo.execution_pipeline.v1', 'points': [pipeline_point]}
            completed = row(status='completed')
            completed.update(execution_pipeline=pipeline, completion={'execution_pipeline':pipeline},
                             completion_evidence={'execution_pipeline': pipeline, 'native': True})
            writer.commit_candidates([completed])
            before = writer.reference()
            completed['artifact_summary'] = {'raw_deleted': True}
            writer.commit_candidates([completed])
            after = writer.reference()
            self.assertEqual(dataset_from_reference(after)['candidates'], [completed])
            self.assertNotIn('artifact_summary', dataset_from_reference(before)['candidates'][0])
            docs = self.documents(writer.path)
            self.assertEqual(sum(d['kind'] == 'candidate_patch' for d in docs), 1)
            text = json.dumps(docs) + ''.join(zlib.decompress(base64.b64decode(d['payload']['value'])).decode()
                for d in docs if d['kind'] == 'evidence' and d['payload']['codec'] == 'zlib-json')
            self.assertIn('$point', text)
            self.assertIn('aliases', text)
            blob_hashes = [d['payload']['reference']['sha256'] for d in docs if d['kind']=='evidence']
            self.assertEqual(len(blob_hashes), len(set(blob_hashes)))
            target = export_dataset(writer.path, os.path.join(root, 'portable'))
            with open(writer.path) as stream:
                embedded = stream.read(after['bytes'])
            writer.close()
            os.unlink(writer.path)
            # No external blob tree exists for retention/GC to accidentally prune.
            self.assertEqual(RunDataReader(target).read(strict=True)['candidates'], [completed])
            self.assertEqual(dataset_from_reference(after, embedded)['candidates'], [completed])

    def test_missing_tampered_out_of_bounds_forward_refs_fail_closed(self):
        for mode in ('missing','sha','length','revision','forward','schema','type'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                writer = self.store(root)
                writer.append_event('point', {'proof': 'x'*4000})
                path = writer.path
                writer.close()
                docs = self.documents(path)
                ref = docs[-1]['payload']['$evidence']
                if mode == 'missing':
                    del docs[1]
                elif mode == 'sha':
                    ref['sha256'] = '0'*64
                elif mode == 'length':
                    ref['bytes'] += 1
                elif mode == 'revision':
                    ref['revision'] += 1
                elif mode == 'forward':
                    docs[1],docs[-1] = docs[-1],docs[1]
                elif mode == 'schema':
                    ref['schema'] = 'wrong.v2'
                else:
                    ref['type'] = 'str'
                self.rewrite(path, docs)
                with self.assertRaises(RunDataError):
                    RunDataReader(path).read(strict=True)

    def test_blob_content_and_verified_prefix_are_binding(self):
        with tempfile.TemporaryDirectory() as root:
            writer = self.store(root)
            writer.append_event('point', {'proof': 'x'*4000})
            reference = writer.reference()
            path = writer.path
            writer.close()
            with open(path) as stream:
                raw = stream.read()
            cut = raw.index('\n')+1
            short = dict(reference, bytes=cut, revision=1, prefix_sha256=hashlib.sha256(raw[:cut].encode()).hexdigest(),
                         head_sha256=json.loads(raw[:cut])['sha256'])
            self.assertEqual(dataset_from_reference(short)['revision'],1)
            docs = self.documents(path)
            docs[1]['payload']['value'] = 'not compressed data'
            self.rewrite(path, docs)
            with self.assertRaises(RunDataError):
                RunDataReader(path).read(strict=True)

    def test_delta_rejects_wrong_base_and_protected_fields(self):
        for mode in ('base','values','status','candidate'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                writer = self.store(root)
                candidate = row(status='completed')
                writer.commit_candidates([candidate])
                candidate['artifact_summary'] = {'ok':True}
                writer.commit_candidates([candidate])
                path = writer.path
                writer.close()
                docs = self.documents(path)
                patch = docs[-1]['payload'][0]
                if mode == 'base': patch['base_sha256'] = '0'*64
                elif mode == 'candidate': patch['candidate_id'] = 'g000_c0001'
                else: patch['set'][mode] = {} if mode == 'values' else 'planned'
                self.rewrite(path, docs)
                with self.assertRaises(RunDataError): RunDataReader(path).read(strict=True)

    def test_blob_sync_failure_cannot_publish_reference_or_ack(self):
        with tempfile.TemporaryDirectory() as root:
            writer = self.store(root)
            revision = writer.revision
            with mock.patch.object(storage.os, 'fsync', side_effect=OSError('disk full')):
                with self.assertRaises(OSError): writer.append_event('point', {'proof':'x'*9000})
            self.assertEqual(writer.revision, revision)
            with self.assertRaises(RunDataError): writer.reference()
            writer.close()

    def test_v2_intra_generation_incremental_hash_and_generation_scan(self):
        with tempfile.TemporaryDirectory() as root:
            writer = self.store(root)
            writer.reference()
            with writer.checkpoint_evidence(), count_disk_reads() as counts:
                start = writer.offset
                for index in range(4):
                    writer.append_event('point', {'index':index,'proof':'x'*10000})
                    dataset_from_reference(writer.reference())
                self.assertEqual(counts['hash_bytes'],0)
                self.assertEqual(counts['parse_bytes'],writer.offset-start)
                writer.verify_generation(0)
                self.assertEqual(counts['hash_bytes'],writer.offset)
            writer.close()

    def test_scheduler_query_log_preserves_every_original_after_point_cleanup(self):
        import shutil
        from crescendo_engine.netlist_execution import NetlistExecutionContext, task_termination_evidence
        from tests.test_scheduler_stop_proofs import ExactCancelDialectTests
        from crescendo_engine.scheduler_audit import verify_segment
        with tempfile.TemporaryDirectory() as root:
            task_root = os.path.join(root, 'candidates', 'g000_c0000', 'simulation', 'tasks', 'main_TT_1', 'execution')
            os.makedirs(task_root)
            helper = ExactCancelDialectTests()
            context, calls, submit, query_binary = helper._context(task_root, True, 'JOB_ID: 7391\nSTATE: UNKNOWN')
            context.run_data_version = 2
            identity = {'candidate_id': 'g000_c0000', 'task_id': 'main_TT_1', 'test': 'main',
                        'corner': 'TT', 'point_id': 1, 'receipt_token': 'a'*32}
            tracking = None
            try:
                for index in range(44):
                    state = 'PENDING' if index == 0 else 'UNKNOWN' if index < 42 else 'DONE'
                    text = 'JOB_ID JOB_STATE DETAIL\n7391 %s %06d_%s' % (state, index, 'Q'*2656)
                    context.command_runner = lambda argv, text=text, **kw: {'returncode':0, 'stdout_tail':text}
                    tracking = context._scheduler_observe({'submit_argv':[submit]}, '7391',task_root,dict(os.environ),tracking,identity,final=index==43)
                self.assertEqual(tracking['query_count'],44)
                self.assertEqual(len(tracking['samples']),2)
                self.assertEqual(tracking['omitted_query_count'],42)
                writer = self.store(root)
                point = dict(identity, token='a'*32, scheduler_observation=tracking, candidate_completed=False)
                writer.append_event('point', point)
                reference = writer.reference()
                writer.close()
                shutil.rmtree(os.path.join(root,'candidates'))
                reader = RunDataReader(reference['path'])
                reader.read(strict=True)
                stored = next(value for ref,value in reader.codec.blobs.values()
                              if isinstance(value,dict) and value.get('schema_version')=='crescendo.scheduler_task_tracking.v2')
                originals = reader.codec.query_originals(stored['query_log'])
                self.assertEqual(len(originals),44)
                self.assertIn('000017_',originals[17]['query']['stdout_tail'])
                changed = copy.deepcopy(stored['query_log'])
                changed['entries'][1]['offset'] += 1
                with self.assertRaises(RunDataError): reader.codec.query_originals(changed)
                changed = copy.deepcopy(stored['query_log'])
                changed['entries'][1]['sha256'] = '0'*64
                with self.assertRaises(RunDataError): reader.codec.query_originals(changed)
                owned = {'remote':True,'token':'a'*32,'scheduler_job_id':'7391',
                         'submission_observation':{'accepted':True,'job_id':'7391'},
                         'task':{'task_id':'main_TT_1'}, 'scheduler_observation':stored}
                self.assertTrue(task_termination_evidence(owned,None)['cleanup_proven'])
                for mutation in ('unknown','ack','foreign'):
                    proof = copy.deepcopy(owned)
                    if mutation == 'unknown': proof['scheduler_observation']['terminal_observation']['state']='UNKNOWN'
                    elif mutation == 'ack': proof['scheduler_observation'].pop('terminal_observation')
                    else: proof['scheduler_observation']['terminal_observation']['observed_job_ids']=['9999']
                    self.assertFalse(task_termination_evidence(proof,None)['cleanup_proven'])
                resumed_root = os.path.join(root, 'resumed')
                os.mkdir(resumed_root)
                resumed = self.store(resumed_root)
                resumed.append_event('status', {'marker':'ensure different destination revisions'})
                resumed.import_point_history(reference, candidate_ids=['g000_c0000'])
                candidate = row(status='completed')
                candidate['execution_pipeline']={'points':[{'scheduler_observation':stored}]}
                resumed.commit_candidates([candidate])
                destination = resumed.reference()
                resumed.close()
                next_reader = RunDataReader(destination['path'])
                next_data = next_reader.read(strict=True)
                rebound = next_data['candidates'][0]['execution_pipeline']['points'][0]['scheduler_observation']['query_log']
                self.assertNotEqual(rebound['evidence'],stored['query_log']['evidence'])
                self.assertEqual(len(next_reader.codec.query_originals(rebound)),44)
                with open(reference['path']) as stream:
                    embedded = stream.read(reference['bytes'])
                os.unlink(reference['path'])
                embedded_root = os.path.join(root, 'embedded')
                os.mkdir(embedded_root)
                embedded_writer = self.store(embedded_root)
                embedded_writer.import_point_history(reference, embedded, ['g000_c0000'])
                embedded_writer.commit_candidates([candidate])
                embedded_reference = embedded_writer.reference()
                embedded_writer.close()
                self.assertEqual(len(dataset_from_reference(embedded_reference)['candidates']),1)
                from crescendo_engine.diagnostic_focus import volume_check
                report = volume_check(destination['path'])
                self.assertEqual(report['point_count'],1)
                self.assertEqual(report['file_bytes'],os.path.getsize(destination['path']))
                self.assertEqual(report['scheduler_observation_count'], 2)
                self.assertEqual(report['scheduler_unknown_occurrences'], 82)
                self.assertEqual(report['scheduler_unknown_queries'], 41)
                self.assertTrue(report['scheduler_unknown_census_complete'])
            finally:
                context.close(cancel=True)

    def test_terminal_delta_survives_writer_reopen(self):
        with tempfile.TemporaryDirectory() as root:
            writer = self.store(root)
            original = row(status='completed')
            writer.commit_candidates([original])
            path = writer.path
            writer.close()
            writer = RunDataStore(path)
            original['artifact_summary']={'ok':True}
            writer.commit_candidates([original])
            self.assertEqual(dataset_from_reference(writer.reference())['candidates'],[original])
            writer.close()

    def test_point_projection_completion_flags_round_trip_live_and_reopened(self):
        for completed in (True, False, None):
            for reopen in (False, True):
                with self.subTest(completed=completed, reopen=reopen), tempfile.TemporaryDirectory() as root:
                    writer = self.store(root)
                    point = {'candidate_id': 'g000_c0000', 'token': 'a'*32,
                             'task_id': 'p1', 'test': 'main', 'corner': 'TT', 'point_id': 1,
                             'terminal_receipt': {'token': 'a'*32},
                             'effective_terminal_receipt': {'token': 'a'*32}}
                    if completed is not None:
                        point['candidate_completed'] = completed
                    writer.append_event('point', point)
                    if reopen:
                        path = writer.path
                        writer.close()
                        writer = RunDataStore(path)
                    self.assertEqual(bool(writer.codec.points), completed is False)
                    pipeline_point = {key: point[key] for key in ('task_id', 'test', 'corner', 'point_id')}
                    pipeline_point.update(raw_terminal_receipt=point['terminal_receipt'],
                                          terminal_receipt=point['effective_terminal_receipt'])
                    candidate = row(status='completed')
                    candidate['execution_pipeline'] = {'points': [pipeline_point]}
                    writer.commit_candidates([candidate])
                    reference = writer.reference()
                    writer.close()
                    reader = RunDataReader(reference['path'])
                    self.assertEqual(reader.read(strict=True)['candidates'], [candidate])
                    self.assertEqual([receipt[1] for receipt in reader.codec.point_receipts.values()], [point])
                    docs = self.documents(reference['path'])
                    wire = json.dumps(docs) + ''.join(
                        zlib.decompress(base64.b64decode(d['payload']['value'])).decode()
                        for d in docs if d['kind'] == 'evidence' and d['payload']['codec'] == 'zlib-json')
                    self.assertEqual('$point' in wire, completed is False)

    def test_point_projection_cannot_cross_candidate(self):
        with tempfile.TemporaryDirectory() as root:
            writer = self.store(root)
            point = {'candidate_id':'g000_c0000', 'candidate_completed':False, 'token':'a'*32,
                     'task_id':'p1','test':'main','corner':'TT','point_id':1,
                     'terminal_receipt':{'token':'a'*32}, 'effective_terminal_receipt':{'token':'a'*32},
                     'scheduler_observation':{'metadata':'Q'*2000}}
            writer.append_event('point',point)
            pipeline_point = {key:point[key] for key in ('task_id','test','corner','point_id')}
            pipeline_point.update(raw_terminal_receipt=point['terminal_receipt'],terminal_receipt=point['effective_terminal_receipt'],
                                  scheduler_observation=point['scheduler_observation'])
            candidate = row(status='completed')
            candidate['execution_pipeline']={'points':[pipeline_point]}
            writer.commit_candidates([candidate])
            writer.reference()
            other = dict(candidate,candidate_id='g000_c0001',index=1)
            writer.commit_candidates([other])
            with self.assertRaisesRegex(RunDataError,'another candidate'):
                writer.reference()
            writer.close()

    def test_fsynced_orphan_blob_is_not_a_committed_point_receipt(self):
        with tempfile.TemporaryDirectory() as root:
            writer = self.store(root)
            point = {'candidate_id':'g000_c0000', 'candidate_completed':False, 'token':'a'*32,
                     'task_id':'p1','test':'main','corner':'TT','point_id':1,
                     'terminal_receipt':{'token':'a'*32}, 'effective_terminal_receipt':{'token':'a'*32},
                     'scheduler_observation':{'metadata':'Q'*2000}}
            reference = writer.codec.pack(point,force=True)['$evidence']
            # Emulate a reference injected after a crash between blob fsync and
            # the referencing point record. The reader must independently gate it.
            writer.codec.points[(point['candidate_id'],point['task_id'],point['token'])] = (reference,point)
            pipeline_point = {key:point[key] for key in ('task_id','test','corner','point_id','scheduler_observation')}
            pipeline_point.update(raw_terminal_receipt=point['terminal_receipt'],terminal_receipt=point['effective_terminal_receipt'])
            candidate = row(status='completed')
            candidate['execution_pipeline']={'points':[pipeline_point]}
            writer.commit_candidates([candidate])
            with self.assertRaisesRegex(RunDataError,'no committed point receipt'):
                writer.reference()
            self.assertTrue(writer.failed)
            writer.close()

    def test_legacy_v2_exact_schema_alias_producer_consumer(self):
        program = """
import os,sys,subprocess
from crescendo_engine.runtime import initialize,ApplicationIdentity,ProtocolDialect,StorageLayout,child_environment
initialize(identity=ApplicationIdentity(schema_namespace='archive', schema_aliases={
 'crescendo.optimization_dataset.v2':'archive.dataset_legacy.v2',
 'crescendo.optimization_record.v2':'archive.record_legacy.v2',
 'crescendo.evidence_ref.v2':'archive.proof_pointer.v2',
 'crescendo.evidence_blob.v2':'archive.proof_blob.v2'}),
 storage=StorageLayout(leaf_aliases={'optimization.crescendo':'run.archive'}),
 dialect=ProtocolDialect(procedure_prefix='arch',symbol_prefix='ar_'))
from crescendo_engine.run_data_store import RunDataStore,CORE_NAME
path=os.path.join(sys.argv[1],CORE_NAME)
w=RunDataStore(path,{'spec':{}})
w.append_event('point',{'proof':'x'*4000})
assert w.reference()['schema_version']=='archive.dataset_legacy.v2'
w.close()
subprocess.check_call([sys.executable,'-B','-c',
 'import sys;from crescendo_engine.run_data_store import RunDataReader;assert RunDataReader(sys.argv[1]).read(strict=True)["revision"]==3',path],env=child_environment())
"""
        with tempfile.TemporaryDirectory() as root:
            subprocess.check_call([sys.executable,'-B','-c',program,root])


if __name__ == '__main__':
    unittest.main()
