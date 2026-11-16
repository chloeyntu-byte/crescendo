"""Measure real checkpoint transactions with a synthetic two-point evaluator.

Run in a fresh process for each engine checkout. No EDA tools are invoked.
"""
import argparse
import json
import os
import sys
import time
from unittest import mock

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--engine-root', required=True)
parser.add_argument('--output-root', required=True)
args = parser.parse_args()
sys.path.insert(0, os.path.abspath(args.engine_root))
from crescendo_engine import run_data_store as storage
from crescendo_engine import runner

os.makedirs(args.output_root, exist_ok=True)
prepared = {
    'maestro_path': '/portable/design/maestro',
    'setup': {'tests': [{'name': 'main', 'enabled': True}], 'selected_test': 'main',
              '_source_tests': [{'name': 'main', 'enabled': True}],
              'corners': [{'name': 'tt'}, {'name': 'ss'}]},
    'hierarchy': {'designs': []},
    'parameters': [{'parameter_id': 'x', 'backend': 'schematic', 'optimizable': True}],
    'outputs': [{'key': 'mock::cost'}],
}
spec = {
    'evaluator': 'mock', 'generations': 4, 'population': 4, 'jobs': 1, 'seed': 7,
    'variables': [{'parameter_id': 'x', 'minimum': 0.0, 'maximum': 2.0}],
    'objectives': [{'output_key': 'mock::cost', 'direction': 'minimize'}],
    'constraints': [],
}
# Reader evidence is intentionally large. Each separate durable point record
# is approximately 1 MiB; candidate rows stay small.
evidence = 'synthetic-reader-evidence-' * ((1024 * 1024) // 26)
point_bytes = len(storage._encoded({'corner': 'tt', 'evidence': evidence}))
stores = []
original_store = runner.RunDataStore


def open_store(*values, **keywords):
    writer = original_store(*values, **keywords)
    stores.append(writer)
    return writer


def evaluate(root, index, generation, values, *unused):
    value = values['x']
    writer = stores[-1]
    for corner in ('tt', 'ss'):
        writer.append_event('point', {'candidate_id': 'g%03d_c%04d' % (generation, index),
                                     'corner': corner, 'evidence': evidence,
                                     'candidate_completed': False})
    return {
        'candidate_id': 'g%03d_c%04d' % (generation, index), 'generation': generation,
        'index': index, 'values': values, 'status': 'completed',
        'metrics': {'objectives': [{'score': value, 'value': value}], 'constraints': [],
                    'feasible': True, 'total_violation': 0.0, 'merit': value},
    }


counts = {'hash_bytes': 0, 'parse_bytes': 0}
original_open = storage._open_read


class CountedFile(object):
    def __init__(self, path):
        self.stream = original_open(path)

    def __enter__(self):
        self.stream.__enter__()
        return self

    def __exit__(self, *values):
        return self.stream.__exit__(*values)

    def __getattr__(self, name):
        return getattr(self.stream, name)

    def read(self, size=-1):
        raw = self.stream.read(size)
        counts['hash_bytes'] += len(raw)
        return raw

    def readline(self, size=-1):
        raw = self.stream.readline(size)
        counts['parse_bytes'] += len(raw)
        return raw


audits = []
original_audit = storage.RunDataStore.verify_generation
previous_parse = [0]


def audit(writer, generation):
    started = time.monotonic()
    original_audit(writer, generation)
    audits.append({'generation': generation, 'file_bytes_at_commit': writer.offset,
                   'audit_seconds': time.monotonic() - started,
                   'parse_bytes': counts['parse_bytes'] - previous_parse[0]})
    previous_parse[0] = counts['parse_bytes']


started = time.monotonic()
with mock.patch.object(storage, '_open_read', side_effect=CountedFile), \
        mock.patch.object(runner, 'RunDataStore', side_effect=open_store), \
        mock.patch.object(runner, '_evaluate_candidate', side_effect=evaluate), \
        mock.patch.object(storage.RunDataStore, 'verify_generation', autospec=True, side_effect=audit):
    result = runner.run_optimization(spec, {'runtime': {'run_root': args.output_root,
                                      'max_parallel_jobs': 1}}, prepared=prepared,
                                     run_root=os.path.join(args.output_root, 'run'))
assert len(result['candidates']) == 16
rows = []
for timing, audit_row in zip(result['generation_timings'], audits):
    operations = timing['storage_operations']
    transaction = operations['checkpoint_transaction']
    scan = operations.get('dataset_prefix_scan', {})
    rows.append({'generation': timing['generation'], 'transactions': transaction['calls'],
                 'checkpoint_seconds': transaction['seconds'],
                 'checkpoint_max_seconds': transaction['max_seconds'],
                 'hash_bytes': scan.get('bytes', 0), 'hash_calls': scan.get('calls', 0),
                 'appended_bytes': operations['dataset_append_write']['bytes'],
                 'file_bytes_at_commit': audit_row['file_bytes_at_commit'],
                 'audit_seconds': audit_row['audit_seconds'],
                 'parse_bytes': audit_row['parse_bytes']})
summary = {'generations': rows, 'disk_reads': counts, 'point_bytes': point_bytes,
           'file_bytes': os.path.getsize(os.path.join(args.output_root, 'run', storage.CORE_NAME)),
           'total_seconds': time.monotonic() - started, 'candidate_count': 16,
           'point_count': 32, 'verification_policy': 'writer_bytes_and_generation_audit'}
with open(os.path.join(args.output_root, 'benchmark.json'), 'w') as stream:
    json.dump(summary, stream, sort_keys=True, indent=2)
print(json.dumps(summary, sort_keys=True, indent=2))
