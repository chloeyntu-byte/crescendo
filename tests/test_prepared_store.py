"""Large synthetic catalogues, legacy byte protocols and bounded worker work."""
import copy
import ast
import hashlib
import io
import json
import os
import sys
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import catalog_json, prepared_store, runner
from crescendo_engine.setup_private_session import _snapshot_hash
from crescendo_engine.prepared_freshness import _canonical_fingerprint


class PreparedStoreTests(unittest.TestCase):
    def catalogue(self, count=2000):
        rows = []
        for index in range(count):
            for name, eligible in (('w', True), ('modelDescription', False), ('displayControl', False)):
                rows.append({'parameter_id': '%d:%s' % (index, name), 'hierarchy_path': '/I%d' % index,
                             'name': name, 'optimizable': eligible, 'callback': 'synthetic metadata ' * 32,
                             'numeric_value': 2e-6 if eligible else None, 'editable': eligible,
                             'occurrence_chain': [{'hierarchy_path': '/BANK', 'master_cell': 'SYNTH'}]})
        return {'schema_version': 'crescendo.prepared.v1', 'parameters': rows, 'setup': {},
                'hierarchy': {'designs': [{'design': {'library': 'SYNTH', 'cell': 'TOP', 'view': 'schematic'},
                    'hierarchy': {'parameters': copy.deepcopy(rows), 'instances': [{'hierarchy_path': '/I0'}],
                                  'errors': [{'kind': 'warning', 'values': ['kept']}],
                                  'config_bindings': [{'resolved_view': 'schematic'}]}}]}}

    def test_ui_context_projection_preserves_configuration_evidence(self):
        from crescendo_engine.settings_state import settings_snapshot_context, build_settings_state
        rows = self.catalogue(40)['parameters']
        context = {'setup': {'identity': {'library': 'SYNTH', 'cell': 'TOP', 'view': 'maestro'}},
                   'parameter_by_id': dict((row['parameter_id'], row) for row in rows),
                   'cell_scopes': [{'scope_id': 'scope', 'hierarchy_path': '/', 'selectable': False,
                                    'design': {'library': 'SYNTH', 'cell': 'TOP', 'view': 'schematic'},
                                    'discarded': ['native evidence'] * 100}],
                   'outputs': [{'key': 'test::value', 'name': 'value', 'test': 'test', 'expression': 'large'}]}
        raw = {'setup': context['setup'], 'parameter_configs': {
            row['parameter_id']: {'minimum': 'invalid draft', 'selected': True} for row in rows}}
        projected = settings_snapshot_context(context)
        cached = settings_snapshot_context(context, parameter_evidence=projected['parameter_by_id'])
        self.assertEqual(projected, cached)
        first = next(iter(cached['parameter_by_id']))
        cached['parameter_by_id'][first]['name'] = 'changed'
        self.assertNotEqual(projected['parameter_by_id'][first]['name'], 'changed')
        before = build_settings_state(raw, context['parameter_by_id'], context['cell_scopes'], context['outputs'])
        after = build_settings_state(raw, projected['parameter_by_id'], projected['cell_scopes'], projected['outputs'])
        before.pop('saved_at');after.pop('saved_at')
        self.assertEqual(before, after)
        self.assertNotIn('occurrence_chain', next(iter(projected['parameter_by_id'].values())))
        self.assertFalse(projected['cell_scopes'][0]['selectable'])
        self.assertLess(len(json.dumps(projected)), len(json.dumps(context)) / 5)

    def test_native_window_cache_keeps_instance_cdf_and_condition_guards(self):
        from crescendo_engine.hierarchy import probe_skill
        script = probe_skill('SYNTH', 'TOP', 'schematic', '/synthetic/records', 32)
        self.assertIn('cr_EditorSeen[key] = cached', script)
        self.assertIn('when(!cr_CandidateOnly || cached ==', script)
        self.assertIn('cdfGetInstCDF(inst)', script)
        self.assertIn('cdfgData = instCdf', script)
        self.assertIn('cdfgInst = inst', script)
        self.assertIn('car(editContext) == cv', script)
        self.assertIn('conditionEditable = crescProbeCondition(param~>editable t cv)', script)
        self.assertIn('hiSetCurrentWindow(car(cached))', script)
        self.assertIn('foreach(cr_EditorWindow cr_EditorWindows', script)

    def test_native_cpu_profile_is_diagnostic_and_preserved(self):
        from crescendo_engine.hierarchy import normalize_probe
        result = normalize_probe([{'kind': 'PROBE_CPU', 'values': [11, 12, 13, 14, 15, 80]}])
        self.assertEqual(result['probe_cpu_ticks'], {
            'cdf': 11, 'editor': 12, 'conditions': 13,
            'candidate_filter': 14, 'emit': 15, 'total': 80})
        self.assertEqual(result['parameters'], [])
        self.assertEqual(result['errors'], [])

    def test_catalogue_keeps_eligible_metadata_and_every_native_gate(self):
        value = self.catalogue()
        before = len(json.dumps(value, indent=2))
        gates = copy.deepcopy(value['hierarchy']['designs'][0]['hierarchy'])
        prepared_store.compact_catalog(value)
        self.assertEqual(value['schema_version'], 'crescendo.prepared.v2')
        self.assertEqual(len(value['parameters']), 2000)
        self.assertTrue(all(row['optimizable'] for row in value['parameters']))
        body = value['hierarchy']['designs'][0]['hierarchy']
        self.assertEqual(len(body['parameters']), 2000)
        self.assertEqual(set(body['parameters'][0]), {'parameter_id', 'hierarchy_path'})
        for key in ('errors', 'instances', 'config_bindings'):
            self.assertEqual(body[key], gates[key])
        self.assertLess(len(''.join(catalog_json.chunks(value))), before / 5)

    def test_four_snapshots_share_one_parameter_and_hierarchy_object(self):
        value = prepared_store.compact_catalog(self.catalogue())
        with tempfile.TemporaryDirectory() as root:
            for index in range(4):
                path = os.path.join(root, 'snapshot%d.json' % index)
                prepared_store.write(path, value, root=root)
                self.assertEqual(prepared_store.load(path), value)
                self.assertLess(os.path.getsize(path), 2048)
            objects = os.path.join(root, 'prepared_objects')
            names = os.listdir(objects)
            self.assertEqual(len(names), 3)
            stamps = dict((name, os.stat(os.path.join(objects, name)).st_mtime_ns) for name in names)
            prepared_store.write(os.path.join(root, 'snapshot4.json'), value, root=root)
            self.assertEqual(stamps, dict((name, os.stat(os.path.join(objects, name)).st_mtime_ns) for name in names))
            total = sum(os.path.getsize(os.path.join(objects, name)) for name in names)
            original = len(json.dumps(self.catalogue(), indent=2).encode('utf-8')) * 4
            self.assertLess(total, original / 20)

    def test_payload_tamper_and_symlink_fail_closed(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'prepared.json')
            value = prepared_store.compact_catalog(self.catalogue(2))
            prepared_store.write(path, value)
            with open(path) as stream:
                reference = json.load(stream)
            payload = prepared_store._get('prepared', reference['payload'])
            target = payload['objects']['parameters']['path']
            with open(target, 'w') as stream:
                stream.write('[]')
            with self.assertRaisesRegex(ValueError, 'changed'):
                prepared_store.load(path)
            with self.assertRaisesRegex(ValueError, 'changed'):
                prepared_store.write(path, value)
            os.unlink(target)
            os.symlink(path, target)
            with self.assertRaisesRegex(ValueError, 'Unsafe'):
                prepared_store.load(path)

    def test_concurrent_publication_retains_single_objects(self):
        value = prepared_store.compact_catalog(self.catalogue(3))
        with tempfile.TemporaryDirectory() as root:
            real_link = os.link
            real_makedirs = os.makedirs
            initial_creation = threading.Barrier(2)
            arrived = threading.local()
            directory = os.path.join(root, 'prepared_objects')
            def create_directory(path, *args, **kwargs):
                if path == directory and not getattr(arrived, 'once', False):
                    arrived.once = True
                    initial_creation.wait(10)
                return real_makedirs(path, *args, **kwargs)
            errors = []
            def publish(source, target):
                real_link(source, target)
                time.sleep(.03)  # expose the two-link publication interval
            def worker(index):
                try:
                    prepared_store.write(os.path.join(root, str(index) + '.json'), value, root=root)
                except Exception as error:
                    errors.append(error)
            with mock.patch.object(prepared_store.os, 'link', side_effect=publish), \
                    mock.patch.object(prepared_store.os, 'makedirs', side_effect=create_directory):
                threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(30)
                    self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(len(os.listdir(os.path.join(root, 'prepared_objects'))), 3)
            self.assertEqual(prepared_store.load(os.path.join(root, '0.json')), value)
            self.assertEqual(prepared_store.load(os.path.join(root, '1.json')), value)

    def test_legacy_inline_keeps_schema_and_original_hash(self):
        value = self.catalogue(3)
        expected = hashlib.sha256(json.dumps(value, ensure_ascii=True, sort_keys=True,
                                              separators=(',', ':')).encode('utf-8')).hexdigest()
        self.assertEqual(_snapshot_hash(value), expected)
        self.assertEqual(_canonical_fingerprint(value), expected)
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'legacy.json')
            with open(path, 'w') as stream:
                json.dump(value, stream, indent=2)
            loaded = prepared_store.load(path)
            self.assertEqual(loaded, value)
            self.assertEqual(_snapshot_hash(loaded), expected)
            self.assertEqual(loaded['schema_version'], 'crescendo.prepared.v1')
        changed = copy.deepcopy(value)
        prepared_store.compact_catalog(changed)
        self.assertNotEqual(_snapshot_hash(changed), expected)
        self.assertNotEqual(runner._prepared_schema_fingerprint_v2(changed),
                            runner._prepared_schema_fingerprint_v2(value))

    def test_injected_legacy_identity_roundtrips_without_resigning(self):
        code = r'''import sys, json, os, hashlib
from crescendo_engine.runtime import initialize, ApplicationIdentity, StorageLayout, ProtocolDialect
initialize(ApplicationIdentity(display_name='Fable', env_prefix='FABLE', schema_namespace='fable',
    schema_aliases={'crescendo.prepared.v1':'fable.historical.prepared',
                    'crescendo.prepared_reference.v2':'fable.catalogue.ref2',
                    'crescendo.prepared_payload.v2':'fable.catalogue.body2'}),
    StorageLayout(), ProtocolDialect(procedure_prefix='fb', symbol_prefix='fb_'))
from crescendo_engine import prepared_store
from crescendo_engine.setup_private_session import _snapshot_hash
from crescendo_engine.runner import _prepared_schema_fingerprint_v1, _prepared_schema_fingerprint_v2
value={'schema_version':'fable.historical.prepared','setup':{},'parameters':[],
       'hierarchy':{'designs':[]},'outputs':[]}
expected=hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
path=os.path.join(sys.argv[1],'prepared.json')
prepared_store.write(path,value)
loaded=prepared_store.load(path)
assert loaded == value
assert _snapshot_hash(loaded) == expected
assert _prepared_schema_fingerprint_v1(loaded) == _prepared_schema_fingerprint_v1(value)
assert _prepared_schema_fingerprint_v2(loaded) == _prepared_schema_fingerprint_v2(value)
assert json.load(open(path))['schema_version'] == 'fable.catalogue.ref2'
from crescendo_engine.hierarchy import probe_skill
script=probe_skill('cr_UserLibrary','crescUserCell','schematic',os.path.join(sys.argv[1],'cr_output.records'),32)
assert 'procedure(fb_ProbeCandidate(' in script
assert 'procedure(fb_ProbeTicks(' in script
assert 'fb_ProbeCandidate(param)' in script
assert 'cr_ProbeCandidate' not in script and 'cr_CandSeen' not in script
assert '"cr_UserLibrary"' in script and '"crescUserCell"' in script
assert 'cr_output.records' in script
'''
        with tempfile.TemporaryDirectory() as root:
            env = dict(os.environ, HOME=root, TMPDIR=root)
            env.pop('CRESCENDO_ENGINE_RUNTIME', None)
            env.pop('CRESCENDO_ENGINE_BOOTSTRAP', None)
            result = subprocess.run([sys.executable, '-B', '-c', code, root], env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_stream_decoder_matches_json_at_chunk_boundaries(self):
        samples = [{'unicode': '\u2603\\"\n\t', 'large': 'a' * 65533, 'tail': [1e-90, -12, None, False, {}, []]},
                   [float('inf'), float('-inf')], {'a': [i / 7 for i in range(10000)]}]
        for value in samples:
            text = json.dumps(value, ensure_ascii=True)
            self.assertEqual(catalog_json.load(io.StringIO(text)), value)
        for text in ('', '[1,]', '{"a":1,}', '{"a":1} {}', '\v{}', '01', '"bad\\q"'):
            with self.subTest(text=text), self.assertRaises(ValueError):
                catalog_json.load(io.StringIO(text))
        self.assertEqual(catalog_json.load(io.StringIO('{"a":0,"a":1}')), {'a': 1})

    def test_probe_fast_strings_preserve_legacy_escapes(self):
        from crescendo_engine.hierarchy import _literal
        for value in (r'"a\/b"', r'"a\nb"', r'"a\\b"', r'"a\vb"'):
            self.assertEqual(_literal(value), ast.literal_eval(value))

    def test_checkpoint_selects_explicit_legacy_discovery(self):
        self.assertEqual(runner.checkpoint_catalog_options({'manifest': {}}), {'legacy_catalog': True})
        self.assertEqual(runner.checkpoint_catalog_options({'manifest': {
            'prepared_catalog_version': 'crescendo.prepared.v1'}}), {'legacy_catalog': True})
        self.assertEqual(runner.checkpoint_catalog_options({'manifest': {
            'prepared_catalog_version': 'crescendo.prepared.v2'}}), {})
        with self.assertRaisesRegex(ValueError, 'Unsupported'):
            runner.checkpoint_catalog_options({'manifest': {'prepared_catalog_version': 'future'}})

    def _responsive(self, operation, heartbeat_wait=.005):
        # Wall gaps include OS descheduling and unrelated main-thread GC. Check
        # the worker's CPU budget between explicit GIL releases instead. Keep
        # wall distributions as evidence; the product's 200 ms goal is unchanged.
        import resource
        def cpu_time():
            usage = resource.getrusage(resource.RUSAGE_THREAD)
            return usage.ru_utime + usage.ru_stime
        previous, real_sleep = sys.getswitchinterval(), time.sleep
        done, errors, output = threading.Event(), [], []
        chunks, yields = [], []
        def work():
            last = [cpu_time()]
            def sleep(seconds):
                if threading.current_thread() is thread and seconds > 0:
                    now = cpu_time()
                    chunks.append(now - last[0])
                    yields.append(seconds)
                    real_sleep(seconds)
                    last[0] = cpu_time()
                else:
                    real_sleep(seconds)
            try:
                with mock.patch.object(time, 'sleep', side_effect=sleep):
                    output.append(operation())
            except Exception as error:
                errors.append(error)
            finally:
                chunks.append(cpu_time() - last[0])
                done.set()
        try:
            sys.setswitchinterval(.5)
            thread = threading.Thread(target=work)
            gaps, last = [], time.monotonic()
            thread.start()
            while not done.is_set():
                done.wait(heartbeat_wait)
                now = time.monotonic()
                gaps.append(now - last)
                last = now
            thread.join(30)
            self.assertFalse(errors, repr(errors))
            ordered = sorted(gaps or [0.])
            self.responsiveness_samples = {'samples': len(gaps), 'max_wall': ordered[-1],
                'p95_wall': ordered[int(.95 * (len(ordered) - 1))],
                'over_200ms': sum(gap > .2 for gap in gaps),
                'max_worker_cpu': max(chunks), 'explicit_yields': len(yields)}
            self.assertLess(max(chunks), .2, repr(self.responsiveness_samples))
            if sum(chunks) > .1:
                self.assertGreater(len(yields), 2, repr(self.responsiveness_samples))
            return output[0]
        finally:
            sys.setswitchinterval(previous)

    def test_responsiveness_rejects_cpu_stall_without_cooperation(self):
        import resource
        def cpu_time():
            usage = resource.getrusage(resource.RUSAGE_THREAD)
            return usage.ru_utime + usage.ru_stime
        def hog():
            deadline = cpu_time() + .23
            while cpu_time() < deadline:
                pass
        with self.assertRaises(AssertionError):
            self._responsive(hog)

    def test_responsiveness_ignores_descheduled_heartbeat(self):
        # Deliberately suspend observation for 250 ms while the worker yields.
        # The old single wall-maximum assertion rejected this healthy worker.
        def cooperative():
            end = time.monotonic() + .35
            while time.monotonic() < end:
                time.sleep(.001)
            return 'completed'
        self.assertEqual(self._responsive(cooperative, heartbeat_wait=.25), 'completed')
        self.assertGreater(self.responsiveness_samples['max_wall'], .2)
        self.assertLess(self.responsiveness_samples['max_worker_cpu'], .2)

    def test_encode_decode_and_hash_yield_to_heartbeat_with_long_switch_interval(self):
        value = self.catalogue(1200)
        encoded = json.dumps(value, sort_keys=True, separators=(',', ':'))
        for operation in (lambda: catalog_json.digest(value), lambda: ''.join(catalog_json.chunks(value)),
                          lambda: catalog_json.load(io.StringIO(encoded))):
            self._responsive(operation)

    def test_compact_hierarchy_reclassification_keeps_eligible_metadata(self):
        hierarchy = self.catalogue(4)['hierarchy']
        for row in hierarchy['designs'][0]['hierarchy']['parameters']:
            row.update(source='cdf', tunable=True, editable=True, display=True,
                       current_value='2u', default_value='1u', owner_library='SYNTH', owner_cell='TOP')
        rows = runner.parameter_catalog({}, hierarchy)
        value = prepared_store.compact_catalog({'parameters': rows, 'hierarchy': hierarchy})
        before = catalog_json.digest(value)
        restored = runner.parameter_catalog({}, prepared_store.catalogue_hierarchy(value))
        self.assertEqual([row for row in restored if row.get('optimizable')], value['parameters'])
        self.assertTrue(value['parameters'])
        self.assertEqual(catalog_json.digest(value), before)
        value['parameters'] = []
        with self.assertRaisesRegex(ValueError, 'no catalogue metadata'):
            prepared_store.catalogue_hierarchy(value)

    def test_discovery_scans_and_native_script_build_yield_to_heartbeat(self):
        from crescendo_engine.workspace import bounded_skill_body_lines
        from crescendo_engine.config_binding import validate_config_binding_records
        from crescendo_engine.environment_identity import _collect_model_tree
        lines = ['(printf "' + 'x' * 2000 + '")'] * 1200
        self.assertTrue(self._responsive(lambda: bounded_skill_body_lines(lines, result_name='cr_TestResult')))
        hierarchy = self.catalogue(3500)['hierarchy']
        self.assertEqual(len(self._responsive(lambda: runner.parameter_catalog({}, hierarchy))), 10500)
        with tempfile.TemporaryDirectory() as root:
            model = os.path.join(root, 'root.scs')
            child = os.path.join(root, 'child.scs')
            with open(model, 'w') as stream:
                stream.write('// synthetic model record\n' * 100000 + 'include "child.scs"\n')
            with open(child, 'w') as stream:
                stream.write('// child\n')
            class Collector(object):
                def __init__(self):
                    self.paths = []
                def add_file(self, role, path):
                    self.paths.append(path)
                    return path
                def limitation(self, *args, **kwargs):
                    raise AssertionError((args, kwargs))
            collector = Collector()
            self._responsive(lambda: _collect_model_tree(collector, model, []))
            self.assertEqual(set(collector.paths), {model, child})
            config, top = ('SYNTH', 'TOP', 'config'), ('SYNTH', 'TOP', 'schematic')
            bindings = [{'hierarchy_path': '/I%d' % index, 'target': ('SYNTH', 'UNIT', 'schematic'), 'stop': False}
                        for index in range(4000)]
            path = os.path.join(root, 'binding.records')
            with open(path, 'w') as stream:
                def record(kind, values):
                    stream.write(kind + '\t' + '\t'.join('nil' if value is False else json.dumps(value)
                                                       for value in values) + '\n')
                record('CONFIG_TOP', config + top)
                record('CONFIG_DEFAULTS', config + (False, False, 'schematic', 'spectre'))
                for row in bindings:
                    record('CONFIG_BINDING', config + (row['hierarchy_path'],) + row['target'] + (False,))
            result = self._responsive(lambda: validate_config_binding_records(
                [{'config': config, 'top': top, 'bindings': bindings, 'source_config': None,
                  'fallback_view_list': 'schematic', 'fallback_stop_list': 'spectre'}], path))
            self.assertEqual(result['bindings'], 4000)


if __name__ == '__main__':
    unittest.main()
