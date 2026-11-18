"""Explicit test startup for the GUI transport; never selected by data."""
import gc
import hashlib
import json
import os
import sys
import threading
import tempfile
import time
from crescendo_engine.runtime import bind_text, contract
from crescendo_engine.gui_service import run_worker

_text = bind_text()


class FixtureService(object):
    def __init__(self, config):
        self.config = config
        self.prepared = {}
        self.cancel = threading.Event()
        if config.get('fail_startup'):
            raise ValueError('synthetic worker startup failure')

    def prepare(self, count=18000):
        rows = [{'parameter_id': 'p%d' % i, 'hierarchy_path': '/I%d' % i,
                 'optimizable': i % 20 == 0, 'current_value': '2u', 'name': 'w',
                 'callback': 'synthetic callback ' * 30,
                 'occurrence_chain': [{'path': '/TOP', 'instance': 'I%d' % i}]} for i in range(count)]
        value = {'schema_version': _text('crescendo.prepared.v1'), 'parameters': rows,
                 'hierarchy': {'designs': [{'hierarchy': {'parameters': rows, 'errors': [], 'cells': []}}]},
                 'setup': {'selected_test': 'synthetic'}}
        self.prepared['prepared'] = value
        return 'prepared', value

    def load_private_suite(self, name):
        identifier, prepared = self.prepare()
        return {'prepared_id': identifier, 'prepared': prepared,
                'inspection': {'hierarchy': prepared['hierarchy'], 'cell_scopes': []}}

    def start(self, prepared_id, spec=None):
        value = self.prepared[prepared_id]
        # The worker deliberately holds its own GIL and collects a large heap.
        before = sys.getswitchinterval()
        try:
            sys.setswitchinterval(1.)
            until = time.monotonic() + .35
            while time.monotonic() < until:
                pass
            gc.collect()
            identity = hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        finally:
            sys.setswitchinterval(before)
        return {'original_count': len(value['parameters']), 'original_schema': value['schema_version'], 'sha256': identity}

    def diagnostic_end_to_end(self):
        from unittest import mock
        from tests.test_environment_test import FixtureService as DiagnosticFixture
        from crescendo_engine.environment_test import EnvironmentTestManager
        with tempfile.TemporaryDirectory(dir=self.config['runtime']['run_root']) as root:
            service = DiagnosticFixture(root)
            service.diagnostic_gui_observer = self.diagnostic_gui_observer
            service.diagnostic_section_collector = self.diagnostic_section_collector
            manager = EnvironmentTestManager(service)
            service.manager = manager
            spec = {'variables': [{'parameter_id': 'x', 'minimum': 1e-6, 'maximum': 2e-6}],
                    'objectives': [{'output_key': 'main::gain', 'direction': 'maximize'}], 'simulator': 'spectre'}
            with mock.patch('crescendo_engine.environment_test._case_capability', return_value={'ready': True}), \
                 mock.patch('crescendo_engine.environment_test.run_environment_probes', return_value=[]), \
                 mock.patch('crescendo_engine.environment_test.query_scheduler_jobs', return_value=[]):
                identifier = manager.start('prepared-fixture', spec)['test_id']
                if not manager.join(30):
                    raise RuntimeError('Fixture diagnostic did not terminate')
                payload = manager.progress(identifier)
            with open(payload['main_log'], encoding='utf-8') as stream:
                payload['exported_report'] = stream.read()
            return payload

    def diagnostic_begin(self, identifier):
        self.diagnostic_gui_observer.begin(identifier)

    def diagnostic_report(self, identifier):
        from crescendo_engine.diagnostic_focus import setup_check
        gui = self.diagnostic_gui_observer.diagnostic_snapshot(identifier)
        sections = self.diagnostic_section_collector({'test_id': identifier, 'run_ids': {}}, {})
        return {'F4': setup_check({'available': True}, gui), 'sections': sections}

    def wait_for_cancel(self):
        if not self.cancel.wait(10):
            raise RuntimeError('cancel request was not delivered')
        return {'cancelled': True}

    def stop_setup_preparation(self):
        self.cancel.set()
        return {'stop_requested': True}

    def request_force_exit(self):
        self.cancel.set()

    def range_fail(self):
        from crescendo_engine.range_validation import VariableRangeValidationError
        raise VariableRangeValidationError({'issues': [{'message': 'synthetic range failure'}],
                                            'variables': [{'parameter_id': 'p1'}]})

    def fail(self):
        error = ValueError('synthetic commit failure')
        error.configuration_commit_state = {'phase': 'committed', 'revision': 'r1'}
        error.prepared = self.prepared.get('prepared')
        raise error

    def cancel_configuration(self, partial=None, result=None):
        from crescendo_engine.cadence import CommandCancelled
        error = CommandCancelled('synthetic configuration cancelled', result=result)
        error.configuration_import = partial
        raise error

    def non_json_result(self):
        return {1, 2}

    def non_json_config(self):
        self.config['invalid'] = {1, 2}
        return None

    def repair_config(self):
        self.config.pop('invalid', None)

    def crash(self):
        os._exit(7)

    def contract(self):
        return contract()

    def exit_optimizer(self):
        self.cancel.set()
        if self.config.pop('fail_cleanup', False):
            raise RuntimeError('synthetic cleanup unproven')
        if self.config.get('shutdown_file'):
            with open(self.config['shutdown_file'], 'w') as stream:
                json.dump({'state': 'closed'}, stream)
        return {'state': 'closed', 'cleanup_confirmed': True}

    def force_exit_optimizer(self):
        return {'state': 'detached', 'cleanup_confirmed': False}


if __name__ == '__main__':
    run_worker(int(sys.argv[1]), FixtureService)
