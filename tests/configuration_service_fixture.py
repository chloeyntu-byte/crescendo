"""Use the real configuration orchestration with fixture-only catalogue probes."""
import copy
import os

from crescendo_engine.ade_setup import resolve_setup_locator
from crescendo_engine.server import OptimizerService


def load_fixture_configuration(service, document, path=None):
    config = copy.deepcopy(getattr(service, 'config', {}) or {})
    config.setdefault('runtime', {})['run_root'] = os.path.join(
        os.path.dirname(path), '.fixture-configuration-runtime')
    backend = OptimizerService(config)
    backend._inspect_cells_registered = service.inspect_cells
    if hasattr(service, 'fixture_tests'):
        tests = service.fixture_tests()
        backend._inspect_tests_registered = lambda path, backend=None: (
            'fixture-tests', {'tests': copy.deepcopy(tests), 'setup': {'tests': copy.deepcopy(tests)}})
        backend._inspect_test_cells_registered = lambda token, name, max_depth=32: service.inspect_cells(
            path, max_depth=max_depth, selected_test=name)
    backend._prepare_cells_registered = service.prepare_cells
    backend._restore_configuration_solver_overrides = lambda document, warnings: {}
    def select(selected):
        locator = resolve_setup_locator(selected)
        backend.config.setdefault('_selected_setup_source', {
            'library': locator['library_name'], 'cell': locator['cell_name'], 'view': locator['view_name']})
        return {'setup_path': selected, 'context': None, 'mode': 'manual'}
    backend._select_setup_registered = select
    backend.eda_setup = lambda *args, **kwargs: service.eda_setup()
    return backend.load_configuration(document, path=path)
