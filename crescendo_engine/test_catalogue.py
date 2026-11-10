"""Saved Test headers only; analysis/output/CDF inspection waits for selection."""
import copy
import os

from .ade_setup import (resolve_setup_locator, _setupdb_root, _active_node,
                        _children, _child, _name, _text, _option_map,
                        _enabled, _disabled_test_names)
from .utils import sha256_file


def load_test_catalogue(path, config, backend='auto'):
    binding = (config or {}).get('_setup_baseline')
    if binding and os.path.realpath(path) == os.path.realpath(binding.get('origin_setup_path') or ''):
        from .setup_baseline import load_bound_setup
        setup = load_bound_setup(path, config, backend=backend)
        rows = setup.get('_source_tests') or setup.get('tests') or []
        result = {key: copy.deepcopy(setup[key]) for key in
                  ('backend', 'setup_locator', 'source', 'baseline', 'mode') if key in setup}
        result['tests'] = [{key: row[key] for key in
                            ('name', 'enabled', 'library', 'cell', 'view', 'simulator', 'state') if key in row}
                           for row in rows]
    else:
        locator = resolve_setup_locator(path, backend=backend)
        state = os.path.join(locator['view_dir'], 'active.state')
        def identity():
            return {'sha256': sha256_file(locator['path']),
                    'active_state_sha256': sha256_file(state) if os.path.isfile(state) else None}
        before = identity()
        root = _setupdb_root(locator['path'])
        active = _active_node(root)
        disabled = _disabled_test_names(root)
        rows = []
        for test in _children(_child(active, 'tests'), 'test'):
            name, options = _name(test, 'unnamed'), _option_map(test)
            rows.append({'name': name, 'enabled': _enabled(test) and name not in disabled,
                         'library': options.get('lib') or options.get('library') or locator['library_name'],
                         'cell': options.get('cell') or locator['cell_name'],
                         'view': options.get('view') or 'schematic',
                         'simulator': options.get('sim') or options.get('simulator'),
                         'state': options.get('state')})
        # Old ADE views without SetupDB Test entries require their state
        # directory to identify Tests. Keep that existing fallback explicit.
        if not rows:
            from .ade_setup import inspect_setup
            setup = inspect_setup(path, backend=backend)
            rows = [{key: row[key] for key in
                     ('name', 'enabled', 'library', 'cell', 'view', 'simulator', 'state') if key in row}
                    for row in setup.get('tests') or []]
        source = {'setup_db': locator['path'], 'setup_dir': locator['view_dir'],
                  'active_state': state if os.path.isfile(state) else None,
                  'library_root': locator['library_root'], 'sha256': before['sha256']}
        source['maestro_sdb' if locator['backend'] == 'maestro' else 'data_sdb'] = locator['path']
        result = {'backend': locator['backend'], 'setup_locator': locator,
                  'source': source, 'mode': _text(_child(active, 'currentmode')) or None, 'tests': rows}
        if identity() != before:
            raise ValueError('ADE setup changed while reading Tests; Load and Inspect again.')
        result['metadata_identity'] = before
    result['schema_version'] = 'crescendo.test_catalogue.v1'
    result['metadata_only'] = True
    return result
