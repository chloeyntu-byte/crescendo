"""Source-aware storage roots; no simulator data is written to GUI cache.

All decisions are pure until the caller explicitly creates a run. A native
readback is evidence; an environment setting is only an input to that decision.
"""
from __future__ import print_function

import copy
import hashlib
import json
import os
import re
import stat

from .runtime_layout import design_storage_root, legacy_design_storage_root, project_directory

SAVE_SCHEMA = 'crescendo.maestro_save_options.v1'
POLICY_SCHEMA = 'crescendo.storage_policy.v1'
SAVE_FIELDS = {
    'save_res_dir': ('adexl.results', 'saveResDir'),
    'save_dir': ('adexl.results', 'saveDir'),
    'project_dir': ('asimenv.startup', 'projectDir'),
    'fallback': ('adexl.results', 'fallbackToDirWhenSimResultsUnspecified'),
    'use_local_psf_dir': ('adexl.results', 'useLocalPsfDir'),
    'use_local_rdb_dir': ('adexl.results', 'useLocalRdbDir'),
    'local_psf_dir': ('adexl.results', 'saveLocalPsfDir'),
    'local_rdb_dir': ('adexl.results', 'saveLocalRdbDir'),
}
PATH_FIELDS = frozenset(('save_res_dir', 'save_dir', 'project_dir', 'local_psf_dir', 'local_rdb_dir'))
BOOL_FIELDS = frozenset(('use_local_psf_dir', 'use_local_rdb_dir'))
MAX_PATH_CHARS = 16384


def unavailable_save_options(source='unknown', message=None):
    return {'schema_version': SAVE_SCHEMA, 'source': source, 'available': False,
            'raw': {}, 'field_available': {}, 'effective': {},
            'warnings': [message] if message else []}


def resolve_source_path(value, work_dir, source_home=None, environment=None):
    """Expand only captured source variables; never eval/substitute shell code."""
    if not isinstance(value, str) or not value or len(value) > MAX_PATH_CHARS:
        raise ValueError('Data path must be a nonempty string of at most 16384 characters')
    if any(ord(char) < 32 for char in value):
        raise ValueError('Data path cannot contain control characters')
    environment = dict(environment or {})
    if source_home:
        environment['HOME'] = source_home
    if value == '~' or value.startswith('~/'):
        if not source_home:
            raise ValueError('Source HOME is unavailable for this data path')
        value = source_home + value[1:]
    elif value.startswith('~'):
        raise ValueError('Named-user home expansion is not supported; use an absolute data path')
    def expand(match):
        key = match.group(1) or match.group(2)
        if key not in environment or environment[key] is None:
            raise ValueError('Source environment variable %s is unavailable for this data path' % key)
        return str(environment[key])
    value = re.sub(r'\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)', expand, value)
    if not os.path.isabs(value):
        if not work_dir:
            raise ValueError('Source working directory is unavailable for this data path')
        value = os.path.join(work_dir, value)
    return os.path.realpath(os.path.abspath(value))


def save_options_from_records(rows, source='invoking_native_environment', identity=None):
    identity = identity or {}
    result = unavailable_save_options(source)
    seen = set()
    for tag, fields in rows:
        if tag != 'V' or len(fields) != 3:
            raise ValueError('Invalid native Save Options record')
        key, available, value = fields
        if key in seen or key not in set(SAVE_FIELDS) | {'results_location', 'source_home', 'source_work_dir'}:
            raise ValueError('Duplicate or unsupported native Save Options field')
        seen.add(key)
        if available not in (None, True):
            raise ValueError('Invalid Save Options availability')
        if key in BOOL_FIELDS:
            if value not in (None, True):
                raise ValueError('Invalid Save Options boolean')
            value = value is True if available is True else None
        elif value is not None and (not isinstance(value, str) or len(value) > MAX_PATH_CHARS or any(ord(c) < 32 for c in value)):
            raise ValueError('Invalid native Save Options value')
        result['field_available'][key] = available is True
        result['raw'][key] = value
    result['source_home'] = result['raw'].pop('source_home', None)
    result['work_dir'] = result['raw'].pop('source_work_dir', None) or identity.get('work_dir')
    result['identity'] = dict((key, identity.get(key)) for key in ('library', 'cell', 'view', 'setup_path'))
    result['available'] = all(result['field_available'].get(key) for key in ('save_res_dir', 'save_dir', 'project_dir'))
    work = result['work_dir'] or identity.get('work_dir')
    home = result['source_home']
    effective = result['effective']
    for raw_key, effective_key in (('save_res_dir', 'simulation_root'), ('save_dir', 'rdb_root'), ('project_dir', 'project_dir'), ('results_location', 'results_location')):
        value = result['raw'].get(raw_key)
        if result['field_available'].get(raw_key) and value:
            try:
                effective[effective_key] = resolve_source_path(value, work, home)
            except ValueError as error:
                result['warnings'].append(str(error))
    # axlGetResultsLocation is the public effective simulation location. Strip
    # only its exact native LCV suffix; no history names or guessed parent dirs.
    location = effective.get('results_location')
    suffixes = [os.path.join(identity.get('library') or '', identity.get('cell') or '', identity.get('view') or '', 'results', ending)
                for ending in ('maestro', 'data')]
    if location and all(identity.get(k) for k in ('library', 'cell', 'view')):
        for suffix in suffixes:
            if location.endswith(os.sep + suffix):
                effective['simulation_root'] = location[:-len(suffix)-1] or os.sep
                effective['simulation_root_evidence'] = 'axlGetResultsLocation_exact_lcv'
                break
        else:
            # Valid public path may vary by release: preserve it explicitly,
            # while an explicit raw root remains usable without pretending the
            # unknown suffix was understood.
            result['warnings'].append('Native results location has an unfamiliar LCV suffix; raw explicit save root is shown separately.')
    if not effective.get('simulation_root'):
        fallback = result['raw'].get('fallback')
        if result['field_available'].get('fallback') and fallback == 'projectDir' and effective.get('project_dir'):
            effective['simulation_root'] = effective['project_dir']
            effective['simulation_root_evidence'] = 'native_projectDir_fallback'
        elif result['field_available'].get('fallback') and fallback == 'RdbDir' and effective.get('rdb_root'):
            effective['simulation_root'] = effective['rdb_root']
            effective['simulation_root_evidence'] = 'native_RdbDir_fallback'
    if not effective.get('rdb_root') and result['field_available'].get('save_dir') and result['raw'].get('save_dir') in (None, ''):
        view_dir = identity.get('view_dir')
        if view_dir:
            effective['rdb_root'] = os.path.realpath(view_dir)
            effective['rdb_root_evidence'] = 'native_empty_saveDir_source_view'
    if not effective.get('simulation_root'):
        result['warnings'].append('Original simulation data root is unavailable; select an explicit data-root override before a real run.')
    return result


def resolve_storage_policy(config=None, setup=None, source=None, libraries=None):
    config, setup = config or {}, setup or {}
    context = config.get('_maestro_launch_context') or {}
    source = source or config.get('_selected_setup_source') or context.get('source') or {}
    if not source:
        locator = setup.get('setup_locator') or {}
        source = {'library': locator.get('library_name'), 'cell': locator.get('cell_name'),
                  'view': locator.get('view_name'), 'setup_path': locator.get('path'),
                  'view_dir': locator.get('view_dir'), 'work_dir': project_directory(config)}
    work = project_directory(config, source)
    runtime = config.get('runtime') or {}
    metadata_override = os.environ.get('CRESCENDO_RUN_ROOT') or runtime.get('storage_base_override') or context.get('storage_base_override')
    design_root = runtime.get('design_root') or context.get('design_root') or design_storage_root(source, libraries or context.get('libraries') or {}, base_root=metadata_override)
    original = copy.deepcopy(setup.get('save_options') or context.get('save_options') or unavailable_save_options())
    override = runtime.get('data_root_override')
    explicit_run = (os.environ.get('CRESCENDO_RUN_ROOT') or runtime.get('run_root_override') or runtime.get('storage_base_override') or
                    (runtime.get('run_root') if runtime.get('run_root_origin') not in ('derived', 'native_save_options') else None))
    effective = original.get('effective') or {}
    source_home = original.get('source_home') or (context.get('source') or {}).get('source_home') or os.environ.get('HOME')
    if override is not None:
        data_root = resolve_source_path(override, work, source_home)
        origin = 'manual_override'
    elif explicit_run:
        namespace_override = bool(context.get('storage_base_override') or runtime.get('storage_namespace_override'))
        run_root = os.path.join(design_root, 'runs') if namespace_override else resolve_source_path(explicit_run, work, source_home)
        data_root, origin = run_root, 'explicit_run_root'
    elif effective.get('simulation_root'):
        data_root = resolve_source_path(effective['simulation_root'], work, source_home)
        origin = original.get('source') or 'native_save_options'
    else:
        # UI/probes/mock still work when discovery is optional. Real Start must
        # require available native roots or a user override via require_data_root.
        data_root, origin = os.path.join(work, 'simulation'), 'working_directory_fallback'
    refresh = context.get('saved_setup_refresh') or {}
    unconfirmed_reload = bool(refresh and origin == 'working_directory_fallback')
    warnings = (["Original Maestro save root was unavailable; using the working directory simulation root. Data Root Override can change it."] if origin == 'working_directory_fallback' else [])
    if unconfirmed_reload:
        warnings = ["Saved setup was reloaded, but its current simulation data root is unconfirmed. Select Data Root Override before a real run; no working-directory fallback will be used."]
        hint = refresh.get('previous_simulation_root_hint')
        if hint:
            warnings.append("Previous simulation root is for comparison only: " + str(hint))
    module = os.path.basename(design_root)
    run_root = (run_root if origin == 'explicit_run_root' else os.path.join(data_root, 'crescendo', module)) if data_root else os.path.join(design_root, 'runs')
    return {'schema_version': POLICY_SCHEMA, 'design_root': design_root,
            'states_root': os.path.join(design_root, 'states'), 'state_dir': os.path.join(design_root, 'states'),
            'cache_root': os.path.join(design_root, 'cache'), 'index_path': os.path.join(design_root, 'index.json'),
            'legacy_design_root': legacy_design_storage_root(source, libraries or context.get('libraries') or {}, base_root=metadata_override),
            'legacy_settings_root': os.path.join(legacy_design_storage_root(source, libraries or context.get('libraries') or {}, base_root=metadata_override), 'settings'),
            'current_legacy_settings_root': os.path.join(design_root, 'settings'),
            'run_root': run_root, 'data_root': data_root, 'source': origin,
            'available': data_root is not None and not unconfirmed_reload, 'override': override,
            'saved_reload_root_unconfirmed': unconfirmed_reload,
            'original': original, 'native_original_available': bool(effective.get('simulation_root')),
            'warnings': warnings,
            'effective': dict(copy.deepcopy(effective), data_root=data_root), 'work_dir': work}


def validate_native_data_path(path):
    """Bounded native execution contract, separate from portable UI/file paths.

    IC618 CLA expands literal dollar signs in its own cwd, and native job/wave
    directory handling fails with whitespace. Quoting Python argv or cds.lib
    tokens cannot qualify those native data paths. Never create an alias to
    conceal the real run root from ownership/completion checks.
    """
    if not isinstance(path, str) or not path or any(char in path for char in "\x00\r\n"):
        raise ValueError('Native simulation data directory is invalid. Set Data Root Override in Optimization.')
    if any(char.isspace() or char in "\"'$" for char in path):
        raise ValueError('Cadence native simulation data paths currently require no spaces, quotes or dollar signs. Set Data Root Override in Optimization to a plain directory: %s' % path)
    return path


def require_data_root(policy):
    if not policy.get('available'):
        if policy.get('saved_reload_root_unconfirmed'):
            raise ValueError('Saved setup reload could not confirm its current simulation data root. Select Data Root Override in Optimization; the previous path is only a hint and no fallback directory will be used.')
        raise ValueError('Simulation data root is unavailable. Set Data Root Override in Optimization; the source Maestro Save Options were not readable.')
    validate_native_data_path(policy.get('data_root'))
    validate_native_data_path(policy.get('run_root'))
    return policy


def native_workspace_storage(root, save_options=None):
    root = os.path.realpath(os.path.abspath(root))
    base = os.path.join(root, 'simulation')
    return {'schema_version': 'crescendo.native_storage.v1', 'project_dir': base,
            'save_res_dir': base, 'save_dir': base, 'scratch_root': os.path.join(root, '_scratch'),
            'readback_path': os.path.join(root, 'native_storage.records'), 'verified': False,
            'local_psf_dir': os.path.join(root, '_scratch', 'local_psf'),
            'local_rdb_dir': os.path.join(root, '_scratch', 'local_rdb'),
            'source_local_options': copy.deepcopy(save_options or {})}


def native_storage_init_lines(storage):
    quote = lambda value: json.dumps(value, ensure_ascii=False)
    lines = ["envSetVal(\"%s\" \"%s\" 'string %s)" % (partition, field, quote(storage[key]))
             for key, partition, field in [('project_dir', 'asimenv.startup', 'projectDir'),
                                           ('save_res_dir', 'adexl.results', 'saveResDir'),
                                           ('save_dir', 'adexl.results', 'saveDir')]]
    original = storage.get('source_local_options') or {}
    raw, available = original.get('raw') or {}, original.get('field_available') or {}
    for key in ('use_local_psf_dir', 'use_local_rdb_dir', 'local_psf_dir', 'local_rdb_dir'):
        if available.get(key):
            partition, field = SAVE_FIELDS[key]
            value = raw.get(key)
            if key in BOOL_FIELDS:
                lines.append("envSetVal(\"%s\" \"%s\" 'boolean %s)" % (partition, field, 't' if value else 'nil'))
            elif isinstance(value, str):
                # Preserve the source enable flags, but native local staging
                # must share this run's ownership and cleanup boundary.
                local = storage[key]
                enabled = 'use_local_psf_dir' if key == 'local_psf_dir' else 'use_local_rdb_dir'
                if raw.get(enabled) and available.get(enabled):
                    from .utils import ensure_dir
                    ensure_dir(local)
                lines.append("envSetVal(\"%s\" \"%s\" 'string %s)" % (partition, field, quote(local)))
    return lines


def native_storage_capture_definitions():
    return r'''
procedure(crescWriteStorageReadback(port hsdb)
  let((result)
    result = nil
    when(isCallable('axlGetResultsLocation) result = errset(axlGetResultsLocation(hsdb) t))
    fprintf(port "V\t%L\t%L\t%L\n" "results_location" if(result && stringp(car(result)) then t else nil) if(result then car(result) else nil))
  )
)
procedure(crescWriteSaveOptionsRecords(port hsdb)
  let((result)
    foreach(row '(("save_res_dir" "adexl.results" "saveResDir")
                  ("save_dir" "adexl.results" "saveDir")
                  ("project_dir" "asimenv.startup" "projectDir")
                  ("fallback" "adexl.results" "fallbackToDirWhenSimResultsUnspecified")
                  ("use_local_psf_dir" "adexl.results" "useLocalPsfDir")
                  ("use_local_rdb_dir" "adexl.results" "useLocalRdbDir")
                  ("local_psf_dir" "adexl.results" "saveLocalPsfDir")
                  ("local_rdb_dir" "adexl.results" "saveLocalRdbDir"))
      result = errset(envGetVal(cadr(row) caddr(row)) nil)
      fprintf(port "V\t%L\t%L\t%L\n" car(row) if(result then t else nil) if(result then car(result) else nil))
    )
    fprintf(port "V\t%L\tt\t%L\n" "source_home" getShellEnvVar("HOME"))
    fprintf(port "V\t%L\tt\t%L\n" "source_work_dir" getWorkingDir())
    crescWriteStorageReadback(port hsdb)
  )
)
procedure(crescWriteNativeHistoryPaths(path history)
  let((port name result rdb location)
    when(!history || equal(history 0) error("Crescendo native history handle invalid.\n"))
    printf("CRESCENDO_NATIVE_RUN_STAGE HistoryName BEGIN\n")
    result = errset(axlGetHistoryName(history) t)
    unless(result error("Crescendo native history name lookup failed.\n"))
    name = car(result)
    unless(stringp(name) error("Crescendo native history name unavailable.\n"))
    printf("CRESCENDO_NATIVE_RUN_STAGE HistoryName RETURN\n")
    rdb = nil location = nil
    when(isCallable('axlGetHistoryResults)
      result = errset(axlGetHistoryResults(history) t)
      when(result && stringp(car(result)) rdb = car(result))
    )
    when(isCallable('axlGetHistoryResultsDir)
      result = errset(axlGetHistoryResultsDir(history) t)
      when(result && stringp(car(result)) location = car(result))
    )
    port = outfile(path)
    unless(port error("Crescendo native history path readback cannot be created.\n"))
    fprintf(port "CRESCENDO_NATIVE_HISTORY_STORAGE_1\nH\t%L\t%L\t%L\n" name rdb location)
    close(port)
  )
)
'''


def native_storage_readback_lines(workspace, setup_expr='mainSDB'):
    storage = workspace.get('native_storage')
    if not storage:
        return []
    quote = lambda value: json.dumps(value, ensure_ascii=False)
    return ['crescStoragePort = outfile(%s)' % quote(storage['readback_path']),
            'unless(crescStoragePort error("Crescendo native storage readback cannot be created.\\n"))',
            'fprintf(crescStoragePort "CRESCENDO_NATIVE_STORAGE_1\\n")',
            'crescWriteSaveOptionsRecords(crescStoragePort %s)' % setup_expr,
            'close(crescStoragePort)', 'crescStoragePort = nil']


def load_native_storage_readback(workspace, required=False):
    storage = workspace.get('native_storage')
    if not storage:
        return None
    from .maestro_context import _fields
    try:
        with open(storage['readback_path'], 'rb') as stream:
            raw = stream.read(256*1024+1)
        if len(raw) > 256*1024:
            raise ValueError('Native storage readback exceeds size limit')
        lines = raw.decode('utf-8').splitlines()
        if not lines or lines[0] != 'CRESCENDO_NATIVE_STORAGE_1':
            raise ValueError('Native storage readback schema mismatch')
        rows = []
        for line in lines[1:]:
            if line.strip():
                tag, sep, fields = line.partition('\t')
                if not sep:
                    raise ValueError('Invalid native storage readback')
                rows.append((tag, _fields(fields)))
        identity = {'library': workspace['primary_library'], 'cell': workspace['setup_cell'],
                    'view': workspace.get('setup_view') or 'maestro', 'work_dir': workspace['root']}
        options = save_options_from_records(rows, source='owned_native_session', identity=identity)
        location = (options.get('effective') or {}).get('results_location')
        expected = os.path.realpath(os.path.join(storage['save_res_dir'], identity['library'], identity['cell'], identity['view'], 'results', 'maestro'))
        if location is not None and location != expected:
            raise ValueError('Native simulation root differs from the owned run: expected %s; actual %s' % (expected, location))
        for key in ('save_dir', 'save_res_dir', 'project_dir'):
            actual = resolve_source_path(options['raw'].get(key), workspace['root'], options.get('source_home'))
            if actual != os.path.realpath(storage[key]):
                raise ValueError('Native %s readback differs from the owned run' % key)
        original = storage.get('source_local_options') or {}
        original_available = original.get('field_available') or {}
        original_raw = original.get('raw') or {}
        local_warnings = []
        for key in ('use_local_psf_dir', 'use_local_rdb_dir', 'local_psf_dir', 'local_rdb_dir'):
            if not original_available.get(key):
                continue
            if not options['field_available'].get(key):
                local_warnings.append('%s readback unavailable; fresh owned point completion remains required' % key)
                continue
            actual = options['raw'].get(key)
            expected_local = original_raw.get(key) if key in BOOL_FIELDS else storage[key]
            if key not in BOOL_FIELDS:
                actual = resolve_source_path(actual, workspace['root'], options.get('source_home'))
            if actual != expected_local:
                raise ValueError('Native %s readback differs from the owned local staging configuration' % key)
        storage.update(verified=location is not None, configured_roots_verified=True, results_location=location, readback=options)
        if local_warnings:
            storage['local_readback_warnings'] = local_warnings
        if location is None:
            storage['readback_warning'] = 'Public setup results location was unavailable; fresh RDB/point completion and native history paths remain required.'
        return options
    except (OSError, ValueError, KeyError) as error:
        storage.update(verified=False, readback_error=str(error))
        if required:
            raise ValueError('Owned native storage qualification failed: %s' % error)
        return None


def workspace_rdb_path(workspace, history_name):
    storage = workspace.get('native_storage')
    if storage:
        return os.path.join(storage['save_dir'], workspace['primary_library'], workspace['setup_cell'],
                            workspace.get('setup_view') or 'maestro', 'results', 'maestro', history_name + '.rdb')
    return os.path.join(workspace['setup_dir'], 'results', 'maestro', history_name + '.rdb')


def native_history_readback_path(workspace, artifact_root):
    return os.path.join(artifact_root, 'native_history_storage.records') if workspace.get('native_storage') else None


def native_history_readback_line(workspace, artifact_root, history_expr='runData'):
    path = native_history_readback_path(workspace, artifact_root)
    return 'crescWriteNativeHistoryPaths(%s %s)' % (json.dumps(path, ensure_ascii=False), history_expr) if path else None


def qualify_native_history_paths(workspace, artifact_root, history_name, started_epoch=None, required=True):
    """Bind public history paths to this protocol-completed history, then let
    existing completion validate unique fresh RDB/PSF/netlist/point evidence.
    """
    path = native_history_readback_path(workspace, artifact_root)
    if not path:
        return None
    from .maestro_context import _fields
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        if not required:
            return None
        raise
    with os.fdopen(descriptor, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 65536:
            raise ValueError('Native history storage readback is not a unique regular bounded file')
        if started_epoch is not None and info.st_mtime + 2 < started_epoch:
            raise ValueError('Native history storage readback is stale')
        rows = stream.read(65537).decode('utf-8').splitlines()
    if len(rows) != 2 or rows[0] != 'CRESCENDO_NATIVE_HISTORY_STORAGE_1' or not rows[1].startswith('H\t'):
        raise ValueError('Invalid native history storage readback')
    fields = _fields(rows[1].partition('\t')[2])
    if len(fields) != 3 or fields[0] != history_name:
        raise ValueError('Native history storage readback identity mismatch')
    expected_rdb = workspace_rdb_path(workspace, history_name)
    storage = workspace['native_storage']
    expected_history = os.path.join(storage['save_res_dir'], workspace['primary_library'], workspace['setup_cell'],
                                    workspace.get('setup_view') or 'maestro', 'results', 'maestro', history_name)
    result = {'history_name': history_name, 'record_path': path, 'available': fields[1] is not None and fields[2] is not None}
    for value, expected, key in ((fields[1], expected_rdb, 'rdb_path'), (fields[2], expected_history, 'results_dir')):
        if value is not None:
            actual = resolve_source_path(value, workspace['root'])
            if actual != os.path.realpath(expected):
                raise ValueError('Native history %s differs from the owned run: expected %s; actual %s' % (key, expected, actual))
            result[key] = actual
    if not result['available']:
        result['warning'] = 'Public history paths partially unavailable; existing fresh file/point completion remains required.'
    return result
