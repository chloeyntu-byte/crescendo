"""Immutable application contract and data-only child-process bootstrap.

Initialize before importing engine consumers. Descriptors carry no import paths
or executable code; only the application's explicit startup may register code.
"""
import collections
import hashlib
import json
import os
import re
import threading
import stat
from types import MappingProxyType

ENGINE_ABI = 'crescendo.engine.v1'
DESCRIPTOR_ENV = 'CRESCENDO_ENGINE_RUNTIME'
DESCRIPTOR_SCHEMA = 'engine.runtime.v1'
BOOTSTRAP_ENV = 'CRESCENDO_ENGINE_BOOTSTRAP'
MAX_BOOTSTRAP_BYTES = 1024 * 1024


def _mapping(value):
    result = dict(value or {})
    if any(not isinstance(k, str) or not k or not isinstance(v, str) or not v or
           any(ord(c) < 32 for c in k + v) for k, v in result.items()):
        raise ValueError('Aliases must be nonempty strings without controls')
    return MappingProxyType(result)


class ApplicationIdentity(collections.namedtuple('_Identity',
        'display_name env_prefix env_aliases schema_namespace schema_aliases slug version')):
    __slots__ = ()

    def __new__(cls, display_name='Crescendo', env_prefix='CRESCENDO', env_aliases=(),
                schema_namespace='crescendo', schema_aliases=None, slug='crescendo', version=None):
        if version is not None and (not isinstance(version,str) or not re.fullmatch(r'[0-9][A-Za-z0-9_.+-]*',version)):
            raise ValueError('Invalid application version')
        for value in (env_prefix,) + tuple(env_aliases):
            if not isinstance(value, str) or not re.fullmatch(r'[A-Z][A-Z0-9_]*', value):
                raise ValueError('Invalid environment prefix')
        for value in (schema_namespace, slug):
            if not isinstance(value, str) or not re.fullmatch(r'[a-z][a-z0-9_.-]*', value):
                raise ValueError('Invalid application namespace')
        if (not isinstance(display_name, str) or not display_name or
                any(ord(c) < 32 or c in '"\'\\$`' for c in display_name)):
            raise ValueError('Invalid application display name')
        for key, value in dict(schema_aliases or {}).items():
            if any(not isinstance(v, str) or not re.fullmatch(r'[a-z][a-z0-9_.-]*', v)
                   for v in (key, value)):
                raise ValueError('Invalid schema alias')
        return super(ApplicationIdentity, cls).__new__(cls, display_name, env_prefix,
            tuple(env_aliases), schema_namespace, _mapping(schema_aliases), slug, version)


class StorageLayout(collections.namedtuple('_Storage',
        'config_home state_directory result_extension log_filename owned_marker view_prefix temporary_prefix leaf_aliases')):
    __slots__ = ()

    def __new__(cls, config_home='crescendo', state_directory='.crescendo',
                result_extension='.crescendo', log_filename='crescendo.log',
                owned_marker='.crescendo-owned-view.json', view_prefix='cr_s_',
                temporary_prefix='.crescendo-create-', leaf_aliases=None):
        values = (config_home, state_directory, result_extension, log_filename,
                  owned_marker, view_prefix, temporary_prefix)
        if any(not isinstance(v, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+', v) or
               v in ('.', '..') for v in values):
            raise ValueError('Layout values must be safe leaf names')
        for key, value in dict(leaf_aliases or {}).items():
            if any(not isinstance(v, str) or not re.fullmatch(r'[A-Za-z0-9_.-]+', v) or
                   v in ('.', '..') for v in (key, value)):
                raise ValueError('Storage aliases must be safe leaf names')
        return super(StorageLayout, cls).__new__(cls, *(values + (_mapping(leaf_aliases),)))


class ProtocolDialect(collections.namedtuple('_Dialect',
        'procedure_prefix symbol_prefix procedure_aliases callback_aliases error_prefix magic_aliases guardian_token_prefix adapter_abi adapter_entry scheduler_aliases')):
    __slots__ = ()

    def __new__(cls, procedure_prefix='cresc', symbol_prefix='cr_', procedure_aliases=None,
                callback_aliases=None, error_prefix='CRESCENDO', magic_aliases=None,
                guardian_token_prefix='crescendo2-', adapter_abi='crescendo.pdk_adapter.v1',
                adapter_entry='crescPdkApplyParameter', scheduler_aliases=None):
        for value in (procedure_prefix, symbol_prefix, adapter_entry):
            if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
                raise ValueError('Invalid protocol symbol')
        if not isinstance(adapter_abi, str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_.-]*', adapter_abi):
            raise ValueError('Invalid adapter ABI')
        if not re.fullmatch(r'[A-Za-z0-9_-]+', guardian_token_prefix):
            raise ValueError('Invalid guardian token prefix')
        if not isinstance(error_prefix, str) or not re.fullmatch(r'[A-Z][A-Z0-9_]*', error_prefix):
            raise ValueError('Invalid error prefix')
        for key, value in dict(magic_aliases or {}).items():
            if any(not isinstance(v, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]+', v) for v in (key, value)):
                raise ValueError('Invalid protocol magic alias')
        for aliases in (procedure_aliases, callback_aliases):
            for key, value in dict(aliases or {}).items():
                if any(not isinstance(v, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', v)
                       for v in (key, value)):
                    raise ValueError('Protocol aliases must be single identifiers')
        for alias, mode in dict(scheduler_aliases or {}).items():
            if (not isinstance(alias, str) or not re.fullmatch(r'[a-z][a-z0-9_-]*', alias) or
                    alias in ('auto', 'batch', 'receipt') or mode not in ('auto', 'batch', 'receipt')):
                raise ValueError('Invalid scheduler enum alias')
        return super(ProtocolDialect, cls).__new__(cls, procedure_prefix, symbol_prefix,
            _mapping(procedure_aliases), _mapping(callback_aliases), error_prefix,
            _mapping(magic_aliases), guardian_token_prefix, adapter_abi, adapter_entry,
            _mapping(scheduler_aliases))


_lock = threading.RLock()
_state = None
_frozen = False
_codec = None
_bootstrap_running = False


def _plain(value):
    if hasattr(value, '_asdict'):
        return {key: _plain(item) for key, item in value._asdict().items()}
    if isinstance(value, (dict, type(MappingProxyType({})))):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=True).encode('utf-8')).hexdigest()


def initialize(identity=None, storage=None, dialect=None, adapter_manifests=None,
               resource_groups=None, scheduler='template', worker_bootstrap=None):
    """Select one immutable contract, once per process, before engine imports.

    adapter_manifests replaces the built-in registry when supplied. Resource
    groups are explicit {group: {root, files: {relative_name: sha256}}} values.
    No descriptor can name a Python module or authorize an import.
    """
    global _state
    with _lock:
        if _state is not None or _frozen:
            raise RuntimeError('Engine runtime is already initialized')
        value = {'identity': _plain(identity or ApplicationIdentity()),
                 'storage': _plain(storage or StorageLayout()),
                 'dialect': _plain(dialect or ProtocolDialect()),
                 'adapter_manifests': _plain(adapter_manifests),
                 'resource_groups': _plain(resource_groups or {}), 'scheduler': scheduler,
                 'provider_identity': _selected_provider_identity(scheduler), 'registry_sha256': None}
        _validate(value)
        authority = bootstrap_manifest(worker_bootstrap) if worker_bootstrap is not None else None
        _state = json.loads(json.dumps(value))
        os.environ.pop(BOOTSTRAP_ENV, None)
        if worker_bootstrap is not None:
            # This authority comes from explicit application startup, never
            # from a checkpoint or the data-only runtime descriptor.
            os.environ[BOOTSTRAP_ENV] = json.dumps(authority, sort_keys=True)
        _bridge_environment()
        if adapter_manifests is not None:
            from .pdk import adapter_registry_identity
            _state['registry_sha256'] = adapter_registry_identity()['fingerprint']
        # Propagate data even through subprocesses using a copied environment.
        os.environ[DESCRIPTOR_ENV] = descriptor_json()


def _validate(value):
    if not isinstance(value, dict) or set(value) != {'identity', 'storage', 'dialect', 'adapter_manifests', 'resource_groups', 'scheduler', 'provider_identity', 'registry_sha256'}:
        raise ValueError('Unsupported runtime fields')
    ApplicationIdentity(**value['identity'])
    StorageLayout(**value['storage'])
    ProtocolDialect(**value['dialect'])
    if not isinstance(value['scheduler'], str) or not re.fullmatch(r'[a-z][a-z0-9_-]*', value['scheduler']):
        raise ValueError('Invalid scheduler provider identity')
    registry = value['registry_sha256']
    if registry is not None and (not isinstance(registry, str) or not re.fullmatch(r'[0-9a-f]{64}', registry)):
        raise ValueError('Invalid adapter registry content identity')
    provider = value['provider_identity']
    if provider is not None and (not isinstance(provider, dict) or
            set(provider) != {'name', 'abi', 'version', 'sha256'} or
            provider['name'] != value['scheduler'] or
            any(not isinstance(v, str) or not v for v in provider.values()) or
            not re.fullmatch(r'[0-9a-f]{64}', provider['sha256'])):
        raise ValueError('Invalid provider content identity')
    if value['adapter_manifests'] is not None and not isinstance(value['adapter_manifests'], list):
        raise ValueError('Adapter manifests must be data objects')
    if not isinstance(value['resource_groups'], dict):
        raise ValueError('Resource groups must be an object')
    for group, item in value['resource_groups'].items():
        if (not isinstance(group, str) or not re.fullmatch(r'[a-z][a-z0-9_-]*', group) or
                not isinstance(item, dict) or set(item) != {'root', 'files'} or not isinstance(item['files'], dict)):
            raise ValueError('Invalid resource group')
        root = item['root']
        if not isinstance(root, str) or not os.path.isabs(root) or os.path.realpath(root) != root:
            raise ValueError('Resource root must be an explicit canonical directory')
        for name, digest in item['files'].items():
            if (not isinstance(name, str) or os.path.isabs(name) or
                    any(part in ('', '.', '..') for part in name.replace('\\', '/').split('/')) or
                    not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest)):
                raise ValueError('Invalid resource identity')


def _bridge_environment():
    identity = _state['identity']
    prefix = identity['env_prefix'] + '_'
    for alias in identity['env_aliases']:
        for name, value in list(os.environ.items()):
            if name.startswith(alias + '_'):
                key = prefix + name[len(alias) + 1:]
                if key not in os.environ:
                    os.environ[key] = value


def contract():
    global _state, _frozen
    with _lock:
        if _state is None:
            # The public default adds no environment variables or descriptor.
            _state = {'identity': _plain(ApplicationIdentity()), 'storage': _plain(StorageLayout()),
                      'dialect': _plain(ProtocolDialect()), 'adapter_manifests': None,
                      'resource_groups': {}, 'scheduler': 'template', 'provider_identity': None, 'registry_sha256': None}
        _frozen = True
        return json.loads(json.dumps(_state))


def descriptor():
    value = contract()
    payload = {'schema': DESCRIPTOR_SCHEMA, 'engine_abi': ENGINE_ABI, 'runtime': value}
    return dict(payload, sha256=_digest(payload))


def descriptor_json():
    return json.dumps(descriptor(), sort_keys=True, separators=(',', ':'))


def validate_descriptor(raw):
    if not isinstance(raw, str) or len(raw) > 1024 * 1024:
        raise ValueError('Invalid runtime descriptor size')
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Duplicate runtime descriptor key')
            result[key] = value
        return result
    data = json.loads(raw, object_pairs_hook=unique_pairs)
    if not isinstance(data, dict) or set(data) != {'schema', 'engine_abi', 'runtime', 'sha256'}:
        raise ValueError('Invalid runtime descriptor fields')
    payload = {key: data[key] for key in ('schema', 'engine_abi', 'runtime')}
    if (data['schema'] != DESCRIPTOR_SCHEMA or data['engine_abi'] != ENGINE_ABI or
            data['sha256'] != _digest(payload)):
        raise ValueError('Runtime descriptor identity mismatch')
    _validate(data['runtime'])
    return data['runtime']


def child_environment(environ=None):
    result = dict(os.environ if environ is None else environ)
    if DESCRIPTOR_ENV in os.environ:
        result[DESCRIPTOR_ENV] = descriptor_json()
        if BOOTSTRAP_ENV in os.environ:
            result[BOOTSTRAP_ENV] = os.environ[BOOTSTRAP_ENV]
    return result


def text(value):
    """Resolve a source literal at construction, before user data interpolation.

    Call sites are explicit. Never run this over generated scripts, persisted
    documents, user expressions, paths, or other caller supplied text.
    """
    global _codec
    if _codec is not None:
        return _codec(value)
    c = contract()
    i, s, d = c['identity'], c['storage'], c['dialect']
    if (i == _plain(ApplicationIdentity()) and s == _plain(StorageLayout()) and
            d == _plain(ProtocolDialect())):
        _codec = lambda source: source
        return value
    replacements = dict(s['leaf_aliases'])
    replacements.update(d['magic_aliases'])
    replacements.update(d['procedure_aliases'])
    replacements.update(d['callback_aliases'])
    replacements.update(i['schema_aliases'])
    replacements['crescPdkApplyParameter'] = d['adapter_entry']
    replacements['crescendo.pdk_adapter.v1'] = d['adapter_abi']
    replacements.update({'.crescendo-owned-view.json': s['owned_marker'],
        '.crescendo-create-': s['temporary_prefix'], 'cr_s_': s['view_prefix'],
        'crescendo.log': s['log_filename'], '.crescendo': s['state_directory'],
        'optimization.crescendo': 'optimization' + s['result_extension'],
        '*.crescendo': '*' + s['result_extension'], 'crescendo2-': d['guardian_token_prefix'],
        'CRESCENDO_ADAPTER_ERROR': d['error_prefix'] + '_ADAPTER_ERROR',
        'CRESCENDOFW_': i['env_prefix'] + 'FW_'})
    # Caller-supplied exact aliases override generic field defaults.
    for aliases in (s['leaf_aliases'], d['magic_aliases'], d['procedure_aliases'],
                    d['callback_aliases'], i['schema_aliases']):
        replacements.update(aliases)
    # Exact aliases take priority, and substitutions never cascade into aliases.
    pattern = '|'.join(re.escape(key) for key in sorted(replacements, key=len, reverse=True))
    pattern += r'|CRESCENDO(?=[_-])|Crescendo|crescendo(?=\\?\.)|\bcresc(?=[A-Z])|\bcr(?=[A-Z])|\bCR_|\bcr_|\bcrescendo\b'
    def replace(match):
        key = match.group(0)
        if key in replacements:
            return replacements[key]
        if key == 'CRESCENDO':
            return d['error_prefix'] if match.string[match.end():].startswith('-') else i['env_prefix']
        if key == 'crescendo':
            return i['schema_namespace'] if match.string[match.end():].startswith(('.', '\\.')) else i['slug']
        return {'Crescendo': i['display_name'], 'crescendo': i['schema_namespace'],
                'cresc': d['procedure_prefix'], 'cr': d['symbol_prefix'].rstrip('_'),
                'CR_': d['symbol_prefix'].upper(), 'cr_': d['symbol_prefix']}[key]
    compiled = re.compile(pattern)

    def resolve(source):
        if (source not in replacements and
                re.fullmatch(r'[A-Za-z0-9_.-]+\.crescendo', source)):
            return source[:-len('.crescendo')] + s['result_extension']
        return compiled.sub(replace, source)
    _codec = resolve
    return resolve(value)


def bind_text():
    """Freeze at consumer import, including modules with only function bodies."""
    contract()
    return text



def schema_family(value, prefix):
    """Classify an aliased type without rewriting the recorded schema value."""
    if not isinstance(value, str):
        return False
    aliases = contract()['identity']['schema_aliases']
    return value.startswith(text(prefix)) or any(
        key.startswith(prefix) and alias == value for key, alias in aliases.items())


def scheduler_choices():
    """Accepted persisted values, including application-declared legacy names."""
    return ('auto', 'batch', 'receipt') + tuple(contract()['dialect'].get('scheduler_aliases', {}))


def scheduler_mode(value):
    """Resolve execution semantics without changing any persisted identity."""
    if not isinstance(value, str) or value not in scheduler_choices():
        raise ValueError('Command scheduler must be auto, batch or receipt')
    return contract()['dialect'].get('scheduler_aliases', {}).get(value, value)


def config_home():
    return contract()['storage']['config_home']


def result_extension():
    return contract()['storage']['result_extension']


def _selected_provider_identity(name):
    if name == 'template':
        return None
    from .scheduler_providers import registered_provider_identity
    return registered_provider_identity(name)


def bootstrap_manifest(path):
    """Pin one application-owned worker startup file, supplied by startup code."""
    if not isinstance(path, str) or not os.path.isabs(path) or os.path.realpath(path) != path:
        raise ValueError('Worker bootstrap must be an explicit canonical file')
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or
                info.st_uid not in (0, os.getuid()) or info.st_mode & 0o022 or
                info.st_size > MAX_BOOTSTRAP_BYTES):
            raise ValueError('Worker bootstrap must be a protected owned regular file')
        data = stream.read(MAX_BOOTSTRAP_BYTES + 1)
    return {'engine_abi': ENGINE_ABI, 'path': path, 'sha256': hashlib.sha256(data).hexdigest()}


def _load_worker_bootstrap():
    global _bootstrap_running
    raw = os.environ.get(BOOTSTRAP_ENV)
    expected = _state.get('provider_identity')
    if not raw:
        if expected is not None:
            raise ValueError('Selected provider needs application-owned worker bootstrap')
        return
    if len(raw) > 16384:
        raise ValueError('Worker bootstrap authority exceeds size limit')
    authority = json.loads(raw)
    if not isinstance(authority, dict) or set(authority) != {'engine_abi', 'path', 'sha256'}:
        raise ValueError('Invalid worker bootstrap authority')
    actual = bootstrap_manifest(authority['path'])
    if actual != authority:
        raise ValueError('Worker bootstrap content identity changed')
    # Read and hash the exact bytes that execute; a path replacement after the
    # previous stat/check cannot substitute unverified code.
    with open(actual['path'], 'rb') as stream:
        data = stream.read(MAX_BOOTSTRAP_BYTES + 1)
    if hashlib.sha256(data).hexdigest() != actual['sha256']:
        raise ValueError('Worker bootstrap changed while loading')
    _bootstrap_running = True
    try:
        import sys
        import types
        module = types.ModuleType('__engine_worker_startup__')
        module.__file__ = actual['path']
        sys.modules[module.__name__] = module
        exec(compile(data, actual['path'], 'exec'), module.__dict__)
    finally:
        _bootstrap_running = False
    if expected is not None and _selected_provider_identity(_state['scheduler']) != expected:
        raise ValueError('Child scheduler provider content identity mismatch')


def application_root(fallback):
    """Use the registered code resource root as the application's launch cwd.

    The manifest-backed application_code group is optional. Its canonical root
    and file hashes have already passed the immutable runtime validation.
    """
    registration = contract()['resource_groups'].get('application_code')
    return registration['root'] if registration is not None else fallback


def build_identity():
    """Diagnostic identity only; never a Resume or execution authorization."""
    value = contract()
    return {'engine_abi': ENGINE_ABI, 'runtime_sha256': descriptor()['sha256'],
            'identity': value['identity'], 'provider': value['provider_identity'],
            'resource_groups_sha256': _digest(value['resource_groups']),
            'registry_sha256': value['registry_sha256']}


if DESCRIPTOR_ENV in os.environ:
    _incoming = validate_descriptor(os.environ[DESCRIPTOR_ENV])
    _state = _incoming
    _bridge_environment()
    _load_worker_bootstrap()
