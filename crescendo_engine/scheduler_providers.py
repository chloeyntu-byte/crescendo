"""Explicit scheduler plugins; execution and evidence authority stay in the engine.

Plugins never receive a process handle. The caller owns the bounded runner,
Stop deadline, output archive and one owned Job ID. Registration is startup
code, not a module name recovered from a checkpoint or runtime descriptor.
"""
import copy
import re
import threading

PROVIDER_ABI = 'scheduler.provider.v1'
_METHODS = ('probe', 'prepare_submission', 'query_command', 'cancel_command',
            'parse_submission', 'parse_observation', 'resource_request', 'log_artifacts')
_providers = {}
_frozen = False
_lock = threading.RLock()


class TemplateProvider(object):
    abi = PROVIDER_ABI
    version = '1'

    def probe(self, commands):
        from .scheduler_cli import TEMPLATE_KEYS, SCHEMA
        missing = [key for key in TEMPLATE_KEYS if not commands.get(key)]
        return {'schema_version': SCHEMA, 'source': 'user_templates',
                'available': not missing, 'query_template': commands.get(TEMPLATE_KEYS[0], ''),
                'cancel_template': commands.get(TEMPLATE_KEYS[1], ''),
                'reason': ('Cluster mode requires ' + ', '.join(missing)) if missing else None}

    def prepare_submission(self, plan):
        return copy.deepcopy(plan)

    def submission_bindings(self, plan):
        # Optional argv-position declarations; no execution or evidence authority.
        return None

    def query_command(self, commands, job_id, values):
        from .scheduler_cli import scheduler_command_argv
        template = (commands['scheduler_query_template'] if 'scheduler_query_template' in commands
                    else self.probe(copy.deepcopy(commands)).get('query_template'))
        return scheduler_command_argv(template, job_id, values)

    def cancel_command(self, commands, job_id, values):
        from .scheduler_cli import scheduler_command_argv
        template = (commands['scheduler_cancel_template'] if 'scheduler_cancel_template' in commands
                    else self.probe(copy.deepcopy(commands)).get('cancel_template'))
        return scheduler_command_argv(template, job_id, values)

    def parse_submission(self, stdout, stderr):
        from .scheduler_protocol import parse_submit_streams
        return parse_submit_streams(stdout, stderr)

    def parse_observation(self, text, job_id):
        from .scheduler_protocol import parse_scheduler_observation
        return parse_scheduler_observation(text, job_id)

    def resource_request(self, argv):
        from .simulators import submission_cpu
        return {'cpu': submission_cpu(argv)}

    def log_artifacts(self, task_root):
        import os
        return {key: os.path.join(task_root, 'scheduler.' + key) for key in ('stdout', 'stderr')}


def register_provider(name, provider):
    with _lock:
        if _frozen:
            raise RuntimeError('Scheduler registry is already in use')
        if not re.fullmatch(r'[a-z][a-z0-9_-]*', name) or name in _providers:
            raise ValueError('Invalid or duplicate scheduler provider')
        if (getattr(provider, 'abi', None) != PROVIDER_ABI or
                not isinstance(getattr(provider, 'version', None), str) or
                any(not callable(getattr(provider, method, None)) for method in _METHODS)):
            raise ValueError('Incomplete scheduler provider contract')
        _providers[name] = provider


def get_provider():
    global _frozen
    from .runtime import contract
    with _lock:
        _frozen = True
        name = contract()['scheduler']
        if name not in _providers:
            raise ValueError('Application must register the selected provider before engine use')
        expected = contract().get('provider_identity')
        if expected is not None and registered_provider_identity(name) != expected:
            raise ValueError('Scheduler provider identity changed after startup')
        return _providers[name]


def registered_provider_identity(name):
    import hashlib
    import json
    import os
    import sys
    provider = _providers.get(name)
    if provider is None:
        raise ValueError('Provider must be explicitly registered by application startup')
    module = sys.modules.get(type(provider).__module__)
    path = getattr(module, '__file__', None)
    if path and os.path.isfile(path):
        with open(path, 'rb') as stream:
            digest = hashlib.sha256(stream.read()).hexdigest()
    else:
        # Interactive startup can use a provider locally. A child still needs
        # a pinned bootstrap file and must match the parent's content identity.
        methods = _METHODS + tuple(name for name in ('submission_bindings', 'probe_context') if callable(getattr(provider, name, None)))
        code = [getattr(type(provider), method).__code__.co_code.hex() for method in methods]
        digest = hashlib.sha256(json.dumps(code).encode('ascii')).hexdigest()
    return {'name': name, 'abi': provider.abi, 'version': provider.version, 'sha256': digest}


def provider_identity():
    provider = get_provider()
    from .runtime import contract
    return {'name': contract()['scheduler'], 'abi': provider.abi, 'version': provider.version}


def _provider_probe(commands, context=None):
    provider = get_provider()
    contextual = getattr(provider, 'probe_context', None)
    if context is None or not callable(contextual):
        return provider.probe(copy.deepcopy(commands))
    import os
    import time
    supplied = dict(context)
    runner = supplied.pop('_help_runner', None)
    cache = supplied.pop('_help_cache', None)
    deadline = supplied.pop('deadline', None)
    cancel = supplied.pop('cancel', None)
    environment = supplied.get('environment')
    root = supplied.get('root')
    # The provider may request help only. It receives neither the general
    # command runner nor a process handle, and cannot issue query/cancel here.
    def help_probe(binary, option):
        if (not isinstance(binary, str) or not os.path.isabs(binary) or
                option not in ('--help', '-h')):
            raise ValueError('Provider probe requires an absolute help executable')
        remaining = 5.0 if deadline is None else min(5.0, deadline - time.monotonic())
        if remaining <= 0 or (cancel and (cancel.is_set() if hasattr(cancel, 'is_set') else cancel())):
            return {'returncode': None, 'cancelled': True, 'stdout_tail': '', 'stderr_tail': ''}
        if runner is not None:
            return runner([binary, option], cwd=root, env=environment, timeout_seconds=remaining,
                          termination_grace_seconds=1, termination_kill_seconds=1)
        from .environment_probes import bounded_command
        info = os.stat(binary) if cache is not None else None
        key = (os.path.realpath(binary), info.st_ino, info.st_size, info.st_mtime, option) if info else None
        if cache is not None and key in cache:
            return copy.deepcopy(cache[key])
        result = bounded_command([binary, option], cwd=root, env=environment, timeout=remaining, cancel=cancel)
        if cache is not None:
            cache[key] = copy.deepcopy(result)
        return result
    return contextual(copy.deepcopy(commands), copy.deepcopy(supplied), help_probe)


def _execution_templates(commands, context=None):
    """Resolve absent legacy capabilities only in a transient execution plan."""
    legacy_fields = {'schema_version', 'fingerprint', 'solver_templates', 'submission_template'}
    legacy = set(commands) == legacy_fields
    if legacy:
        from .execution_commands import validate_execution_commands
        validate_execution_commands(commands)
    provider = get_provider()
    proposed = _provider_probe(commands, context)
    if not isinstance(proposed, dict):
        raise ValueError('Invalid scheduler capability')
    resolved = copy.deepcopy(commands)
    if legacy:
        from .execution_commands import _template, scheduler_template_argv
        for kind in ('query', 'cancel'):
            template = _template(proposed.get(kind + '_template'), 'Scheduler capability')
            if template:
                scheduler_template_argv(template)
            resolved['scheduler_' + kind + '_template'] = template
    return resolved, proposed


def capability(commands, context=None):
    resolved, proposed = _execution_templates(commands, context)
    baseline = TemplateProvider().probe(resolved)
    result = dict(proposed)
    result['available'] = bool(baseline['available'] and proposed.get('available'))
    # Read-only observation can be proven without any cancellation authority.
    result['query_available'] = bool(baseline['query_template'] and
                                     proposed.get('query_available', proposed.get('available')))
    for key in ('query_template', 'cancel_template'):
        result[key] = baseline[key]
    if not result['available'] and not result.get('reason'):
        result['reason'] = baseline['reason'] or 'Scheduler provider is unavailable'
    return result


def command(kind, commands, job_id, values=None, context=None):
    from .scheduler_cli import scheduler_command_argv
    if kind not in ('query', 'cancel'):
        raise ValueError('Invalid scheduler command kind')
    resolved, unused = _execution_templates(commands, context)
    expected = scheduler_command_argv(resolved['scheduler_' + kind + '_template'], job_id, values)
    proposed = getattr(get_provider(), kind + '_command')(copy.deepcopy(resolved if context is not None else commands), job_id, dict(values or {}))
    # Every command must bind the resolved one-ID template, including legacy
    # provider capabilities. Explicit empty settings remain unconfigured.
    if proposed != expected:
        raise ValueError('Provider command differs from the configured owned-job template')
    return proposed


def submission(stdout, stderr):
    from .scheduler_protocol import parse_submit_streams
    raw = parse_submit_streams(stdout, stderr)
    proposed = get_provider().parse_submission(stdout, stderr)
    if not isinstance(proposed, dict):
        raise ValueError('Invalid provider submission observation')
    if any(proposed.get(key) != raw.get(key) for key in
           ('accepted', 'invalid', 'rejected', 'ambiguous', 'job_id', 'observed_job_ids')):
        raise ValueError('Provider submission is unsupported by raw evidence')
    return raw


def observation(text, job_id):
    from .scheduler_protocol import parse_scheduler_observation
    raw = parse_scheduler_observation(text, job_id)
    proposed = get_provider().parse_observation(text, job_id)
    if not isinstance(proposed, dict) or any(proposed.get(key) != raw.get(key) for key in
            ('job_id', 'observed_job_ids', 'state', 'terminal', 'invalid', 'not_found', 'task_states')):
        raise ValueError('Provider observation is unsupported by raw evidence')
    return raw


def prepare_submission(plan):
    from .execution_commands import _fingerprint, _reject_shell
    if not isinstance(plan, dict) or plan.get('fingerprint') != _fingerprint(plan):
        raise ValueError('Provider received an invalid submission plan')
    proposed = get_provider().prepare_submission(copy.deepcopy(plan))
    if not isinstance(proposed, dict) or set(proposed) != set(plan):
        raise ValueError('Provider changed the submission plan contract')
    if any(proposed[key] != plan[key] for key in plan if key != 'argv'):
        raise ValueError('Provider changed frozen task bindings or evidence')
    argv = proposed['argv']
    original = plan.get('argv') or []
    if (not isinstance(argv, list) or not argv or
            any(not isinstance(arg, str) or not arg or '\x00' in arg or '\n' in arg for arg in argv)):
        raise ValueError('Invalid provider submission arguments')
    _reject_shell(argv)
    if argv != original:
        script = (plan.get('bindings') or {}).get('job_script')
        # Additional scheduler arguments are allowed only before the engine
        # freezes the resulting plan. Original argv stay ordered and intact;
        # the selected executable and exact owned script cannot be replaced.
        remaining = iter(argv)
        ordered = all(any(value == item for value in remaining) for item in original)
        if (not plan.get('remote') or not original or argv[0] != original[0] or
                not script or argv.count(script) != 1 or not ordered):
            raise ValueError('Provider changed the owned submission boundary')
        proposed['resource_cpu'] = resource_request(argv)['cpu']
        if 'scheduler_io' in proposed:
            bindings = plan['bindings']
            proposed['scheduler_io'] = {'workdir': bindings.get('cwd')}
            for kind in ('stdout', 'stderr'):
                path = bindings['scheduler_' + kind]
                if sum(arg.count(path) for arg in argv) > 1:
                    raise ValueError('Provider duplicated the owned scheduler log')
                proposed['scheduler_io'][kind] = path if any(
                    arg == path or arg.endswith('=' + path) for arg in argv) else None
    _bind_submission_io(proposed)
    proposed['fingerprint'] = _fingerprint(proposed)
    return proposed


def _owned_path(path, base, root):
    import os
    if not isinstance(path, str) or not path or any(ord(c) < 32 for c in path):
        raise ValueError('Invalid scheduler path')
    absolute = os.path.abspath(os.path.join(base, path))
    if os.path.commonpath([root, absolute]) != root:
        raise ValueError('Scheduler path escapes the owned point')
    # Check every existing component, including broken links. Future paths are
    # permitted, but an existing symlink is never a source of task evidence.
    current = absolute
    while True:
        if os.path.islink(current):
            raise ValueError('Scheduler path contains a symlink')
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return absolute


def _bind_submission_io(plan):
    import os
    provider = get_provider()
    declare = getattr(provider, 'submission_bindings', None)
    declarations = declare(copy.deepcopy(plan)) if callable(declare) else None
    if declarations is None:
        return
    if not plan.get('remote') or not isinstance(declarations, dict) or set(declarations) != {'workdir', 'stdout', 'stderr'}:
        raise ValueError('Invalid scheduler binding declarations')
    bindings = plan['bindings']
    root = bindings.get('owned_task_root')
    cwd = bindings.get('cwd')
    if (not isinstance(root, str) or not os.path.isabs(root) or
            not isinstance(cwd, str) or not os.path.isabs(cwd)):
        raise ValueError('Scheduler bindings require an explicit owned point boundary')
    root = os.path.abspath(root)
    cwd = _owned_path(cwd, root, root)
    _owned_path(bindings['job_script'], root, root)
    argv = plan['argv']
    raw = {}
    used_options, used_indices = set(), set()
    for kind in ('workdir', 'stdout', 'stderr'):
        declaration = declarations[kind]
        if declaration is None:
            raw[kind] = None
            continue
        if not isinstance(declaration, dict) or set(declaration) != {'option', 'index'}:
            raise ValueError('Invalid scheduler binding declaration')
        option, index = declaration['option'], declaration['index']
        if (not isinstance(option, str) or not re.fullmatch(r'--?[A-Za-z][A-Za-z0-9_-]*', option) or
                type(index) is not int or not 0 < index < len(argv) or
                option in used_options or index in used_indices):
            raise ValueError('Invalid or duplicate scheduler binding')
        occurrences = [i for i, arg in enumerate(argv) if arg == option or arg.startswith(option + '=')]
        if len(occurrences) != 1:
            raise ValueError('Scheduler option must occur exactly once')
        occurrence = occurrences[0]
        if argv[occurrence] == option:
            if index != occurrence + 1:
                raise ValueError('Scheduler binding is not its option value')
            value = argv[index]
        else:
            if index != occurrence:
                raise ValueError('Scheduler binding is not its joined option value')
            value = argv[index][len(option) + 1:]
        if value == bindings['job_script']:
            raise ValueError('Scheduler binding consumes the owned job script')
        raw[kind] = value
        used_options.add(option)
        used_indices.add(index)
    workdir = _owned_path(raw['workdir'] or cwd, cwd, root)
    result = {'workdir': workdir}
    for kind in ('stdout', 'stderr'):
        path = raw[kind]
        result[kind] = _owned_path(path, workdir, root) if path is not None else None
        if result[kind] == os.path.abspath(bindings['job_script']):
            raise ValueError('Scheduler log aliases the owned job script')
        if result[kind] is not None:
            bindings['scheduler_' + kind] = result[kind]
    if result['stdout'] is not None and result['stdout'] == result['stderr']:
        raise ValueError('Scheduler streams require distinct paths')
    plan['scheduler_io'] = result
    plan['scheduler_binding_declarations'] = copy.deepcopy(declarations)


def resource_request(argv):
    result = get_provider().resource_request(list(argv))
    if not isinstance(result, dict) or set(result) != {'cpu'}:
        raise ValueError('Invalid scheduler resource request')
    cpu = result['cpu']
    if cpu is not None and (type(cpu) is not int or not 1 <= cpu <= 4096):
        raise ValueError('Invalid scheduler CPU request')
    return result


def log_artifacts(task_root):
    import os
    result = get_provider().log_artifacts(task_root)
    if not isinstance(result, dict) or set(result) != {'stdout', 'stderr'}:
        raise ValueError('Invalid scheduler log artifacts')
    root = os.path.realpath(task_root)
    if any(not isinstance(path, str) or os.path.realpath(path) != path or
           os.path.dirname(path) != root for path in result.values()):
        raise ValueError('Scheduler log artifacts escape the owned task')
    return result


register_provider('template', TemplateProvider())
