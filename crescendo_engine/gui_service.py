"""GUI-only service process. Original catalogues and authority stay in the worker.

The wire format contains data, never import paths or executable callbacks. The
startup runtime/bootstrap environment is inherited from explicit application
startup. A GUI summary cannot be used as a prepared execution identity.
"""
import io
import functools
import inspect
import json
import os
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import traceback

from .runtime import bind_text, child_environment
from . import catalog_json

_contract_text = bind_text()
PROTOCOL = _contract_text('crescendo.gui_service.v1')
SUMMARY = _contract_text('crescendo.prepared_gui_summary.v1')
HIERARCHY_SUMMARY = _contract_text('crescendo.hierarchy_gui_summary.v1')
_HEADER = struct.Struct('!Q')
_MAX_MESSAGE = 1024 * 1024 * 1024


def _receive(sock):
    def read(size):
        chunks = []
        while size:
            data = sock.recv(min(size, 65536))
            if not data:
                raise EOFError('GUI service connection closed')
            chunks.append(data)
            size -= len(data)
        return b''.join(chunks)
    size = _HEADER.unpack(read(_HEADER.size))[0]
    if not size or size > _MAX_MESSAGE:
        raise ValueError('Invalid GUI service message size')
    value = catalog_json.load(io.TextIOWrapper(io.BytesIO(read(size)), encoding='utf-8'))
    if not isinstance(value, dict) or value.get('protocol') != PROTOCOL:
        raise ValueError('Unsupported GUI service protocol')
    return value


def _send(sock, lock, value):
    value = dict(value, protocol=PROTOCOL)
    encoded = ''.join(catalog_json.chunks(value)).encode('utf-8')
    if len(encoded) > _MAX_MESSAGE:
        raise ValueError('GUI service message is too large')
    with lock:
        sock.sendall(_HEADER.pack(len(encoded)))
        sock.sendall(encoded)


def prepared_summary(prepared):
    """Project a view, preserving the untouched worker object and its protocol."""
    result = dict(prepared)
    result['source_schema_version'] = prepared.get('schema_version')
    result['schema_version'] = SUMMARY
    rows = [row for row in prepared.get('parameters') or [] if row.get('optimizable')]
    result['parameters'] = rows
    ids = set(row['parameter_id'] for row in rows)
    result['hierarchy'] = hierarchy_summary(prepared.get('hierarchy') or {}, ids)
    return result


def hierarchy_summary(hierarchy, eligible_ids=None):
    designs = []
    for design in hierarchy.get('designs') or []:
        body = dict(design.get('hierarchy') or {})
        body['parameters'] = [dict(parameter_id=row['parameter_id'], hierarchy_path=row.get('hierarchy_path'))
                              for row in body.get('parameters') or []
                              if eligible_ids is not None and row.get('parameter_id') in eligible_ids]
        designs.append(dict(design, hierarchy=body))
    return dict(hierarchy, designs=designs, schema_version=HIERARCHY_SUMMARY,
                source_schema_version=hierarchy.get('schema_version'))


def _project(value, eligible_ids=None):
    if isinstance(value, dict):
        if (value.get('schema_version') in (_contract_text('crescendo.prepared.v1'),
                                          _contract_text('crescendo.prepared.v2')) and
                'parameters' in value and 'hierarchy' in value):
            return prepared_summary(value)
        prepared = value.get('prepared')
        if isinstance(prepared, dict) and isinstance(prepared.get('parameters'), list):
            eligible_ids = set(row['parameter_id'] for row in prepared['parameters'] if row.get('optimizable'))
        designs = value.get('designs')
        if isinstance(designs, list) and any(isinstance(row, dict) and 'hierarchy' in row for row in designs):
            return hierarchy_summary(value, eligible_ids)
        return dict((key, _project(item, eligible_ids)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return [_project(item, eligible_ids) for item in value]
    return value


def gui_service_class(service_type):
    """Keep public method descriptors/signatures, including class-method calls."""
    def dispatch(name, function):
        @functools.wraps(function)
        def method(self, *args, **kwargs):
            delegate = vars(self).get('_gui_delegate')
            if delegate is not None:
                return delegate.call(name, *args, **kwargs)
            return function(self, *args, **kwargs)
        return method
    for name in dir(service_type):
        if name.startswith('_') or name == 'enable_gui_process':
            continue
        descriptor = inspect.getattr_static(service_type, name)
        if isinstance(descriptor, (staticmethod, classmethod, property)):
            continue
        if inspect.isfunction(descriptor):
            setattr(service_type, name, dispatch(name, descriptor))
    return service_type


class GuiServiceError(RuntimeError):
    pass


def _error_record(error):
    attrs = {}
    for key, value in vars(error).items():
        try:
            json.dumps(value)
        except (TypeError, ValueError):
            continue
        attrs[key] = value
    attrs = _project(attrs)
    for key in ('errno', 'strerror', 'filename', 'filename2'):
        if getattr(error, key, None) is not None:
            attrs[key] = getattr(error, key)
    return {'type': type(error).__name__, 'message': str(error), 'attributes': attrs,
            'traceback': traceback.format_exc()[-65536:]}


def _remote_error(record):
    attrs = record.get('attributes') or {}
    name, message = record.get('type'), record.get('message') or 'GUI service failed'
    if name == 'CommandCancelled':
        from .cadence import CommandCancelled
        error = CommandCancelled(message, result=attrs.get('result'))
    elif name == 'VariableRangeValidationError' and isinstance(attrs.get('report'), dict):
        from .range_validation import VariableRangeValidationError
        error = VariableRangeValidationError(attrs['report'])
    else:
        kind = {'ValueError': ValueError, 'RuntimeError': RuntimeError, 'KeyError': KeyError,
                'TypeError': TypeError, 'OSError': OSError, 'PermissionError': PermissionError,
                'FileNotFoundError': FileNotFoundError}.get(name, GuiServiceError)
        if issubclass(kind, OSError) and attrs.get('errno') is not None:
            error = kind(attrs['errno'], attrs.get('strerror') or message, attrs.get('filename'))
        else:
            error = kind(message)
    for key, value in attrs.items():
        try:
            setattr(error, key, value)
        except (AttributeError, TypeError):
            pass
    error.remote_traceback_tail = record.get('traceback') or ''
    return error


class GuiServiceDelegate(object):
    """Concurrent RPC; monitor/control lanes never wait behind a long request."""
    def __init__(self, service, command=None):
        self.service = service
        self.command = command
        self.process = None
        self.sock = None
        self.start_lock = threading.Lock()
        self.send_lock = threading.Lock()
        self.pending_lock = threading.Lock()
        self.pending = {}
        self.sequence = 0
        self.closed = False
        self.failure = None
        self.stderr_path = None
        self.config_revision = 0
        self.exit_lock = threading.Lock()
        self.exit_result = None
        self.diagnostic_registrations = {}
        self.diagnostic_lock = threading.Lock()
        self.methods = set(name for name in dir(type(service)) if not name.startswith('_')
                           and name != 'enable_gui_process' and callable(getattr(type(service), name, None)))

    def _start(self):
        with self.start_lock:
            if self.process is not None:
                return
            if self.closed:
                raise GuiServiceError('GUI service is closed')
            config = self.service.config
            from .config import default_run_root
            from .utils import ensure_dir
            root = os.path.abspath((config.get('runtime') or {}).get('run_root') or default_run_root())
            ensure_dir(root)
            metadata = os.lstat(root)
            if (os.path.realpath(root) != root or not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid()):
                raise ValueError('Unsafe GUI service work root')
            folder = tempfile.mkdtemp(prefix='.gui-service-', dir=root)
            self.stderr_path = os.path.join(folder, 'worker.log')
            parent, child = socket.socketpair()
            environment = child_environment()
            # Preserve the caller's environment identities. Source selection is
            # explicit startup authority, never supplied by a saved document.
            project = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            startup = ('import sys; sys.path.insert(0, %r); '
                       'from crescendo_engine.gui_service import run_worker; run_worker(int(sys.argv[1]))') % project
            command = self.command or [sys.executable, '-B', '-c', startup]
            try:
                with open(self.stderr_path, 'ab') as log:
                    self.process = subprocess.Popen(command + [str(child.fileno())], pass_fds=(child.fileno(),),
                        stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=environment, start_new_session=True)
                self.sock = parent
                threading.Thread(target=self._read, name='cr_gui_service_reader', daemon=True).start()
                _send(parent, self.send_lock, {'id': 0, 'operation': 'initialize', 'config': config})
            except BaseException:
                parent.close()
                raise
            finally:
                child.close()

    def _read(self):
        try:
            while True:
                response = _receive(self.sock)
                if response.get('operation') == 'diagnostic':
                    threading.Thread(target=self._diagnostic, args=(response,),
                                     name='cr_gui_diagnostic', daemon=True).start()
                    continue
                with self.pending_lock:
                    request = self.pending.pop(response.get('id'), None)
                if request is None:
                    raise ValueError('Unexpected GUI service response identity')
                request[1].append(response)
                request[0].set()
        except BaseException as error:
            self.failure = GuiServiceError('GUI service worker disconnected: %s; evidence: %s' % (error, self.stderr_path))
            with self.pending_lock:
                pending, self.pending = self.pending, {}
                for done, output in pending.values():
                    output.append({'transport_error': self.failure})
                    done.set()
        finally:
            self.sock.close()
            self.process.wait()

    def _diagnostic(self, request):
        # Trusted callbacks remain in their startup process. This lane never
        # runs Tk calls or blocks the response reader / cancellation lane.
        response = {'operation': 'diagnostic_result', 'id': request.get('id')}
        try:
            action, identifier = request['action'], request['identifier']
            observer = getattr(self.service, 'diagnostic_gui_observer', None)
            if action == 'begin':
                from .diagnostic_sections import registered_sections
                with self.diagnostic_lock:
                    self.diagnostic_registrations[identifier] = registered_sections()
                if observer is not None:
                    observer.begin(identifier)
                result = None
            elif action == 'snapshot':
                result = (observer.diagnostic_snapshot(identifier) if observer is not None
                          else {'available': False, 'windows': {}})
            elif action == 'sections':
                from .diagnostic_sections import collect_sections
                from .environment_probes import _redact
                with self.diagnostic_lock:
                    registrations = self.diagnostic_registrations.pop(identifier)
                result = collect_sections(request['context'], registrations,
                    redactor=lambda value: _redact(value, request['environment']))
            else:
                raise ValueError('Unsupported GUI diagnostic action')
            response['result'] = result
        except Exception as error:
            response['error'] = _error_record(error)
        try:
            _send(self.sock, self.send_lock, response)
        except (OSError, EOFError):
            pass  # existing reader reports transport failure and wakes callers

    def _call(self, method, args, kwargs):
        self._start()
        done, output = threading.Event(), []
        with self.pending_lock:
            if self.failure is not None:
                raise self.failure
            if self.closed:
                raise GuiServiceError('GUI service is closed')
            self.sequence += 1
            request_id = self.sequence
            self.pending[request_id] = (done, output)
        try:
            _send(self.sock, self.send_lock, {'id': request_id, 'operation': 'call',
                                             'method': method, 'args': args, 'kwargs': kwargs})
        except BaseException:
            with self.pending_lock:
                self.pending.pop(request_id, None)
            raise
        done.wait()
        response = output[0]
        if 'transport_error' in response:
            raise response['transport_error']
        with self.pending_lock:
            if response.get('config_revision', 0) >= self.config_revision:
                self.config_revision = response.get('config_revision', 0)
                if 'config' in response:
                    self.service.config = response['config']
                self.prepared_ids = set(response.get('prepared_ids') or [])
                for key in list(self.service.prepared):
                    if key not in self.prepared_ids:
                        self.service.prepared.pop(key, None)
        if 'error' in response:
            raise _remote_error(response['error'])
        result = response.get('result')
        if method in ('prepare', 'prepare_cells') and isinstance(result, list) and len(result) == 2:
            if result[0] in self.prepared_ids:
                self.service.prepared[result[0]] = result[1]
            result = tuple(result)
        elif isinstance(result, dict) and result.get('prepared_id') and result.get('prepared'):
            if result['prepared_id'] in self.prepared_ids:
                self.service.prepared[result['prepared_id']] = result['prepared']
        if method == 'select_setup':
            self.service.prepared.clear()
        return result

    def call(self, method, *args, **kwargs):
        if method in ('request_force_exit', 'stop_setup_preparation'):
            # These two existing GUI controls run on Tk. Delivery is asynchronous;
            # this acknowledgement grants no stop/cleanup proof.
            def deliver():
                try:
                    self._call(method, args, kwargs)
                except Exception:
                    pass  # the following control/status/exit RPC reports transport failure
            threading.Thread(target=deliver, name='cr_gui_service_control', daemon=True).start()
            return {'delivery_pending': True, 'cleanup_confirmed': False}
        if method in ('exit_optimizer', 'force_exit_optimizer'):
            with self.exit_lock:
                if self.closed and self.exit_result is not None:
                    return self.exit_result
                result = self._call(method, args, kwargs)
                self.exit_result = result
                self.close(force=method == 'force_exit_optimizer')
                return result
        return self._call(method, args, kwargs)

    def catalogue_action(self, prepared_id, action):
        """Owned diagnostic operations; original objects never cross the wire."""
        return self._call('@catalogue', (prepared_id, action), {})

    def close(self, force=False):
        with self.start_lock:
            if self.closed:
                return
            if self.process is not None and self.failure is None:
                _send(self.sock, self.send_lock, {'operation': 'close', 'force': bool(force)})
            self.closed = True
            if self.sock is not None:
                try:
                    self.sock.shutdown(socket.SHUT_WR)
                except OSError:
                    pass


def _catalogue_action(service, prepared_id, action):
    prepared = service.prepared[prepared_id]
    if action == 'freshness':
        from .prepared_freshness import require_prepared_freshness
        require_prepared_freshness(prepared, service.config, action='start optimization')
    elif action in ('snapshot_save', 'snapshot_read'):
        from .setup_private_session import update_setup_private_session_snapshot, setup_private_session_snapshot
        reference = prepared['setup_private_session']
        if action == 'snapshot_save':
            update_setup_private_session_snapshot(reference, prepared)
        else:
            value = setup_private_session_snapshot(reference)
            return {'parameter_count': len(value.get('parameters') or []), 'schema_version': value.get('schema_version')}
    elif action == 'identity':
        from .setup_private_session import _snapshot_hash
        return {'parameter_count': len(prepared.get('parameters') or []),
                'schema_version': prepared.get('schema_version'), 'sha256': _snapshot_hash(prepared)}
    else:
        raise ValueError('Unsupported GUI catalogue diagnostic')
    return {'ok': True}


class _DiagnosticBridge(object):
    """Worker-side data RPC for parent-owned timing and report callbacks."""
    def __init__(self, sock, send_lock):
        self.sock, self.send_lock = sock, send_lock
        self.lock, self.pending, self.sequence = threading.Lock(), {}, 0

    def _call(self, action, identifier, **data):
        done, output = threading.Event(), []
        with self.lock:
            self.sequence += 1
            identifier_rpc = self.sequence
            self.pending[identifier_rpc] = (done, output)
        try:
            _send(self.sock, self.send_lock, dict(data, operation='diagnostic',
                id=identifier_rpc, action=action, identifier=identifier))
            if not done.wait(30):
                raise GuiServiceError('GUI diagnostic response timed out')
            if 'error' in output[0]:
                raise _remote_error(output[0]['error'])
            return output[0].get('result')
        finally:
            with self.lock:
                self.pending.pop(identifier_rpc, None)

    def receive(self, response):
        with self.lock:
            pending = self.pending.pop(response.get('id'), None)
        if pending is not None:
            pending[1].append(response)
            pending[0].set()

    def begin(self, identifier):
        self._call('begin', identifier)

    def diagnostic_snapshot(self, identifier):
        return self._call('snapshot', identifier)

    def collect_sections(self, context, environment):
        return self._call('sections', context['test_id'], context=context, environment=environment)


def run_worker(descriptor, factory=None):
    # The worker owns the large heap. C JSON and cyclic GC may block its GIL,
    # without blocking Tk or changing the original canonical byte protocol.
    catalog_json._isolated_process = True
    sock = socket.socket(fileno=descriptor)
    sock.set_inheritable(False)
    send_lock = threading.Lock()
    service = None
    force = False
    shutdown_ack = [False]
    cleanup_error = None
    config_lock, config_state = threading.Lock(), [None, 0]
    try:
        request = _receive(sock)
        if request.get('operation') != 'initialize' or request.get('id') != 0:
            raise ValueError('GUI service initialization required')
        if factory is None:
            from .server import OptimizerService
            factory = OptimizerService
        service = factory(request['config'])
        diagnostic_bridge = _DiagnosticBridge(sock, send_lock)
        service.diagnostic_gui_observer = diagnostic_bridge
        service.diagnostic_section_collector = diagnostic_bridge.collect_sections
        allowed = set(name for name in dir(type(service)) if not name.startswith('_')
                      and name != 'enable_gui_process' and callable(getattr(type(service), name, None)))
        def invoke(request):
            response = {'id': request.get('id')}
            try:
                method = request.get('method')
                if method == '@catalogue':
                    result = _catalogue_action(service, *request.get('args', []), **request.get('kwargs', {}))
                elif method in allowed:
                    result = getattr(service, method)(*request.get('args', []), **request.get('kwargs', {}))
                else:
                    raise ValueError('Unsupported GUI service method')
                if method in ('prepare', 'prepare_cells', 'inspect_cells', 'inspect_test_cells', 'inspect_instance_paths', 'load_private_suite', 'update_private_suite', 'load_configuration', 'resume'):
                    result = _project(result)
                response['result'] = result
                if method in ('exit_optimizer', 'force_exit_optimizer'):
                    shutdown_ack[0] = True
            except Exception as error:
                response['error'] = _error_record(error)
            try:
                with config_lock:
                    serialized_config = json.dumps(service.config, sort_keys=True)
                    config_state[:] = [serialized_config, config_state[1] + 1]
                    response['prepared_ids'] = list(service.prepared)
                    response['config'] = json.loads(serialized_config)
                    response['config_revision'] = config_state[1]
                _send(sock, send_lock, response)
            except (OSError, EOFError):
                # A partial frame cannot be retried on the same stream. EOF
                # wakes every caller and requests the existing owned cleanup.
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            except Exception as error:
                # Backend serialization bugs must fail a request, not strand
                # its task lane forever. Preserve an already reported commit
                # error when only the configuration snapshot is unserializable.
                failure = {'id': request.get('id'), 'error': response.get('error') or _error_record(error)}
                try:
                    _send(sock, send_lock, failure)
                except Exception:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
        while True:
            request = _receive(sock)
            if request.get('operation') == 'diagnostic_result':
                diagnostic_bridge.receive(request)
                continue
            if request.get('operation') == 'close':
                force = bool(request.get('force'))
                break
            if request.get('operation') != 'call' or not isinstance(request.get('id'), int):
                raise ValueError('Unsupported GUI service operation')
            threading.Thread(target=invoke, args=(request,), name='cr_gui_service_call', daemon=True).start()
    except EOFError:
        pass
    finally:
        if service is not None and not shutdown_ack[0]:
            try:
                if force:
                    service.force_exit_optimizer()
                else:
                    service.exit_optimizer()
            except Exception as error:
                cleanup_error = error
                traceback.print_exc()
        sock.close()
        if cleanup_error is not None:
            raise RuntimeError("GUI service cleanup is unproven: %s" % cleanup_error)


if __name__ == '__main__':
    run_worker(int(sys.argv[1]))
