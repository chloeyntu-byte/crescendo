"""UI detachment is not a simulation Stop or a resource deletion receipt."""
from __future__ import print_function

import copy
import os
import threading
import uuid

from .utils import atomic_write_json, utc_timestamp


def request_detach(service):
    """Lock-free cancellation fence; the GUI must not wait for a worker lock."""
    service.closing = True
    service.force_exit_requested = True
    service.prepare_cancel_event.set()
    for event in list(service.range_validation_cancels.values()):
        event.set()


def detach_service(service, reason='User forced UI exit'):
    """Best-effort durable isolation on the independent exit thread.

    Never releases a live lease, removes artifacts, marks a task stopped, or
    waits for a native controller. The production GUI owns the final process
    exit; original launch/capture records remain authoritative if storage fails.
    """
    request_detach(service)
    from .setup_private_session import close_setup_private_session, detach_setup_private_session
    references = copy.deepcopy(list(service.setup_private_sessions.values()))
    result = {'schema_version': 'crescendo.ui_detach.v1', 'ui_exit_requested': True,
              'cleanup_confirmed': False, 'artifact_policy': 'retain_and_isolate',
              'pid': os.getpid(), 'owner_uid': os.getuid(), 'reason': str(reason),
              'created_at': utc_timestamp(), 'suites': [], 'runs': [], 'warnings': []}
    for reference in references:
        try:
            # Healthy idle suites keep their normal reuse path. Only a suite
            # whose release is unproven is isolated; never quarantine every
            # suite merely because a different run could not be stopped.
            released = close_setup_private_session(reference, timeout_seconds=0.01)
            if released.get('cleanup_proven') is True:
                result['suites'].append(dict(released, detached=False, state='released'))
            else:
                result['suites'].append(detach_setup_private_session(reference, str(reason)))
        except Exception as error:
            try:
                result['suites'].append(detach_setup_private_session(reference, str(reason) + ': ' + str(error)))
            except Exception as isolation_error:
                result['suites'].append({'session_id': reference.get('session_id'),
                    'root': reference.get('root'), 'detached': False, 'durable': False,
                    'error': '%s: %s' % (type(isolation_error).__name__, isolation_error)})
                result['warnings'].append('Suite isolation record unavailable: ' + str(isolation_error))
    for name in sorted(service.session_run_names):
        result['runs'].append({'run_id': name, 'stop_proven': False,
                               'state': 'retained_pending_reconciliation'})
    runtime = service.config.get('runtime') or {}
    root = runtime.get('design_root') or runtime.get('run_root')
    try:
        if not root or not os.path.isdir(root) or os.path.realpath(root) != os.path.abspath(root):
            raise ValueError('Exit report requires a real configured state directory')
        directory = os.path.join(root, 'ui_exit_reports')
        try:
            os.mkdir(directory, 0o700)
        except FileExistsError:
            pass
        if os.path.islink(directory) or not os.path.isdir(directory) or os.stat(directory).st_uid != os.getuid():
            raise ValueError('Exit report directory is not owned by this user')
        path = os.path.join(directory, uuid.uuid4().hex + '.json')
        result['report_path'] = path
        atomic_write_json(path, result)
    except Exception as error:
        result['warnings'].append('Exit report not durable: ' + str(error))
    # These may wait on a native lock. They must never hold up detachment or
    # prevent the independent GUI timer from closing the process.
    def request_stops():
        manager = getattr(service, 'environment_tests', None)
        if manager is not None:
            try:
                manager.cancel_all()
            except Exception:
                pass
        for name in list(service.session_run_names):
            try:
                service.stop(name)
            except Exception:
                pass
    threading.Thread(target=request_stops, name='crescendo-detached-stop-request', daemon=True).start()
    return result
