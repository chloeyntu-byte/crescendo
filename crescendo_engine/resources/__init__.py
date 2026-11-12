"""Filesystem-safe access to resources shipped with the optimizer."""

from __future__ import print_function

import hashlib
import os
import stat
import tempfile


_PACKAGE_ROOT = os.path.realpath(os.path.dirname(os.path.abspath(__file__)))
_SOURCE_ROOT = os.path.realpath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
_KNOWN_TOOLS = frozenset((
    "ocean_worker_guardian.py",
    "maestro_job_observer.py",
    "simulation_toolchain.example.json",
    "submit_spectre.py",
    "submit_spectre_job.sh",
))
_KNOWN_PDK_ADAPTERS = frozenset((
    "cadence_pas_transaction.il",
))
_KNOWN_CADENCE_RESOURCES = frozenset((
    "crescendo_launch",
    "crescendo_maestro_menu.il",
))


def _safe_join(root, parts):
    if not parts:
        return root
    normalized = []
    for part in parts:
        if not isinstance(part, str) or not part or os.path.isabs(part):
            raise ValueError("resource path components must be non-empty relative strings")
        normalized.extend(part.replace("\\", "/").split("/"))
    if any(part in ("", ".", "..") for part in normalized):
        raise ValueError("resource path contains an unsafe component")
    candidate = os.path.realpath(os.path.join(root, *normalized))
    if candidate == root or not candidate.startswith(root + os.sep):
        raise ValueError("resource path escapes its resource root")
    return candidate


def _candidate_roots(group):
    return (
        _safe_join(_PACKAGE_ROOT, (group,)),
        _safe_join(_SOURCE_ROOT, (group,)),
    )


def resource_path(group, *parts):
    """Return an existing resource path from an install or source checkout."""
    from ..runtime import contract
    registration = contract()['resource_groups'].get(group)
    if registration is not None:
        root = registration['root']
        if not parts:
            return root
        candidate = _safe_join(root, parts)
        relative = os.path.relpath(candidate, root).replace(os.sep, '/')
        expected = registration['files'].get(relative)
        if expected is None or not os.path.isfile(candidate) or os.path.islink(candidate):
            raise ValueError('Resource is not in the application manifest')
        with open(candidate, 'rb') as stream:
            observed = hashlib.sha256(stream.read()).hexdigest()
        if observed != expected:
            raise ValueError('Application resource content identity changed')
        return candidate
    for root in _candidate_roots(group):
        candidate = _safe_join(root, parts) if parts else root
        if os.path.exists(candidate):
            return _source_resource(candidate, group, parts)
    relative = "/".join((group,) + parts)
    raise RuntimeError("packaged resource is missing: %s" % relative)



# Only immutable engine source templates are rendered, before any application
# data interpolation. Registered private resources are returned byte-for-byte.
_SOURCE_TEMPLATES = frozenset((('tools', 'submit_spectre_job.sh'),
    ('cadence', 'crescendo_launch'), ('cadence', 'crescendo_maestro_menu.il')))


def _source_resource(path, group, parts):
    if len(parts) != 1 or (group, parts[0]) not in _SOURCE_TEMPLATES:
        return path
    from ..runtime import text
    with open(path, 'r', encoding='utf-8') as stream:
        source = stream.read()
    rendered = text(source)
    if rendered == source:
        return path
    if group == 'cadence' and parts[0] == 'crescendo_launch':
        import shlex
        marker = 'cr_module_root="$(cd -- "$cr_resource_dir/.." && pwd -P)"'
        # The source template is resolved against the shipped package root.
        module_root = os.path.dirname(os.path.dirname(_PACKAGE_ROOT))
        rendered = rendered.replace(text(marker), text('cr_module_root=') + shlex.quote(module_root))
    raw = rendered.encode('utf-8')
    digest = hashlib.sha256(raw).hexdigest()
    directory = os.path.join(_cache_root(), 'runtime-' + digest)
    if not os.path.isdir(directory):
        os.makedirs(directory, 0o700)
    info = os.lstat(directory)
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or
            info.st_mode & 0o077 or os.path.realpath(directory) != directory):
        raise ValueError('Runtime template cache is not owner-only')
    destination = os.path.join(directory, parts[0])
    if os.path.lexists(destination):
        flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
        fd = os.open(destination, flags)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or
                    info.st_nlink != 1 or info.st_mode & 0o077 or stream.read() != raw):
                raise ValueError('Runtime template content identity changed')
        return destination
    fd, temporary = tempfile.mkstemp(prefix='.template-', dir=directory)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o700)
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return destination

def resource_origin(group, *parts):
    """Describe whether a resolved resource came from a package or checkout."""
    path = resource_path(group, *parts)
    package_group = _safe_join(_PACKAGE_ROOT, (group,))
    origin = "package" if path == package_group or path.startswith(package_group + os.sep) else "source"
    return {"origin": origin, "path": path}


def web_root():
    return resource_path("web")


def tool_path(name):
    if name not in _KNOWN_TOOLS:
        raise ValueError("unknown packaged tool: %s" % name)
    path = resource_path("tools", name)
    if not os.path.isfile(path):
        raise RuntimeError("packaged tool is not a regular file: %s" % path)
    return path


def pdk_adapter_path(name):
    from ..runtime import contract
    registered = contract()['resource_groups'].get('pdk_adapters', {}).get('files', {})
    if name not in _KNOWN_PDK_ADAPTERS and name not in registered:
        raise ValueError("unknown packaged PDK adapter: %s" % name)
    path = resource_path("pdk_adapters", name)
    if not os.path.isfile(path):
        raise RuntimeError("packaged PDK adapter is not a regular file: %s" % path)
    return path


def cadence_resource_path(name):
    """Resolve native menu resources in an installed wheel or source tree."""
    from ..runtime import text
    if name not in {text(value) for value in _KNOWN_CADENCE_RESOURCES}:
        raise ValueError("unknown packaged Cadence resource: %s" % name)
    path = resource_path("cadence", name)
    if not os.path.isfile(path):
        raise RuntimeError("packaged Cadence resource is not a regular file: %s" % path)
    return path


def _cache_root(environ=None):
    environ = os.environ if environ is None else environ
    base = environ.get("XDG_CACHE_HOME")
    if not base:
        base = os.path.join(os.path.expanduser("~"), ".cache")
    from ..runtime import contract
    return os.path.join(os.path.abspath(os.path.expanduser(base)), contract()["identity"]["slug"], "tools")


def executable_tool_path(name, cache_root=None):
    """Return an executable tool, materializing a private copy if required."""
    source = tool_path(name)
    if os.access(source, os.X_OK):
        return source
    with open(source, "rb") as stream:
        payload = stream.read()
    digest = hashlib.sha256(payload).hexdigest()[:16]
    destination_root = os.path.abspath(cache_root or _cache_root())
    if not os.path.isdir(destination_root):
        os.makedirs(destination_root, 0o700)
    destination = os.path.join(destination_root, "%s-%s" % (name, digest))
    if os.path.isfile(destination):
        mode = stat.S_IMODE(os.stat(destination).st_mode)
        if mode & stat.S_IXUSR:
            return destination
    descriptor, temporary = tempfile.mkstemp(prefix=".%s." % name, dir=destination_root)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o700)
        os.replace(temporary, destination)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    return destination


def submit_spectre_script_path():
    return executable_tool_path("submit_spectre_job.sh")


def guardian_script_path():
    return tool_path("ocean_worker_guardian.py")


class ResourceResolver(object):
    """Resolve explicit manifest-backed groups without granting import rights."""
    def __call__(self, group, name):
        return resource_path(group, name)

    @staticmethod
    def group(root, names):
        root = os.path.realpath(root)
        files = {}
        for name in names:
            path = _safe_join(root, (name,))
            if not os.path.isfile(path) or os.path.islink(path):
                raise ValueError('Application resource must be a regular file')
            with open(path, 'rb') as stream:
                files[name] = hashlib.sha256(stream.read()).hexdigest()
        return {'root': root, 'files': files}
