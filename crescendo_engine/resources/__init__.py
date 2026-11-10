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
    for root in _candidate_roots(group):
        candidate = _safe_join(root, parts) if parts else root
        if os.path.exists(candidate):
            return candidate
    relative = "/".join((group,) + parts)
    raise RuntimeError("packaged resource is missing: %s" % relative)


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
    if name not in _KNOWN_PDK_ADAPTERS:
        raise ValueError("unknown packaged PDK adapter: %s" % name)
    path = resource_path("pdk_adapters", name)
    if not os.path.isfile(path):
        raise RuntimeError("packaged PDK adapter is not a regular file: %s" % path)
    return path


def cadence_resource_path(name):
    """Resolve native menu resources in an installed wheel or source tree."""
    if name not in _KNOWN_CADENCE_RESOURCES:
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
    return os.path.join(os.path.abspath(os.path.expanduser(base)), "crescendo", "tools")


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
