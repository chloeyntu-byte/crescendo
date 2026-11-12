"""Project-local persistent state and readable, collision-safe run names."""
from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import hashlib
import json
import os
import re
import time
import uuid


def path_component(value, fallback="TB", limit=72):
    text = str(value or fallback).strip()
    safe = re.sub(r"[^\w.-]+", "_", text, flags=re.UNICODE).strip("._") or fallback
    if not safe[0].isalnum():
        safe = fallback + "_" + safe
    changed = safe != text or len(safe.encode("utf-8")) > limit
    if changed:
        suffix = "_" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]
        safe = safe.encode("utf-8")[:limit - len(suffix)].decode("utf-8", "ignore") + suffix
    return safe


def project_directory(config=None, source=None, cwd=None):
    config = config or {}
    source = source or ((config.get("_selected_setup_source") or {}) or
                        ((config.get("_maestro_launch_context") or {}).get("source") or {}))
    path = ((config.get("cadence") or {}).get("project_dir") or source.get("work_dir") or cwd or os.getcwd())
    if not isinstance(path, str) or not path or "\x00" in path:
        raise ValueError("Project working directory is unavailable")
    return os.path.realpath(os.path.abspath(os.path.expanduser(path)))


def legacy_design_storage_root(source, libraries, base_root=None):
    """Library aliases and real setup paths remain distinct under a named TB."""
    payload = {"source": dict((key, source.get(key)) for key in
                             ("library", "cell", "view", "setup_path", "cds_lib", "ocean")),
               "libraries": libraries or {}}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:8]
    name = "%s__%s_%s_%s" % (path_component(source.get("cell")),
                              path_component(source.get("library"), "lib", 36),
                              path_component(source.get("view"), "maestro", 24), digest)
    base = os.path.realpath(os.path.abspath(base_root)) if base_root else os.path.join(project_directory(source=source), _contract_text(".crescendo"))
    return os.path.join(base, name)


def design_storage_root(source, libraries=None, base_root=None):
    """Stable settings namespace; full source/tool/map identity stays in runs.

    The real work directory isolates projects. Relative setup paths distinguish
    same-named TBs/libraries without binding state to a machine's mount prefix.
    """
    work = project_directory(source=source)
    setup = source.get('setup_path') or source.get('view_dir')
    relative = os.path.relpath(os.path.realpath(setup), work) if setup else None
    payload = {'library': source.get('library'), 'cell': source.get('cell'),
               'view': source.get('view'), 'relative_setup': relative}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode('utf-8')).hexdigest()[:8]
    name = '%s__%s_%s_%s' % (path_component(source.get('cell')),
                              path_component(source.get('library'), 'lib', 36),
                              path_component(source.get('view'), 'maestro', 24), digest)
    base = os.path.realpath(os.path.abspath(base_root)) if base_root else os.path.join(work, _contract_text('.crescendo'))
    return os.path.join(base, name)


def optimization_run_name(display_name=None):
    # The optimization name comes first. A short time/token suffix distinguishes
    # repeated names and simultaneous mock launches without overwriting a run.
    return "%s__%s_%s" % (path_component(display_name, "optimization"),
                           time.strftime("%Y%m%d_%H%M%S", time.localtime()), uuid.uuid4().hex[:4])


def settings_filename(config=None, source=None):
    config = config or {}
    source = source or config.get("_selected_setup_source") or ((config.get("_maestro_launch_context") or {}).get("source") or {})
    return _contract_text("CRESCENDO_%s_Settings.json") % path_component(source.get("cell"))
