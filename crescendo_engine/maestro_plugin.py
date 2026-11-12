"""Create a project-owned Maestro loader without editing its startup files."""

from __future__ import print_function

import json
import os
import shlex
import shutil
import subprocess
import sys

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .resources import cadence_resource_path


def _selected_python(value):
    selected = sys.executable if value is None else value
    if not isinstance(selected, str) or not selected:
        raise ValueError("the selected Python executable is empty")
    selected = os.path.expanduser(selected)
    if os.sep in selected:
        # Preserve a virtual environment's invocation path, including symlinks.
        selected = os.path.abspath(selected)
    else:
        selected = shutil.which(selected)
    if not selected or not os.path.isfile(selected) or not os.access(selected, os.X_OK):
        raise ValueError("the selected Python executable is unavailable")
    try:
        completed = subprocess.run(
            [selected, "-c", "import json, sys; print(json.dumps(list(sys.version_info[:3])))"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ValueError("selected Python could not run: %s" % error)
    if completed.returncode:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()[:1000]
        raise ValueError("selected Python failed its execution check (exit %s): %s" %
                         (completed.returncode, detail or "no diagnostic returned"))
    try:
        version = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeError, ValueError):
        raise ValueError("selected executable did not return a Python version")
    if not isinstance(version, list) or len(version) != 3 or any(type(item) is not int for item in version):
        raise ValueError("selected executable did not return a valid Python version")
    if tuple(version) < (3, 6, 0):
        raise ValueError("selected Python requires version 3.6 or newer")
    return selected


def _create_private_file(path, text, mode):
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(path, mode)
    except BaseException:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise


def install_maestro_menu(loader_path, python=None):
    """Create an explicit local loader and launcher; refuse existing outputs.

    Re-run this command on the destination machine or after moving the package.
    Installed paths belong to the target machine, never to a published template.
    """
    if not isinstance(loader_path, str) or not loader_path:
        raise ValueError("a new project-local SKILL loader path is required")
    loader = os.path.abspath(os.path.expanduser(loader_path))
    parent = os.path.dirname(loader)
    if not os.path.isdir(parent):
        raise ValueError("the loader's parent directory must already exist")
    launcher = os.path.splitext(loader)[0] + _contract_text(".crescendo_launch")
    if loader == launcher:
        raise ValueError("loader and launcher paths must differ")
    for path in (loader, launcher):
        if os.path.lexists(path):
            raise ValueError("refusing to overwrite an existing plugin file: %s" % path)
    executable = _selected_python(python)
    resource_launcher = cadence_resource_path(_contract_text("crescendo_launch"))
    menu = cadence_resource_path(_contract_text("crescendo_maestro_menu.il"))
    if not os.access(resource_launcher, os.X_OK):
        raise ValueError("the packaged Maestro launcher is not executable")
    wrapper = (
        _contract_text("#!/bin/bash\n"
        "set -euo pipefail\n"
        "# The install-time interpreter is an automatic-selection hint.\n"
        "# An explicitly inherited CRESCENDO_PYTHON remains authoritative.\n"
        "if [[ ! ${CRESCENDO_PREFERRED_PYTHON+x} ]]; then\n"
        "  export CRESCENDO_PREFERRED_PYTHON=%s\n"
        "fi\n"
        "exec %s \"$@\"\n")
    ) % (shlex.quote(executable), shlex.quote(resource_launcher))
    skill = (
        _contract_text("; Generated for this machine; regenerate after moving Crescendo.\n"
        "; Load this file from the project's .cdsinit; other plugins are preserved.\n"
        "crescPluginLauncher = %s\n"
        "load(%s)\n")
    ) % (json.dumps(launcher, ensure_ascii=False), json.dumps(menu, ensure_ascii=False))
    _create_private_file(launcher, wrapper, 0o700)
    try:
        _create_private_file(loader, skill, 0o600)
    except BaseException:
        os.unlink(launcher)
        raise
    return {
        "loader": loader,
        "launcher": launcher,
        "menu_resource": menu,
        "python": executable,
        "startup_changed": False,
        "next_step": "Add load(%s) to this project's .cdsinit, then open a new ADE window." %
                     json.dumps(loader, ensure_ascii=False),
    }
