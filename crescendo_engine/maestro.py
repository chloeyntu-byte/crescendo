from __future__ import print_function

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .ade_setup import inspect_setup, resolve_setup_locator


def resolve_setup_path(path):
    return resolve_setup_locator(path, backend="maestro")["path"]


def inspect_maestro(path):
    setup = inspect_setup(path, backend="maestro")
    setup["schema_version"] = _contract_text("crescendo.maestro_setup.v1")
    return setup
