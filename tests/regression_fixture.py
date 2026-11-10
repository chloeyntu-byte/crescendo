"""Explicit saved-Test data for service tests with synthetic prepared inputs."""
import copy

def qualify(prepared):
    setup = prepared.setdefault("setup", {})
    rows = setup.get("tests") or [{"name": "main", "enabled": True}]
    setup["tests"] = rows
    setup["selected_test"] = rows[0]["name"]
    setup["_source_tests"] = copy.deepcopy(rows)
    setup.setdefault("test_corner_masks", {})[rows[0]["name"]] = {
        "known": True, "disabled_corners": [], "nominal_enabled": True}
    return prepared

def qualified_service(service, prepared_id):
    qualify(service.prepared[prepared_id])
    return service


def saved_setup(test, backend="maestro"):
    import os
    import tempfile
    temporary = tempfile.TemporaryDirectory(prefix="crescendo-saved-setup-")
    test.addCleanup(temporary.cleanup)
    directory = os.path.join(temporary.name, backend)
    os.makedirs(directory)
    path = os.path.join(directory, "data.sdb" if backend == "adexl" else "maestro.sdb")
    with open(path, "w") as stream:
        stream.write('<setupdb Type="%s"><active><tests><test Name="T"/></tests></active></setupdb>' % ("adexl" if backend == "adexl" else "maestro"))
    return path
