"""Versioned native design-variable settings and mixed parametric columns."""
from .runtime import bind_text
_contract_text = bind_text()

import copy
import os


def skill_lines(plan, path, session="session", database="mainSDB"):
    from .native_maestro_parameters import native_parameter_skill_lines, SCHEMA, _quote as q, _IDENTIFIER, _text
    if not _IDENTIFIER.match(session) or not _IDENTIFIER.match(database) or not os.path.isabs(_text(path, "verification file", 8192)):
        raise ValueError("Invalid native design-variable session or readback path")
    devices = [r for r in plan["selected"] if r.get("source_kind") not in ("global", "top")]
    controls = [r for r in plan["selected"] if r.get("source_kind") in ("global", "top")]
    lines = []
    if devices:
        scalar = dict(plan, schema_version=SCHEMA, selected=devices)
        lines += native_parameter_skill_lines(scalar, path + ".devices", session, database)
    lines += [_contract_text('cr_VarOut=outfile(%s)') % q(path),
              _contract_text('unless(cr_VarOut error("Design variable readback unavailable\\n"))')]
    for row in controls:
        name, value = q(row["path"]), q(row["value"])
        lines += ['unless(ocnxlSweepVar(%s %s) error("Design variable write failed\\n"))' % (name, value),
            'unless(equal(axlGetVarValue(axlGetVar(%s %s)) %s) error("Design variable getter differs\\n"))' % (database, name, value),
            _contract_text('fprintf(cr_VarOut "%%s\\t%%L\\n" %s axlGetVarValue(axlGetVar(%s %s)))') % (name, database, name)]
    lines += [_contract_text('fprintf(cr_VarOut "DONE\\n") close(cr_VarOut)')]
    return lines


def parse_settings(path, plan):
    from .native_maestro_parameters import parse_native_parameter_settings, SCHEMA
    from .workspace import _record_literal, _physical_values_match
    devices = [r for r in plan["selected"] if r.get("source_kind") not in ("global", "top")]
    controls = [r for r in plan["selected"] if r.get("source_kind") in ("global", "top")]
    records = []
    if devices:
        records += parse_native_parameter_settings(path + ".devices", dict(plan, schema_version=SCHEMA, selected=devices))["records"]
    if os.path.islink(path) or os.path.getsize(path) > 4 * 1024 * 1024:
        raise ValueError("Unsafe design-variable readback")
    with open(path, encoding="utf-8") as stream:
        lines = stream.read().splitlines()
    if len(lines) != len(controls) + 1 or lines[-1] != "DONE":
        raise ValueError("Incomplete design-variable readback")
    for line, row in zip(lines[:-1], controls):
        name, value = line.split("\t")
        if name != row["path"] or not _physical_values_match(row["value"], _record_literal(value)):
            raise ValueError("Design variable getter differs: " + row["path"])
        records.append(dict(row, raw_value=_record_literal(value)))
    return dict(ok=True, proof_kind="native_parameter_settings_only", records=records, point_execution_proven=False)


def batch_lines(batch, scalar, path):
    from .native_maestro_parameters import _quote as q
    from .generation_batch import native_batch_skill_lines
    # Start from the proven P2 device-set contract and specialize each API by
    # explicit source identity. Set membership getters cover both kinds.
    legacy = copy.deepcopy(batch)
    legacy["columns"] = [dict(r) for r in batch["columns"]]
    for row in legacy["columns"]:
        row.pop("source_kind", None)
    lines = native_batch_skill_lines(legacy, scalar, path)
    variables = {r["path"] for r in batch["columns"]}
    names = 'list(%s)' % ' '.join(q(n) for n in sorted(variables))
    text = "\n".join(lines)
    text = text.replace("isCallable('ocnxlSweepParam)", "isCallable('ocnxlSweepVar) && isCallable('ocnxlSweepParam)")
    text = text.replace(_contract_text('foreach(crP crPaths when(axlGetParameter(mainSDB crP) maeDeleteParameter(crP ?session session)))'),
        _contract_text('foreach(crP crPaths unless(member(crP %s) when(axlGetParameter(mainSDB crP) maeDeleteParameter(crP ?session session))))') % names)
    text = text.replace(_contract_text('ocnxlSweepParam(car(crEntry) cadr(crEntry))'),
        _contract_text('if(member(car(crEntry) %s) then ocnxlSweepVar(car(crEntry) cadr(crEntry)) else ocnxlSweepParam(car(crEntry) cadr(crEntry)))') % names)
    text = text.replace('"parametersets"', '"variablesets"').replace('?name "parameterset"', '?name "variableset"')
    text = text.replace(_contract_text('crMembers=nil'), _contract_text('crMembers=nil\ncrVarsRoot=axlSDBGetChild(car(crGroups) "vars")\nwhen(crVarsRoot foreach(crVar axlSDBGetChildren(crVarsRoot ?name "var" ?returnAs \'handles) crMembers=cons(axlSDBGetValue(crVar) crMembers)))'))
    # Global groups have no device-members root. All mixed axes are explicit
    # Global aliases above, so only vars/var members belong to this set.
    start = text.index(_contract_text('crMembersRoot='))
    end = text.index(_contract_text('unless(length(crMembers)'), start)
    text = text[:start] + text[end:]
    text = text.replace(_contract_text('maeGetParameter(crP ?session session)'),
        _contract_text('if(member(crP %s) then axlGetVarValue(axlGetVar(mainSDB crP)) else maeGetParameter(crP ?session session))') % names)
    text = text.replace(_contract_text('axlGetEnabled(axlGetParameter(mainSDB crP))'),
        _contract_text('axlGetEnabled(if(member(crP %s) then axlGetVar(mainSDB crP) else axlGetParameter(mainSDB crP)))') % names)
    text = text.replace(_contract_text('crBeforeCorners=ocnxlGetCorners()'), _contract_text('crDeviceSets=axlSDBGetChild(crActive "parametersets")\nwhen(crDeviceSets && axlSDBGetChildren(crDeviceSets ?name "parameterset") error("Source device sets need serial execution\\n"))\ncrBeforeCorners=ocnxlGetCorners()'))
    text = text.replace(_contract_text('crBeforeCorners=ocnxlGetCorners()'), _contract_text('crBeforeCorners=ocnxlGetCorners()\nwhen(axlGetAllVarsDisabled(mainSDB) error("Global variables disabled; no batch override applied\\n"))'))
    text = text.replace('"allparametersetsdisabled"', '"allvariablesetsdisabled"')
    mixed = [c for c in batch["columns"] if c.get("native_device_path")]
    prefix = []
    for c in batch["columns"]:
        if c.get("auxiliary"):
            prefix.append(_contract_text('crOwnedCheck=axlGetVar(mainSDB %s) when(crOwnedCheck && crOwnedCheck!=0 error("Owned batch companion collides with source variable\\n"))') % q(c["path"]))
    if mixed:
        from .native_maestro_parameters import native_parameter_skill_lines, SCHEMA
        device_plan = dict(scalar, schema_version=SCHEMA,
            selected=[dict(parameter_id=c["parameter_id"], path=c["native_device_path"], value=c["path"]) for c in mixed],
            allowed_symbols=[c["path"] for c in mixed])
        for c in mixed:
            prefix.append(_contract_text('crOwnedCheck=axlGetVar(mainSDB %s) when(crOwnedCheck && crOwnedCheck!=0 error("Owned batch symbol collides with source variable\\n"))') % q(c["path"]))
        prefix += native_parameter_skill_lines(device_plan, path + ".devices")
        old = 'list(%s)' % ' '.join(q(p) for p in scalar.get("owned_paths") or [])
        owned = 'list(%s)' % ' '.join(q(p) for p in sorted(set((scalar.get("owned_paths") or []) + [c["native_device_path"] for c in mixed])))
        text = text.replace(_contract_text('unless(member(crP ') + old + ')', _contract_text('unless(member(crP ') + owned + ')')
    return prefix + text.splitlines()
