from __future__ import print_function

import ast
import contextlib
import hashlib
import json
import math
import os
import re

from .cadence import (
    check_command_cancellation, child_environment, run_command,
    write_readonly_cds_context,
)
from .headless_display import owned_headless_display
from .config_hierarchy import config_probe_skill, is_schematic_implementation
from .utils import atomic_write_json, atomic_write_text, ensure_dir, run_id, utc_timestamp


SPICE_SUFFIXES = {
    "t": 1.0e12,
    "g": 1.0e9,
    "meg": 1.0e6,
    "k": 1.0e3,
    "m": 1.0e-3,
    "u": 1.0e-6,
    "n": 1.0e-9,
    "p": 1.0e-12,
    "f": 1.0e-15,
    "a": 1.0e-18,
}


def parse_physical_number(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(float(value)) else None
    text = str(value).strip().strip('"')
    match = re.match(r"^([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)([A-Za-z]+)?$", text)
    if not match:
        return None
    number = float(match.group(1))
    suffix = (match.group(2) or "").lower()
    if suffix:
        if suffix not in SPICE_SUFFIXES:
            return None
        number *= SPICE_SUFFIXES[suffix]
    return number if math.isfinite(number) else None


def numeric_parameter_semantic(parameter):
    name = "".join(character for character in str(parameter.get("name") or "").lower() if character.isalnum())
    prompt = "".join(character for character in str(parameter.get("prompt") or "").lower() if character.isalnum())
    denied_fragments = (
        "area", "perim", "coverage", "dogbone", "edge", "shrink", "route", "total",
        "spacinginc", "extension", "diffext", "textsize", "polycoef", "fnpairs",
    )
    denied_exact = {
        "ad", "as", "pd", "ps", "nrd", "nrs", "sa", "sb", "sc",
        "dwidth", "swidth", "tiew", "tiel", "tiex", "tiey", "simm",
        "totalwidth", "dl", "dw", "deltal", "deltaw", "laddr", "waddr",
    }
    if name in denied_exact or any(fragment in name or fragment in prompt for fragment in denied_fragments):
        return None
    if any(fragment in prompt for fragment in (
            "offset", "delta", "calculated", "effective", "pcell", "addwidth", "addlength")):
        return None

    aliases = {
        "width": {
            "w", "width", "devicewidth", "channelwidth", "gatewidth",
            "fingerwidth", "unitwidth", "stripwidth", "reswidth", "capwidth",
            "wf", "fw", "wr", "wg", "wgate", "widthperfinger",
        },
        "length": {
            "l", "length", "devicelength", "channellength", "gatelength",
            "unitlength", "striplength", "reslength", "caplength", "lr", "lg", "lgate",
        },
        "fingers": {
            "nf", "nfin", "nfing", "finger", "fingers", "numfinger",
            "numfingers", "numberoffingers", "gatefingers",
        },
        "multiplier": {"m", "mult", "multi", "multiplier", "multiplicity", "parallel", "parallels", "nmult", "nmultiplier"},
        "segments": {
            "seg", "nseg", "nsegs", "segment", "segments", "numsegments",
            "numberofsegments", "seriessegments", "sections", "nsections",
        },
        "array": {"nx", "ny", "nrow", "nrows", "ncol", "ncols", "rows", "cols", "columns"},
        "radius": {"rad", "radius", "innerradius", "outerradius"},
        "spacing": {"sp", "spacing", "segsp", "segmentspacing", "coilspacing"},
        "turns": {"turn", "turns", "nturn", "nturns", "numturns", "numberofturns"},
        "resistance": {"r", "res", "resistance", "rvalue", "resvalue", "sheetresistance"},
        "capacitance": {"c", "cap", "capacitance", "cvalue", "capvalue"},
        "inductance": {"ind", "inductance", "lvalue", "indvalue"},
        "voltage": {"vdc", "dcvoltage", "voltage", "voffset", "v1", "v2"},
        "current": {"idc", "dccurrent", "current", "ioffset"},
        "amplitude": {"dc", "acm", "acmag", "acmagnitude", "va", "ampl", "amplitude"},
        "phase": {"acp", "acphase", "phase", "sinephase", "initialphase"},
        "frequency": {"f", "freq", "frequency", "sinefreq"},
        "delay": {"td", "delay", "delaytime", "rise", "risetime", "fall", "falltime", "pulsewidth", "period"},
        "impedance": {"z", "z0", "impedance", "characteristicimpedance"},
    }
    # A prompt can disambiguate vendor names such as nr (fingers or turns).
    if "resistance" in prompt:
        return "resistance"
    if "capacitance" in prompt:
        return "capacitance"
    if "inductance" in prompt:
        return "inductance"
    if "turn" in prompt:
        return "turns"
    if "radius" in prompt:
        return "radius"
    if "finger" in prompt and any(word in prompt for word in ("number", "count", "numfinger")):
        return "fingers"
    for semantic, names in aliases.items():
        if name in names or prompt in names:
            return semantic
    meaning = name + " " + prompt
    if "finger" in meaning and any(word in meaning for word in ("number", "count", "numfinger")):
        return "fingers"
    if "width" in meaning:
        return "width"
    if "length" in meaning:
        return "length"
    if "finger" in meaning and "inc" not in meaning:
        return "fingers"
    if "segment" in meaning:
        return "segments"
    if any(word in meaning for word in ("multiplier", "multiplicity")):
        return "multiplier"
    return None


def numeric_candidate_expression(parameter):
    """A writable numeric device control may be replaced without a numeric seed.

    The original expression is not evaluated here. Inherited parameter APIs,
    opaque views and reference-library internals retain their existing gates.
    """
    value = parameter.get("current_value")
    return bool(
        parameter.get("source") == "cdf"
        and parameter.get("editable") is True
        and parameter.get("display", True) is not False
        and isinstance(value, str) and value.strip()
        and parse_physical_number(value) is None
        and not parameter.get("choices_raw")
        and not parameter.get("inherited_expression")
        and not re.search(r"\b(?:pPar|iPar)\s*\(", value)
        and not parameter.get("netlister_ignored")
        and not parameter.get("symbol_masked")
        and parameter.get("resolved_view_type") != "unknown"
        and numeric_parameter_semantic(parameter)
    )



def _skill_quote(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def probe_skill(lib, cell, view, output_path, max_depth, cells_only=False,
                selected_cell_paths=None):
    selected = list(selected_cell_paths or [])
    return r'''crescOut = outfile("%s")
crescCellsOnly = %s
crescScoped = %s
crescSelectedCellPaths = list(%s)
crescTechSeen = nil
crescProbeStack = nil
crescSchematicSeen = makeTable('crescSchematicSeen 'crescNotSeen)
crescSimInfoSeen = makeTable('crescSimInfoSeen 'crescNotSeen)
%s
procedure(crescProbeParameterScope(path)
  if(crescCellsOnly then nil else
    if(crescScoped then
      exists(scope crescSelectedCellPaths
        scope == "/" || path == scope ||
        (strlen(path) > strlen(scope) &&
         substring(path 1 strlen(scope)) == scope &&
         substring(path strlen(scope)+1 1) == "/"))
    else t)
  )
)
procedure(crescProbeNeedsEditor(raw)
  stringp(raw) && raw != "t" && raw != "nil"
)
procedure(crescProbeMasterSchematic(libName cellName)
  let((key result childCv)
    key = strcat(libName "/" cellName)
    result = crescSchematicSeen[key]
    when(result == 'crescNotSeen
      childCv = dbOpenCellViewByType(libName cellName "schematic" "schematic" "r")
      result = if(childCv then t else nil)
      when(childCv dbClose(childCv))
      crescSchematicSeen[key] = result
    )
    result
  )
)
procedure(crescProbeMasterSimInfo(libName cellName)
  let((key result baseCdf simInfo spectreInfo metadataResult)
    key = strcat(libName "/" cellName)
    result = crescSimInfoSeen[key]
    when(result == 'crescNotSeen
      metadataResult = errset(progn(
        baseCdf = cdfGetBaseCellCDF(ddGetObj(libName cellName))
        simInfo = get(baseCdf 'simInfo)
        spectreInfo = get(simInfo 'spectre)
        list(if(spectreInfo then
          list(get(spectreInfo 'instParameters) get(spectreInfo 'propMapping))
        else nil))
      ) t)
      if(metadataResult then
        result = car(metadataResult)
      else
        result = list(nil)
        fprintf(crescOut "MASTER_SIMINFO_WARNING\t%%L\t%%L\t%%L\t%%L\n"
          nil libName cellName "optional base CDF simulator metadata unavailable; inspect the native log")
      )
      crescSimInfoSeen[key] = result
    )
    car(result)
  )
)
procedure(crescProbeCondition(raw defaultValue cv)
  let((conditionResult editContext)
    cond(
      ((raw == nil) list(defaultValue "default"))
      ((raw == t) list(t "literal"))
      ((raw == "t") list(t "literal"))
      ((raw == "nil") list(nil "literal"))
      (stringp(raw)
        editContext = errset(deGetEditCellView() t)
        if(editContext && car(editContext) == cv then
          conditionResult = errset(evalstring(raw) t)
          if(conditionResult then
            list(if(car(conditionResult) then t else nil) "evaluated")
          else
            list(nil "unresolved")
          )
        else
          list(nil "context_unavailable")
        )
      )
      (t list(nil "unresolved"))
    )
  )
)
procedure(crescProbeTechnology(libName)
  let((libId techResult techFile techLibName techPath)
    unless(member(libName crescTechSeen)
      crescTechSeen = cons(libName crescTechSeen)
      libId = ddGetObj(libName)
      techFile = nil
      techLibName = nil
      techPath = nil
      when(ddIsId(libId) && isCallable('techGetTechFile)
        techResult = errset(techGetTechFile(libId) t)
        when(techResult techFile = car(techResult))
        when(techFile
          techLibName = techFile~>libName
          techPath = techFile~>path
        )
      )
      fprintf(crescOut "TECH\t%%L\t%%L\t%%L\n" libName techLibName techPath)
    )
  )
)
procedure(crescProbeCell(libName cellName viewName hierPath depth @optional bindingContext bindingSignature)
  prog((cv inst master childExists instPath instCdfResult instCdf param prop currentValue
       cdfgData cdfgInst cdfgForm conditionEditable conditionDisplay probeIdentity spectreInfo
       previousWindow probeWindowResult probeWindow probeWindowAttempted pcdb binding childLib childCell childView ignored ignoreReason)
    probeIdentity = list(libName cellName viewName)
    when(bindingContext probeIdentity = append(probeIdentity list(bindingSignature)))
    when(member(probeIdentity crescProbeStack)
      fprintf(crescOut "CYCLE\t%%L\t%%L\t%%L\t%%L\n" hierPath libName cellName viewName)
      return(nil)
    )
    crescProbeTechnology(libName)
    when(isCallable('hdbIsConfig) && hdbIsConfig(libName cellName viewName)
      crescProbeConfig(libName cellName viewName hierPath depth)
      return(nil)
    )
    cv = dbOpenCellViewByType(libName cellName viewName "schematic" "r")
    if(cv then
      when(bindingContext && cv~>cellViewType != "schematic"
        crescConfigFailure("CONFIG_VIEW_UNSUPPORTED" hierPath "non-stop implementation is not an OA schematic")
        dbClose(cv)
        return(nil)
      )
      when(bindingContext
        pcdb = crescConfigPcdb(libName cellName viewName hierPath)
        unless(pcdb dbClose(cv) return(nil))
        unless(cadr(pcdb) == length(cv~>instances)
          crescConfigFailure("CONFIG_PCDB_ERROR" hierPath
            sprintf(nil "PCDB and actual OA instance counts disagree: PCDB=%%d OA=%%d ignored=%%L; save/check the source schematic and inspect this cell"
              cadr(pcdb) length(cv~>instances)
              foreach(mapcar item setof(item cv~>instances crescConfigIsIgnored(item)) item~>name)))
          dbClose(cv)
          return(nil)
        )
      )
      crescProbeStack = cons(probeIdentity crescProbeStack)
      fprintf(crescOut "CELL\t%%L\t%%L\t%%L\t%%L\t%%L\n" hierPath libName cellName viewName depth)
      fprintf(crescOut "CELL_VIEW_TYPE\t%%L\t%%L\n" hierPath cv~>cellViewType)
      foreach(inst cv~>instances
        master = inst~>master
        if(hierPath == "/" then
          instPath = strcat("/" inst~>name)
        else
          instPath = strcat(hierPath "/" inst~>name)
        )
        ignoreReason = crescConfigIgnoreReason(inst)
        ignored = if(ignoreReason then t else nil)
        binding = nil
        if(bindingContext then
          binding = crescConfigBind(bindingContext pcdb inst libName cellName viewName hierPath instPath)
          childExists = binding && nth(9 binding)
          childLib = if(binding then car(binding) else nil)
          childCell = if(binding then cadr(binding) else nil)
          childView = if(binding then nth(2 binding) else nil)
          when(binding
            crescProbeTechnology(childLib)
            crescProbeTechnology(nth(3 binding))
          )
        else
          childExists = !ignored && crescProbeMasterSchematic(master~>libName master~>cellName)
          childLib = master~>libName
          childCell = master~>cellName
          childView = "schematic"
        )
        when(bindingContext crescProbeTechnology(master~>libName))
        fprintf(crescOut "INSTANCE\t%%L\t%%L\t%%L\t%%L\t%%L\t%%L\t%%L\t%%L\t%%L\t%%L\t%%L\n"
          instPath inst~>name master~>libName master~>cellName master~>viewName childExists hierPath depth master~>cellViewType ignored ignoreReason)
        ; Scoped scans retain netlister master metadata for unselected owners;
        ; no instance CDF, parameter values or editor conditions are read here.
        when(!ignored && !crescCellsOnly && crescScoped && !crescProbeParameterScope(instPath) &&
          (!bindingContext || (binding && !nth(10 binding) && nth(11 binding) != "unknown"))
          spectreInfo = crescProbeMasterSimInfo(master~>libName master~>cellName)
          when(spectreInfo
            fprintf(crescOut "SIMINFO\t%%L\t%%L\t%%L\n" instPath car(spectreInfo) cadr(spectreInfo))
          )
        )
        when(!ignored && crescProbeParameterScope(instPath) &&
          (!bindingContext || (binding && !nth(10 binding) && nth(11 binding) != "unknown"))
        instCdfResult = errset(cdfGetInstCDF(inst) t)
        when(instCdfResult
          instCdf = car(instCdfResult)
          when(instCdf
            ; Literal conditions need no editor. Dynamic vendor conditions still
            ; receive the same read-only edit context, once per cell occurrence.
            when(!probeWindowAttempted && isCallable('deOpenCellView) &&
                 exists(param instCdf~>parameters
                   crescProbeNeedsEditor(param~>editable) || crescProbeNeedsEditor(param~>display))
              probeWindowAttempted = t
              previousWindow = hiGetCurrentWindow()
              probeWindowResult = errset(deOpenCellView(libName cellName viewName "schematic" nil "r") t)
              when(probeWindowResult probeWindow = car(probeWindowResult))
            )
            cdfgData = instCdf
            cdfgInst = inst
            spectreInfo = crescProbeMasterSimInfo(master~>libName master~>cellName)
            when(spectreInfo
              fprintf(crescOut "SIMINFO\t%%L\t%%L\t%%L\n"
                instPath car(spectreInfo) cadr(spectreInfo))
            )
            foreach(param instCdf~>parameters
              prop = dbFindProp(inst param~>name)
              currentValue = if(prop then prop~>value else param~>value)
              conditionEditable = crescProbeCondition(param~>editable t cv)
              conditionDisplay = crescProbeCondition(param~>display t cv)
              fprintf(crescOut "PARAM\t%%L\t%%L\t%%L\t%%L\t%%L\t%%L"
                instPath param~>name param~>prompt param~>type currentValue param~>defValue)
              fprintf(crescOut "\t%%L\t%%L\t%%L\t%%L\t%%L"
                param~>units param~>choices param~>editable param~>display param~>parseAsNumber)
              fprintf(crescOut "\t%%L\t%%L\t%%L\t%%L\t%%L\t%%L"
                param~>min param~>max libName cellName param~>callback param~>paramType)
              fprintf(crescOut "\t%%L\t%%L\t%%L\t%%L\t%%L\t%%L"
                get(param 'step) car(conditionEditable) car(conditionDisplay)
                cadr(conditionEditable) cadr(conditionDisplay) get(param 'parseAsCEL))
              fprintf(crescOut "\t%%L\t%%L\t%%L\t%%L\n"
                get(param 'increment) get(param 'resolution) get(param 'minVal) get(param 'maxVal))
            )
          )
        )
        foreach(prop inst~>prop
          fprintf(crescOut "PROP\t%%L\t%%L\t%%L\t%%L\t%%L\t%%L\n"
            instPath prop~>name prop~>valueType prop~>value libName cellName)
        )
        )
        when(childExists && depth < %d
          crescProbeCell(childLib childCell childView instPath depth+1 bindingContext
            if(binding then nth(7 binding) else nil))
        )
        when(childExists && depth >= %d
          fprintf(crescOut "DEPTH_LIMIT\t%%L\t%%L\t%%L\t%%L\n"
            instPath childLib childCell childView)
        )
        when(bindingContext && binding && !nth(8 binding) hdbPopCell(car(bindingContext)))
      )
      when(probeWindow
        errset(hiCloseWindow(probeWindow) t)
        when(previousWindow errset(hiSetCurrentWindow(previousWindow) t))
      )
      dbClose(cv)
      crescProbeStack = cdr(crescProbeStack)
    else
      fprintf(crescOut "ERROR\t%%L\t%%L\t%%L\t%%L\n" hierPath libName cellName viewName)
    )
    return(t)
  )
)
fprintf(crescOut "DESIGN\t%%L\t%%L\t%%L\n" "%s" "%s" "%s")
crescProbeResult = errset(crescProbeCell("%s" "%s" "%s" "/" 0) t)
unless(crescProbeResult
  fprintf(crescOut "PROBE_FAILED\t%%L\n" "/")
)
close(crescOut)
exit()
''' % (
        _skill_quote(output_path), "t" if cells_only else "nil",
        "t" if selected_cell_paths is not None else "nil",
        " ".join('"%s"' % _skill_quote(path) for path in selected),
        config_probe_skill(),
        int(max_depth), int(max_depth),
        _skill_quote(lib), _skill_quote(cell), _skill_quote(view),
        _skill_quote(lib), _skill_quote(cell), _skill_quote(view),
    )


def _literal(token):
    token = token.strip()
    if token.startswith("(") and token.endswith(")"):
        # SKILL lists are whitespace separated; Python would concatenate
        # adjacent quoted choices such as ("9u" "15u") into one string.
        tokens = re.findall(r'"(?:[^"\\]|\\.)*"|[()]|[^\s()]+', token)
        position = [0]

        def parse_list():
            result = []
            position[0] += 1
            while position[0] < len(tokens):
                item = tokens[position[0]]
                if item == ")":
                    position[0] += 1
                    return result
                if item == "(":
                    result.append(parse_list())
                else:
                    result.append(_literal(item))
                    position[0] += 1
            raise ValueError("unterminated SKILL list")

        try:
            result = parse_list()
            if position[0] == len(tokens):
                return result
        except ValueError:
            pass
        return token
    if token == "nil":
        return None
    if token == "t":
        return True
    try:
        return ast.literal_eval(token)
    except Exception:
        return token


def _cdf_condition(raw, resolved=None, status=None):
    if status:
        return bool(resolved), status
    if raw is None or raw == "":
        return True, "default"
    if raw is True or raw == "t":
        return True, "literal"
    if raw is False or raw == "nil":
        return False, "literal"
    return False, "unresolved"


def _iter_probe_records(path):
    """Read large probes once without retaining a second full record tree."""
    with open(path, "r") as stream:
        for line_number, raw in enumerate(stream, 1):
            if line_number % 256 == 1:
                check_command_cancellation()
            raw = raw.rstrip("\n")
            if not raw:
                continue
            columns = raw.split("\t")
            kind = columns[0]
            yield {"kind": kind, "values": [_literal(value) for value in columns[1:]], "line": line_number}


def parse_probe_records(path):
    """Preserve the public list-returning API for callers inspecting records."""
    return list(_iter_probe_records(path))


def _identifier(*parts):
    raw = "\x1f".join(str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def _parameter_from_record(values, source):
    if source == "cdf":
        fields = list(values) + [None] * max(0, 27 - len(values))
        path, name, prompt, cdf_type, current, default, units, choices, editable, display, parse_as_number, minimum, maximum, owner_lib, owner_cell, callback, cdf_parameter_type = fields[:17]
        step, editable_resolved, display_resolved, editable_status, display_status, parse_as_cel = fields[17:23]
        increment, resolution, minimum_alias, maximum_alias = fields[23:27]
        editable_value, editable_status = _cdf_condition(editable, editable_resolved, editable_status)
        display_value, display_status = _cdf_condition(display, display_resolved, display_status)
        return {
            "hierarchy_path": path,
            "name": name,
            "prompt": prompt,
            "cdf_type": cdf_type,
            "current_value": current,
            "default_value": default,
            "unit": units,
            "choices_raw": choices,
            "editable": editable_value,
            "display": display_value,
            "editable_raw": editable,
            "display_raw": display,
            "editable_status": editable_status,
            "display_status": display_status,
            "parse_as_number": parse_as_number is True or parse_as_number == "t",
            "parse_as_cel": parse_as_cel is True or parse_as_cel == "t",
            "minimum": minimum if minimum is not None else minimum_alias,
            "maximum": maximum if maximum is not None else maximum_alias,
            "step": step if step is not None else (increment if increment is not None else resolution),
            "increment": increment,
            "resolution": resolution,
            "owner_library": owner_lib,
            "owner_cell": owner_cell,
            "callback": callback,
            "cdf_parameter_type": cdf_parameter_type,
            "source": source,
        }
    fields = list(values) + [None] * (7 - len(values))
    path, name, prop_type, current, owner_lib, owner_cell = fields[:6]
    return {
        "hierarchy_path": path,
        "name": name,
        "prompt": name,
        "cdf_type": prop_type,
        "current_value": current,
        "default_value": None,
        "unit": None,
        "choices_raw": None,
        "editable": True,
        "display": True,
        "parse_as_number": False,
        "minimum": None,
        "maximum": None,
        "owner_library": owner_lib,
        "owner_cell": owner_cell,
        "source": source,
    }


def normalize_probe(records, design=None):
    cells = []
    instances = []
    parameters = []
    technology_bindings = []
    errors = []
    instance_by_path = {}
    seen_parameters = set()
    resolved_top = None
    config_dependencies = []
    config_dependency_seen = set()
    config_bindings = []
    binding_by_path = {}
    cell_view_types = {}
    design_identity = dict(design or {})
    for record_index, record in enumerate(records):
        if record_index % 256 == 0:
            check_command_cancellation()
        kind = record["kind"]
        values = record["values"]
        if kind == "DESIGN" and len(values) >= 3 and not design_identity:
            design_identity = {"library": values[0], "cell": values[1], "view": values[2]}
        elif kind == "RESOLVED_TOP":
            if len(values) == 3 and all(isinstance(value, str) and value for value in values):
                resolved_top = dict(zip(("library", "cell", "view"), values))
            else:
                errors.append({"kind": "CONFIG_TOP_ERROR", "hierarchy_path": "/",
                               "values": ["/", "malformed resolved top identity"]})
        elif kind == "CONFIG_DEP" and len(values) >= 3:
            identity = tuple(values[:3])
            if identity not in config_dependency_seen:
                config_dependency_seen.add(identity)
                config_dependencies.append(dict(zip(("library", "cell", "view"), identity)))
        elif kind == "CELL_VIEW_TYPE" and len(values) >= 2:
            cell_view_types[values[0]] = values[1]
        elif kind == "CONFIG_BINDING" and len(values) >= 25:
            names = ("hierarchy_path", "name", "parent_path", "master_library", "master_cell", "master_view",
                     "owner_library", "owner_cell", "owner_view", "bound_library", "bound_cell", "bound_view",
                     "resolved_library", "resolved_cell", "resolved_view", "stop")
            binding = dict(zip(names, values[:16]))
            binding["config_context"] = dict(zip(("library", "cell", "view"), values[16:19]))
            binding["config_context_path"] = values[19]
            binding["config_local_path"] = values[20]
            binding["fixed_library"], binding["fixed_cell"], binding["fixed_view"] = [bool(x) for x in values[21:24]]
            binding["resolved_view_type"] = values[24]
            binding["netlister_ignored"] = bool(values[25]) if len(values) > 25 else False
            binding["pcdb_non_subdesign"] = bool(values[26]) if len(values) > 26 else False
            binding["stop"] = bool(binding["stop"])
            if len(values) > 27:
                binding["scan_traversable"] = bool(values[27])
                binding["symbol_masked"] = bool(values[28]) if len(values) > 28 else False
            config_bindings.append(binding)
            binding_by_path[binding["hierarchy_path"]] = binding
        elif kind == "CELL" and len(values) >= 5:
            cells.append({"hierarchy_path": values[0], "library": values[1], "cell": values[2], "view": values[3], "depth": values[4]})
        elif kind == "INSTANCE" and len(values) >= 8:
            row = {
                "hierarchy_path": values[0],
                "name": values[1],
                "master_library": values[2],
                "master_cell": values[3],
                "master_view": values[4],
                "has_schematic": bool(values[5]),
                "parent_path": values[6],
                "depth": values[7] + 1 if isinstance(values[7], int) else values[7],
                "master_view_type": values[8] if len(values) > 8 else None,
            }
            if len(values) > 9:
                row["netlister_ignored"] = bool(values[9])
                row["ignore_reason"] = values[10] if len(values) > 10 else None
            row["kind"] = "hierarchical" if row["has_schematic"] else "device"
            binding = binding_by_path.get(row["hierarchy_path"])
            if binding:
                row.update(binding)
            instances.append(row)
            instance_by_path[row["hierarchy_path"]] = row
        elif kind == "TECH" and len(values) >= 3:
            technology_bindings.append({
                "library": values[0],
                "technology_library": values[1],
                "technology_path": values[2],
            })
        elif kind in ("PARAM", "PROP"):
            row = _parameter_from_record(values, "cdf" if kind == "PARAM" else "db_property")
            for condition_name in ("editable", "display"):
                if row.get(condition_name + "_status") in ("unresolved", "context_unavailable"):
                    errors.append({
                        "kind": "CDF_CONDITION_UNRESOLVED",
                        "hierarchy_path": row["hierarchy_path"],
                        "parameter": row["name"], "condition": condition_name,
                        "values": [row.get(condition_name + "_raw"), row.get(condition_name + "_status")],
                    })
            key = (row["hierarchy_path"], row["name"])
            if key in seen_parameters:
                continue
            seen_parameters.add(key)
            instance = instance_by_path.get(row["hierarchy_path"], {})
            numeric = parse_physical_number(row["current_value"])
            if numeric is None and row["current_value"] in (None, ""):
                numeric = parse_physical_number(row["default_value"])
            row["numeric_value"] = numeric
            row["value_kind"] = "number" if numeric is not None else ("enum" if row.get("choices_raw") else "expression")
            row["inherited_expression"] = bool(
                isinstance(row["current_value"], str)
                and re.search(r"\b(?:pPar|iPar)\s*\(", row["current_value"])
            )
            row["candidate_expression"] = numeric_candidate_expression(row)
            row["requires_point_numeric_value"] = row["candidate_expression"]
            row["tunable"] = bool(
                row["editable"] and row["display"] and not row["inherited_expression"]
                and (numeric is not None or row.get("choices_raw") or row["candidate_expression"])
            )
            if instance.get("netlister_ignored"):
                row["netlister_ignored"] = True
                row["tunable"] = False
                row["candidate_expression"] = False
                row["requires_point_numeric_value"] = False
                row["parameter_policy_reason"] = instance.get("ignore_reason") or "instance is ignored by the netlister"
            row["instance_kind"] = instance.get("kind")
            row["master_library"] = instance.get("master_library")
            row["master_cell"] = instance.get("master_cell")
            row["design_library"] = design_identity.get("library")
            row["design_cell"] = design_identity.get("cell")
            row["design_view"] = design_identity.get("view") or "schematic"
            binding = binding_by_path.get(row["hierarchy_path"])
            if binding:
                row["owner_view"] = binding["owner_view"]
                row["owner_view_type"] = "schematic"
                row["resolved_top"] = dict(resolved_top or {})
                for name in ("bound_library", "bound_cell", "bound_view", "resolved_library", "resolved_cell",
                             "resolved_view", "resolved_view_type", "stop", "config_context", "config_context_path",
                             "config_local_path", "netlister_ignored", "pcdb_non_subdesign"):
                    row[name] = binding[name]
                for name in ("scan_traversable", "symbol_masked"):
                    if name in binding:
                        row[name] = binding[name]
                if binding["netlister_ignored"]:
                    row["tunable"] = False
                    row["parameter_policy_reason"] = "instance is ignored by the actual config netlister"
                if binding.get("symbol_masked") or binding.get("resolved_view_type") == "unknown":
                    row["tunable"] = False
                    row["parameter_policy_reason"] = "resolved implementation is a masked symbol or an unknown opaque view"
            row["parameter_id"] = _identifier(
                row["design_library"], row["design_cell"], row["design_view"],
                row["hierarchy_path"], row["name"]
            )
            row["alias_group"] = "%s/%s/%s/%s" % (
                row.get("owner_library"), row.get("owner_cell"), instance.get("name"), row["name"]
            )
            parameters.append(row)
        elif kind == "SIMINFO" and len(values) >= 3:
            instance = instance_by_path.get(values[0])
            if instance is not None:
                instance["spectre_inst_parameters"] = values[1]
                instance["spectre_prop_mapping"] = values[2]
        elif kind in ("ERROR", "CYCLE", "DEPTH_LIMIT", "PROBE_FAILED", "UNSUPPORTED_CONFIG", "MASTER_SIMINFO_WARNING") or kind.startswith("CONFIG_"):
            errors.append({"kind": kind, "hierarchy_path": values[0] if values else None, "values": values})
    children = {}
    for instance in instances:
        children.setdefault(instance["parent_path"], []).append(instance["hierarchy_path"])
    for cell in cells:
        cell["children"] = children.get(cell["hierarchy_path"], [])
    technology_by_library = {}
    for row in technology_bindings:
        library = row.get("library")
        previous = technology_by_library.get(library)
        if previous and previous.get("technology_library") != row.get("technology_library"):
            errors.append({
                "hierarchy_path": None,
                "values": [
                    "conflicting technology binding", library,
                    previous.get("technology_library"), row.get("technology_library"),
                ],
            })
            continue
        technology_by_library[library] = row
    technology_bindings = [
        technology_by_library[library] for library in sorted(technology_by_library)
    ]
    for cell in cells:
        binding = technology_by_library.get(cell.get("library")) or {}
        cell["technology_library"] = binding.get("technology_library")
        cell["technology_path"] = binding.get("technology_path")
        if cell.get("hierarchy_path") in cell_view_types:
            cell["view_type"] = cell_view_types[cell["hierarchy_path"]]
    ancestor_cache = {}
    cell_by_path = dict((cell["hierarchy_path"], cell) for cell in cells)
    for parameter_index, row in enumerate(parameters):
        if parameter_index % 256 == 0:
            check_command_cancellation()
        path = str(row.get("hierarchy_path") or "")
        instance = instance_by_path.get(path) or {}
        owner = cell_by_path.get(instance.get("parent_path") or "/") or {}
        if (owner.get("library"), owner.get("cell")) == (row.get("owner_library"), row.get("owner_cell")):
            row.setdefault("owner_view", owner.get("view") or "schematic")
            row.setdefault("owner_view_type", owner.get("view_type") or "schematic")
        ancestors = ancestor_cache.get(path)
        if ancestors is None:
            parts = [part for part in path.split("/") if part]
            ancestors = []
            for depth in range(1, len(parts)):
                occurrence_path = "/" + "/".join(parts[:depth])
                occurrence = instance_by_path.get(occurrence_path)
                if occurrence and occurrence.get("has_schematic"):
                    ancestors.append(occurrence)
            ancestor_cache[path] = ancestors
        # Keep independent mutable metadata for each parameter's clone plan.
        chain = [dict(occurrence) for occurrence in ancestors]
        row["owner_occurrence_path"] = chain[-1]["hierarchy_path"] if chain else "/"
        row["occurrence_chain"] = chain
    parameters.sort(key=lambda row: (row.get("hierarchy_path") or "", row.get("name") or ""))
    check_command_cancellation()
    result = {
        "cells": cells,
        "instances": instances,
        "parameters": parameters,
        "technology_bindings": technology_bindings,
        "errors": errors,
        "counts": {
            "cells": len(cells),
            "instances": len(instances),
            "hierarchical_instances": sum(row["kind"] == "hierarchical" for row in instances),
            "devices": sum(row["kind"] == "device" for row in instances),
            "parameters": len(parameters),
            "netlister_ignored_instances": sum(bool(row.get("netlister_ignored")) for row in instances),
            "symbol_masked_instances": sum(bool(row.get("symbol_masked")) for row in instances),
            "tunable_parameters": sum(bool(row["tunable"]) for row in parameters),
        },
    }
    if resolved_top is not None or config_dependencies:
        result.update({"resolved_top": resolved_top, "config_dependencies": config_dependencies,
                       "config_bindings": config_bindings})
    return result


def validate_native_hierarchy(hierarchy, design, evidence_path=None):
    """Reject lost native cell/master identities without requiring optional CDF."""
    problems = []
    config_errors = [row for row in hierarchy.get("errors", []) if str(row.get("kind") or "").startswith("CONFIG_")]
    if not config_errors and not any(cell.get("hierarchy_path") == "/" for cell in hierarchy.get("cells", [])):
        problems.append("root cell was not opened")
    for error in hierarchy.get("errors", []):
        if error.get("kind") == "ERROR":
            problems.append("cell could not be opened at %s" % (error.get("hierarchy_path") or "/"))
        elif error.get("kind") == "PROBE_FAILED":
            problems.append("native hierarchy scan aborted; inspect its Virtuoso log for the actual error, including any SKILL quota limit")
        elif error.get("kind") == "UNSUPPORTED_CONFIG":
            problems.append("configuration binding traversal is unsupported; Crescendo will not substitute a schematic for the actual config")
        elif str(error.get("kind") or "").startswith("CONFIG_"):
            detail = error.get("values", [])[1:]
            problems.append("%s at %s: %s" % (error["kind"], error.get("hierarchy_path") or "/",
                                               "; ".join(str(x) for x in detail)))
        elif hierarchy.get("resolved_top") and error.get("kind") in ("CYCLE", "DEPTH_LIMIT"):
            problems.append("config traversal %s at %s" % (error["kind"].lower(), error.get("hierarchy_path") or "/"))
    cell_paths = set(row.get("hierarchy_path") for row in hierarchy.get("cells", []))
    for instance in hierarchy.get("instances", []):
        if instance.get("netlister_ignored") and (instance.get("has_schematic") or instance.get("hierarchy_path") in cell_paths):
            problems.append("netlister-ignored instance was unexpectedly expanded at %s" % instance.get("hierarchy_path"))
        if not instance.get("master_library") or not instance.get("master_cell"):
            problems.append("missing master identity at %s" % instance.get("hierarchy_path"))
    if hierarchy.get("config_dependencies") and not hierarchy.get("resolved_top"):
        problems.append("config traversal has no complete resolved top identity")
    if hierarchy.get("resolved_top"):
        bindings = hierarchy.get("config_bindings") or []
        binding_paths = [row.get("hierarchy_path") for row in bindings]
        instance_paths = [row.get("hierarchy_path") for row in hierarchy.get("instances", [])]
        if len(binding_paths) != len(set(binding_paths)) or set(binding_paths) != set(instance_paths):
            problems.append("config occurrence binding evidence does not match actual OA instances")
        cells_by_path = dict((row.get("hierarchy_path"), row) for row in hierarchy.get("cells", []))
        for binding in bindings:
            traversable = binding.get("scan_traversable")
            if traversable is None:
                traversable = not binding.get("stop") and not binding.get("netlister_ignored")
            else:
                expected = bool(not binding.get("stop") and not binding.get("netlister_ignored")
                                and is_schematic_implementation(binding.get("resolved_view"),
                                                                binding.get("resolved_view_type")))
                if traversable != expected:
                    problems.append("config view traversal evidence is inconsistent at %s" % binding.get("hierarchy_path"))
                if not binding.get("netlister_ignored") and not binding.get("resolved_view_type"):
                    problems.append("config resolved view type is missing at %s" % binding.get("hierarchy_path"))
            if traversable:
                cell = cells_by_path.get(binding.get("hierarchy_path")) or {}
                if any(cell.get(name) != binding.get("resolved_" + name) for name in ("library", "cell", "view")):
                    problems.append("config resolved implementation was not opened at %s" % binding.get("hierarchy_path"))
            elif binding.get("hierarchy_path") in cells_by_path:
                problems.append("config opaque or stopped implementation was unexpectedly expanded at %s" % binding.get("hierarchy_path"))
        top = hierarchy["resolved_top"]
        roots = [row for row in hierarchy.get("cells", []) if row.get("hierarchy_path") == "/"]
        if len(roots) != 1 or any(roots[0].get(key) != top.get(key) for key in ("library", "cell", "view")):
            problems.append("config resolved top does not match the opened root cell")
    if problems:
        identity = "/".join(str(design.get(key) or "") for key in ("library", "cell", "view"))
        detail = "; ".join(problems[:8])
        if len(problems) > 8:
            detail += "; %d more incomplete identities" % (len(problems) - 8)
        if evidence_path:
            detail += "; evidence: %s" % evidence_path
        raise RuntimeError("hierarchy probe is incomplete for %s: %s" % (identity, detail))


def _design_key(design):
    if isinstance(design, (list, tuple)):
        return tuple(design[:3]) if len(design) >= 3 else tuple(design[:2]) + ("schematic",)
    design = design or {}
    return (design.get("library"), design.get("cell"), design.get("view") or "schematic")


def cell_scope_records(hierarchy, setup=None, config=None):
    """Return exact occurrence choices using the existing user/reference policy."""
    if setup is not None:
        from .workspace import resolve_copy_libraries
        _primary, _mapping, user_libraries = resolve_copy_libraries(setup, hierarchy, config=config)
        selectable_libraries = set(user_libraries)
    else:
        from .pdk import hierarchy_library_references
        selectable_libraries = set(hierarchy_library_references(hierarchy)["design_libraries"])
    result = []
    seen = set()
    for design_result in (hierarchy or {}).get("designs", []):
        check_command_cancellation()
        design_key = _design_key(design_result.get("design"))
        design = dict(zip(("library", "cell", "view"), design_key))
        body = design_result.get("hierarchy") or {}
        instances = dict((row.get("hierarchy_path"), row) for row in body.get("instances", []))
        for cell in body.get("cells", []):
            path = cell.get("hierarchy_path")
            key = design_key + (path,)
            if not path or key in seen:
                continue
            seen.add(key)
            instance = instances.get(path) or {}
            allowed = cell.get("library") in selectable_libraries
            row = {
                "scope_id": "cell_scope:" + _identifier(*key),
                "design": dict(design), "hierarchy_path": path,
                "library": cell.get("library"), "cell": cell.get("cell"),
                "view": cell.get("view"), "depth": cell.get("depth"),
                "master_library": cell.get("library"), "master_cell": cell.get("cell"),
                "master_view": cell.get("view"),
                "parent_path": instance.get("parent_path") if path != "/" else None,
                "selectable": allowed,
            }
            if not allowed:
                row["reason"] = "reference-library internal cell; select its containing user-design occurrence"
            result.append(row)
    result.sort(key=lambda row: (_design_key(row["design"]), int(row.get("depth") or 0), row["hierarchy_path"]))
    return result


def _selected_scopes(selected_cell_paths, unique_designs):
    if selected_cell_paths is None:
        return None
    if not isinstance(selected_cell_paths, (list, tuple)) or not selected_cell_paths:
        raise ValueError("Select at least one exact cell occurrence before applying the selection")
    valid_designs = set(unique_designs)
    scopes = {}
    for selected in selected_cell_paths:
        if not isinstance(selected, dict):
            raise ValueError("Selected cell occurrences require design identity and hierarchy_path")
        design = _design_key(selected.get("design"))
        path = selected.get("hierarchy_path")
        if design not in valid_designs:
            raise ValueError("Selected cell occurrence refers to an unknown ADE design: %s" % (design,))
        if not isinstance(path, str) or not path.startswith("/") or "//" in path:
            raise ValueError("Selected cell occurrence requires an absolute exact hierarchy path")
        path = path.rstrip("/") or "/"
        paths = scopes.setdefault(design, [])
        if path not in paths:
            paths.append(path)
    return scopes


@contextlib.contextmanager
def _hierarchy_display(config, design_root, cells_only):
    environment = child_environment(config, design_root, include_runtime_ld=True)
    if cells_only:
        yield environment, {"owned": False, "status": "not_needed", "reason": "cell-only scan does not evaluate CDF editor conditions"}
    else:
        with owned_headless_display(environment, design_root) as result:
            yield result


def probe_hierarchy(setup, config, max_depth=32, work_root=None, cells_only=False,
                    selected_cell_paths=None):
    check_command_cancellation()
    work_root = work_root or config["runtime"].get("hierarchy_probe_root") or os.path.join(config["runtime"]["run_root"], "probes")
    probe_root = ensure_dir(os.path.join(work_root, run_id("hierarchy")))
    all_designs = []
    unique = []
    seen = set()
    for test in setup.get("tests", []):
        design = (test.get("library"), test.get("cell"), test.get("view") or "schematic")
        if all(design) and design not in seen:
            unique.append(design)
            seen.add(design)
    if not unique:
        raise ValueError("Maestro setup contains no resolvable design")
    if cells_only and selected_cell_paths is not None:
        raise ValueError("Cell-only discovery and selected-cell parameter inspection are separate stages")
    scopes = _selected_scopes(selected_cell_paths, unique)
    for index, design in enumerate(unique):
        check_command_cancellation()
        library, cell, view = design
        design_root = ensure_dir(os.path.join(probe_root, "design_%02d" % index))
        cadence = config.get("cadence", {})
        source_override = cadence.get("source_cds_lib_override")
        setup_root = setup["source"]["library_root"]
        context_options = {}
        from .workspace import parse_cds_defines
        if source_override:
            mappings = parse_cds_defines(source_override)
            source_root = mappings.get(library)
            if not source_root or not os.path.isdir(source_root):
                raise ValueError("Invoking Maestro has no readable library mapping for %s" % library)
            context_options["source_cds_lib_override"] = source_override
        else:
            from .cadence import nearest_cds_lib
            source_cds = nearest_cds_lib(setup_root)
            configured_cds = cadence.get("cds_lib")
            mappings = {}
            # Match the actual context order: installation/config SOFTINCLUDE,
            # then the saved setup's source INCLUDE. Resolve the test's design
            # library rather than rebinding it to the Maestro's library root.
            for mapping_file in (configured_cds, source_cds):
                if mapping_file and os.path.isfile(mapping_file):
                    mappings.update(parse_cds_defines(mapping_file))
            source_root = mappings.get(library)
            if source_root is None:
                source_library = ((setup.get("setup_locator") or {}).get("library_name")
                                  or (setup.get("source") or {}).get("library")
                                  or os.path.basename(os.path.abspath(setup_root)))
                if library != source_library:
                    raise ValueError("Saved Maestro has no readable design-library mapping for %s; setup library %s is a different library" %
                                     (library, source_library))
                source_root = setup_root
            if not source_root or not os.path.isdir(source_root):
                raise ValueError("Saved Maestro design-library mapping is unavailable for %s: %s" % (library, source_root))
            mapping_context = source_cds or (configured_cds if configured_cds and os.path.isfile(configured_cds) else None)
            if mapping_context:
                context_options["source_cds_lib_override"] = mapping_context
        context = write_readonly_cds_context(
            design_root, library, source_root, cadence.get("cds_lib"),
            **context_options
        )
        output_path = os.path.join(design_root, "hierarchy.records")
        skill_path = os.path.join(design_root, "probe_hierarchy.il")
        log_path = os.path.join(design_root, "virtuoso.log")
        paths = scopes.get(design, []) if scopes is not None else None
        atomic_write_text(skill_path, probe_skill(library, cell, view, output_path, max_depth,
                                                cells_only=cells_only, selected_cell_paths=paths))
        no_parameter_inspection = cells_only or (paths is not None and not paths)
        with _hierarchy_display(config, design_root, no_parameter_inspection) as (probe_environment, display):
            command = run_command(
                [config["cadence"]["virtuoso_bin"], "-nograph", "-restore", skill_path, "-log", log_path],
                cwd=design_root,
                env=probe_environment,
                timeout_seconds=config["runtime"].get("command_timeout_seconds", 1800),
                stdout_path=os.path.join(design_root, "virtuoso.stdout"),
                stderr_path=os.path.join(design_root, "virtuoso.stderr"),
            )
        command["headless_display"] = display
        if command["returncode"] != 0 or not os.path.isfile(output_path):
            raise RuntimeError("hierarchy probe failed for %s/%s/%s: %s" % (library, cell, view, json.dumps(command, sort_keys=True)))
        normalized = normalize_probe(_iter_probe_records(output_path), {
            "library": library, "cell": cell, "view": view,
        })
        if scopes is not None:
            existing = set(row.get("hierarchy_path") for row in normalized["cells"])
            for selected_path in paths:
                if selected_path not in existing:
                    normalized["errors"].append({"kind": "SCOPE_NOT_FOUND", "hierarchy_path": selected_path,
                                                 "values": [selected_path]})
        design_result = {
            "design": {"library": library, "cell": cell, "view": view},
            "context": context,
            "command": command,
            "hierarchy": normalized,
        }
        if "resolved_top" in normalized:
            for name in ("resolved_top", "config_dependencies", "config_bindings"):
                design_result[name] = normalized[name]
        all_designs.append(design_result)
    result = {
        "schema_version": "crescendo.hierarchy.v1",
        "created_at": utc_timestamp(),
        "read_only": True,
        "probe_root": probe_root,
        "max_depth": int(max_depth),
        "designs": all_designs,
        "scan_mode": "cells_only" if cells_only else "selected_cells" if scopes is not None else "full",
    }
    evidence_path = os.path.join(probe_root, "hierarchy.json")
    check_command_cancellation()
    atomic_write_json(evidence_path, result)
    for design_result in all_designs:
        validate_native_hierarchy(design_result["hierarchy"], design_result["design"], evidence_path)
        missing = [row["hierarchy_path"] for row in design_result["hierarchy"].get("errors", [])
                   if row.get("kind") == "SCOPE_NOT_FOUND"]
        if missing:
            raise ValueError("Selected cell occurrence was not found in the actual hierarchy: %s; evidence: %s" %
                             (", ".join(missing), evidence_path))
    return result
