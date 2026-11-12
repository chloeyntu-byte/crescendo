"""Read-only paired solver/model profiles and bounded model-interface bridges.

An automatic bridge proves a unique relative file and exported interface, not
numerical model equivalence. Unknown/encrypted syntax needs an explicit map and
the normal real-solver/output qualification. No vendor process is launched.
"""
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

import copy
import hashlib
import json
import os
import re
import xml.etree.ElementTree as ET


SCHEMA = _contract_text("crescendo.simulator_profiles.v1")
_NAME = r"[A-Za-z_][A-Za-z0-9_.$-]*"
_SECTION = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_INCLUDE = re.compile(r'^\s*(?:include|\.include)\s+["\']([^"\']+)["\'](?:\s+section\s*=\s*([A-Za-z0-9_.-]+))?', re.I)
_LIB_INCLUDE = re.compile(r'^\s*\.lib\s+["\']([^"\']+)["\']\s+([A-Za-z0-9_.-]+)\s*$', re.I)


class ModelProfileError(ValueError):
    pass


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def selected_result_reader(cadence, simulator):
    value = cadence.get(simulator + "_result_reader")
    if value in (None, ""):
        value = cadence.get("simulator_result_reader", cadence.get("result_reader")) or "spectre"
    if value not in ("spectre", "alps"):
        raise ModelProfileError("Result reader must be Spectre or ALPS for " + simulator)
    return value


def _under(path, root):
    return bool(root and os.path.commonpath([os.path.realpath(path), os.path.realpath(root)]) == os.path.realpath(root))


def _solver_model_root(path, simulator, directory=False):
    if not path or not os.path.isabs(path):
        return None
    current = os.path.realpath(path if directory else os.path.dirname(path))
    while current != os.path.dirname(current):
        if os.path.basename(current).lower() == simulator:
            return current
        current = os.path.dirname(current)
    return None


def _readable_directory(path):
    return bool(path and os.path.isdir(path) and os.access(path, os.R_OK | os.X_OK))


def _model_directory_setting(cadence, simulator):
    """Separate declarations from effective discovery hints, without probing."""
    key = simulator + '_model_dir'
    configured = cadence.get(key)
    origin = ((cadence.get('_model_dir_origins') or {}).get(simulator) or
              ((cadence.get('discovery') or {}).get('sources') or {}).get(key) or 'configured')
    declared = None
    declared_origin = None
    for field in ('_manual_overrides', '_file_overrides'):
        value = (cadence.get(field) or {}).get(key)
        if value not in (None, '', []):
            declared, declared_origin = value, 'configuration file' if field == '_file_overrides' else 'manual'
    if declared is not None:
        return declared, declared_origin, True
    automatic_origins = ('source_model_root', 'source_pdk_sibling', 'saved_setup_include_path',
                         _contract_text('configured_pdk_dir'), 'configured_model_roots')
    # Older native binding could label an automatic effective value 'manual'.
    # The declaration maps are the authority, not that accidental promotion.
    declaration_maps_present = '_manual_overrides' in cadence or '_file_overrides' in cadence
    effective = ((cadence.get('discovery') or {}).get('effective') or {})
    automatic = (origin in automatic_origins or origin.startswith('environment:PDK/') or
                 declaration_maps_present and key in effective and origin in ('manual', 'configured'))
    captured = cadence.get('launch_environment') or {}
    env_name = _contract_text('CRESCENDO_') + simulator.upper() + '_MODEL_DIR'
    if (automatic or not configured) and captured.get(env_name):
        from .native_includes import expand_native_path, native_include_environment
        environment = native_include_environment({'cadence': cadence})
        return expand_native_path(captured[env_name], environment), 'environment:' + env_name, True
    return configured, origin, not automatic


def model_directory(cadence, simulator, capture_simulator="spectre", references=None,
                    environ=None, include_directories=None):
    """Use native/source evidence and a unique direct paired model directory.

    A model root is a location hint. Actual files, sections and exported model
    interfaces are separately qualified; this function never scans a PDK tree.
    Explicit paths are returned unchanged, including invalid selections.
    """
    configured, configured_origin, explicit = _model_directory_setting(cadence, simulator)
    if configured and explicit:
        return configured, configured_origin
    if environ is not None:
        from .native_includes import expand_native_path
        env_name = _contract_text("CRESCENDO_") + simulator.upper() + "_MODEL_DIR"
        if environ.get(env_name):
            return expand_native_path(environ[env_name], environ), "environment:" + env_name
    saved_source, saved_source_origin, source_explicit = _model_directory_setting(cadence, capture_simulator)
    source_root = saved_source if source_explicit else None
    source_origin = "source_model_root"
    if not source_root:
        roots = set()
        for reference in references or []:
            path = reference.get("file")
            root = _solver_model_root(path, capture_simulator)
            if root and os.path.isfile(path) and os.access(path, os.R_OK):
                roots.add(root)
        if len(roots) == 1:
            source_root = next(iter(roots))
        elif len(roots) > 1:
            return None, "ambiguous_source_model_roots"
        else:
            roots = {_solver_model_root(path, capture_simulator, directory=True)
                     for path in include_directories or [] if _readable_directory(path)} - {None}
            if len(roots) == 1:
                source_root, source_origin = next(iter(roots)), "saved_setup_include_path"
            elif len(roots) > 1:
                return None, "ambiguous_saved_setup_include_paths"
    if source_root:
        candidate = source_root if simulator == capture_simulator else os.path.join(os.path.dirname(source_root), simulator)
        if _readable_directory(candidate):
            return os.path.realpath(candidate), source_origin if simulator == capture_simulator else "source_pdk_sibling"
        return None, "source_model_sibling_unavailable"
    # PDK and old effective roots are fallback hints. A newly saved exact
    # source file/includePath above must not be shadowed by launch-time hints.
    if environ is not None and environ.get('PDK'):
        candidate = os.path.join(expand_native_path(environ['PDK'], environ), simulator)
        if _readable_directory(candidate):
            return os.path.realpath(candidate), 'environment:PDK/' + simulator
    if configured:
        return configured, configured_origin
    if saved_source:
        candidate = saved_source if simulator == capture_simulator else os.path.join(os.path.dirname(saved_source), simulator)
        if _readable_directory(candidate):
            return os.path.realpath(candidate), saved_source_origin if simulator == capture_simulator else 'source_pdk_sibling'
    roots = []
    if cadence.get(_contract_text("pdk_dir")):
        roots.append((cadence[_contract_text("pdk_dir")], _contract_text("configured_pdk_dir")))
    roots.extend((path, "configured_model_roots") for path in cadence.get("model_roots") or [])
    candidates = {}
    for path, origin in roots:
        if not isinstance(path, str) or not path:
            continue
        if environ is not None:
            from .native_includes import expand_native_path
            path = expand_native_path(path, environ)
        if not os.path.isabs(path):
            continue
        for base in (path, os.path.join(path, "models"), os.path.join(path, "model")):
            candidate = base if os.path.basename(base).lower() == simulator else os.path.join(base, simulator)
            if _readable_directory(candidate):
                candidates.setdefault(os.path.realpath(candidate), origin)
    if len(candidates) == 1:
        path, origin = next(iter(candidates.items()))
        return path, origin
    if len(candidates) > 1:
        return None, "ambiguous_configured_model_roots"
    return None, "unavailable"


def _canonical_section(section):
    value = section.lower()
    # Only solver labels are removed; arbitrary corner prefixes are not guessed.
    value = re.sub(r"^(?:alps|spectre)[_.-]", "", value)
    return re.sub(r"[_.-](?:alps|spectre)$", "", value)


def _read_model(path, state):
    resolved = os.path.realpath(path)
    if resolved in state["files"]:
        return state["files"][resolved]["text"]
    if len(state["files"]) >= 64:
        raise ModelProfileError("Automatic model bridge exceeded 64 dependency files")
    try:
        size = os.path.getsize(resolved)
        if size > 4 * 1024 * 1024 or state["bytes"] + size > 8 * 1024 * 1024:
            raise ModelProfileError("Automatic model bridge exceeded its bounded file size")
        with open(resolved, "rb") as stream:
            raw = stream.read(4 * 1024 * 1024 + 1)
        if len(raw) != size:
            raise ModelProfileError("Model changed while inspecting its interface")
        text = raw.decode("utf-8")
    except (IOError, OSError, UnicodeDecodeError) as error:
        raise ModelProfileError("Automatic model bridge cannot read model: %s (%s)" % (path, error.__class__.__name__))
    if re.search(r"(?im)^\s*(?:pragma\s+protect|\.protect|protect\b|#pragma\s+protect)|\bencrypted\b", text):
        raise ModelProfileError("Encrypted/protected model requires an explicit model mapping")
    state["bytes"] += size
    state["files"][resolved] = {"text": text, "path": resolved, "size": size,
                                "sha256": hashlib.sha256(raw).hexdigest()}
    return text


def _section_lines(text):
    """Inspect literal Spectre section/.lib blocks; reject nested ambiguity."""
    sections, common, active = {}, [], None
    for raw in text.splitlines():
        line = re.sub(r"//.*$", "", raw).strip()
        if not line or line.startswith(("*", "//")):
            continue
        begin = re.match(r"^(?:section|\.lib)\s+(" + _NAME + r")\s*$", line, re.I)
        end = re.match(r"^(?:endsection|\.endl)(?:\s+(" + _NAME + r"))?\s*$", line, re.I)
        if begin:
            name = begin.group(1)
            if active is not None or name in sections:
                raise ModelProfileError("Duplicate/nested model section requires an explicit mapping")
            sections[name], active = [], name
        elif end:
            if active is None or (end.group(1) and end.group(1) != active):
                raise ModelProfileError("Unbalanced model section requires an explicit mapping")
            active = None
        elif active is None:
            common.append(line)
        else:
            sections[active].append(line)
    if active is not None:
        raise ModelProfileError("Unterminated model section requires an explicit mapping")
    return common, sections


def _parameter_only_statements(lines):
    """Conservative auxiliary-section evidence; unknown syntax stays blocked."""
    statements, names = [], set()
    for line in lines:
        if _LIB_INCLUDE.match(line) or _INCLUDE.match(line):
            continue
        if re.fullmatch(r"(?:library|endlibrary)\s+" + _NAME, line, re.I):
            continue
        if re.fullmatch(r"simulator\s+lang\s*=\s*(?:spectre|spice)", line, re.I):
            continue
        match = re.match(r"^(?:parameters|\.param)\s+(.+)$", line, re.I)
        if not match or line.endswith(chr(92)) or any(char in line for char in (';', chr(34), chr(39))):
            return None
        value = match.group(1).strip()
        declared = re.findall(r"\b(" + _NAME + r")\s*=", value)
        if not declared:
            return None
        # Retain values and expression text exactly. Matching names alone does
        # not prove that two auxiliary settings have compatible meanings.
        statements.append(value)
        names.update(declared)
    return {"statements": statements, "parameter_names": sorted(names)}


def model_interface(path, section, root):
    """Return exported declarations and exact dependencies for one model point."""
    state = {"files": {}, "bytes": 0, "visiting": set(),
             "auxiliary_valid": True, "auxiliary_statements": [], "parameter_names": set()}
    exports = {}
    def inspect(current, selected, depth):
        if depth > 16 or not _under(current, root):
            raise ModelProfileError("Automatic model dependency is outside its selected root or too deep")
        key = (os.path.realpath(current), selected)
        if key in state["visiting"]:
            raise ModelProfileError("Cyclic model dependency requires an explicit mapping")
        state["visiting"].add(key)
        common, sections = _section_lines(_read_model(current, state))
        if selected is not None and selected not in sections:
            raise ModelProfileError("Model section is absent: %s in %s" % (selected, current))
        lines = common + (sections[selected] if selected is not None else [])
        auxiliary = _parameter_only_statements(lines)
        if auxiliary is None:
            state['auxiliary_valid'] = False
        active_subckt = None
        for line in lines:
            parameter = re.match(r'^(?:parameters|\.param)\s+(.+)$', line, re.I)
            if auxiliary is not None and parameter:
                state['auxiliary_statements'].append(parameter.group(1).strip())
                state['parameter_names'].update(re.findall(r'\b(' + _NAME + r')\s*=', parameter.group(1)))
            if re.match(r"^(?:ends|\.ends)\b", line, re.I):
                active_subckt = None
            if active_subckt and re.match(r"^(?:parameters|\.param)\b", line, re.I):
                names = re.findall(r"\b(" + _NAME + r")\s*=", line)
                exports[active_subckt][2] = sorted(set(exports[active_subckt][2]) | set(names))
            include = _LIB_INCLUDE.match(line) or _INCLUDE.match(line)
            if include:
                name = include.group(1)
                if "$" in name or "~" in name:
                    raise ModelProfileError("Nonliteral nested model include requires an explicit mapping")
                nested = name if os.path.isabs(name) else os.path.join(os.path.dirname(current), name)
                if not os.path.isfile(nested):
                    nested = os.path.join(root, name)
                inspect(nested, include.group(2), depth + 1)
                continue
            if re.match(r"^(?:include|\.include|\.lib)\b", line, re.I):
                raise ModelProfileError("Unsupported model include syntax requires an explicit mapping")
            model = re.match(r"^(?:model|\.model)\s+(" + _NAME + r")\s+(" + _NAME + r")", line, re.I)
            subckt = re.match(r"^(?:subckt|\.subckt)\s+(" + _NAME + r")\s+(.*)$", line, re.I)
            if model:
                name, interface = model.group(1), ["model", model.group(2).lower()]
            elif subckt:
                name, ports = subckt.group(1), subckt.group(2).strip()
                if ports.startswith("("):
                    close = ports.find(")")
                    if close < 0:
                        raise ModelProfileError("Multiline subckt header requires an explicit mapping")
                    port_names = ports[1:close].split()
                else:
                    port_names = re.split(r"(?i)\bparams\s*:|\bparameters\b|\s+\w+\s*=", ports, maxsplit=1)[0].split()
                if not port_names or any(not re.match("^" + _NAME + "$", port) for port in port_names):
                    raise ModelProfileError("Unsupported subckt interface requires an explicit mapping")
                parameter_names = re.findall(r"\b(" + _NAME + r")\s*=", ports)
                interface = ["subckt", port_names, sorted(set(parameter_names))]
                active_subckt = name
            else:
                continue
            if name in exports and exports[name] != interface:
                raise ModelProfileError("Conflicting exported model/subckt interface")
            exports[name] = interface
        state["visiting"].remove(key)
    inspect(path, section, 0)
    parameter_only = bool(not exports and section is not None and state['auxiliary_valid'] and
                          state['auxiliary_statements'] and state['parameter_names'])
    if not exports and not parameter_only:
        raise ModelProfileError("No readable model/subckt interface; select an explicit model mapping")
    statements = state['auxiliary_statements'] if parameter_only else None
    identity = {"parameter_statements": statements} if parameter_only else exports
    files = [{key: row[key] for key in ("path", "size", "sha256")} for row in state["files"].values()]
    return {"exports": exports, "interface_sha256": _digest(identity),
            "interface_kind": "parameter_only" if parameter_only else "model_subckt",
            "parameter_statements": statements,
            "parameter_names": sorted(state['parameter_names']) if parameter_only else [],
            "dependencies": sorted(files, key=lambda row: row["path"]),
            "model_equivalence_claimed": False}


def automatic_model_mapping(source, source_section, source_root, target_root):
    if not source_root or not _under(source, source_root):
        raise ModelProfileError("Source model has no exact selected solver model root; use an explicit mapping")
    relative = os.path.relpath(os.path.realpath(source), os.path.realpath(source_root))
    try:
        original = model_interface(source, source_section, source_root)
    except ModelProfileError as error:
        raise ModelProfileError("Source model section cannot be inspected: %s [section=%s]; %s" %
                                (source, source_section, error))
    def qualify(target):
        state = {"files": {}, "bytes": 0}
        unused, sections = _section_lines(_read_model(target, state))
        if source_section is None:
            target_section = None
        else:
            candidates = [name for name in sections if _canonical_section(name) == _canonical_section(source_section)]
            if len(candidates) != 1:
                raise ModelProfileError("Target corner section is missing or ambiguous; use an explicit sections mapping")
            target_section = candidates[0]
        adapted = model_interface(target, target_section, target_root)
        if (original["exports"] != adapted["exports"] or
                original.get('parameter_statements') != adapted.get('parameter_statements')):
            raise ModelProfileError("Model/subckt or auxiliary parameter interfaces differ; use an explicit model mapping")
        return target_section, adapted
    target = os.path.join(target_root, relative)
    target_section, adapted, search_mode = None, None, "exact_relative_counterpart"
    if _under(target, target_root) and os.path.isfile(target):
        try:
            target_section, adapted = qualify(target)
        except ModelProfileError as error:
            if original['interface_kind'] == 'parameter_only':
                raise ModelProfileError("Auxiliary section mapping is unavailable: source=%s [section=%s], target=%s; %s" %
                                        (source, source_section, target, error))
    if adapted is None:
        if original['interface_kind'] == 'parameter_only':
            raise ModelProfileError("Auxiliary section needs its exact relative counterpart: source=%s [section=%s], target=%s" %
                                    (source, source_section, target))
        # Only the explicitly selected/same-PDK target tree is inspected. A
        # bounded incomplete scan must not claim a unique candidate.
        candidates, entries = [], 0
        for current, directories, files in os.walk(target_root, followlinks=False):
            depth = os.path.relpath(current, target_root).count(os.sep) + (current != target_root)
            directories[:] = sorted(name for name in directories if not os.path.islink(os.path.join(current, name)))
            if depth >= 4 and directories:
                raise ModelProfileError("Automatic model search depth bound reached; select an explicit mapping")
            entries += len(directories) + len(files)
            if entries > 512:
                raise ModelProfileError("Automatic model search entry bound reached; select an explicit mapping")
            for filename in sorted(files):
                if not filename.lower().endswith((".scs", ".lib", ".mdl", ".mod", ".inc", ".sp", ".spi", ".model")):
                    continue
                path = os.path.join(current, filename)
                if _under(path, target_root) and os.path.isfile(path):
                    candidates.append(path)
            if len(candidates) > 128:
                raise ModelProfileError("Automatic model search file bound reached; select an explicit mapping")
        matches, unreadable = [], []
        for candidate in candidates:
            try:
                selected, evidence = qualify(candidate)
                matches.append((candidate, selected, evidence))
            except ModelProfileError as error:
                if any(value in str(error) for value in ("Encrypted", "cannot read", "bounded file size", "Unsupported", "Multiline", "Cyclic", "Unbalanced", "Unterminated", "Duplicate/nested", "Nonliteral", "Conflicting")):
                    unreadable.append(candidate)
        if len(matches) != 1 or unreadable:
            raise ModelProfileError("Automatic model search is missing, ambiguous or unreadable; select an explicit mapping")
        target, target_section, adapted = matches[0]
        search_mode = "unique_bounded_interface_candidate"
    return {"path": os.path.realpath(target), "section": target_section,
            "mapping_origin": "automatic_verified_relative_interface",
            "mapping_evidence": {"relative_path": relative, "search_mode": search_mode, "source_section": source_section,
                "target_section": target_section, "interface_sha256": original["interface_sha256"],
                "interface_kind": original['interface_kind'], "parameter_names": original['parameter_names'],
                "source_dependencies": original["dependencies"], "target_dependencies": adapted["dependencies"],
                "section_rule": "unique_exact_or_solver_label_only", "model_equivalence_claimed": False}}


def resolve_model_mapping(cadence, simulator, capture_simulator, reference, target_directory):
    source, section = reference.get("file"), reference.get("section")
    mapping = cadence.get(simulator + "_model_file_map") or {}
    if not isinstance(mapping, dict):
        raise ModelProfileError("Simulator model mapping must be an explicit source-to-file object")
    target = mapping.get(source, mapping.get(os.path.realpath(source)))
    if target is not None:
        selected = section
        if isinstance(target, dict):
            sections = target.get("sections")
            if sections is not None:
                if not isinstance(sections, dict) or any(not isinstance(key, str) or not _SECTION.match(key) or not isinstance(value, str) or not _SECTION.match(value) for key, value in sections.items()):
                    raise ModelProfileError("Manual sections mapping must contain valid source/target section names")
                if section is not None:
                    if section not in sections:
                        raise ModelProfileError("Manual sections mapping has no target for source section: " + section)
                    selected = sections[section]
            else:
                selected = target.get("section", section)
            target = target.get("path")
        return {"path": target, "section": selected, "mapping_origin": "manual", "mapping_evidence": None}
    if simulator == capture_simulator or _under(source, target_directory):
        return {"path": source, "section": section, "mapping_origin": "source_native", "mapping_evidence": None}
    source_root, unused = model_directory(cadence, capture_simulator, capture_simulator, [reference])
    if (source_root and os.path.realpath(os.path.dirname(source_root)) != os.path.realpath(os.path.dirname(target_directory))
            and not (cadence.get(capture_simulator + "_model_dir") and cadence.get(simulator + "_model_dir"))):
        raise ModelProfileError("Automatic model roots are unrelated; select both model directories or an explicit mapping")
    return automatic_model_mapping(source, section, source_root, target_directory)


def _saved_model_settings(setup):
    """Read current literal model metadata; uncertain data stays nonauthoritative."""
    from .ade_setup import _active_node, _enabled, _local_name, _name, _skill_string
    from .environment_identity import _extract_directory_references
    from .native_includes import saved_model_file_rows
    source = (setup or {}).get('source') or {}
    result = {'references': [], 'include_paths': [], 'deferred': [], 'disabled': [], 'sources': []}
    paths = [source.get('setup_db') or source.get('maestro_sdb') or source.get('data_sdb'), source.get('active_state')]
    selected_tests = {row.get('name'): row.get('enabled', True) for row in (setup or {}).get('tests') or [] if row.get('name')}
    selected_groups = {name for row in (setup or {}).get('corners') or [] if row.get('enabled', True)
                       for name in row.get('model_groups') or []}
    history_tags = {'history', 'historyentry', 'checkpoint', 'snapshot', 'snapshots', 'disabledtests'}
    for path in dict.fromkeys(path for path in paths if path):
        try:
            with open(path, 'rb') as stream:
                raw = stream.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise ValueError('Current saved model metadata exceeds its size bound')
            root = ET.fromstring(raw)
            result['sources'].append({'path': path, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)})
        except (OSError, ET.ParseError, ValueError) as error:
            result['deferred'].append({'source_file': path, 'reason': str(error), 'reason_code': 'saved_model_metadata_unreadable'})
            continue

        def visit(node, location, parent=None):
            tag = _local_name(node)
            enabled = selected_tests.get(_name(node), _enabled(node)) if tag == 'test' else _enabled(node)
            if tag in history_tags or not enabled:
                return
            if tag == 'modelgroup' and selected_groups and _name(node) not in selected_groups:
                return
            value = (node.text or '').strip()
            origin = {'source_file': path, 'xml_path': location,
                      'value_sha256': hashlib.sha256(value.encode('utf-8')).hexdigest()}
            field = str(node.attrib.get('Name') or node.attrib.get('name') or '').lower() if tag == 'field' else ''
            if field == 'includepath':
                result['include_paths'].extend((value, os.path.dirname(path)) for value in _extract_directory_references(value))
            elif field == 'modelfiles':
                try:
                    for row in saved_model_file_rows(value):
                        record = dict(row, origin='saved_current_model_files', **origin)
                        result['references' if row['enabled'] else 'disabled'].append(record)
                except (ValueError, SyntaxError) as error:
                    result['deferred'].append(dict(origin, reason=str(error), reason_code='unsupported_saved_model_files'))
            elif tag == 'modelfile' and value:
                reference = _skill_string(value)
                section = next((_skill_string((child.text or '').strip()) for child in list(parent or [])
                                if _local_name(child) == 'modelsection'), None)
                if isinstance(reference, str) and reference:
                    result['references'].append(dict(origin, reference=reference, section=section,
                        enabled=True, format='saved_literal_model_file', origin='saved_current_model_file'))
                else:
                    result['deferred'].append(dict(origin, reason_code='unsupported_saved_model_file',
                                                   reason='Saved model file is not a literal filename'))
            counts = {}
            for child in list(node):
                name = _local_name(child)
                counts[name] = counts.get(name, 0) + 1
                visit(child, '%s/%s[%d]' % (location, name, counts[name]), node)
        active = _active_node(root)
        visit(active, '/' + _local_name(active))
    return result


def _setup_model_locations(setup, config):
    """Collect exact saved references and search locations before resolution."""
    from .native_includes import native_include_environment, resolve_include_file, expand_native_path
    setup = setup or {}
    env = native_include_environment(config)
    source = setup.get("source") or {}
    cadence = (config or {}).get("cadence") or {}
    native = ((config or {}).get("_maestro_launch_context") or {}).get("source") or {}
    cwd = (source.get("work_dir") or source.get("project_dir") or cadence.get("project_dir") or
           native.get("work_dir") or (config or {}).get("work_dir") or source.get("setup_dir") or os.getcwd())
    cwd = os.path.abspath(expand_native_path(cwd, env))
    directory = cadence.get("spectre_model_dir")
    directories = []
    saved = _saved_model_settings(setup)
    deferred, disabled = list(saved['deferred']), list(saved['disabled'])
    limitations = []
    # Saved includePath is needed even when model_groups already contain the
    # relative model filename. It must be collected before resolving groups.
    for value, base in saved['include_paths']:
        expanded = expand_native_path(value, env)
        path = expanded if os.path.isabs(expanded) else os.path.join(cwd, expanded)
        if path not in directories:
            directories.append(path)
    saved_directories = list(directories)
    if directory and directory not in directories:
        directories.append(directory)
    path, unused_origin = model_directory(cadence, "spectre", environ=env)
    if path and path not in directories:
        directories.append(path)
    pending = []
    groups = {name for row in setup.get("corners") or [] if row.get("enabled", True) for name in row.get("model_groups") or []}
    for group in setup.get("model_groups") or []:
        if groups and group.get("name") not in groups:
            continue
        for row in group.get("models") or []:
            if row.get("enabled", True) and row.get("file"):
                pending.append({"reference": row["file"], "section": row.get("section"), "base": None,
                                "origin": "saved_setup_model_group", "model_group": group.get('name'),
                                "source_file": source.get('setup_db') or source.get('maestro_sdb') or source.get('data_sdb')})
    if not pending:
        pending.extend(dict(row, base=os.path.dirname(row['source_file'])) for row in saved['references'])
    references, failures = [], []
    for row in pending:
        try:
            search = directories + ([row["base"]] if row["base"] else [])
            path = resolve_include_file(row["reference"], cwd, search, env)
            references.append({"file": path, "section": row["section"]})
        except (ValueError, OSError) as error:
            record = {"reference": row["reference"], "origin": row["origin"],
                      "error": str(error), "error_type": error.__class__.__name__}
            record.update({key: row[key] for key in ('source_file', 'xml_path', 'value_sha256', 'model_group') if key in row})
            if isinstance(row['reference'], str) and re.match(r'^#\s+', row['reference']):
                # The saved representation may encode a disabled row, but '#'
                # is also legal in filenames. Never strip it or invent a file.
                deferred.append(dict(record, reason_code='ambiguous_saved_hash_model_reference',
                    reason='Saved hash-prefixed model metadata needs current native netlist interpretation; exact literal filename was unavailable'))
            else:
                failures.append(record)
    unique = {(row["file"], row["section"]): row for row in references}
    return {"references": sorted(unique.values(), key=lambda row: (row["file"], row["section"] or "")),
            "include_directories": directories, "saved_include_directories": saved_directories, "native_work_dir": cwd,
            "failures": failures, "limitations": limitations,
            "deferred_references": deferred, "disabled_model_references": disabled,
            "source_metadata": saved['sources'], "static_references_complete": not bool(deferred),
            "native_capture_required": True}


def setup_model_references(setup, config):
    """Resolve saved file/section pairs; current native tasks remain authority."""
    from .native_includes import NativeIncludeError
    result = _setup_model_locations(setup, config)
    if result["failures"]:
        raise NativeIncludeError(result["failures"][0]["error"])
    return result["references"]


def apply_source_profile_defaults(config, setup):
    """Freeze source-derived directory defaults without altering user settings."""
    result = copy.deepcopy(config or {})
    try:
        locations = _setup_model_locations(setup, result)
    except (ValueError, OSError) as error:
        # Workspace relocation is separately qualified. Do not fabricate a
        # directory from an unresolved reference or overwrite an explicit one.
        locations = {"references": [], "include_directories": [], "failures": [
            {"error": str(error), "error_type": error.__class__.__name__}], "limitations": []}
    cadence = result.setdefault("cadence", {})
    from .native_includes import native_include_environment
    environment = native_include_environment(result)
    origins = {}
    for simulator in ("spectre", "alps"):
        directory, origin = model_directory(cadence, simulator, "spectre", locations["references"],
                                            environ=environment, include_directories=(locations.get("saved_include_directories") or locations["include_directories"]))
        origins[simulator] = origin
        if directory:
            cadence[simulator + "_model_dir"] = directory
            cadence.setdefault("_model_dir_origins", {})[simulator] = origin
            discovery = cadence.setdefault("discovery", {})
            discovery.setdefault("effective", {})[simulator + "_model_dir"] = directory
            discovery.setdefault("sources", {})[simulator + "_model_dir"] = origin
    cadence["_model_discovery"] = dict(locations, schema_version=_contract_text("crescendo.source_model_discovery.v1"),
                                      origins=origins, model_sections_qualified=False,
                                      history_results_used=False)
    return result


def discover_simulator_profiles(config, setup=None, model_references=None):
    """Pure read-only UI discovery; solver/result acceptance is always separate."""
    if setup:
        config = apply_source_profile_defaults(config, setup)
    return _discover_projected_simulator_profiles(config, setup, model_references)


def _discover_projected_simulator_profiles(config, setup=None, model_references=None):
    """Consume this call's projection; callers must not reuse an older snapshot."""
    from .simulators import execution_contract, SimulatorConfigurationError
    cadence = (config or {}).get("cadence") or {}
    capture = {(row.get("simulator") or "spectre").lower() for row in (setup or {}).get("tests") or [] if row.get("enabled", True)}
    capture_source = next(iter(capture)) if len(capture) == 1 else "spectre"
    reference_error = None
    if model_references is None:
        # The projection above already read and hashed each source once. Use
        # that same observation, rather than combine two different file reads.
        locations = cadence.get('_model_discovery') or {}
        references = copy.deepcopy(locations.get('references') or []) if setup else []
        failures = locations.get('failures') or []
        if failures:
            references, reference_error = [], failures[0].get('error') or 'Saved model reference is unavailable'
    else:
        references = list(model_references)
    locations = cadence.get('_model_discovery') or {}
    deferred = locations.get('deferred_references') or []
    defer_saved = bool(deferred and model_references is None)
    source_directory, unused = model_directory(cadence, capture_source, capture_source, references)
    profiles = {}
    for simulator in ("spectre", "alps"):
        directory, origin = model_directory(cadence, simulator, capture_source, references)
        profile = {"simulator": simulator, "executable": cadence.get(simulator + "_bin"),
            "model_dir": directory, "model_dir_origin": origin,
            "capture_source_model_dir": source_directory,
            "source_model_references": copy.deepcopy(references), "target_model_references": [],
            "result_reader": None, "ready": False, "status": "unqualified",
            "reasons": [], "resolved_model_map": {}, "model_evidence": [],
            "mapping_origin": [], "native_result_qualification_required": True,
            "model_equivalence_claimed": False, "reason_code": "native_model_points_not_loaded",
            "qualification_stage": "solver_locations",
            "next_action": "Run the existing netlist-source diagnostic to capture the current input.scs, working directory and include search order."}
        try:
            if reference_error:
                profile['qualification_stage'] = 'saved_model_reference_resolution'
                profile['reason_code'] = 'saved_model_reference_unavailable'
                raise ModelProfileError(reference_error)
            if capture and capture != {"spectre"}:
                profile['reason_code'] = 'unsupported_capture_adapter'
                raise ModelProfileError("Native netlist source must use the qualified Spectre capture adapter")
            profile["result_reader"] = selected_result_reader(cadence, simulator)
            # Incomplete saved metadata cannot authorize a bridge of only the
            # remaining rows. Still validate actual executable/model locations;
            # an explicit invalid override must never become a deferred pass.
            contract_references = [] if defer_saved else references
            profile['qualification_stage'] = 'solver_locations' if defer_saved else 'model_mapping'
            contract = execution_contract(config, capture_source, contract_references, spec={"simulator": simulator})
            profile["executable"] = contract["executable"]["path"]
            profile["model_dir"] = contract["model_dir"]
            profile["model_dir_origin"] = contract["model_dir_origin"]
            for row in contract["models"]:
                path = row["source"]["path"]
                target = profile["resolved_model_map"].setdefault(path, {"path": row["target"]["path"], "sections": {}})
                if row.get("source_section") is not None:
                    target["sections"][row["source_section"]] = row["section"]
                origin = row.get("mapping_origin", "source_native")
                if origin not in profile["mapping_origin"]:
                    profile["mapping_origin"].append(origin)
                target_reference = {"file": row["target"]["path"], "section": row.get("section")}
                if target_reference not in profile['target_model_references']:
                    profile['target_model_references'].append(target_reference)
                profile["model_evidence"].append({"source": path, "target": row["target"]["path"],
                    "source_section": row.get("source_section"), "section": row.get("section"),
                    "origin": origin, "evidence": copy.deepcopy(row.get("mapping_evidence"))})
            if defer_saved:
                profile.update(reason_code='native_model_capture_required', qualification_stage='saved_model_metadata')
                profile['reasons'].append('Saved model metadata is incomplete; capture the original Maestro input.scs and cwd/-I before checking model files and sections. No source-to-target model mapping has been accepted.')
                profile['deferred_model_metadata'] = copy.deepcopy(deferred)
            elif model_references is not None or references:
                profile.update(ready=True, status="ready", reason_code='model_structure_available',
                    qualification_stage='model_structure',
                    next_action='Model paths and structural mapping are available; the selected solver and its matching results still require native execution qualification.')
            else:
                profile["reasons"].append("Tools discovered; exact native model/section points are not loaded yet")
        except (SimulatorConfigurationError, ModelProfileError, OSError) as error:
            message = str(error)
            if 'no readable model/subckt interface' in message.lower():
                profile.update(reason_code='model_interface_unavailable', qualification_stage='model_mapping',
                    next_action='The saved source model has no declarations readable by the automatic bridge. Check the captured source include/section evidence first; if the current native model has the same unsupported format, select an explicit source-to-target model mapping. The target model directory shown above remains separate from the source path in this error.')
            elif profile['reason_code'] == 'native_model_points_not_loaded':
                profile.update(reason_code='simulator_configuration_unavailable',
                    next_action='Inspect the error and effective executable/model directory for this simulator; correct only the unavailable or invalid setting, then reload and inspect the setup.')
            profile.update(status="blocked", reasons=[message])
        profiles[simulator] = profile
    return {"schema_version": SCHEMA, "capture_source_simulator": capture_source,
            "generation_strategy": "spectre_capture_then_verified_model_adapter",
            "model_discovery": copy.deepcopy(cadence.get("_model_discovery") or {}),
            "profiles": profiles}


def frozen_model_profile_identity(manifest):
    """Content identity shared across candidate generation and checkpoint resume.

    Candidate netlist numbers/paths and source capture threads are deliberately
    absent; the actual solver, reader, resource threads and model interface are
    frozen. Duplicate point profiles are compacted without dropping sections.
    """
    from .simulators import validate_execution_contract
    identities = {}
    for task in (manifest or {}).get("tasks") or []:
        contract = task.get("simulator_contract")
        if not contract:
            raise ModelProfileError("Actual task has no frozen simulator/model contract")
        validate_execution_contract(contract)
        models = []
        for row in contract.get("models") or []:
            evidence = row.get("mapping_evidence") or {}
            models.append({"source_sha256": row["source"]["sha256"], "target_sha256": row["target"]["sha256"],
                "source_section": row.get("source_section"), "section": row.get("section"),
                "source_dependencies": sorted([dependency["sha256"], dependency["size"]] for dependency in evidence.get("source_dependencies") or []),
                "target_dependencies": sorted([dependency["sha256"], dependency["size"]] for dependency in evidence.get("target_dependencies") or []),
                "interface_sha256": evidence.get("interface_sha256")})
        identity = {"test": task.get("test"), "capture_source_simulator": contract["capture_source_simulator"],
            "execution_simulator": contract["execution_simulator"], "executable_sha256": contract["executable"]["sha256"],
            "threads": contract["threads"], "result_reader": contract["result_reader"],
            "models": sorted(models, key=lambda row: json.dumps(row, sort_keys=True))}
        identities[_digest(identity)] = identity
    if not identities:
        raise ModelProfileError("No actual simulator/model tasks are available to freeze")
    payload = {"schema_version": _contract_text("crescendo.simulator_model_profile_identity.v1"),
               "profiles": [identities[key] for key in sorted(identities)],
               "model_equivalence_claimed": False}
    payload["fingerprint"] = _digest(payload)
    return payload


def require_frozen_model_profile(spec, manifest):
    """Call under the optimizer's existing lock before any common task launch."""
    identity = frozen_model_profile_identity(manifest)
    frozen = spec.get("simulator_model_profile_identity")
    if frozen is not None and frozen != identity:
        error = ModelProfileError("Frozen run simulator/model/section/reader profile changed; start a fresh optimization")
        error.detail = {"failure_stage": "simulator_model_profile_identity", "execution_started": False,
                        "frozen_fingerprint": frozen.get("fingerprint") if isinstance(frozen, dict) else None,
                        "current_fingerprint": identity["fingerprint"]}
        raise error
    if frozen is None:
        spec["simulator_model_profile_identity"] = copy.deepcopy(identity)
    return copy.deepcopy(identity)
