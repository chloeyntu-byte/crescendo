from __future__ import print_function

import ast
import hashlib
import json
import os
import re
import xml.etree.ElementTree as ET

from .utils import atomic_write_text, require_under, sha256_file


MODEL_PATH_SCHEMA_VERSION = "crescendo.workspace_model_paths.v1"
MODEL_SUFFIXES = (
    ".scs", ".sp", ".spi", ".spice", ".cir", ".lib", ".mdl",
    ".model", ".va", ".vams", ".inc",
)
PATH_MARKERS = frozenset((
    "models", "model", "spice_model", "spectre", "hspice",
    "include", "includes", "technology", "tech",
))
DIRECT_TAG_ROLES = {
    "modelfile": "model",
    "includefile": "include",
    "definitionfile": "definition",
    "techfile": "technology",
    "technologyfile": "technology",
}
FIELD_ROLES = {
    "modelfiles": "model",
    "includefiles": "include",
    "definitionfiles": "definition",
    "techfile": "technology",
    "techfiles": "technology",
    "technologyfile": "technology",
    "technologyfiles": "technology",
}


class ModelPathRemapError(ValueError):
    pass


class AutomaticModelPathResolutionError(ModelPathRemapError):
    """An optional relocation search could not select one proven target."""

    def __init__(self, reason_code, message):
        self.reason_code = reason_code
        ModelPathRemapError.__init__(self, message)


def _stable_unique(values):
    result = []
    seen = set()
    for value in values or []:
        if value in (None, ""):
            continue
        value = os.path.abspath(os.path.expandvars(os.path.expanduser(str(value))))
        value = os.path.normpath(value)
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _as_list(value):
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _path_is_under(path, root):
    if not path or not root:
        return False
    path = os.path.realpath(os.path.abspath(path)) if os.path.exists(path) else os.path.abspath(path)
    root = os.path.realpath(os.path.abspath(root)) if os.path.exists(root) else os.path.abspath(root)
    return path == root or path.startswith(root + os.sep)


def _profile_matches(profile, pdk_context):
    if not pdk_context:
        return False
    context_names = set(str(value) for value in (
        list(pdk_context.get("reference_libraries") or [])
        + list(pdk_context.get("candidate_libraries") or [])
    ) if value)
    for key in ("pdk_name", "name"):
        if pdk_context.get(key):
            context_names.add(str(pdk_context[key]))
    profile_names = set(str(value) for value in (
        list(profile.get("reference_libraries") or [])
        + list(profile.get("libraries") or [])
        + list(profile.get("technology_libraries") or [])
    ) if value)
    for key in ("pdk_name", "name"):
        if profile.get(key):
            profile_names.add(str(profile[key]))
    if context_names & profile_names:
        return True
    context_root = pdk_context.get("package_root") or pdk_context.get("pdk_dir")
    profile_root = profile.get("package_root") or profile.get("pdk_dir")
    return bool(
        context_root and profile_root
        and (
            _path_is_under(context_root, profile_root)
            or _path_is_under(profile_root, context_root)
        )
    )


def _context_containers(pdk_context):
    if not isinstance(pdk_context, dict):
        return []
    rows = [row for row in pdk_context.get("contexts") or [] if isinstance(row, dict)]
    return rows or [pdk_context]


def _matching_profiles(config, pdk_context):
    profiles = list(((config or {}).get("cadence") or {}).get("pdk_profiles") or [])
    contexts = _context_containers(pdk_context)
    excluded = set()
    for context in ([pdk_context] if isinstance(pdk_context, dict) else []) + contexts:
        excluded.update(context.get("excluded_profile_indices") or [])
    matching = [
        profile for index, profile in enumerate(profiles)
        if index not in excluded
        and any(_profile_matches(profile, context) for context in contexts)
    ]
    if not pdk_context and len(profiles) == 1:
        matching = profiles
    return matching


def _root_values(container):
    if not isinstance(container, dict):
        return []
    values = []
    for key in ("package_root", "pdk_dir"):
        if container.get(key):
            values.append(container[key])
    values.extend(_as_list(container.get("model_roots")))
    values.extend((container.get("library_paths") or {}).values())
    return values


def configured_model_roots(config, pdk_context=None):
    cadence = ((config or {}).get("cadence") or {})
    values = _root_values(cadence)
    for context in _context_containers(pdk_context):
        values.extend(_root_values(context))
    for profile in _matching_profiles(config, pdk_context):
        values.extend(_root_values(profile))
    roots = _stable_unique(values)
    return roots


def _remap_values(container):
    if not isinstance(container, dict):
        return []
    raw = container.get("model_path_remaps") or container.get("path_remaps") or []
    if isinstance(raw, dict):
        raw = [{"source": source, "destination": destination} for source, destination in raw.items()]
    result = []
    for row in _as_list(raw):
        if isinstance(row, (list, tuple)) and len(row) == 2:
            source, destination = row
        elif isinstance(row, dict):
            source = row.get("source") or row.get("from") or row.get("old_root")
            destination = row.get("destination") or row.get("to") or row.get("new_root")
        else:
            raise ModelPathRemapError("model path remap must contain source and destination roots")
        if not source or not destination:
            raise ModelPathRemapError("model path remap must contain source and destination roots")
        result.append((
            os.path.abspath(os.path.expandvars(os.path.expanduser(str(source)))),
            os.path.abspath(os.path.expandvars(os.path.expanduser(str(destination)))),
        ))
    return result


def configured_path_remaps(config, pdk_context=None):
    cadence = ((config or {}).get("cadence") or {})
    rows = _remap_values(cadence)
    for context in _context_containers(pdk_context):
        rows.extend(_remap_values(context))
    for profile in _matching_profiles(config, pdk_context):
        rows.extend(_remap_values(profile))
    by_source = {}
    for source, destination in rows:
        previous = by_source.get(source)
        if previous and previous != destination:
            raise ModelPathRemapError(
                "conflicting model path remaps for %s: %s and %s"
                % (source, previous, destination)
            )
        by_source[source] = destination
    return sorted(by_source.items(), key=lambda row: (-len(row[0]), row[0], row[1]))


def _local_tag(element):
    return str(element.tag).rsplit("}", 1)[-1].lower()


def _quoted_values(text):
    values = []
    for token in re.findall(r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'', text or ""):
        try:
            values.append(ast.literal_eval(token))
        except (SyntaxError, ValueError):
            continue
    return values


def _path_values(text):
    values = _quoted_values(text)
    raw = str(text or "").strip().strip('"').strip("'")
    if not values and raw:
        values = [raw]
    result = []
    for value in values:
        expanded = os.path.expandvars(os.path.expanduser(str(value or "").strip()))
        if os.path.isabs(expanded):
            result.append(str(value))
    return result


def _setup_xml_files(setup_dir, setup_sdb=None):
    candidates = []
    if setup_sdb:
        candidates.append(os.path.abspath(setup_sdb))
    for current, _directories, files in os.walk(setup_dir):
        for name in files:
            lowered = name.lower()
            if lowered in ("maestro.sdb", "data.sdb", "active.state") or lowered.endswith(".state"):
                candidates.append(os.path.join(current, name))
    return sorted(set(candidates))


def discover_setup_path_references(setup_dir, setup_sdb=None):
    references = []
    limitations = []
    for path in _setup_xml_files(setup_dir, setup_sdb=setup_sdb):
        try:
            root = ET.parse(path).getroot()
        except (ET.ParseError, IOError, OSError) as error:
            if setup_sdb and os.path.abspath(path) == os.path.abspath(setup_sdb):
                raise ModelPathRemapError(
                    "private ADE SetupDB cannot be parsed for model path portability: %s (%s)"
                    % (path, error)
                )
            limitations.append({
                "file": path, "reason": "xml_parse_failed",
                "error_type": error.__class__.__name__,
            })
            continue
        for element in root.iter():
            tag = _local_tag(element)
            text = (element.text or "").strip()
            role = DIRECT_TAG_ROLES.get(tag)
            directory = False
            if tag == "field":
                name = str(element.attrib.get("Name") or element.attrib.get("name") or "").lower()
                if name == "includepath":
                    role = "include_path"
                    directory = True
                else:
                    role = FIELD_ROLES.get(name)
            if not role or not text:
                continue
            for value in _path_values(text):
                references.append({
                    "file": os.path.abspath(path),
                    "role": role,
                    "reference": value,
                    "directory": directory,
                })
    unique = {}
    for row in references:
        unique[(row["file"], row["role"], row["reference"], row["directory"])] = row
    return {
        "references": [unique[key] for key in sorted(unique)],
        "limitations": limitations,
    }


def _exists_as(path, directory):
    return os.path.isdir(path) if directory else os.path.isfile(path)


def _candidate_suffixes(reference):
    normalized = os.path.normpath(reference)
    components = [value for value in normalized.split(os.sep) if value]
    suffixes = []
    for index, component in enumerate(components):
        if component.lower() in PATH_MARKERS:
            suffixes.append(os.path.join(*components[index:]))
    for count in (3, 2, 1):
        if len(components) >= count:
            suffixes.append(os.path.join(*components[-count:]))
    return list(dict((value, True) for value in suffixes).keys())


def _explicit_remap_candidates(reference, remaps):
    candidates = []
    normalized = os.path.normpath(reference)
    for source, destination in remaps:
        if normalized == source or normalized.startswith(source + os.sep):
            candidates.append(os.path.join(destination, os.path.relpath(normalized, source)))
    return _stable_unique(candidates)


def _direct_candidates(reference, roots):
    candidates = []
    normalized = os.path.normpath(reference)
    suffixes = _candidate_suffixes(normalized)
    for root in roots:
        for suffix in suffixes:
            candidates.append(os.path.join(root, suffix))
            candidates.append(os.path.join(os.path.dirname(root), suffix))
            first = suffix.split(os.sep, 1)[0]
            if os.path.basename(root).lower() == first.lower():
                candidates.append(os.path.join(os.path.dirname(root), suffix))
    return _stable_unique(candidates)


def _bounded_basename_search(roots, basename, directory, max_entries=30000):
    matches = []
    inspected = 0
    for root in roots:
        if not os.path.isdir(root):
            continue
        for current, directories, files in os.walk(root):
            inspected += len(directories) + len(files)
            if inspected > max_entries:
                return matches, True
            names = directories if directory else files
            if basename in names:
                matches.append(os.path.join(current, basename))
    return _stable_unique(matches), False


def resolve_relocated_reference(reference, roots, remaps=None, directory=False):
    expanded = os.path.abspath(os.path.normpath(
        os.path.expandvars(os.path.expanduser(str(reference)))
    ))
    roots = _stable_unique(roots)
    remaps = list(remaps or [])
    explicit_candidates = _explicit_remap_candidates(expanded, remaps)
    explicitly_mapped = [
        path for path in explicit_candidates
        if _exists_as(path, directory)
    ]
    explicitly_mapped = _stable_unique(explicitly_mapped)
    if len(explicitly_mapped) == 1:
        return {
            "path": explicitly_mapped[0], "mode": "explicit_remap",
            "search_limited": False,
        }
    if len(explicitly_mapped) > 1:
        raise ModelPathRemapError(
            "explicit model/include path remap is ambiguous for %s: %s"
            % (reference, ", ".join(explicitly_mapped))
        )
    if explicit_candidates:
        raise ModelPathRemapError(
            "explicit model/include path remap target is unavailable for %s: %s"
            % (reference, ", ".join(explicit_candidates))
        )
    if _exists_as(expanded, directory) and (
            not roots or any(_path_is_under(expanded, root) for root in roots)):
        return {"path": expanded, "mode": "already_valid", "search_limited": False}
    direct = [
        path for path in _direct_candidates(expanded, roots)
        if _exists_as(path, directory)
    ]
    direct = _stable_unique(direct)
    if len(direct) == 1:
        return {"path": direct[0], "mode": "profile_remap", "search_limited": False}
    if len(direct) > 1:
        raise AutomaticModelPathResolutionError(
            "ambiguous",
            "model/include path remap is ambiguous for %s: %s"
            % (reference, ", ".join(direct))
        )
    searched, limited = _bounded_basename_search(
        roots, os.path.basename(expanded), directory,
    )
    if limited:
        raise AutomaticModelPathResolutionError(
            "search_limited",
            "model/include path search is incomplete under the active PDK roots for %s; "
            "narrow cadence.pdk_profiles[].model_roots or configure model_path_remaps explicitly"
            % reference
        )
    if len(searched) == 1:
        return {"path": searched[0], "mode": "profile_search", "search_limited": limited}
    if len(searched) > 1:
        raise AutomaticModelPathResolutionError(
            "ambiguous",
            "model/include path basename is ambiguous under configured PDK roots for %s: %s"
            % (reference, ", ".join(searched))
        )
    hint = (
        "configure cadence.pdk_profiles[].model_roots or model_path_remaps"
        if roots else "configure cadence.pdk_dir/model_roots or a PDK profile"
    )
    raise AutomaticModelPathResolutionError(
        "missing",
        "absolute model/include path cannot be resolved in the active PDK context: %s; %s"
        % (reference, hint)
    )


def _replace_path_token(text, source, destination):
    # A failed reference may start with a successfully relocated path. Replace
    # complete ADE path tokens so the retained reference is not changed too.
    delimiters = r'[\s\'"<>]|&quot;|&apos;'
    pattern = r'(^|%s)%s(?=$|%s)' % (delimiters, re.escape(source), delimiters)
    return re.sub(pattern, lambda match: match.group(1) + destination, text)


def remap_workspace_setup_paths(workspace, config, pdk_context=None):
    workspace_root = os.path.abspath(workspace["root"])
    private_setup_root = workspace_root
    if workspace.get("same_library_objects") is True:
        from .same_library_views import MARKER, owns_view
        setup_dir = os.path.abspath(workspace["setup_dir"])
        matches = [row for row in workspace.get("same_library_registry") or []
                   if row.get("role") == "maestro" and row.get("path") == setup_dir]
        if len(matches) != 1:
            raise ModelPathRemapError("Private same-library setup lacks exact Maestro ownership")
        row = matches[0]
        with open(os.path.join(setup_dir, MARKER)) as stream:
            marker = json.load(stream)
        target = row.get("target") or []
        source = row.get("source") or []
        library_root = (workspace.get("source_libraries") or {}).get(target[0] if target else None)
        if (len(target) != 3 or len(source) != 3 or not library_root or
                any(not isinstance(part, str) or not part or os.path.basename(part) != part or part in (".", "..") for part in target) or
                setup_dir != os.path.join(os.path.realpath(library_root), target[1], target[2]) or
                os.path.realpath(setup_dir) != setup_dir or row.get("existed_before") is not False or
                row.get("token") != workspace.get("same_library_token") or
                marker.get("token") != row.get("token") or marker.get("target") != target or
                target == source or target[:2] != source[:2] or
                not owns_view(workspace, tuple(target), role="maestro", require_marker=True)):
            raise ModelPathRemapError("Private same-library setup ownership does not match")
        private_setup_root = setup_dir
    setup_dir = require_under(workspace["setup_dir"], private_setup_root, "private ADE setup")
    setup_sdb = require_under(workspace["setup_sdb"], setup_dir, "private ADE SetupDB")
    require_under(os.path.realpath(setup_sdb), os.path.realpath(setup_dir), "private ADE SetupDB target")
    discovery = discover_setup_path_references(setup_dir, setup_sdb=setup_sdb)
    roots = configured_model_roots(config, pdk_context=pdk_context)
    remaps = configured_path_remaps(config, pdk_context=pdk_context)
    private_model_roots = []
    copied_roots = {} if workspace.get("same_library_objects") is True else workspace.get("copied_libraries") or {}
    for library_root in copied_roots.values():
        private_root = require_under(library_root, workspace_root, "private model library")
        require_under(os.path.realpath(private_root), os.path.realpath(workspace_root),
                      "private model library target")
        private_model_roots.append(private_root)
    replacements_by_file = {}
    records = []
    limitations = list(discovery["limitations"])
    for row in discovery["references"]:
        require_under(row["file"], private_setup_root, "private ADE setup file")
        require_under(os.path.realpath(row["file"]), os.path.realpath(private_setup_root), "private ADE setup file target")
        original = os.path.abspath(os.path.normpath(
            os.path.expandvars(os.path.expanduser(row["reference"]))
        ))
        for private_root in private_model_roots:
            if ((original == private_root or original.startswith(private_root + os.sep))
                    and not _path_is_under(os.path.realpath(original), private_root)):
                raise ModelPathRemapError(
                    "private model reference escapes its copied library: %s" % row["reference"]
                )
        # Project-local models have already been materialized and rebased by
        # the workspace copier. Trust that exact private file without widening
        # PDK relocation searches to unrelated candidate artifacts.
        if (_exists_as(original, row["directory"])
                and any(_path_is_under(original, root) for root in private_model_roots)):
            resolved = {"path": original, "mode": "private_library", "search_limited": False}
        else:
            try:
                resolved = resolve_relocated_reference(
                    original, roots, remaps=remaps, directory=row["directory"],
                )
            except AutomaticModelPathResolutionError as error:
                # Keep the ADE path exactly as supplied. Spectre can report a
                # missing include; incomplete or ambiguous searches must never
                # select a guessed model or block unrelated references.
                diagnostic = str(error)
                records.append({
                    "file": row["file"], "role": row["role"],
                    "original": row["reference"], "resolved": row["reference"],
                    "directory": row["directory"], "changed": False,
                    "status": "unresolved", "resolution": "unresolved",
                    "reason_code": error.reason_code, "warning": diagnostic,
                    "search_limited": error.reason_code == "search_limited",
                    "target_identity": {
                        "basename": os.path.basename(original), "size": None, "sha256": None,
                        "reference_sha256": hashlib.sha256(
                            row["reference"].encode("utf-8")
                        ).hexdigest(),
                        "reason_code": error.reason_code,
                    },
                })
                limitations.append({
                    "file": row["file"], "role": row["role"],
                    "reference": row["reference"], "directory": row["directory"],
                    "reason": "automatic_model_path_unresolved",
                    "reason_code": error.reason_code, "error": diagnostic,
                })
                continue
        target = resolved["path"]
        changed = target != original
        if changed:
            replacements_by_file.setdefault(row["file"], {})[row["reference"]] = target
        target_identity = {
            "basename": os.path.basename(target),
            "size": os.path.getsize(target) if os.path.isfile(target) else None,
            "sha256": sha256_file(target) if os.path.isfile(target) else None,
        }
        records.append({
            "file": row["file"],
            "role": row["role"],
            "original": row["reference"],
            "resolved": target,
            "directory": row["directory"],
            "changed": changed,
            "resolution": resolved["mode"],
            "target_identity": target_identity,
        })
    changed_files = []
    for path, replacements in sorted(replacements_by_file.items()):
        require_under(path, private_setup_root, "private ADE setup file")
        require_under(os.path.realpath(path), os.path.realpath(private_setup_root), "private ADE setup file target")
        with open(path, "r") as stream:
            text = stream.read()
        original_text = text
        for source, destination in sorted(
                replacements.items(), key=lambda item: (-len(item[0]), item[0])):
            text = _replace_path_token(text, source, destination)
        if text != original_text:
            atomic_write_text(path, text)
            changed_files.append(path)
        for source in replacements:
            if _replace_path_token(text, source, "__cr_obsolete_model_reference__") != text:
                raise ModelPathRemapError(
                    "private ADE setup still contains an obsolete model/include path after remap: %s"
                    % source
                )
    identity_rows = [dict(
        (key, row[key]) for key in (
            "role", "directory", "changed", "resolution", "target_identity",
        )
    ) for row in records]
    fingerprint = hashlib.sha256(json.dumps(
        identity_rows, separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")).hexdigest()
    return {
        "schema_version": MODEL_PATH_SCHEMA_VERSION,
        "roots": roots,
        "path_remaps": [
            {"source": source, "destination": destination}
            for source, destination in remaps
        ],
        "references": records,
        "changed_files": changed_files,
        "limitations": limitations,
        "fingerprint": fingerprint,
    }
