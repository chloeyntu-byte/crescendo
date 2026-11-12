from __future__ import print_function

import ast
import errno
import hashlib
import json
import os
import re
import stat
import xml.etree.ElementTree as ET

from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import sha256_file


MODEL_SUFFIXES = (
    ".scs", ".sp", ".spi", ".spice", ".cir", ".lib", ".mdl", ".model",
    ".va", ".vams", ".inc",
)
TECHNOLOGY_FILENAMES = (
    "tech.db", "techfile.cds", "techfile.tf", "technology.tf", "tech.tf", "lib.defs",
)
SPECTRE_ENGINE_RELATIVES = (
    "tools.lnx86/spectre/bin/64bit/spectre",
    "tools.lnx86/spectre/bin/spectre",
)
MAX_RECURSIVE_FILES = 4096
MAX_PARSE_BYTES = 32 * 1024 * 1024
MAX_SYMLINK_HOPS = 64


def _json_sha256(value):
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_external_eda_environment_identity(identity):
    if not isinstance(identity, dict) or identity.get("schema_version") != _contract_text("crescendo.external_eda_environment.v1"):
        raise ValueError("external EDA environment identity schema is not supported")
    expected = identity.get("fingerprint")
    if not isinstance(expected, str) or not re.match(r"^[0-9a-f]{64}$", expected):
        raise ValueError("external EDA environment identity fingerprint is malformed")
    payload = dict(identity)
    payload.pop("fingerprint", None)
    if _json_sha256(payload) != expected:
        raise ValueError("external EDA environment identity fingerprint does not match its payload")
    artifacts = identity.get("artifacts")
    limitations = identity.get("limitations")
    coverage = identity.get("coverage")
    if not isinstance(artifacts, list) or not isinstance(limitations, list) or not isinstance(coverage, dict):
        raise ValueError("external EDA environment identity payload is malformed")
    for artifact in artifacts:
        if (
            not isinstance(artifact, dict)
            or artifact.get("kind") != "file"
            or not isinstance(artifact.get("role"), str)
            or not re.match(r"^[0-9a-f]{64}$", str(artifact.get("sha256") or ""))
            or isinstance(artifact.get("size"), bool)
            or not isinstance(artifact.get("size"), int)
            or artifact.get("size") < 0
            or "path" in artifact
        ):
            raise ValueError("external EDA artifact identity is malformed")
    if any(not isinstance(row, dict) for row in limitations):
        raise ValueError("external EDA identity limitation is malformed")
    return identity


def _lstat_metadata(path):
    inspected = os.lstat(path)
    return {
        "device": int(inspected.st_dev),
        "inode": int(inspected.st_ino),
        "size": int(inspected.st_size),
        "mtime_ns": int(getattr(
            inspected, "st_mtime_ns", int(inspected.st_mtime * 1000000000),
        )),
        "ctime_ns": int(getattr(
            inspected, "st_ctime_ns", int(inspected.st_ctime * 1000000000),
        )),
        "mode": int(stat.S_IMODE(inspected.st_mode)),
        "regular": bool(stat.S_ISREG(inspected.st_mode)),
        "directory": bool(stat.S_ISDIR(inspected.st_mode)),
        "symlink": bool(stat.S_ISLNK(inspected.st_mode)),
    }


def _capture_path_resolution(path):
    reference_path = os.path.abspath(path)
    pending = reference_path
    symlink_chain = []
    path_components = []
    recorded_components = set()
    seen = set()
    for _hop in range(MAX_SYMLINK_HOPS + 1):
        components = [item for item in pending.split(os.path.sep) if item]
        current = os.path.sep
        followed = False
        for index, component in enumerate(components):
            current = os.path.join(current, component)
            metadata = _lstat_metadata(current)
            component_row = {"path": current, "lstat": metadata}
            if metadata.get("symlink"):
                component_row["target"] = os.readlink(current)
            if current not in recorded_components:
                recorded_components.add(current)
                path_components.append(component_row)
            if not metadata.get("symlink"):
                continue
            target = component_row["target"]
            signature = (
                current, metadata.get("device"), metadata.get("inode"), target,
            )
            if signature in seen:
                raise OSError(errno.ELOOP, "symbolic link cycle detected", current)
            seen.add(signature)
            symlink_chain.append({
                "path": current, "target": target, "lstat": metadata,
            })
            remainder = components[index + 1:]
            if os.path.isabs(target):
                pending = os.path.join(target, *remainder)
            else:
                pending = os.path.join(
                    os.path.dirname(current), target, *remainder
                )
            pending = os.path.abspath(pending)
            followed = True
            break
        if followed:
            continue
        target_path = os.path.abspath(pending)
        return {
            "reference_path": reference_path,
            "target_path": target_path,
            "symlink_chain": symlink_chain,
            "path_components": path_components,
            "target_lstat": _lstat_metadata(target_path),
        }
    raise OSError(errno.ELOOP, "too many symbolic link hops", reference_path)


def _metadata_matches(expected, observed):
    if not isinstance(expected, dict) or not isinstance(observed, dict):
        return False
    required = (
        "device", "inode", "size", "mtime_ns", "mode", "regular", "symlink",
    )
    if any(key not in expected or key not in observed for key in required):
        return False
    return all(observed.get(key) == value for key, value in expected.items())


def _resolution_matches(expected, observed):
    if (
        expected.get("reference_path") != observed.get("reference_path")
        or expected.get("target_path") != observed.get("target_path")
        or not _metadata_matches(
            expected.get("target_lstat"), observed.get("target_lstat"),
        )
    ):
        return False
    expected_chain = expected.get("symlink_chain")
    observed_chain = observed.get("symlink_chain")
    if not isinstance(expected_chain, list) or len(expected_chain) != len(observed_chain):
        return False
    for before, after in zip(expected_chain, observed_chain):
        if (
            not isinstance(before, dict)
            or before.get("path") != after.get("path")
            or before.get("target") != after.get("target")
            or not _metadata_matches(before.get("lstat"), after.get("lstat"))
        ):
            return False
    expected_components = expected.get("path_components")
    observed_components = observed.get("path_components")
    if expected_components is not None:
        if (
            not isinstance(expected_components, list)
            or len(expected_components) != len(observed_components)
        ):
            return False
        for before, after in zip(expected_components, observed_components):
            if (
                not isinstance(before, dict)
                or before.get("path") != after.get("path")
                or before.get("target") != after.get("target")
                or not _metadata_matches(before.get("lstat"), after.get("lstat"))
            ):
                return False
    return True


def _stable_component_matches(expected, observed, final_target=False):
    if not isinstance(expected, dict) or not isinstance(observed, dict):
        return False
    if final_target or expected.get("symlink") or expected.get("regular"):
        return _metadata_matches(expected, observed)
    stable_keys = (
        "device", "inode", "mode", "regular", "directory", "symlink",
    )
    return all(expected.get(key) == observed.get(key) for key in stable_keys)


def _fast_resolution_unchanged(expected, metadata_cache, readlink_cache):
    components = expected.get("path_components")
    if not isinstance(components, list) or not components:
        return False
    target_path = expected.get("target_path")
    for component in components:
        if not isinstance(component, dict) or not isinstance(component.get("path"), str):
            return False
        path = component["path"]
        if path not in metadata_cache:
            metadata_cache[path] = _lstat_metadata(path)
        expected_metadata = component.get("lstat")
        if not _stable_component_matches(
            expected_metadata, metadata_cache[path], final_target=(path == target_path),
        ):
            return False
        if expected_metadata.get("symlink"):
            if path not in readlink_cache:
                readlink_cache[path] = os.readlink(path)
            if component.get("target") != readlink_cache[path]:
                return False
    return True


def _build_path_watch(rows):
    watched = {}
    for row in rows:
        target_path = row.get("path")
        for component in row.get("path_components") or []:
            path = component.get("path")
            metadata = component.get("lstat")
            if not isinstance(path, str) or not isinstance(metadata, dict):
                continue
            current = watched.get(path)
            full_metadata = bool(
                path == target_path or metadata.get("regular")
                or metadata.get("symlink")
            )
            if current is None:
                current = {
                    "path": path, "lstat": metadata,
                    "full_metadata": full_metadata,
                }
                if metadata.get("symlink"):
                    current["target"] = component.get("target")
                watched[path] = current
            else:
                current["full_metadata"] = bool(
                    current.get("full_metadata") or full_metadata
                )
    return sorted(watched.values(), key=lambda row: row["path"])


def _stat_result_matches(expected, inspected, full_metadata=False):
    if not isinstance(expected, dict):
        return False
    inspected_mode = inspected.st_mode
    if (
        expected.get("device") != int(inspected.st_dev)
        or expected.get("inode") != int(inspected.st_ino)
        or expected.get("mode") != int(stat.S_IMODE(inspected_mode))
        or expected.get("regular") != bool(stat.S_ISREG(inspected_mode))
        or expected.get("directory") != bool(stat.S_ISDIR(inspected_mode))
        or expected.get("symlink") != bool(stat.S_ISLNK(inspected_mode))
    ):
        return False
    if not full_metadata:
        return True
    observed_size = int(inspected.st_size)
    observed_mtime_ns = int(getattr(
        inspected, "st_mtime_ns", int(inspected.st_mtime * 1000000000),
    ))
    if expected.get("size") != observed_size or expected.get("mtime_ns") != observed_mtime_ns:
        return False
    if "ctime_ns" in expected:
        observed_ctime_ns = int(getattr(
            inspected, "st_ctime_ns", int(inspected.st_ctime * 1000000000),
        ))
        if expected.get("ctime_ns") != observed_ctime_ns:
            return False
    return True


def _path_watch_unchanged(watch):
    if not isinstance(watch, list) or not watch:
        return None
    for row in watch:
        if (
            not isinstance(row, dict)
            or not isinstance(row.get("path"), str)
            or not isinstance(row.get("lstat"), dict)
        ):
            return None
        observed = os.lstat(row["path"])
        if not _stat_result_matches(
            row["lstat"], observed, full_metadata=bool(row.get("full_metadata")),
        ):
            return False
        if row["lstat"].get("symlink") and os.readlink(row["path"]) != row.get("target"):
            return False
    return True


def check_external_eda_environment_metadata(diagnostics):
    rows = (diagnostics or {}).get("resolved_paths")
    if not isinstance(rows, list) or not rows:
        return {
            "ok": False, "changed": True, "state": "metadata_unavailable",
            "error": "external EDA resolved-file metadata is unavailable",
        }
    try:
        watch_unchanged = _path_watch_unchanged(
            (diagnostics or {}).get("path_watch"),
        )
    except OSError:
        watch_unchanged = False
    if watch_unchanged is True:
        return {
            "ok": True, "changed": False, "unsafe": False,
            "state": "unchanged", "checked_files": len(rows),
            "checked_paths": len(diagnostics["path_watch"]), "changes": [],
        }
    checked = 0
    changes = []
    metadata_cache = {}
    readlink_cache = {}
    for row in rows:
        if not isinstance(row, dict):
            return {
                "ok": False, "changed": True, "state": "metadata_malformed",
                "error": "external EDA resolved-file metadata is malformed",
            }
        path = row.get("path")
        expected = row.get("lstat")
        if not isinstance(path, str) or not os.path.isabs(path) or not isinstance(expected, dict):
            return {
                "ok": False, "changed": True, "state": "metadata_malformed",
                "error": "external EDA resolved-file metadata is malformed",
            }
        reference_path = row.get("reference_path")
        expected_chain = row.get("symlink_chain")
        expected_components = row.get("path_components")
        has_resolution = reference_path is not None or expected_chain is not None
        if has_resolution and (
            not isinstance(reference_path, str)
            or not os.path.isabs(reference_path)
            or not isinstance(expected_chain, list)
        ):
            return {
                "ok": False, "changed": True, "state": "metadata_malformed",
                "error": "external EDA path-resolution metadata is malformed",
            }
        try:
            fast_unchanged = bool(
                has_resolution and expected_components is not None
                and _fast_resolution_unchanged({
                    "path_components": expected_components,
                    "target_path": path,
                }, metadata_cache, readlink_cache)
            )
            resolution = (
                None if fast_unchanged or not has_resolution
                else _capture_path_resolution(reference_path)
            )
            observed = (
                expected if fast_unchanged else resolution["target_lstat"]
                if resolution is not None
                else _lstat_metadata(path)
            )
        except OSError as error:
            changes.append({
                "role": row.get("role"), "path": path,
                "reason": "lstat_failed", "error_type": error.__class__.__name__,
            })
            continue
        checked += 1
        if observed.get("symlink") or not observed.get("regular"):
            changes.append({
                "role": row.get("role"), "path": path,
                "reason": "unsafe_file_type", "observed": observed,
            })
            continue
        if has_resolution:
            if fast_unchanged:
                continue
            expected_resolution = {
                "reference_path": reference_path,
                "target_path": path,
                "symlink_chain": expected_chain,
                "path_components": expected_components,
                "target_lstat": expected,
            }
            if not _resolution_matches(expected_resolution, resolution):
                unexpected_symlink = bool(
                    not expected_chain and resolution.get("symlink_chain")
                )
                changes.append({
                    "role": row.get("role"), "path": path,
                    "reason": (
                        "unexpected_symlink" if unexpected_symlink
                        else "path_resolution_changed"
                    ),
                    "before": expected_resolution, "after": resolution,
                })
            continue
        if not _metadata_matches(expected, observed):
            changes.append({
                "role": row.get("role"), "path": path,
                "reason": "metadata_changed", "before": expected, "after": observed,
            })
    unsafe = any(
        row.get("reason") in ("unsafe_file_type", "unexpected_symlink")
        for row in changes
    )
    return {
        "ok": not changes,
        "changed": bool(changes),
        "unsafe": unsafe,
        "state": "unchanged" if not changes else "changed",
        "checked_files": checked,
        "changes": changes,
    }


def _expand_reference(value):
    return os.path.expandvars(os.path.expanduser(str(value or "").strip()))


def _reference_token(value):
    reference = str(value or "").replace("\\", "/").strip()
    if "/" in reference:
        reference = reference.rsplit("/", 1)[-1]
    return hashlib.sha256(reference.encode("utf-8")).hexdigest()


def _quoted_values(text):
    values = []
    for token in re.findall(r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'', text or ""):
        try:
            values.append(ast.literal_eval(token))
        except (SyntaxError, ValueError):
            continue
    return values


def _portable_reference(value):
    normalized = str(value or "").replace("\\", "/").strip()
    return normalized.rsplit("/", 1)[-1] if "/" in normalized else normalized


def _normalized_cds_lib(path):
    normalized = []
    with open(path, "r") as stream:
        for line in stream:
            stripped = line.strip()
            if not stripped or stripped.startswith(("#", ";")):
                continue
            include = re.match(r"^(SOFTINCLUDE|INCLUDE)\s+(.+?)\s*$", stripped, re.I)
            if include:
                reference = include.group(2).strip().strip('"').strip("'")
                normalized.append("%s %s" % (
                    include.group(1).upper(), _portable_reference(reference),
                ))
                continue
            define = re.match(r"^DEFINE\s+(\S+)\s+(.+?)\s*$", stripped, re.I)
            if define:
                reference = define.group(2).strip().strip('"').strip("'")
                normalized.append("DEFINE %s %s" % (
                    define.group(1), _portable_reference(reference),
                ))
                continue
            normalized.append(" ".join(stripped.split()))
    return ("\n".join(normalized) + "\n").encode("utf-8")


def _normalized_cdsinit(path):
    normalized = []
    with open(path, "r") as stream:
        for line in stream:
            stripped = line.strip()
            if not stripped or stripped.startswith(";"):
                continue

            def portable_load(match):
                return '%s("%s"' % (
                    match.group(1), _portable_reference(match.group(2)),
                )

            stripped = re.sub(
                r"\b(loadi?)\s*\(\s*\"([^\"]+)\"",
                portable_load, stripped, flags=re.I,
            )
            normalized.append(stripped)
    return ("\n".join(normalized) + "\n").encode("utf-8")


def _looks_like_file_reference(value):
    value = str(value or "").strip()
    lowered = value.lower()
    return bool(
        value
        and value.lower() not in ("nil", "t")
        and (
            "/" in value or "\\" in value or value.startswith((".", "$", "~"))
            or lowered.endswith(MODEL_SUFFIXES)
        )
    )


def _extract_file_references(text):
    quoted = [value for value in _quoted_values(text) if _looks_like_file_reference(value)]
    if quoted:
        return quoted
    raw = str(text or "").strip().strip('"').strip("'")
    return [raw] if _looks_like_file_reference(raw) else []


def _extract_directory_references(text):
    quoted = _quoted_values(text)
    if quoted:
        return [value for value in quoted if str(value or "").strip()]
    raw = str(text or "").strip().strip('"').strip("'")
    if not raw or raw.lower() in ("nil", "t"):
        return []
    return [item for item in raw.split(os.pathsep) if item]


def _local_tag(element):
    return str(element.tag).rsplit("}", 1)[-1].lower()


def _ade_setup_references(setup):
    source = (setup or {}).get("source") or {}
    references = {"model": [], "include": [], "definition": [], "technology": []}
    include_directories = []
    limitations = []
    paths = [
        source.get("setup_db") or source.get("maestro_sdb") or source.get("data_sdb"),
        source.get("active_state"),
    ]
    origin_directory = None
    if (setup or {}).get("baseline"):
        from .setup_baseline import effective_setup_path, effective_setup_dir, origin_setup_path
        paths = [effective_setup_path(setup), os.path.join(effective_setup_dir(setup), "active.state")]
        origin_directory = os.path.dirname(origin_setup_path(setup))
        paths = [path for path in paths if os.path.isfile(path)]
    direct_tags = {
        "modelfile": "model", "includefile": "include",
        "definitionfile": "definition", "techfile": "technology",
        "technologyfile": "technology",
    }
    field_roles = {
        "modelfiles": "model", "includefiles": "include",
        "definitionfiles": "definition", "techfile": "technology",
        "techfiles": "technology", "technologyfile": "technology",
        "technologyfiles": "technology",
    }
    for path in paths:
        if not path:
            continue
        try:
            root = ET.parse(path).getroot()
        except (ET.ParseError, IOError, OSError) as error:
            limitations.append({
                "role": "ade_setup_xml", "reason": "xml_parse_failed",
                "reference_token_sha256": _reference_token(os.path.basename(path)),
                "error_type": error.__class__.__name__,
            })
            continue
        base = origin_directory or os.path.dirname(path)
        for element in root.iter():
            tag = _local_tag(element)
            role = direct_tags.get(tag)
            text = (element.text or "").strip()
            if role and text:
                references[role].append((text, base))
            if tag != "field":
                continue
            name = str(element.attrib.get("Name") or "").lower()
            if name == "includepath":
                for value in _extract_directory_references(text):
                    include_directories.append((value, base))
                continue
            role = field_roles.get(name)
            if not role or not text or text.lower() == "nil":
                continue
            extracted = _extract_file_references(text)
            literal_values = _quoted_values(text)
            explicitly_empty = bool(literal_values) and all(
                not str(value or "").strip() for value in literal_values
            )
            if not extracted and not explicitly_empty:
                limitations.append({
                    "role": role, "reason": "maestro_reference_expression_not_literal",
                    "reference_token_sha256": _reference_token(text),
                })
            for value in extracted:
                references[role].append((value, base))
    return references, include_directories, limitations


def _resolve_reference(value, base, search_directories=()):
    expanded = _expand_reference(value)
    candidates = []
    if os.path.isabs(expanded):
        candidates.append(expanded)
    else:
        candidates.append(os.path.join(base, expanded))
        candidates.extend(os.path.join(directory, expanded) for directory in search_directories)
    for candidate in candidates:
        if os.path.isfile(candidate):
            return os.path.abspath(candidate)
    return os.path.abspath(candidates[0]) if candidates else None


class _ArtifactCollector(object):
    def __init__(self):
        self.artifacts = []
        self.limitations = []
        self.diagnostic_limitations = []
        self.diagnostic_paths = []
        self._seen = set()
        self._diagnostic_seen = set()

    def limitation(self, role, reason, reference=None, error=None):
        row = {"role": role, "reason": reason}
        if reference is not None:
            row["reference_token_sha256"] = _reference_token(reference)
            basename = os.path.basename(str(reference).replace("\\", "/"))
            if basename and len(basename) <= 255:
                row["reference_basename"] = basename
        if error is not None:
            row["error_type"] = error.__class__.__name__
        if row not in self.limitations:
            self.limitations.append(row)
        diagnostic = dict(row)
        if reference is not None:
            diagnostic["reference"] = str(reference)
        if diagnostic not in self.diagnostic_limitations:
            self.diagnostic_limitations.append(diagnostic)

    def add_file(self, role, path, executable=False, normalizer=None,
                 normalization=None):
        if not path:
            self.limitation(role, "path_not_resolved")
            return None
        try:
            resolution_before = _capture_path_resolution(path)
            resolved = resolution_before["target_path"]
            metadata_before = resolution_before["target_lstat"]
            if metadata_before.get("symlink") or not metadata_before.get("regular"):
                self.limitation(role, "referenced_file_not_regular", reference=path)
                return None
        except (IOError, OSError) as error:
            reason = (
                "referenced_file_missing" if isinstance(error, OSError)
                and getattr(error, "errno", None) == errno.ENOENT
                else "referenced_file_unreadable"
            )
            self.limitation(role, reason, reference=path, error=error)
            return None
        key = (role, resolved)
        diagnostic_key = (role, resolution_before["reference_path"])
        if diagnostic_key not in self._diagnostic_seen:
            self._diagnostic_seen.add(diagnostic_key)
            self.diagnostic_paths.append({
                "role": role,
                "path": resolved,
                "lstat": metadata_before,
                "reference_path": resolution_before["reference_path"],
                "symlink_chain": resolution_before["symlink_chain"],
                "path_components": resolution_before["path_components"],
            })
        if key in self._seen:
            return resolved
        self._seen.add(key)
        try:
            normalized_content = normalizer(resolved) if normalizer is not None else None
            identity = {
                "role": role,
                "kind": "file",
                "size": (
                    len(normalized_content) if normalized_content is not None
                    else os.path.getsize(resolved)
                ),
                "sha256": (
                    hashlib.sha256(normalized_content).hexdigest()
                    if normalized_content is not None else sha256_file(resolved)
                ),
            }
        except (UnicodeDecodeError, IOError, OSError) as error:
            self.limitation(role, "referenced_file_unreadable", reference=path, error=error)
            return None
        if normalized_content is not None:
            identity["normalization"] = str(normalization or "semantic_v1")
        if executable:
            identity["executable"] = bool(os.access(resolved, os.X_OK))
        try:
            resolution_after = _capture_path_resolution(path)
            metadata = resolution_after["target_lstat"]
        except OSError as error:
            self.limitation(role, "referenced_file_unreadable", reference=path, error=error)
            return None
        if not _resolution_matches(resolution_before, resolution_after):
            self.limitation(role, "referenced_file_changed_while_reading", reference=path)
            return None
        if metadata.get("symlink") or not metadata.get("regular"):
            self.limitation(role, "referenced_file_not_regular", reference=path)
            return None
        self.artifacts.append(identity)
        return resolved


def _literal_include(line):
    match = re.match(
        r"^\s*(?:include|ahdl_include|\.include)\s+(?:\"([^\"]+)\"|'([^']+)'|(\S+))",
        line, re.I,
    )
    if match:
        return next(value for value in match.groups() if value is not None)
    match = re.match(
        r"^\s*\.lib\s+(?:\"([^\"]+)\"|'([^']+)'|(\S+))",
        line, re.I,
    )
    if match:
        value = next(value for value in match.groups() if value is not None)
        return value if _looks_like_file_reference(value) else None
    return None


def _collect_model_tree(collector, path, include_directories):
    pending = [path]
    visited = set()
    while pending:
        reference_path = pending.pop()
        current = os.path.realpath(reference_path)
        if current in visited:
            continue
        visited.add(current)
        if len(visited) > MAX_RECURSIVE_FILES:
            collector.limitation("model_dependency", "recursive_file_limit_exceeded")
            break
        role = "model_file" if len(visited) == 1 else "model_dependency"
        resolved = collector.add_file(role, reference_path)
        if resolved is None:
            continue
        try:
            size = os.path.getsize(resolved)
            if size > MAX_PARSE_BYTES:
                collector.limitation(role, "include_scan_size_limit", reference=os.path.basename(resolved))
                continue
            with open(resolved, "r") as stream:
                for line_number, line in enumerate(stream, 1):
                    reference = _literal_include(line)
                    if reference is None:
                        if re.match(r"^\s*(?:include|ahdl_include|\.include)\b", line, re.I):
                            collector.limitation(
                                "model_dependency", "dynamic_include_expression",
                                reference="%s:%d:%s" % (
                                    os.path.basename(resolved), line_number, line.strip(),
                                ),
                            )
                        continue
                    included = _resolve_reference(
                        reference, os.path.dirname(resolved), include_directories,
                    )
                    if included and os.path.isfile(included):
                        pending.append(included)
                    else:
                        collector.limitation(
                            "model_dependency", "included_file_missing", reference=reference,
                        )
        except (UnicodeDecodeError, IOError, OSError) as error:
            collector.limitation(role, "include_scan_failed", reference=os.path.basename(resolved), error=error)


def _collect_cds_chain(collector, role, path, seen=None):
    if not path:
        return
    seen = seen or set()
    resolved = os.path.realpath(os.path.abspath(path))
    if resolved in seen:
        return
    if len(seen) >= 256:
        collector.limitation(role, "recursive_file_limit_exceeded")
        return
    seen.add(resolved)
    actual = collector.add_file(
        role, path, normalizer=_normalized_cds_lib,
        normalization="cadence_cds_lib_semantic_v1",
    )
    if actual is None:
        return
    try:
        with open(actual, "r") as stream:
            for line in stream:
                match = re.match(r"^\s*(SOFTINCLUDE|INCLUDE)\s+(.+?)\s*$", line, re.I)
                if not match:
                    continue
                reference = match.group(2).strip().strip('"').strip("'")
                included = _resolve_reference(reference, os.path.dirname(actual))
                if included and os.path.isfile(included):
                    _collect_cds_chain(collector, role + "_include", included, seen)
                elif match.group(1).upper() == "INCLUDE":
                    collector.limitation(role + "_include", "included_file_missing", reference=reference)
    except (UnicodeDecodeError, IOError, OSError) as error:
        collector.limitation(role, "include_scan_failed", reference=os.path.basename(actual), error=error)


def _collect_cdsinit_chain(collector, path, seen=None, root_role="startup_extension"):
    if not path:
        return
    seen = seen or set()
    resolved = os.path.realpath(os.path.abspath(path))
    if resolved in seen:
        return
    if len(seen) >= 256:
        collector.limitation(root_role + "_load", "recursive_file_limit_exceeded")
        return
    seen.add(resolved)
    role = root_role if len(seen) == 1 else root_role + "_load"
    actual = collector.add_file(
        role, path, normalizer=_normalized_cdsinit,
        normalization="cadence_cdsinit_load_semantic_v1",
    )
    if actual is None:
        return
    try:
        with open(actual, "r") as stream:
            for line in stream:
                for match in re.finditer(r"\bloadi?\s*\(\s*\"([^\"]+)\"", line, re.I):
                    reference = match.group(1)
                    included = _resolve_reference(reference, os.path.dirname(actual))
                    if included and os.path.isfile(included):
                        _collect_cdsinit_chain(
                            collector, included, seen, root_role=root_role,
                        )
                    else:
                        collector.limitation(root_role + "_load", "loaded_file_missing", reference=reference)
    except (UnicodeDecodeError, IOError, OSError) as error:
        collector.limitation(role, "load_scan_failed", reference=os.path.basename(actual), error=error)


def collect_external_eda_environment(setup, cadence, pdk, submission=None):
    cadence = dict(cadence or {})
    pdk = dict(pdk or {})
    collector = _ArtifactCollector()

    saved_simulators = set((row.get("simulator") or "spectre").lower() for row in (setup or {}).get("tests", []) if row.get("enabled", True))
    capture_simulator = "alps" if saved_simulators == {"alps"} else "spectre"
    launcher = cadence.get(capture_simulator + "_bin")
    resolved_launcher = collector.add_file(
        "simulator_executable", launcher,
        executable=True,
    )
    spectre_root = cadence.get("spectre_root") if capture_simulator == "spectre" else None
    engine = None
    if spectre_root:
        for relative in SPECTRE_ENGINE_RELATIVES:
            candidate = os.path.join(spectre_root, relative)
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                engine = candidate
                break
    if engine and os.path.realpath(engine) != resolved_launcher:
        collector.add_file("simulator_engine", engine, executable=True)
    elif resolved_launcher is None:
        collector.limitation("simulator_engine", "engine_not_resolved")

    if cadence.get("alps_bin") and capture_simulator != "alps":
        collector.add_file("alps_simulator_executable", cadence["alps_bin"], executable=True)
    model_mapping = cadence.get("alps_model_file_map") or {}
    if not isinstance(model_mapping, dict):
        raise ValueError("ALPS model mapping must be an object")
    for index, (source, target) in enumerate(sorted(model_mapping.items())):
        path = target.get("path") if isinstance(target, dict) else target
        collector.add_file("alps_mapped_model_%d" % index, path)

    if submission is not None:
        collector.add_file(
            "submit_script", (submission or {}).get("submit_script"), executable=True,
        )

    source_cds_lib = pdk.get("source_cds_lib")
    _collect_cds_chain(collector, "source_cds_lib", source_cds_lib)
    installation_cds_lib = cadence.get("cds_lib")
    if installation_cds_lib and (
            not source_cds_lib
            or os.path.realpath(installation_cds_lib) != os.path.realpath(source_cds_lib)):
        _collect_cds_chain(collector, "installation_cds_lib", installation_cds_lib)
    if not source_cds_lib:
        collector.limitation("source_cds_lib", "path_not_resolved")
    startup_extensions = list(cadence.get("startup_extensions") or [])
    startup_extensions.extend(pdk.get("startup_extensions") or [])
    for context in pdk.get("contexts") or []:
        startup_extensions.extend(context.get("startup_extensions") or [])
    startup_seen = set()
    for extension in startup_extensions:
        path = extension.get("path") if isinstance(extension, dict) else extension
        if not path:
            continue
        resolved = os.path.realpath(os.path.abspath(os.path.expanduser(str(path))))
        if resolved in startup_seen:
            continue
        startup_seen.add(resolved)
        _collect_cdsinit_chain(
            collector, resolved, root_role="startup_extension",
        )

    references, raw_include_directories, parser_limitations = _ade_setup_references(setup)
    collector.limitations.extend(parser_limitations)
    include_directories = []
    for reference, base in raw_include_directories:
        expanded = _expand_reference(reference)
        path = expanded if os.path.isabs(expanded) else os.path.join(base, expanded)
        if os.path.isdir(path):
            include_directories.append(os.path.abspath(path))
        else:
            collector.limitation("include_path", "directory_missing", reference=reference)
    include_directories = sorted(set(include_directories))

    model_paths = []
    for role in ("model", "include", "definition", "technology"):
        for reference, base in references[role]:
            resolved = _resolve_reference(reference, base, include_directories)
            if role == "model":
                if resolved and os.path.isfile(resolved):
                    model_paths.append(resolved)
                else:
                    collector.limitation("model_file", "referenced_file_missing", reference=reference)
            else:
                collector.add_file("%s_file" % role, resolved)
    for path in sorted(set(model_paths)):
        _collect_model_tree(collector, path, include_directories)
    # The ALPS bridge has independent recursive dependencies. Use the same
    # bounded include parser and publish limitations rather than scan a PDK.
    alps_include_dirs = list(include_directories)
    if cadence.get("alps_model_dir"):
        alps_include_dirs.append(cadence["alps_model_dir"])
    for target in model_mapping.values():
        path = target.get("path") if isinstance(target, dict) else target
        if path:
            _collect_model_tree(collector, path, alps_include_dirs)

    pdk_dir = pdk.get(_contract_text("pdk_dir")) or cadence.get(_contract_text("pdk_dir"))
    technology_found = False
    candidate_roots = []
    if pdk_dir:
        candidate_roots.append(os.path.abspath(pdk_dir))
        if pdk.get("pdk_name"):
            candidate_roots.append(os.path.join(candidate_roots[0], str(pdk["pdk_name"])))
    for root in candidate_roots:
        if not os.path.isdir(root):
            continue
        for name in TECHNOLOGY_FILENAMES:
            path = os.path.join(root, name)
            if os.path.isfile(path):
                collector.add_file("technology_file", path)
                technology_found = True
    if not technology_found:
        collector.limitation("technology_file", "recognized_top_level_file_not_found")

    artifacts = sorted(
        collector.artifacts,
        key=lambda row: (row.get("role") or "", row.get("sha256") or "", row.get("size") or 0),
    )
    limitations_by_value = dict(
        (json.dumps(row, separators=(",", ":"), sort_keys=True), row)
        for row in collector.limitations
    )
    limitations = sorted(
        limitations_by_value.values(),
        key=lambda row: json.dumps(row, separators=(",", ":"), sort_keys=True),
    )
    identity = {
        "schema_version": _contract_text("crescendo.external_eda_environment.v1"),
        "artifacts": artifacts,
        "coverage": {
            "simulator_executable": any(row["role"] == "simulator_executable" for row in artifacts),
            "submit_script": any(row["role"] == "submit_script" for row in artifacts),
            "source_cds_lib": any(row["role"] == "source_cds_lib" for row in artifacts),
            "source_cdsinit": False,
            "startup_extensions": len([
                row for row in artifacts if row["role"] == "startup_extension"
            ]),
            "model_files": len([row for row in artifacts if row["role"] in ("model_file", "model_dependency")]),
            "technology_files": len([row for row in artifacts if row["role"] == "technology_file"]),
            "include_parser": "literal Spectre/SPICE include, .include, .lib and ahdl_include",
        },
        "limitations": limitations,
    }
    identity["fingerprint"] = _json_sha256(identity)
    validate_external_eda_environment_identity(identity)
    resolved_paths = sorted(
        collector.diagnostic_paths,
        key=lambda row: (row.get("role") or "", row.get("path") or ""),
    )
    diagnostics = {
        "resolved_paths": resolved_paths,
        "path_watch": _build_path_watch(resolved_paths),
        "include_directories": include_directories,
        "limitations": sorted(
            collector.diagnostic_limitations,
            key=lambda row: json.dumps(row, separators=(",", ":"), sort_keys=True),
        ),
    }
    return {"identity": identity, "diagnostics": diagnostics}
