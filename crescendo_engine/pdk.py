from __future__ import print_function

import copy
import hashlib
import json
import os
import re
import shlex

from .resources import pdk_adapter_path


SCHEMA_CDS_LIBRARY_MAP = "crescendo.cds_library_map.v1"
SCHEMA_PDK_CONTEXTS = "crescendo.pdk_contexts.v2"
SCHEMA_PDK_PROFILE = "crescendo.pdk_profile.v1"
SCHEMA_ADAPTER_MANIFEST = "crescendo.pdk_adapter_manifest.v1"
ADAPTER_ABI = "crescendo.pdk_adapter.v1"
GENERIC_ADAPTER_ID = "generic-cdf"

SYSTEM_LIBRARIES = frozenset((
    "analogLib", "basic", "functional", "sample", "US_8ths",
    "cdsDefTechLib", "ahdlLib", "rfLib", "connectLib",
))

FALLBACK_ELIGIBLE_FAILURES = frozenset((
    "cdf_callback_incompatible",
    "cdf_update_order_incompatible",
    "cdf_update_api_incompatible",
    "cdf_parameter_mapping_incompatible",
    "netlist_effect_unverified",
))

FALLBACK_FORBIDDEN_FAILURES = {
    "simulation_nonconvergence": "simulation convergence is not an adapter compatibility problem",
    "simulation_timeout": "simulation timeout is not an adapter compatibility problem",
    "license_error": "license failures must be repaired before adapter selection",
    "model_missing": "missing models must be repaired before adapter selection",
    "model_section_missing": "missing model sections must be repaired before adapter selection",
    "technology_missing": "missing technology data must be repaired before adapter selection",
    "technology_mismatch": "technology binding must be repaired before adapter selection",
    "metric_missing": "missing metrics are result-extraction failures",
    "metric_evaluation_failed": "metric evaluation is not an adapter compatibility problem",
    "constraint_infeasible": "constraint feasibility is not an adapter compatibility problem",
    "pdk_clamped": "legal PDK clamping is an effective-value observation, not a fallback trigger",
    "pdk_quantized": "legal PDK quantization is an effective-value observation, not a fallback trigger",
    "infrastructure_error": "generic infrastructure errors are too broad for vendor fallback",
}


def _builtin_manifest(adapter_id, label, vendor, patterns, skill_name=None):
    return {
        "schema_version": SCHEMA_ADAPTER_MANIFEST,
        "id": adapter_id,
        "version": "1",
        "abi": ADAPTER_ABI,
        "label": label,
        "vendor": vendor,
        "priority": 100,
        "capabilities": {
            "cdf_parameter_write": True,
            "cdf_callback": "capability-probed",
            "effective_value_verification": True,
        },
        "matcher": {
            "minimum_strong_matches": 2,
            "rules": [
                {"field": "library_names", "patterns": patterns, "strength": "strong"},
                {"field": "technology_libraries", "patterns": patterns, "strength": "strong"},
                {"field": "package_markers", "patterns": patterns, "strength": "strong"},
                {"field": "metadata_tokens", "patterns": patterns, "strength": "strong"},
            ],
        },
        "implementation": {
            "type": "skill" if skill_name else "generic-delegate",
            "skill_path": pdk_adapter_path(skill_name) if skill_name else None,
            "overrides": [],
        },
    }


GENERIC_ADAPTER_MANIFEST = {
    "schema_version": SCHEMA_ADAPTER_MANIFEST,
    "id": GENERIC_ADAPTER_ID,
    "version": "1",
    "abi": ADAPTER_ABI,
    "label": "Generic CDF",
    "vendor": None,
    "priority": 0,
    "capabilities": {
        "cdf_parameter_write": True,
        "cdf_callback": "capability-probed",
        "effective_value_verification": True,
    },
    "matcher": {"minimum_strong_matches": 0, "rules": []},
    "implementation": {
        "type": "generic-cdf",
        "skill_path": None,
        "overrides": [],
    },
}


# Built-in example adapters for a fictional "examplekit" process kit. Real kits
# are matched by user-provided adapter manifests (see the Custom Adapter
# setting); these examples only document the manifest format and matching.
BUILTIN_ADAPTER_MANIFESTS = (
    GENERIC_ADAPTER_MANIFEST,
    _builtin_manifest(
        "builtin.examplekit18-1", "Example kit 0.18 fallback", "Example",
        (r"(?i)examplekit[^/]*18(?![^/]*pas)", r"(?i)examplekit18\b"),
    ),
    _builtin_manifest(
        "builtin.examplekit18pas-1", "Example kit 0.18 PAS transaction", "Example",
        (r"(?i)examplekit[^/]*18[^/]*pas", r"(?i)examplekit18pas"),
        skill_name="cadence_pas_transaction.il",
    ),
    _builtin_manifest(
        "builtin.examplekit65pas-1", "Example kit 65 PAS transaction", "Example",
        (r"(?i)examplekit[^/]*65[^/]*pas", r"(?i)examplekit65pas"),
        skill_name="cadence_pas_transaction.il",
    ),
)


def _canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _sha256_text(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _as_list(value):
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set, frozenset)):
        return list(value)
    return [value]


def _stable_unique(values):
    result = []
    seen = set()
    for value in values or []:
        if value in (None, ""):
            continue
        text = str(value)
        if text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _expand_cds_value(value, base_dir, environment, unresolved):
    value = str(value).strip().strip('"').strip("'")

    def replace_parenthesized(match):
        name = match.group(1)
        if name not in environment:
            unresolved.add(name)
            return match.group(0)
        return str(environment[name])

    value = re.sub(r"\$\(([_A-Za-z][_A-Za-z0-9]*)\)", replace_parenthesized, value)

    def replace_shell(match):
        name = match.group(1) or match.group(2)
        if name not in environment:
            unresolved.add(name)
            return match.group(0)
        return str(environment[name])

    value = re.sub(r"\$\{([_A-Za-z][_A-Za-z0-9]*)\}|\$([_A-Za-z][_A-Za-z0-9]*)", replace_shell, value)
    value = os.path.expanduser(value)
    if not os.path.isabs(value):
        value = os.path.join(base_dir, value)
    return os.path.abspath(os.path.normpath(value))


def _directive_tokens(raw):
    lexer = shlex.shlex(raw, posix=True)
    lexer.whitespace_split = True
    lexer.commenters = "#"
    try:
        return list(lexer)
    except ValueError:
        return []


def parse_cds_lib(path, environment=None, max_depth=64):
    environment = dict(os.environ if environment is None else environment)
    root = os.path.abspath(os.path.expanduser(str(path))) if path else None
    definitions = {}
    definition_records = []
    include_records = []
    files = []
    cycles = []
    errors = []
    warnings = []
    unresolved_variables = set()
    active = []

    def read_file(current, directive=None, depth=0):
        current = os.path.realpath(os.path.abspath(current))
        if depth > max_depth:
            errors.append({
                "code": "include_depth_exceeded", "path": current,
                "max_depth": int(max_depth),
            })
            return
        if current in active:
            cycle = active[active.index(current):] + [current]
            if cycle not in cycles:
                cycles.append(cycle)
                errors.append({"code": "include_cycle", "paths": cycle})
            return
        if not os.path.isfile(current):
            issue = {
                "code": (
                    "softinclude_missing" if directive == "SOFTINCLUDE"
                    else "cds_lib_missing" if directive is None
                    else "include_missing"
                ),
                "path": current,
            }
            if directive == "SOFTINCLUDE":
                warnings.append(issue)
            else:
                errors.append(issue)
            return
        if current not in files:
            files.append(current)
        active.append(current)
        try:
            with open(current, "r", encoding="utf-8-sig") as stream:
                for line_number, raw in enumerate(stream, 1):
                    tokens = _directive_tokens(raw)
                    if not tokens:
                        continue
                    directive_name = tokens[0].upper()
                    if directive_name in ("INCLUDE", "SOFTINCLUDE") and len(tokens) >= 2:
                        raw_value = " ".join(tokens[1:])
                        include_path = _expand_cds_value(
                            raw_value, os.path.dirname(current), environment,
                            unresolved_variables,
                        )
                        record = {
                            "directive": directive_name,
                            "path": include_path,
                            "raw_path": raw_value,
                            "source_file": current,
                            "line": line_number,
                            "exists": os.path.isfile(include_path),
                        }
                        include_records.append(record)
                        read_file(include_path, directive_name, depth + 1)
                        continue
                    if directive_name == "DEFINE" and len(tokens) >= 3:
                        library = tokens[1]
                        raw_value = " ".join(tokens[2:])
                        library_path = _expand_cds_value(
                            raw_value, os.path.dirname(current), environment,
                            unresolved_variables,
                        )
                        previous = definitions.get(library)
                        record = {
                            "library": library,
                            "path": library_path,
                            "raw_path": raw_value,
                            "source_file": current,
                            "line": line_number,
                            "exists": os.path.isdir(library_path),
                            "precedence": len(definition_records),
                            "overrides": previous.get("precedence") if previous else None,
                        }
                        definition_records.append(record)
                        definitions[library] = record
        except (IOError, OSError) as error:
            errors.append({"code": "cds_lib_unreadable", "path": current, "message": str(error)})
        finally:
            active.pop()

    if root:
        read_file(root)
    else:
        errors.append({"code": "cds_lib_missing", "path": None})
    return {
        "schema_version": SCHEMA_CDS_LIBRARY_MAP,
        "ok": not errors,
        "root_file": root,
        "files": files,
        "definitions": dict(
            (name, record["path"]) for name, record in sorted(definitions.items())
        ),
        "active_definitions": dict(
            (name, copy.deepcopy(record)) for name, record in sorted(definitions.items())
        ),
        "definition_records": definition_records,
        "includes": include_records,
        "cycles": cycles,
        "unresolved_variables": sorted(unresolved_variables),
        "errors": errors,
        "warnings": warnings,
    }


def _normalize_path(value, base_dir=None):
    if value in (None, ""):
        return None
    value = os.path.expanduser(str(value))
    if not os.path.isabs(value) and base_dir:
        value = os.path.join(base_dir, value)
    return os.path.abspath(os.path.normpath(value))


def _normalize_paths(values, base_dir=None):
    return _stable_unique(_normalize_path(value, base_dir) for value in _as_list(values))


def _normalize_path_remaps(values, base_dir=None):
    if isinstance(values, dict):
        values = [
            {"source": source, "destination": destination}
            for source, destination in values.items()
        ]
    result = []
    for raw in _as_list(values):
        if isinstance(raw, (list, tuple)) and len(raw) == 2:
            source, destination = raw
        elif isinstance(raw, dict):
            source = raw.get("source") or raw.get("from") or raw.get("old_root")
            destination = raw.get("destination") or raw.get("to") or raw.get("new_root")
        else:
            raise ValueError("PDK model path remap must contain source and destination")
        if not source or not destination:
            raise ValueError("PDK model path remap must contain source and destination")
        result.append({
            "source": _normalize_path(source, base_dir),
            "destination": _normalize_path(destination, base_dir),
        })
    return result


def _profile_identifier(profile):
    seed = {
        "name": profile.get("name"),
        "package_root": profile.get("package_root"),
        "reference_libraries": sorted(profile.get("reference_libraries") or []),
        "technology_libraries": sorted(profile.get("technology_libraries") or []),
    }
    return "pdk-" + _sha256_text(_canonical_json(seed))[:12]


def normalize_pdk_profile(profile, base_dir=None):
    profile = dict(profile or {})
    name = profile.get("name") or profile.get("pdk_name")
    reference_libraries = _stable_unique(
        _as_list(profile.get("reference_libraries")) + _as_list(profile.get("libraries"))
    )
    if name and not reference_libraries:
        reference_libraries = [str(name)]
    technology_libraries = _stable_unique(
        _as_list(profile.get("technology_libraries")) + _as_list(profile.get("technology_library"))
    )
    package_root = _normalize_path(
        profile.get("package_root") or profile.get("pdk_dir"), base_dir,
    )
    custom_adapter = profile.get("custom_adapter")
    legacy_adapter = profile.get("pdk_adapter")
    adapter_policy = profile.get("adapter_policy")
    if adapter_policy is None:
        adapter_policy = normalize_adapter_policy(legacy_adapter)
        if legacy_adapter not in (None, "", GENERIC_ADAPTER_ID):
            adapter_policy["legacy_direct"] = True
    else:
        adapter_policy = normalize_adapter_policy(adapter_policy)
    if custom_adapter is None and adapter_policy.get("mode") == "custom":
        custom_adapter = adapter_policy.get("path")
    custom_adapter = _normalize_path(custom_adapter, base_dir)
    if custom_adapter:
        adapter_policy = dict(adapter_policy)
        adapter_policy["path"] = custom_adapter
    result = {
        "schema_version": SCHEMA_PDK_PROFILE,
        "id": profile.get("id"),
        "name": str(name) if name else None,
        "package_root": package_root,
        "reference_libraries": reference_libraries,
        "technology_libraries": technology_libraries,
        "model_roots": _normalize_paths(profile.get("model_roots"), base_dir),
        "model_path_remaps": _normalize_path_remaps(
            profile.get("model_path_remaps") or profile.get("path_remaps"), base_dir,
        ),
        "startup_extensions": _normalize_paths(profile.get("startup_extensions"), base_dir),
        "adapter_policy": adapter_policy,
        "custom_adapter": custom_adapter,
    }
    if not result["id"]:
        result["id"] = _profile_identifier(result)
    return result


def normalize_pdk_profiles(config, base_dir=None):
    config = dict(config or {})
    cadence = config.get("cadence") if isinstance(config.get("cadence"), dict) else config
    profiles = cadence.get("pdk_profiles")
    if profiles:
        normalized = []
        for raw_profile in profiles:
            profile = copy.deepcopy(raw_profile or {})
            if (
                    profile.get("adapter_policy") is None
                    and cadence.get("pdk_adapter_policy") is not None
            ):
                profile["adapter_policy"] = cadence.get("pdk_adapter_policy")
            normalized.append(normalize_pdk_profile(profile, base_dir))
        return normalized
    legacy_adapter = cadence.get("pdk_adapter")
    legacy_present = any(cadence.get(key) not in (None, "", []) for key in (
        "pdk_name", "pdk_dir", "model_roots", "model_path_remaps", "startup_extensions",
    )) or legacy_adapter not in (None, "", GENERIC_ADAPTER_ID)
    if not legacy_present:
        return []
    legacy = {
        "name": cadence.get("pdk_name"),
        "package_root": cadence.get("pdk_dir"),
        "pdk_adapter": legacy_adapter,
        "model_roots": cadence.get("model_roots"),
        "model_path_remaps": cadence.get("model_path_remaps"),
        "startup_extensions": cadence.get("startup_extensions"),
    }
    if cadence.get("pdk_adapter_policy") is not None:
        legacy["adapter_policy"] = cadence.get("pdk_adapter_policy")
    return [normalize_pdk_profile(legacy, base_dir)]


def normalize_pdk_configuration(config, base_dir=None):
    result = copy.deepcopy(config or {})
    cadence = result.setdefault("cadence", {}) if "cadence" in result else result
    cadence["pdk_profiles"] = normalize_pdk_profiles(result, base_dir)
    return result


def _path_is_under(path, root):
    if not path or not root:
        return False
    path = os.path.realpath(os.path.abspath(path))
    root = os.path.realpath(os.path.abspath(root))
    return path == root or path.startswith(root + os.sep)


def _package_markers(path):
    if not path:
        return []
    result = []
    current = os.path.realpath(os.path.abspath(path))
    for _depth in range(5):
        result.append(os.path.basename(current))
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return _stable_unique(result)


def _automatic_package_root(library_path):
    library_path = os.path.realpath(os.path.abspath(library_path))
    current = library_path
    marker_names = (
        "models", "model", "spectre", "skill", "technology", "tech",
    )
    for _depth in range(5):
        if any(os.path.exists(os.path.join(current, name)) for name in marker_names):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
    return library_path


def _context_id(libraries, package_root):
    value = {"libraries": sorted(libraries), "package_root_name": os.path.basename(package_root or "")}
    return "pdkctx-" + _sha256_text(_canonical_json(value))[:12]


def _profile_for_library(library, path, profiles):
    matches = []
    for profile in profiles:
        if library in profile.get("reference_libraries", ()) or library in profile.get("technology_libraries", ()):
            matches.append(profile)
            continue
        if profile.get("package_root") and _path_is_under(path, profile["package_root"]):
            matches.append(profile)
    return matches


def discover_pdk_contexts(cds_lib, referenced_libraries, design_libraries=None,
                          technology_libraries=None, profiles=None, environment=None,
                          system_libraries=None):
    library_map = parse_cds_lib(cds_lib, environment=environment)
    referenced_libraries = _stable_unique(referenced_libraries)
    design_libraries = set(_stable_unique(design_libraries))
    technology_libraries = set(_stable_unique(technology_libraries))
    excluded = set(SYSTEM_LIBRARIES if system_libraries is None else system_libraries)
    profile_base = os.path.dirname(os.path.abspath(cds_lib)) if cds_lib else None
    normalized_profiles = [
        normalize_pdk_profile(profile, profile_base) for profile in (profiles or [])
    ]
    definitions = library_map["definitions"]

    def is_pdk_library(library):
        if library in excluded:
            return False
        # A vendor wrapper may have its own schematic. An explicit profile is
        # stronger evidence than that structural classification; ordinary shared
        # user libraries remain excluded.
        return library not in design_libraries or bool(_profile_for_library(
            library, definitions.get(library), normalized_profiles,
        ))

    candidates = [library for library in referenced_libraries if is_pdk_library(library)]
    for library in sorted(technology_libraries):
        if is_pdk_library(library) and library not in candidates:
            candidates.append(library)
    undefined = sorted(library for library in candidates if library not in definitions)
    unavailable = sorted(
        library for library in candidates
        if library in definitions and not os.path.isdir(definitions[library])
    )
    unresolved = sorted(set(undefined + unavailable))
    ambiguous = []
    grouped = {}
    for library in candidates:
        path = definitions.get(library)
        if not path:
            continue
        profile_matches = _profile_for_library(library, path, normalized_profiles)
        if len(profile_matches) > 1:
            ambiguous.append({
                "library": library,
                "profile_ids": sorted(profile["id"] for profile in profile_matches),
            })
            continue
        profile = profile_matches[0] if profile_matches else None
        package_root = profile.get("package_root") if profile else None
        package_root = package_root or _automatic_package_root(path)
        key = (profile.get("id") if profile else None, package_root)
        bucket = grouped.setdefault(key, {
            "profile": profile,
            "package_root": package_root,
            "libraries": [],
            "library_paths": {},
            "technology_libraries": [],
        })
        bucket["libraries"].append(library)
        bucket["library_paths"][library] = path
        if library in technology_libraries:
            bucket["technology_libraries"].append(library)
    contexts = []
    for key in sorted(grouped, key=lambda item: ((item[0] or ""), item[1] or "")):
        bucket = grouped[key]
        profile = bucket["profile"]
        libraries = sorted(bucket["libraries"])
        technology = sorted(set(bucket["technology_libraries"]))
        if profile:
            technology = sorted(set(technology + profile.get("technology_libraries", [])))
        context = {
            "schema_version": "crescendo.pdk_context.v2",
            "id": profile.get("id") if profile else _context_id(libraries, bucket["package_root"]),
            "name": profile.get("name") if profile and profile.get("name") else "+".join(libraries),
            "package_root": bucket["package_root"],
            "reference_libraries": libraries,
            "technology_libraries": technology,
            "library_paths": dict(sorted(bucket["library_paths"].items())),
            "model_roots": list(profile.get("model_roots", [])) if profile else [],
            "model_path_remaps": list(profile.get("model_path_remaps", [])) if profile else [],
            "startup_extensions": list(profile.get("startup_extensions", [])) if profile else [],
            "adapter_policy": copy.deepcopy(
                profile.get("adapter_policy") if profile else normalize_adapter_policy(None)
            ),
            "profile_id": profile.get("id") if profile else None,
            "detected_from": "profile+cds.lib" if profile else "hierarchy+cds.lib",
        }
        context["adapter_evidence"] = build_adapter_evidence(context)
        contexts.append(context)
    recommendations = []
    if unresolved:
        recommendations.append(
            "Define every referenced PDK library in the recursive cds.lib chain or provide a PDK profile."
        )
    if ambiguous:
        recommendations.append(
            "Make PDK profile library lists or package roots non-overlapping."
        )
    errors = list(library_map.get("errors") or [])
    return {
        "schema_version": SCHEMA_PDK_CONTEXTS,
        "ok": bool(contexts) and not unresolved and not ambiguous and not errors,
        "blocking": bool(unresolved or ambiguous or errors),
        "contexts": contexts,
        "candidate_libraries": candidates,
        "design_libraries": sorted(design_libraries),
        "technology_libraries": sorted(technology_libraries),
        "unresolved_libraries": unresolved,
        "undefined_libraries": undefined,
        "unavailable_libraries": unavailable,
        "ambiguous_profile_matches": ambiguous,
        "library_map": library_map,
        "recommendations": recommendations,
    }


def hierarchy_library_references(hierarchy):
    masters = []
    technologies = []
    designs = []
    root_libraries = set()
    for design in (hierarchy or {}).get("designs", []):
        root_design = design.get("design") or {}
        if isinstance(root_design, (list, tuple)):
            root_design = {"library": root_design[0]} if root_design else {}
        if root_design.get("library"):
            designs.append(root_design["library"])
            root_libraries.add(root_design["library"])
        body = design.get("hierarchy") or {}
        resolved_top = design.get("resolved_top") or body.get("resolved_top") or {}
        if resolved_top.get("library"):
            designs.append(resolved_top["library"])
            root_libraries.add(resolved_top["library"])
        for dependency in design.get("config_dependencies") or body.get("config_dependencies") or []:
            if dependency.get("library"):
                designs.append(dependency["library"])
        for binding in body.get("technology_bindings") or []:
            if binding.get("technology_library"):
                technologies.append(binding["technology_library"])
        for cell in body.get("cells", []):
            if cell.get("library"):
                designs.append(cell["library"])
            for key in ("technology_library", "tech_library"):
                if cell.get(key):
                    technologies.append(cell[key])
        for instance in body.get("instances", []):
            if instance.get("master_library"):
                masters.append(instance["master_library"])
        for binding in design.get("config_bindings") or body.get("config_bindings") or []:
            for key in ("master_library", "bound_library", "resolved_library"):
                if binding.get(key):
                    masters.append(binding[key])
        for key in ("technology_library", "tech_library"):
            if body.get(key):
                technologies.append(body[key])
    technology_libraries = set(technologies)
    # Some PDK devices netlist through internal schematics (for example some RF
    # resistors). Probing those cells must not turn their technology library into
    # a user design library and remove the PDK from discovery. Keep every actual
    # design root and shared libraries attached to an external technology.
    designs = [library for library in designs
               if library in root_libraries or library not in technology_libraries]
    return {
        "master_libraries": _stable_unique(masters),
        "technology_libraries": _stable_unique(technologies),
        "design_libraries": _stable_unique(designs),
    }


def validate_adapter_manifest(manifest):
    manifest = copy.deepcopy(manifest or {})
    required = ("id", "version", "abi", "label", "implementation", "matcher")
    missing = [key for key in required if manifest.get(key) in (None, "")]
    if missing:
        raise ValueError("PDK adapter manifest is missing: %s" % ", ".join(missing))
    if manifest.get("schema_version") != SCHEMA_ADAPTER_MANIFEST:
        raise ValueError("unsupported PDK adapter manifest schema")
    if manifest.get("abi") != ADAPTER_ABI:
        raise ValueError("unsupported PDK adapter ABI: %s" % manifest.get("abi"))
    implementation = manifest.get("implementation") or {}
    if implementation.get("type") not in ("generic-cdf", "generic-delegate", "skill"):
        raise ValueError("unsupported PDK adapter implementation type")
    matcher = manifest.get("matcher") or {}
    minimum = int(matcher.get("minimum_strong_matches", 0))
    if (
            manifest.get("id") != GENERIC_ADAPTER_ID
            and not str(manifest.get("id") or "").startswith("custom.")
            and minimum < 2
    ):
        raise ValueError("vendor fallback requires at least two strong evidence matches")
    for rule in matcher.get("rules") or []:
        if not rule.get("field") or not rule.get("patterns"):
            raise ValueError("adapter matcher rules require field and patterns")
        for pattern in rule.get("patterns") or []:
            re.compile(pattern)
    return manifest


def load_adapter_manifest(path):
    path = os.path.abspath(os.path.expanduser(str(path)))
    with open(path, "r") as stream:
        manifest = json.load(stream)
    implementation = manifest.get("implementation") or {}
    skill_path = implementation.get("skill_path")
    if skill_path and not os.path.isabs(skill_path):
        implementation = dict(implementation)
        implementation["skill_path"] = os.path.abspath(
            os.path.join(os.path.dirname(path), skill_path)
        )
        manifest["implementation"] = implementation
    return validate_adapter_manifest(manifest)


def adapter_registry(custom_manifests=None):
    manifests = [validate_adapter_manifest(row) for row in BUILTIN_ADAPTER_MANIFESTS]
    for row in custom_manifests or []:
        manifests.append(
            load_adapter_manifest(row) if isinstance(row, str) else validate_adapter_manifest(row)
        )
    result = {}
    for manifest in manifests:
        adapter_id = manifest["id"]
        if adapter_id in result:
            raise ValueError("duplicate PDK adapter id: %s" % adapter_id)
        result[adapter_id] = manifest
    return dict((key, result[key]) for key in sorted(result))


def adapter_policy_options(registry=None):
    registry = registry or adapter_registry()
    options = [
        {"id": "auto", "label": "Auto (Generic First)", "mode": "auto"},
        {"id": "generic-only", "label": "Generic Only", "mode": "generic-only"},
    ]
    fallbacks = [
        manifest for manifest in registry.values()
        if manifest["id"] != GENERIC_ADAPTER_ID
    ]
    for manifest in sorted(fallbacks, key=lambda row: (row.get("vendor") or "", row["id"])):
        options.append({
            "id": "allow:%s" % manifest["id"],
            "label": "Allow %s" % manifest["label"],
            "mode": "allow",
            "adapter_id": manifest["id"],
            "available": _fallback_implementation_available(manifest),
        })
    options.append({"id": "custom", "label": "Custom Adapter", "mode": "custom"})
    return options


def normalize_adapter_policy(value):
    if isinstance(value, dict):
        policy = copy.deepcopy(value)
        mode = policy.get("mode") or "auto"
        if mode not in ("auto", "generic-only", "allow", "custom"):
            raise ValueError("unsupported PDK adapter policy mode: %s" % mode)
        policy["mode"] = mode
        if mode == "allow":
            policy["adapter_ids"] = _stable_unique(
                _as_list(policy.get("adapter_ids")) + _as_list(policy.get("adapter_id"))
            )
        return policy
    if value in (None, "", "auto"):
        return {"mode": "auto"}
    text = str(value)
    if text in (GENERIC_ADAPTER_ID, "generic-only"):
        return {"mode": "generic-only"}
    if text.startswith("allow:"):
        return {"mode": "allow", "adapter_ids": [text.split(":", 1)[1]]}
    if text.startswith("builtin."):
        return {"mode": "allow", "adapter_ids": [text]}
    if text == "custom":
        return {"mode": "custom", "path": None}
    return {"mode": "custom", "path": text}


def build_adapter_evidence(context, metadata_tokens=None, extra=None):
    context = context or {}
    package_root = context.get("package_root")
    evidence = {
        "library_names": _stable_unique(context.get("reference_libraries")),
        "technology_libraries": _stable_unique(context.get("technology_libraries")),
        "package_markers": _package_markers(package_root),
        "metadata_tokens": _stable_unique(metadata_tokens),
    }
    for key, values in (extra or {}).items():
        evidence[key] = _stable_unique(values)
    return evidence


def _fallback_implementation_available(manifest):
    implementation = (manifest or {}).get("implementation") or {}
    if implementation.get("type") == "skill":
        return bool(implementation.get("skill_path"))
    return bool(implementation.get("overrides"))


def match_adapter(manifest, evidence):
    manifest = validate_adapter_manifest(manifest)
    matcher = manifest.get("matcher") or {}
    matched = []
    for rule in matcher.get("rules") or []:
        values = _as_list((evidence or {}).get(rule["field"]))
        hits = []
        for value in values:
            if any(re.search(pattern, str(value)) for pattern in rule["patterns"]):
                hits.append(str(value))
        if hits:
            matched.append({
                "field": rule["field"],
                "strength": rule.get("strength") or "weak",
                "values": sorted(set(hits)),
            })
    strong_fields = sorted(set(
        row["field"] for row in matched if row["strength"] == "strong"
    ))
    required = int(matcher.get("minimum_strong_matches", 0))
    return {
        "adapter_id": manifest["id"],
        "matched": len(strong_fields) >= required,
        "strong_match_count": len(strong_fields),
        "required_strong_matches": required,
        "strong_fields": strong_fields,
        "evidence": matched,
    }


def classify_adapter_failure(code):
    code = str(code or "unknown").strip().lower().replace("-", "_")
    if code in FALLBACK_ELIGIBLE_FAILURES:
        return {
            "code": code,
            "fallback_eligible": True,
            "category": "adapter_compatibility",
            "reason": "generic CDF application is incompatible with the detected PDK behavior",
        }
    if code in FALLBACK_FORBIDDEN_FAILURES:
        return {
            "code": code,
            "fallback_eligible": False,
            "category": "fallback_forbidden",
            "reason": FALLBACK_FORBIDDEN_FAILURES[code],
        }
    return {
        "code": code,
        "fallback_eligible": False,
        "category": "unclassified",
        "reason": "unclassified failures fail closed and cannot select a vendor fallback",
    }


def select_adapter(policy, evidence, generic_failure=None, registry=None):
    registry = registry or adapter_registry()
    policy = normalize_adapter_policy(policy)
    generic = registry[GENERIC_ADAPTER_ID]
    if not generic_failure:
        return {
            "selected": copy.deepcopy(generic),
            "used_fallback": False,
            "reason": "generic_first",
            "failure": None,
            "matches": [],
        }
    failure = classify_adapter_failure(
        generic_failure.get("code") if isinstance(generic_failure, dict) else generic_failure
    )
    if not failure["fallback_eligible"]:
        return {
            "selected": copy.deepcopy(generic),
            "used_fallback": False,
            "reason": "fallback_forbidden",
            "failure": failure,
            "matches": [],
        }
    if policy["mode"] == "generic-only":
        return {
            "selected": copy.deepcopy(generic),
            "used_fallback": False,
            "reason": "policy_generic_only",
            "failure": failure,
            "matches": [],
        }
    if policy["mode"] == "custom":
        path = policy.get("path")
        if not path:
            return {
                "selected": copy.deepcopy(generic),
                "used_fallback": False,
                "reason": "custom_adapter_missing",
                "failure": failure,
                "matches": [],
            }
        if not os.path.isfile(os.path.abspath(os.path.expanduser(str(path)))):
            return {
                "selected": copy.deepcopy(generic),
                "used_fallback": False,
                "reason": "custom_adapter_unreadable",
                "failure": failure,
                "matches": [],
            }
        custom = {
            "schema_version": SCHEMA_ADAPTER_MANIFEST,
            "id": "custom." + _sha256_text(os.path.basename(path))[:12],
            "version": "1",
            "abi": ADAPTER_ABI,
            "label": "Custom Adapter",
            "vendor": None,
            "priority": 1000,
            "capabilities": {},
            "matcher": {"minimum_strong_matches": 0, "rules": []},
            "implementation": {"type": "skill", "skill_path": path, "overrides": []},
        }
        return {
            "selected": custom,
            "used_fallback": True,
            "reason": "explicit_custom_fallback",
            "failure": failure,
            "matches": [],
        }
    allowed = None
    if policy["mode"] == "allow":
        allowed = set(policy.get("adapter_ids") or [])
    candidates = []
    matches = []
    for adapter_id, manifest in registry.items():
        if adapter_id == GENERIC_ADAPTER_ID or (allowed is not None and adapter_id not in allowed):
            continue
        report = match_adapter(manifest, evidence)
        report["implementation_available"] = _fallback_implementation_available(manifest)
        matches.append(report)
        if report["matched"] and report["implementation_available"]:
            candidates.append((
                int(manifest.get("priority", 0)), report["strong_match_count"], adapter_id,
                manifest,
            ))
    if not candidates:
        matched_declarations = any(
            row["matched"] and not row["implementation_available"] for row in matches
        )
        return {
            "selected": copy.deepcopy(generic),
            "used_fallback": False,
            "reason": (
                "matched_fallback_not_implemented"
                if matched_declarations else "no_verified_fallback_match"
            ),
            "failure": failure,
            "matches": matches,
        }
    candidates.sort(key=lambda row: (-row[0], -row[1], row[2]))
    selected = candidates[0][3]
    return {
        "selected": copy.deepcopy(selected),
        "used_fallback": True,
        "reason": "verified_vendor_fallback",
        "failure": failure,
        "matches": matches,
    }


def adapter_identity(manifest, skill_path=None):
    manifest = validate_adapter_manifest(manifest)
    implementation = manifest.get("implementation") or {}
    skill_path = skill_path or implementation.get("skill_path")
    semantic_manifest = copy.deepcopy(manifest)
    semantic_manifest.setdefault("implementation", {})["skill_path"] = (
        os.path.basename(skill_path) if skill_path else None
    )
    skill_sha256 = None
    if skill_path:
        skill_path = os.path.abspath(os.path.expanduser(str(skill_path)))
        if not os.path.isfile(skill_path):
            raise ValueError("PDK adapter SKILL file does not exist: %s" % skill_path)
        skill_sha256 = _sha256_file(skill_path)
    identity = {
        "schema_version": "crescendo.pdk_adapter_identity.v1",
        "adapter_id": manifest["id"],
        "version": manifest["version"],
        "abi": manifest["abi"],
        "manifest_sha256": _sha256_text(_canonical_json(semantic_manifest)),
        "skill_sha256": skill_sha256,
    }
    identity["fingerprint"] = _sha256_text(_canonical_json(identity))
    return identity


def adapter_registry_identity(registry=None):
    registry = registry or adapter_registry()
    identities = [adapter_identity(registry[key]) for key in sorted(registry)]
    return {
        "schema_version": "crescendo.pdk_adapter_registry_identity.v1",
        "adapters": identities,
        "fingerprint": _sha256_text(_canonical_json(identities)),
    }
