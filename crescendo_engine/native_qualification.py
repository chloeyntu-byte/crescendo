"""Semantic identity of native point qualification, independent of Run/SCS ABI.

Old results remain readable. Only reuse as new execution or optimizer evidence
requires current corner selection, string handling, exact-Test Local/RDB,
Spectre-header qualification and bounded lexical alias resolution.
"""
from crescendo_engine.runtime import schema_family as _schema_family, bind_text
_contract_text = bind_text()


NATIVE_QUALIFICATION_REVISION = _contract_text("crescendo.native_point_qualification.v5")

_NATIVE_SCOPES = frozenset((
    "native_expression_mapping_and_recorded_reference_points",
    "numeric_native_CDF_and_generated_netlist_readback",
))


def is_native_qualification(document):
    """Identify recorded native evidence without reclassifying legacy templates."""
    if not isinstance(document, dict):
        return False
    generation = document.get("netlist_generation") or {}
    qualification = document.get("qualification") or {}
    protocol = generation.get("generation_protocol") if isinstance(generation, dict) else None
    scope = qualification.get("scope") if isinstance(qualification, dict) else None
    return ((isinstance(protocol, str) and
             _schema_family(protocol, "crescendo.native_run_device_parameters.")) or
            (isinstance(scope, str) and scope in _NATIVE_SCOPES))


def require_current_native_qualification(document):
    """Reject stale native authority; ordinary non-native fixtures stay usable."""
    if not is_native_qualification(document):
        return
    generation = document.get("netlist_generation") or {}
    qualification = document.get("qualification") or {}
    protocol = generation.get("generation_protocol") if isinstance(generation, dict) else None
    scope = qualification.get("scope") if isinstance(qualification, dict) else None
    generation_revision = (generation.get("qualification_revision")
                           if isinstance(generation, dict) else None)
    qualification_revision = (qualification.get("revision")
                              if isinstance(qualification, dict) else None)
    native_generation = (isinstance(protocol, str) and
                         _schema_family(protocol, "crescendo.native_run_device_parameters."))
    native_scope = isinstance(scope, str) and scope in _NATIVE_SCOPES
    if ((native_generation and generation_revision != NATIVE_QUALIFICATION_REVISION) or
            (native_scope and qualification_revision != NATIVE_QUALIFICATION_REVISION)):
        raise ValueError(
            "Native netlist uses an older point-qualification revision; retain "
            "its results for reading/export and generate a fresh native netlist "
            "before execution."
        )
