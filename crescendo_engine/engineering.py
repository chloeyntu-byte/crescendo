from __future__ import print_function

import ast
import math
import re


SPECTRE_SUFFIXES = {
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

SUFFIX_POLICY = (
    "Spectre suffixes are case-insensitive: M means milli, exactly like m; "
    "use meg (in any letter case) for mega."
)

_NUMBER_PATTERN = re.compile(
    r"^([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*([A-Za-z]+)?$"
)


def parse_engineering_number(value, field_name="value"):
    if isinstance(value, bool) or value is None:
        raise ValueError("%s must be a finite number" % field_name)
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value).strip()
        if not text:
            raise ValueError("%s must not be empty" % field_name)
        match = _NUMBER_PATTERN.match(text)
        if not match:
            raise ValueError(
                "%s has invalid numeric syntax: %s (%s)" %
                (field_name, value, SUFFIX_POLICY)
            )
        number = float(match.group(1))
        suffix = match.group(2)
        if suffix:
            normalized_suffix = suffix.lower()
            if normalized_suffix not in SPECTRE_SUFFIXES:
                raise ValueError(
                    "%s has unknown engineering suffix '%s' (%s)" %
                    (field_name, suffix, SUFFIX_POLICY)
                )
            number *= SPECTRE_SUFFIXES[normalized_suffix]
    if not math.isfinite(number):
        raise ValueError("%s must be finite" % field_name)
    return number


def try_parse_engineering_number(value):
    try:
        return parse_engineering_number(value)
    except (TypeError, ValueError):
        return None


def normalize_choices(value, field_name="choices"):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        choices = list(value)
    else:
        text = str(value).strip()
        if not text:
            return []
        quoted = re.findall(r'"((?:\\.|[^"\\])*)"', text)
        remainder = re.sub(r'"(?:\\.|[^"\\])*"', "", text)
        if quoted and not remainder.replace("(", "").replace(")", "").replace(",", "").strip():
            if "((" in text.replace(" ", ""):
                raise ValueError("%s uses nested prompt/value choices that require a PDK adapter" % field_name)
            choices = [ast.literal_eval('"%s"' % item) for item in quoted]
        else:
            try:
                parsed = ast.literal_eval(text)
            except (SyntaxError, ValueError):
                parsed = None
            if isinstance(parsed, (list, tuple)):
                choices = list(parsed)
            elif "," in text:
                choices = [item.strip() for item in text.split(",")]
            else:
                choices = text.split()
    normalized = []
    for choice in choices:
        if choice is None or isinstance(choice, (dict, list, tuple)):
            raise ValueError("%s contains an invalid choice" % field_name)
        if isinstance(choice, str):
            choice = choice.strip()
            if not choice:
                raise ValueError("%s contains an empty choice" % field_name)
        if choice not in normalized:
            normalized.append(choice)
    return normalized


def constrain_numeric_choices(variable):
    """Intersect numeric choices with declared limits without changing labels."""
    row = dict(variable)
    parameter_id = row.get("parameter_id") or "variable"
    choices = normalize_choices(row.get("choices"), "%s choices" % parameter_id)
    numbers = [try_parse_engineering_number(choice) for choice in choices]
    if any(number is None for number in numbers):
        if row.get("numeric_choices"):
            raise ValueError("numeric choices for %s must be finite numbers" % parameter_id)
        row["choices"] = choices
        return row
    if not choices and not row.get("numeric_choices"):
        row["choices"] = choices
        return row

    row["numeric_choices"] = True
    limits = {}
    for key in ("minimum", "maximum", "legal_minimum", "legal_maximum"):
        if row.get(key) is not None:
            limits[key] = parse_engineering_number(row[key], "%s %s" % (parameter_id, key))
    if ("minimum" in limits and "maximum" in limits
            and limits["minimum"] >= limits["maximum"]):
        raise ValueError("invalid bounds for %s: minimum must be less than maximum" % parameter_id)
    if ("legal_minimum" in limits and "legal_maximum" in limits
            and limits["legal_minimum"] > limits["legal_maximum"]):
        raise ValueError("CDF legal minimum exceeds maximum for %s" % parameter_id)

    numeric_type = row.get("numeric_value_type")
    if numeric_type not in (None, "float", "integer"):
        raise ValueError("unsupported numeric choice type for %s" % parameter_id)
    step = row.get("step")
    origin = None
    if step is not None:
        step = parse_engineering_number(step, "%s step" % parameter_id)
        if step <= 0.0:
            raise ValueError("step must be positive for %s" % parameter_id)
        raw_origin = row.get("grid_origin")
        if raw_origin is None:
            raw_origin = limits.get("legal_minimum", 0)
        origin = parse_engineering_number(raw_origin, "%s grid origin" % parameter_id)
        row["step"] = step
        row["grid_origin"] = origin

    allowed = []
    for choice, number in zip(choices, numbers):
        valid = True
        for key, bound in limits.items():
            tolerance = 1.0e-12 * max(abs(number), abs(bound), 1.0e-30)
            if ((key.endswith("minimum") and number < bound - tolerance)
                    or (key.endswith("maximum") and number > bound + tolerance)):
                valid = False
                break
        if not valid or (numeric_type == "integer" and not number.is_integer()):
            continue
        if step is not None:
            offset = (number - origin) / step
            if not math.isfinite(offset):
                raise ValueError("numeric choice grid overflow for %s" % parameter_id)
            if abs(offset - round(offset)) > 1.0e-9 * max(1.0, abs(offset)):
                continue
        allowed.append(choice)
    row["choices"] = allowed
    return row


def merge_catalog_domain(variable, parameter):
    row = dict(variable)
    parameter = parameter or {}
    parameter_id = row.get("parameter_id") or parameter.get("parameter_id") or "variable"

    declared_type = parameter.get("value_type")
    choices = normalize_choices(
        parameter.get("choices") if parameter.get("choices") is not None
        else parameter.get("choices_raw"),
        "%s CDF choices" % parameter_id,
    )
    if choices:
        row["value_type"] = "enum"
        row["choices"] = choices
        numbers = [try_parse_engineering_number(choice) for choice in choices]
        if all(number is not None for number in numbers):
            row["numeric_choices"] = True
            if declared_type in ("float", "integer"):
                row["numeric_value_type"] = declared_type
    elif declared_type:
        row["value_type"] = declared_type

    if parameter.get("minimum") is not None:
        row["legal_minimum"] = parameter["minimum"]
    if parameter.get("maximum") is not None:
        row["legal_maximum"] = parameter["maximum"]

    for key in ("step", "increment", "resolution"):
        if parameter.get(key) is not None:
            row["step"] = parameter[key]
            row["grid_origin"] = parameter.get("grid_origin", parameter.get("minimum") or 0)
            break
    if row.get("value_type") in ("integer", "enum"):
        row["scale"] = "linear"
    if row.get("numeric_choices"):
        row = constrain_numeric_choices(row)
    return row


def normalize_objective_definition(definition):
    row = dict(definition)
    name = row.get("name") or row.get("output_key") or "objective"
    if row.get("scale") is not None:
        row["scale"] = parse_engineering_number(row["scale"], "%s scale" % name)
        if row["scale"] <= 0.0:
            raise ValueError("%s scale must be positive" % name)
    if row.get("direction", "minimize") == "target":
        row["target"] = parse_engineering_number(row.get("target"), "%s target" % name)
    return row


def normalize_constraint_definition(definition):
    row = dict(definition)
    name = row.get("name") or row.get("output_key") or "constraint"
    operator = row.get("operator", "<=")
    if row.get("scale") is not None:
        row["scale"] = parse_engineering_number(row["scale"], "%s scale" % name)
        if row["scale"] <= 0.0:
            raise ValueError("%s scale must be positive" % name)
    if operator in ("<=", ">="):
        row["limit"] = parse_engineering_number(row.get("limit"), "%s limit" % name)
    elif operator == "between":
        row["lower"] = parse_engineering_number(row.get("lower"), "%s lower bound" % name)
        row["upper"] = parse_engineering_number(row.get("upper"), "%s upper bound" % name)
        if row["lower"] >= row["upper"]:
            raise ValueError("%s lower bound must be less than upper bound" % name)
    elif operator == "target":
        row["target"] = parse_engineering_number(row.get("target"), "%s target" % name)
        if row.get("tolerance") is not None:
            row["tolerance"] = parse_engineering_number(row["tolerance"], "%s tolerance" % name)
            if row["tolerance"] < 0.0:
                raise ValueError("%s tolerance must not be negative" % name)
    else:
        raise ValueError("unsupported constraint operator: %s" % operator)
    return row


def normalize_metric_definitions(objectives, constraints):
    return (
        [normalize_objective_definition(row) for row in objectives],
        [normalize_constraint_definition(row) for row in constraints],
    )
