"""Numeric literals in native Spectre syntax (case-sensitive SI suffixes)."""
import math
import re


_SUFFIXES = {"T": 1e12, "G": 1e9, "M": 1e6, "K": 1e3, "k": 1e3,
             "m": 1e-3, "u": 1e-6, "n": 1e-9, "p": 1e-12,
             "f": 1e-15, "a": 1e-18, "meg": 1e6}


def parse_spectre_number(value):
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if math.isfinite(float(value)) else None
    match = re.match(r'^([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)([A-Za-z]+)?$',
                     str(value).strip().strip('"'))
    if not match:
        return None
    suffix = match.group(2) or ""
    if suffix and suffix not in _SUFFIXES:
        return None
    number = float(match.group(1)) * _SUFFIXES.get(suffix, 1.0)
    return number if math.isfinite(number) else None
