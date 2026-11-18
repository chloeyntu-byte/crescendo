"""Frozen CLI resource settings with explicit legacy reader semantics."""
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

import math

from .config import DEFAULTS

FIELDS = ("solver_transient_retries", "solver_transient_last_retry_single_core", "native_candidate_batching", "local_solver_max_jobs", "measurement_jobs", "progress_flush_interval_seconds")
READER_SERIAL = _contract_text("crescendo.point_reader.v1")
READER_BATCH = _contract_text("crescendo.batch_reader.v2")


def normalize_execution_settings(values):
    result = {}
    batching = values.get("native_candidate_batching", DEFAULTS["runtime"]["native_candidate_batching"])
    if batching not in ("serial", "generation"):
        raise ValueError("native_candidate_batching must be serial or generation")
    result["native_candidate_batching"] = batching
    for key, default in (("local_solver_max_jobs", 1), ("measurement_jobs", None)):
        value = values.get(key, default)
        if value in (None, "") and key == "measurement_jobs":
            result[key] = None
            continue
        if isinstance(value, bool):
            raise ValueError(key + " must be a positive integer")
        number = float(value)
        if not math.isfinite(number) or number < 1 or number != int(number):
            raise ValueError(key + " must be a positive integer")
        result[key] = int(number)
    interval = values.get("progress_flush_interval_seconds", 0.5)
    if isinstance(interval, bool):
        raise ValueError("Progress flush interval must be finite nonnegative seconds")
    interval = float(interval)
    if not math.isfinite(interval) or interval < 0:
        raise ValueError("Progress flush interval must be finite nonnegative seconds")
    result["progress_flush_interval_seconds"] = interval
    from .solver_transient import normalize_retry_settings
    result.update(normalize_retry_settings(values))
    return result


def freeze_execution_settings(spec, config, resuming=False):
    source = {} if resuming else dict(config.get("runtime") or {})
    source.update((key, spec[key]) for key in FIELDS if key in spec)
    settings = normalize_execution_settings(source)
    if resuming and 'native_candidate_batching' not in spec:
        settings.pop('native_candidate_batching', None)
    from .solver_transient import RETRY_FIELDS
    frozen = dict(settings)
    if resuming:
        for key in RETRY_FIELDS:
            if key not in spec:
                frozen.pop(key, None)
    spec.update(frozen)
    from .generation_batch import freeze_batching
    freeze_batching(spec, config, resuming=resuming)
    protocol = spec.get("reader_protocol", READER_SERIAL if resuming else READER_BATCH)
    if protocol not in (READER_SERIAL, READER_BATCH):
        raise ValueError("Unsupported frozen reader protocol")
    spec["reader_protocol"] = protocol
    config.setdefault("runtime", {}).update(settings)
