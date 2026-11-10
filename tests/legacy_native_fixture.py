"""Explicit v0.3.0 test loop for retired native-ADE scheduling contracts.

The frozen source is excluded from delivery. Only marked historical tests patch
this loop; current execution routing and unified tests never enter this fixture.
"""
import functools
import hashlib
import json
import os
import types
from unittest import mock

from crescendo_engine import runner


def _load_historical_loop():
    folder = os.path.join(os.path.dirname(__file__), 'fixtures')
    path = os.path.join(folder, 'legacy_native_runner_v0_3_0.py')
    with open(path, 'r', encoding='utf-8') as stream:
        source = stream.read()
    with open(os.path.join(folder, 'legacy_native_runner_v0_3_0.json'), 'r', encoding='utf-8') as stream:
        identity = json.load(stream)
    body = source[source.index('def _run_optimization_unleased('):]
    if hashlib.sha256(body.encode('utf-8')).hexdigest() != identity['function_sha256']:
        raise RuntimeError('Historical native runner fixture identity changed')
    namespace = {}
    exec(compile(body, path, 'exec'), namespace)
    compiled = namespace['_run_optimization_unleased']
    return types.FunctionType(compiled.__code__, runner.__dict__,
                              'historical_native_v0_3_0_loop', compiled.__defaults__)


HISTORICAL_LOOP = _load_historical_loop()


def historical_native_loop(test):
    """Mark a retired whole-loop assertion without changing production routes."""
    @functools.wraps(test)
    def historical(*args, **kwargs):
        with mock.patch.object(runner, '_run_optimization_unleased', HISTORICAL_LOOP):
            return test(*args, **kwargs)
    historical.historical_native_fixture = 'v0.3.0'
    return historical
