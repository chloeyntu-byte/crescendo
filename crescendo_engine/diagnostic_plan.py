"""Deterministic bounded diagnostic candidates shared by all execution modes."""
from __future__ import print_function

import copy
import hashlib
import itertools
import json

from .optimizers import PopulationGenerator, validate_candidate_values, validate_variables


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=True, allow_nan=False).encode('utf-8')).hexdigest()


def diagnostic_shape(spec):
    generations = spec.get('diagnostic_generations', 2)
    samples = spec.get('diagnostic_samples', 3)
    if 'diagnostic_total_candidates' in spec:
        total = spec['diagnostic_total_candidates']
        if (type(generations) is not int or not 1 <= generations <= 10 or
                type(total) is not int or not 1 <= total <= 64 or total % generations):
            raise ValueError('diagnostic total must be 1..64 and divisible by 1..10 generations')
        samples = total // generations
        if 'diagnostic_samples' in spec and (type(spec['diagnostic_samples']) is not int or
                                             spec['diagnostic_samples'] != samples):
            raise ValueError('diagnostic total conflicts with samples per generation')
    if (type(generations) is not int or not 1 <= generations <= 10 or
            type(samples) is not int or not 1 <= samples <= 16 or generations * samples > 64):
        raise ValueError('diagnostics require 1..10 generations, 1..16 samples and at most 64 candidates')
    return {'generations': generations, 'samples_per_generation': samples,
            'total_candidates': generations * samples}


def freeze_plan(spec, baseline):
    shape = diagnostic_shape(spec)
    variables = validate_variables(spec.get('variables') or [])
    baseline = validate_candidate_values(baseline, variables)
    total = shape['total_candidates']
    unique, seen = [baseline], {digest(baseline)}
    generator = PopulationGenerator(variables, 'random', 1, parameters={'population': total, 'seed': 1})
    def add(values):
        values = validate_candidate_values(values, variables)
        key = digest(values)
        if key not in seen:
            unique.append(values)
            seen.add(key)
    # Seeded normal optimizer samples, then finite-grid enumeration guarantees
    # that small legal spaces are exhausted before any repetition is reported.
    for values in generator.ask(max(total * 8, 32)):
        if len(unique) >= total:
            break
        add(values)
    if len(unique) < total:
        axes = []
        for variable in variables:
            if variable.get('value_type') == 'enum':
                axis = variable['choices'][:total]
            elif variable.get('step') is not None:
                axis = [variable['minimum'] + index * variable['step']
                        for index in range(min(total, variable['step_count'] + 1))]
            else:
                axis = [variable['minimum'] + (variable['maximum'] - variable['minimum']) * index / float(max(1, total - 1))
                        for index in range(total)]
            axes.append(axis)
        for values in itertools.islice(itertools.product(*axes), total):
            add(dict((var['parameter_id'], value) for var, value in zip(variables, values)))
            if len(unique) >= total:
                break
    rows = [{'generation': index // shape['samples_per_generation'],
             'index': index % shape['samples_per_generation'],
             'values': copy.deepcopy(unique[index % len(unique)])} for index in range(total)]
    shape.update(distinct_candidates=len({digest(row['values']) for row in rows}),
                 plan_sha256=digest(rows), repeated_candidates=total - len({digest(row['values']) for row in rows}))
    return rows, shape


def validate_plan(plan, variables, generations, population):
    if not isinstance(plan, list) or len(plan) != generations * population or not 1 <= len(plan) <= 64:
        raise ValueError('environment_test frozen plan size does not match diagnostic shape')
    result = []
    for offset, row in enumerate(plan):
        if (not isinstance(row, dict) or set(row) != {'generation', 'index', 'values'} or
                type(row['generation']) is not int or type(row['index']) is not int or
                row['generation'] != offset // population or row['index'] != offset % population):
            raise ValueError('environment_test frozen plan candidate order is invalid')
        if not isinstance(row['values'], dict) or any(isinstance(value, bool) for value in row['values'].values()):
            raise ValueError('environment_test frozen plan values must be numeric parameter mappings')
        result.append(dict(row, values=validate_candidate_values(row['values'], variables)))
    return result
