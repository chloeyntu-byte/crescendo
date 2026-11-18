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
        linked = any(row.get("link_group") for row in variables)
        dimensions = generator._dimensions if linked else variables
        for variable in dimensions:
            if linked and variable.get('value_type') == 'enum':
                # Enumerate each independent choice exactly once in unit space.
                # Repeated decoded choices can exhaust the bounded product
                # before it reaches all legal combinations.
                axis = [index / float(len(variable['choices']) - 1)
                        for index in range(min(total, len(variable['choices'])))]
            elif linked:
                count = min(total, variable["step_count"] + 1) if variable.get("step") is not None else total
                divisor = variable["step_count"] if variable.get("step") is not None else max(1, total - 1)
                axis = [index / float(divisor) for index in range(count)]
            elif variable.get('value_type') == 'enum':
                axis = variable['choices'][:total]
            elif variable.get('step') is not None:
                axis = [variable['minimum'] + index * variable['step']
                        for index in range(min(total, variable['step_count'] + 1))]
            else:
                axis = [variable['minimum'] + (variable['maximum'] - variable['minimum']) * index / float(max(1, total - 1))
                        for index in range(total)]
            axes.append(axis)
        for values in itertools.islice(itertools.product(*axes), total):
            add(generator._decode_vector(values) if linked else
                dict((var['parameter_id'], value) for var, value in zip(variables, values)))
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


def project_frozen_inputs(spec, variables):
    """Project validated diagnostic inputs onto qualified surviving dimensions.

    Preserve order and retained values, even if projection creates repeats.
    Check the original digests before issuing identities for the projection.
    """
    marker = copy.deepcopy(spec['environment_test'])
    original = validate_variables(spec['variables'])
    keep = {row['parameter_id'] for row in variables}
    if not keep.issubset({row['parameter_id'] for row in original}):
        raise ValueError('Diagnostic projection cannot introduce dimensions')
    if 'frozen_candidate' in marker:
        values = validate_candidate_values(marker['frozen_candidate'], original)
        if any(isinstance(value, bool) for value in marker['frozen_candidate'].values()):
            raise ValueError('Diagnostic candidate values cannot be booleans')
        if marker.get('frozen_candidate_sha256') not in (None, digest(values)):
            raise ValueError('Diagnostic candidate digest mismatch before projection')
        marker['frozen_candidate'] = validate_candidate_values(
            {key: value for key, value in values.items() if key in keep}, variables)
        marker['frozen_candidate_sha256'] = digest(marker['frozen_candidate'])
    if 'frozen_plan' in marker:
        plan = validate_plan(marker['frozen_plan'], original, spec['generations'], spec['population'])
        if marker.get('frozen_plan_sha256') not in (None, digest(plan)):
            raise ValueError('Diagnostic plan digest mismatch before projection')
        if 'frozen_candidate' in spec['environment_test'] and plan[0]['values'] != spec['environment_test']['frozen_candidate']:
            raise ValueError('Diagnostic first candidate mismatch before projection')
        for row in plan:
            row['values'] = {key: value for key, value in row['values'].items() if key in keep}
        marker['frozen_plan'] = validate_plan(plan, variables, spec['generations'], spec['population'])
        marker['frozen_plan_sha256'] = digest(marker['frozen_plan'])
    return marker
