from __future__ import print_function

import math
import random

from .engineering import constrain_numeric_choices, parse_engineering_number
from .metrics import dominates


def validate_variables(variables):
    normalized = []
    for variable in variables:
        row = dict(variable)
        parameter_id = row.get("parameter_id") or "variable"
        row["scale"] = row.get("scale", "linear")
        row["value_type"] = row.get("value_type", "float")
        if row["value_type"] not in ("float", "integer", "enum"):
            raise ValueError("unsupported value_type for %s" % parameter_id)
        if row["value_type"] == "enum":
            row = constrain_numeric_choices(row)
            if len(row["choices"]) < 2:
                raise ValueError("enum variable requires at least two choices for %s" % parameter_id)
            if row["scale"] != "linear":
                raise ValueError("enum variables require linear scale for %s" % parameter_id)
            row.pop("minimum", None)
            row.pop("maximum", None)
            normalized.append(row)
            continue

        lower = parse_engineering_number(row.get("minimum"), "%s minimum" % parameter_id)
        upper = parse_engineering_number(row.get("maximum"), "%s maximum" % parameter_id)
        if lower >= upper:
            raise ValueError("invalid bounds for %s: minimum must be less than maximum" % parameter_id)
        row["minimum"] = lower
        row["maximum"] = upper
        legal_lower = None
        legal_upper = None
        if row.get("legal_minimum") is not None:
            legal_lower = parse_engineering_number(row["legal_minimum"], "%s CDF minimum" % parameter_id)
            row["legal_minimum"] = legal_lower
            if lower < legal_lower:
                raise ValueError("minimum for %s is below CDF legal minimum %s" % (parameter_id, legal_lower))
        if row.get("legal_maximum") is not None:
            legal_upper = parse_engineering_number(row["legal_maximum"], "%s CDF maximum" % parameter_id)
            row["legal_maximum"] = legal_upper
            if upper > legal_upper:
                raise ValueError("maximum for %s exceeds CDF legal maximum %s" % (parameter_id, legal_upper))
        if legal_lower is not None and legal_upper is not None and legal_lower > legal_upper:
            raise ValueError("CDF legal minimum exceeds maximum for %s" % parameter_id)

        step = row.get("step")
        if step is not None:
            step = parse_engineering_number(step, "%s step" % parameter_id)
            if step <= 0.0:
                raise ValueError("step must be positive for %s" % parameter_id)
            row["step"] = step
        if row["value_type"] == "integer":
            if not lower.is_integer() or not upper.is_integer():
                raise ValueError("integer bounds must be whole numbers for %s" % parameter_id)
            if row["scale"] != "linear":
                raise ValueError("integer variables require linear scale for %s" % parameter_id)
            if step is None:
                step = 1.0
                row["step"] = step
            if not step.is_integer():
                raise ValueError("integer step must be a whole number for %s" % parameter_id)
        if step is not None:
            if row["scale"] != "linear":
                raise ValueError("stepped variables require linear scale for %s" % parameter_id)
            if row.get("grid_origin") is not None:
                origin = parse_engineering_number(row["grid_origin"], "%s grid origin" % parameter_id)
                row["grid_origin"] = origin
                for bound in (lower, upper):
                    offset = (bound - origin) / step
                    if abs(offset - round(offset)) > 1.0e-9 * max(1.0, abs(offset)):
                        raise ValueError("bounds for %s must align to its step at the CDF grid origin" % parameter_id)
            count = (upper - lower) / step
            rounded_count = round(count)
            tolerance = 1.0e-9 * max(1.0, abs(count))
            if abs(count - rounded_count) > tolerance:
                raise ValueError("bounds for %s must align to its step" % parameter_id)
            if rounded_count < 1:
                raise ValueError("bounds for %s must contain at least two stepped values" % parameter_id)
            row["step_count"] = int(rounded_count)
        normalized.append(row)
    if not normalized:
        raise ValueError("at least one optimization variable is required")
    return normalized


def _decode(unit_value, variable):
    unit_value = min(1.0, max(0.0, unit_value))
    if variable.get("value_type") == "enum":
        choices = variable["choices"]
        index = int(round(unit_value * float(len(choices) - 1)))
        return choices[index]
    lower = variable["minimum"]
    upper = variable["maximum"]
    if variable.get("scale") == "log":
        if lower <= 0:
            raise ValueError("log variable requires positive lower bound")
        value = math.exp(math.log(lower) + unit_value * (math.log(upper) - math.log(lower)))
    else:
        value = lower + unit_value * (upper - lower)
    if variable.get("step") is not None:
        index = int(round((value - lower) / variable["step"]))
        index = min(variable["step_count"], max(0, index))
        value = lower + index * variable["step"]
    if variable.get("value_type") == "integer":
        return int(round(value))
    return value


def _encode(value, variable):
    if variable.get("value_type") == "enum":
        try:
            index = variable["choices"].index(value)
        except ValueError:
            raise ValueError("value is not a legal enum choice for %s" % variable.get("parameter_id"))
        return float(index) / float(len(variable["choices"]) - 1)
    lower = variable["minimum"]
    upper = variable["maximum"]
    if variable.get("scale") == "log":
        return (math.log(float(value)) - math.log(lower)) / (math.log(upper) - math.log(lower))
    return (float(value) - lower) / (upper - lower)


def validate_candidate_values(values, variables):
    normalized_variables = validate_variables(variables)
    expected = set(variable["parameter_id"] for variable in normalized_variables)
    provided = set(values)
    if expected != provided:
        missing = sorted(expected - provided)
        extra = sorted(provided - expected)
        raise ValueError("candidate variables mismatch; missing=%s extra=%s" % (missing, extra))
    normalized = {}
    for variable in normalized_variables:
        parameter_id = variable["parameter_id"]
        value = values[parameter_id]
        if variable["value_type"] == "enum":
            if value not in variable["choices"]:
                raise ValueError("value for %s is not a legal enum choice" % parameter_id)
            normalized[parameter_id] = value
            continue
        number = parse_engineering_number(value, "%s candidate value" % parameter_id)
        lower = variable["minimum"]
        upper = variable["maximum"]
        tolerance = 1.0e-12 * max(abs(lower), abs(upper), 1.0e-30)
        if number < lower - tolerance or number > upper + tolerance:
            raise ValueError("candidate value for %s is outside optimization bounds" % parameter_id)
        if variable["value_type"] == "integer" and not number.is_integer():
            raise ValueError("candidate value for %s must be an integer" % parameter_id)
        if variable.get("step") is not None:
            offset = (number - lower) / variable["step"]
            if abs(offset - round(offset)) > 1.0e-9 * max(1.0, abs(offset)):
                raise ValueError("candidate value for %s is not aligned to its step" % parameter_id)
        if variable["value_type"] == "integer":
            normalized[parameter_id] = int(number)
        else:
            normalized[parameter_id] = number
    return normalized


def _random_vector(rng, dimensions):
    return [rng.random() for _ in range(dimensions)]


def _sbx_child(rng, left, right, probability=0.9, distribution_index=15.0):
    if rng.random() > probability or abs(left - right) <= 1.0e-14:
        return left if rng.random() < 0.5 else right
    lower = min(left, right)
    upper = max(left, right)
    random_value = rng.random()
    beta = 1.0 + 2.0 * lower / (upper - lower)
    alpha = 2.0 - beta ** -(distribution_index + 1.0)
    beta = (random_value * alpha) ** (1.0 / (distribution_index + 1.0)) if random_value <= 1.0 / alpha else (1.0 / (2.0 - random_value * alpha)) ** (1.0 / (distribution_index + 1.0))
    first = 0.5 * ((lower + upper) - beta * (upper - lower))
    beta = 1.0 + 2.0 * (1.0 - upper) / (upper - lower)
    alpha = 2.0 - beta ** -(distribution_index + 1.0)
    beta = (random_value * alpha) ** (1.0 / (distribution_index + 1.0)) if random_value <= 1.0 / alpha else (1.0 / (2.0 - random_value * alpha)) ** (1.0 / (distribution_index + 1.0))
    second = 0.5 * ((lower + upper) + beta * (upper - lower))
    return first if rng.random() < 0.5 else second


def _polynomial_mutate(rng, value, distribution_index=20.0):
    delta_lower = value
    delta_upper = 1.0 - value
    random_value = rng.random()
    exponent = 1.0 / (distribution_index + 1.0)
    if random_value < 0.5:
        base = 2.0 * random_value + (1.0 - 2.0 * random_value) * (1.0 - delta_lower) ** (distribution_index + 1.0)
        delta = base ** exponent - 1.0
    else:
        base = 2.0 * (1.0 - random_value) + 2.0 * (random_value - 0.5) * (1.0 - delta_upper) ** (distribution_index + 1.0)
        delta = 1.0 - base ** exponent
    return min(1.0, max(0.0, value + delta))


def _candidate_key(values, variables):
    return tuple(values[variable["parameter_id"]] for variable in variables)


def _fronts(rows):
    remaining = list(rows)
    fronts = []
    while remaining:
        front = [row for row in remaining if not any(dominates(other["metrics"], row["metrics"]) for other in remaining if other is not row)]
        fronts.append(front)
        remaining = [row for row in remaining if row not in front]
    return fronts


def _crowding_distance(front):
    distances = dict((id(row), 0.0) for row in front)
    if len(front) <= 2:
        return dict((id(row), float("inf")) for row in front)
    objective_count = len(front[0]["metrics"].get("objectives", []))
    for objective_index in range(objective_count):
        ordered = sorted(front, key=lambda row: float(row["metrics"]["objectives"][objective_index]["score"]))
        distances[id(ordered[0])] = float("inf")
        distances[id(ordered[-1])] = float("inf")
        lower = float(ordered[0]["metrics"]["objectives"][objective_index]["score"])
        upper = float(ordered[-1]["metrics"]["objectives"][objective_index]["score"])
        span = upper - lower
        if not math.isfinite(span) or span <= 0.0:
            continue
        for index in range(1, len(ordered) - 1):
            if math.isinf(distances[id(ordered[index])]):
                continue
            previous = float(ordered[index - 1]["metrics"]["objectives"][objective_index]["score"])
            following = float(ordered[index + 1]["metrics"]["objectives"][objective_index]["score"])
            distances[id(ordered[index])] += (following - previous) / span
    return distances


def _rank_and_crowding(rows):
    ranks = {}
    crowding = {}
    for rank, front in enumerate(_fronts(rows)):
        distances = _crowding_distance(front)
        for row in front:
            ranks[id(row)] = rank
            crowding[id(row)] = distances[id(row)]
    return ranks, crowding


def _environmental_select(rows, size):
    selected = []
    for front in _fronts(rows):
        remaining = size - len(selected)
        if remaining <= 0:
            break
        if len(front) <= remaining:
            selected.extend(front)
            continue
        distances = _crowding_distance(front)
        selected.extend(sorted(front, key=lambda row: distances[id(row)], reverse=True)[:remaining])
        break
    return selected


def _evaluated_rows(rows):
    eligible = []
    for row in rows:
        if row.get("status") == "completed" or (row.get("status") == "failed" and row.get("metrics")):
            eligible.append(row)
    return sorted(eligible, key=lambda row: (str(row.get("candidate_id") or ""), repr(row.get("values") or {})))


def _solve_linear_system(matrix, vector):
    size = len(vector)
    augmented = [list(matrix[index]) + [float(vector[index])] for index in range(size)]
    for column in range(size):
        pivot = max(range(column, size), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) <= 1.0e-14:
            raise ValueError("singular surrogate matrix")
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        scale = augmented[column][column]
        augmented[column] = [value / scale for value in augmented[column]]
        for row in range(size):
            if row == column:
                continue
            factor = augmented[row][column]
            if factor:
                augmented[row] = [
                    augmented[row][offset] - factor * augmented[column][offset]
                    for offset in range(size + 1)
                ]
    return [augmented[index][-1] for index in range(size)]


def _cholesky(matrix):
    size = len(matrix)
    lower = [[0.0] * size for _ in range(size)]
    for row in range(size):
        for column in range(row + 1):
            value = matrix[row][column] - sum(
                lower[row][offset] * lower[column][offset] for offset in range(column)
            )
            if row == column:
                if value <= 0.0:
                    raise ValueError("surrogate covariance is not positive definite")
                lower[row][column] = math.sqrt(value)
            else:
                lower[row][column] = value / lower[column][column]
    return lower


def _cholesky_forward(lower, vector):
    size = len(vector)
    forward = [0.0] * size
    for row in range(size):
        forward[row] = (float(vector[row]) - sum(
            lower[row][column] * forward[column] for column in range(row)
        )) / lower[row][row]
    return forward


def _cholesky_solve(lower, vector):
    size = len(vector)
    forward = _cholesky_forward(lower, vector)
    result = [0.0] * size
    for row in range(size - 1, -1, -1):
        result[row] = (forward[row] - sum(
            lower[column][row] * result[column] for column in range(row + 1, size)
        )) / lower[row][row]
    return result


def _squared_distance(left, right):
    return sum((a - b) ** 2 for a, b in zip(left, right))


class PopulationGenerator(object):
    def __init__(self, variables, algorithm="nsga2", seed=1, parameters=None):
        self.variables = validate_variables(variables)
        self.algorithm = algorithm
        self.parameters = dict(parameters or {})
        self.rng = random.Random(int(seed))
        self.previous = []
        self.population_size = None
        self._de_targets = {}
        self.unmatched_trials = []

    def _initial(self, size):
        dimensions = len(self.variables)
        columns = []
        for _dimension in range(dimensions):
            column = [(index + self.rng.random()) / float(size) for index in range(size)]
            self.rng.shuffle(column)
            columns.append(column)
        return [[columns[dimension][index] for dimension in range(dimensions)] for index in range(size)]

    def _de(self, size):
        vectors = []
        population = list(self.previous)
        encoded = [[_encode(row["values"][variable["parameter_id"]], variable) for variable in self.variables] for row in population]
        for index in range(size):
            target_index = index % len(encoded)
            target = encoded[target_index]
            choices = [row for offset, row in enumerate(encoded) if offset != target_index]
            while len(choices) < 3:
                choices.append(_random_vector(self.rng, len(self.variables)))
            a, b, c = self.rng.sample(choices, 3)
            forced = self.rng.randrange(len(self.variables))
            vector = []
            for dimension in range(len(self.variables)):
                trial = a[dimension] + float(self.parameters.get("differential_weight", 0.8)) * (b[dimension] - c[dimension])
                vector.append(min(1.0, max(0.0, trial)) if self.rng.random() < float(self.parameters.get("crossover_rate", 0.9)) or dimension == forced else target[dimension])
            vectors.append(vector)
            decoded = dict((variable["parameter_id"], _decode(vector[offset], variable)) for offset, variable in enumerate(self.variables))
            self._de_targets.setdefault(_candidate_key(decoded, self.variables), []).append(population[target_index])
        return vectors

    def _nsga2(self, size):
        ranks, crowding = _rank_and_crowding(self.previous)

        def choose():
            left = self.rng.choice(self.previous)
            right = self.rng.choice(self.previous)
            left_key = (ranks[id(left)], -crowding[id(left)])
            right_key = (ranks[id(right)], -crowding[id(right)])
            if left_key == right_key:
                return left if self.rng.random() < 0.5 else right
            return left if left_key < right_key else right

        vectors = []
        for _index in range(size):
            left = choose()
            right = choose()
            vector = []
            dimensions = len(self.variables)
            mutation_probability = float(self.parameters.get("mutation_probability", 1.0 / float(dimensions)))
            for variable in self.variables:
                key = variable["parameter_id"]
                a = _encode(left["values"][key], variable)
                b = _encode(right["values"][key], variable)
                child = _sbx_child(
                    self.rng, a, b,
                    probability=float(self.parameters.get("crossover_probability", 0.9)),
                    distribution_index=float(self.parameters.get("crossover_eta", 15.0)),
                )
                if self.rng.random() < mutation_probability:
                    child = _polynomial_mutate(
                        self.rng, child,
                        distribution_index=float(self.parameters.get("mutation_eta", 20.0)),
                    )
                vector.append(min(1.0, max(0.0, child)))
            vectors.append(vector)
        return vectors

    def _bayesian(self, size):
        dimensions = len(self.variables)
        initial_samples = int(self.parameters.get("initial_samples", max(size, 2 * dimensions + 1)))
        if len(self.previous) < initial_samples:
            return self._initial(size)
        observations = self._bayesian_observations(self.previous)
        training_vectors = [[
            _encode(row["values"][variable["parameter_id"]], variable)
            for variable in self.variables
        ] for row in observations]
        training_scores = [float(row["metrics"].get("merit", 0.0)) for row in observations]
        center = sum(training_scores) / float(len(training_scores))
        spread = math.sqrt(sum((score - center) ** 2 for score in training_scores) / float(len(training_scores))) or 1.0
        normalized_scores = [(score - center) / spread for score in training_scores]
        length_scale = float(self.parameters.get("kernel_length_scale", max(0.08, 1.0 / math.sqrt(float(dimensions)))))
        noise = float(self.parameters.get("noise", 1.0e-6))

        def kernel(left, right):
            return math.exp(-_squared_distance(left, right) / (2.0 * length_scale ** 2))

        covariance = [[kernel(left, right) for right in training_vectors] for left in training_vectors]
        for index in range(len(covariance)):
            covariance[index][index] += noise
        try:
            lower = _cholesky(covariance)
        except ValueError:
            for index in range(len(covariance)):
                covariance[index][index] += max(1.0e-8, noise * 100.0)
            lower = _cholesky(covariance)
        weights = _cholesky_solve(lower, normalized_scores)

        best_index = min(range(len(training_scores)), key=lambda index: training_scores[index])
        best = training_vectors[best_index]
        pool_size = max(size, int(self.parameters.get("candidate_pool", max(200, 40 * dimensions))))
        candidates = []
        for index in range(pool_size):
            if index % 2:
                scale = max(0.03, 0.25 / math.sqrt(1.0 + len(self.previous) / float(max(1, size))))
                candidates.append([min(1.0, max(0.0, value + self.rng.gauss(0.0, scale))) for value in best])
            else:
                candidates.append(_random_vector(self.rng, dimensions))
        exploration = float(self.parameters.get("acquisition_exploration", 2.0))

        def acquisition(candidate):
            similarities = [kernel(candidate, training) for training in training_vectors]
            mean = sum(similarity * weight for similarity, weight in zip(similarities, weights))
            projected = _cholesky_forward(lower, similarities)
            posterior_variance = max(noise, 1.0 - sum(value * value for value in projected))
            uncertainty = math.sqrt(posterior_variance)
            return mean - exploration * uncertainty

        scored_candidates = [(acquisition(candidate), candidate) for candidate in candidates]
        selected = []
        while scored_candidates and len(selected) < size:
            score, candidate = min(
                scored_candidates,
                key=lambda row: row[0] - 0.1 * min(
                    [_squared_distance(row[1], chosen) for chosen in selected] or [1.0]
                ),
            )
            selected.append(candidate)
            scored_candidates.remove((score, candidate))
        return selected

    def _bayesian_observations(self, rows):
        limit = int(self.parameters.get("max_observations", max(80, 4 * len(self.variables))))
        if len(rows) <= limit:
            return list(rows)
        best_count = max(1, limit // 2)
        best = sorted(
            rows,
            key=lambda row: (float(row["metrics"].get("merit", float("inf"))), str(row.get("candidate_id") or "")),
        )[:best_count]
        selected = set(id(row) for row in best)
        recent = []
        for row in reversed(rows):
            if id(row) not in selected:
                recent.append(row)
                selected.add(id(row))
            if len(best) + len(recent) >= limit:
                break
        return best + list(reversed(recent))

    def ask(self, size):
        size = int(size)
        if size < 1:
            raise ValueError("population size must be positive")
        if self.population_size is None:
            self.population_size = size
        if not self.previous or self.algorithm == "random":
            vectors = self._initial(size) if not self.previous else [_random_vector(self.rng, len(self.variables)) for _ in range(size)]
        elif self.algorithm == "differential_evolution":
            vectors = self._de(size)
        elif self.algorithm == "nsga2":
            vectors = self._nsga2(size)
        elif self.algorithm == "bayesian_optimization":
            vectors = self._bayesian(size)
        else:
            raise ValueError("unsupported algorithm: %s" % self.algorithm)
        return [dict((variable["parameter_id"], _decode(vector[index], variable)) for index, variable in enumerate(self.variables)) for vector in vectors]

    def tell(self, rows):
        evaluated = _evaluated_rows(rows)
        completed = [row for row in evaluated if row.get("status") == "completed"]
        if not evaluated:
            return False
        if self.algorithm == "random":
            self.previous = evaluated[-self.population_size:]
        elif not self.previous:
            self.previous = _environmental_select(evaluated, self.population_size)
        elif self.algorithm == "nsga2":
            self.previous = _environmental_select(self.previous + evaluated, self.population_size)
        elif self.algorithm == "differential_evolution":
            objective_count = len(evaluated[0]["metrics"].get("objectives", []))
            if objective_count > 1:
                self.previous = _environmental_select(self.previous + evaluated, self.population_size)
            else:
                survivors = list(self.previous)
                self.unmatched_trials = []
                for trial in evaluated:
                    targets = self._de_targets.get(_candidate_key(trial["values"], self.variables), [])
                    target = targets.pop(0) if targets else None
                    if target is None:
                        self.unmatched_trials.append(trial)
                        continue
                    target_index = next((index for index, row in enumerate(survivors) if row is target), None)
                    if target_index is not None and dominates(trial["metrics"], target["metrics"]):
                        survivors[target_index] = trial
                self.previous = _environmental_select(survivors, self.population_size)
        elif self.algorithm == "bayesian_optimization":
            known = set((row.get("candidate_id"), _candidate_key(row["values"], self.variables)) for row in self.previous)
            self.previous.extend(
                row for row in evaluated
                if (row.get("candidate_id"), _candidate_key(row["values"], self.variables)) not in known
            )
            self.previous = self._bayesian_observations(self.previous)
        self._de_targets = {}
        return bool(completed)
