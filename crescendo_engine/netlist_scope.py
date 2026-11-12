"""Bounded lexical bindings shared by template qualification and point readback."""
import re


class BindingError(ValueError):
    def __init__(self, reason, expression, message):
        super(BindingError, self).__init__(message)
        self.detail = {"reason": reason, "expression": expression}


class NetlistScope(object):
    def __init__(self, text, symbols=None):
        from .private_netlist import _logical_statements, _assignments
        from .spectre_parameter_syntax import parameter_declaration_spans
        self.statements = _logical_statements(text, include_view=True)
        self.symbols = set(symbols or [])
        self.parents = {(): None}
        self.declarations = {(): {}}
        self.scopes = []
        self.subcircuits = {}
        self.calls = []
        scope = ()
        for index, row in enumerate(self.statements):
            statement = row[2]
            match = re.match(r"^subckt\s+(\S+)", statement)
            if match:
                if len(scope) >= 64:
                    raise BindingError("binding_unresolved", statement, "lexical nesting complexity limit")
                child = scope + (match.group(1),)
                if child in self.parents:
                    raise BindingError("scope_shadow", statement, "duplicate lexical subcircuit scope")
                self.parents[child] = scope
                self.declarations[child] = {}
                self.subcircuits.setdefault(match.group(1), []).append(child)
                scope = child
            self.scopes.append(scope)
            pairs = (parameter_declaration_spans(statement) if statement.startswith("parameters ")
                     else _assignments(statement) if match else [])
            for name, expression, unused_a, unused_b in pairs:
                if name in self.declarations[scope]:
                    raise BindingError("scope_shadow", expression, "duplicate parameter in lexical scope")
                if scope and name in self.symbols:
                    raise BindingError("scope_shadow", expression, "owned symbol shadowed in local scope")
                self.declarations[scope][name] = expression
            call = re.match(r"^\S+\s*\([^)]*\)\s+(\S+)", statement)
            if call:
                self.calls.append((scope, call.group(1), dict((n, v) for n, v, a, b in _assignments(statement))))
            if re.match(r"^ends(?:\s|$)", statement):
                if not scope:
                    raise BindingError("scope_shadow", statement, "unbalanced lexical scope")
                scope = self.parents[scope]
        if scope:
            raise BindingError("scope_shadow", "", "unterminated lexical scope")

    def lookup(self, scope, name):
        while scope is not None:
            if name in self.declarations[scope]:
                return scope, name
            scope = self.parents[scope]
        return None

    def trace(self, expression, scope, values=None):
        """Return numeric value, owned dependencies and exact DPAR declarations."""
        from .private_netlist import _expression_value
        budget, visiting, cache = [0], set(), {}
        values = values or {}
        references = set()

        def evaluate(expr, current):
            budget[0] += 1
            if budget[0] > 1024 or len(visiting) > 64 or len(expr) > 4096:
                raise BindingError("binding_unresolved", expr, "parameter dependency complexity limit")
            numbers, owned, dpars = {}, set(), set()
            # Engineering suffixes and function names are filtered by lookup.
            for name in set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", expr)):
                key = self.lookup(current, name)
                if name in self.symbols:
                    owned.add(name)
                    if name in values:
                        numbers[name] = values[name]
                    elif key is not None:
                        try:
                            numbers[name] = _expression_value(self.declarations[key[0]][name], {})
                        except (ValueError, SyntaxError, TypeError, ZeroDivisionError, OverflowError):
                            pass
                    continue
                if key is None:
                    continue
                references.add(key)
                if key in visiting:
                    raise BindingError("binding_unresolved", expr, "cyclic parameter reference")
                if key not in cache:
                    visiting.add(key)
                    cache[key] = evaluate(self.declarations[key[0]][name], key[0])
                    visiting.remove(key)
                number, dependencies, bindings = cache[key]
                owned.update(dependencies)
                dpars.update(bindings)
                if re.fullmatch(r"DPAR_[A-Za-z0-9_]+", name):
                    dpars.add(key)
                if number is not None:
                    numbers[name] = number
            try:
                number = _expression_value(expr, numbers)
            except (ValueError, SyntaxError, TypeError, ZeroDivisionError, OverflowError):
                number = None
            return number, owned, dpars

        result = evaluate(expression, scope)
        self.last_references = references
        return result

    def validate_instance_bindings(self, bindings):
        # An instance override of a traversed declaration changes its meaning.
        # Retain this explicit binding evidence and reject ambiguous templates.
        for scope, model, overrides in self.calls:
            for child in self.subcircuits.get(model, []):
                for binding_scope, name in bindings:
                    if binding_scope == child and name in overrides:
                        raise BindingError("scope_shadow", overrides[name], "instance override shadows selected lexical binding")
