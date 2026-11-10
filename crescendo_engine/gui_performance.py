"""Private acceleration helpers; retain Tk encodings and snapshot ownership."""
import copy
import marshal
import math
import json


def snapshot_copy(value):
    """Copy exact builtin graphs in C; custom objects keep deepcopy protocols."""
    pending = [value]
    seen = set()
    scalar = frozenset((str, bytes, int, bool, type(None)))
    containers = (dict, list, tuple, set, frozenset)
    while pending:
        item = pending.pop()
        kind = type(item)
        if kind in scalar:
            continue
        if kind in (float, complex):
            finite = (math.isfinite(item.real) and math.isfinite(item.imag)) if kind is complex else math.isfinite(item)
            if not finite:
                # deepcopy retains atomic NaN identities; marshal would break
                # lookup by an original NaN dictionary key or set member.
                return copy.deepcopy(value)
            continue
        if kind not in containers:
            return copy.deepcopy(value)
        identity = id(item)
        if identity in seen:
            continue
        seen.add(identity)
        if kind is dict:
            pending.extend(child for child in item.keys() if type(child) not in scalar)
            pending.extend(child for child in item.values() if type(child) not in scalar)
        else:
            pending.extend(child for child in item if type(child) not in scalar)
    try:
        return marshal.loads(marshal.dumps(value))
    except (ValueError, TypeError):
        return copy.deepcopy(value)


class ScopeIndex(object):
    """Exact scope test, indexed by design and slash-delimited ancestors."""
    def __init__(self, scopes):
        self.designs = {}
        for scope in scopes:
            owner = scope.get("design") or {}
            identity = tuple(owner.get(key) for key in ("library", "cell", "view"))
            exact, ancestors = self.designs.setdefault(identity, (set(), set()))
            prefix = scope.get("hierarchy_path")
            exact.add(prefix)
            ancestors.add(prefix.rstrip("/"))

    def contains(self, identity, path):
        indexed = self.designs.get(tuple(identity.get(key) for key in ("library", "cell", "view")))
        if indexed is None:
            return False
        exact, ancestors = indexed
        if "/" in exact or path in exact:
            return True
        offset = path.find("/")
        while offset >= 0:
            if path[:offset] in ancestors:
                return True
            offset = path.find("/", offset + 1)
        return False


def parameter_defaults(parameters, initial):
    # Only these fields are consumed by initial_parameter_config and its helpers.
    fields = ("current_value", "default_value", "semantic", "default_selected",
              "value_type", "numeric_value", "choices", "choices_raw", "minimum", "maximum")
    cache, result = {}, {}
    for row in parameters:
        try:
            key = marshal.dumps(tuple((field in row, row.get(field)) for field in fields))
        except (ValueError, TypeError):
            value = initial(row)
        else:
            if key not in cache:
                cache[key] = initial(row)
            value = dict(cache[key])
        result[row["parameter_id"]] = value
    return result


class CachedCanvas(object):
    """Reuse an ordered Canvas display list; unchanged draws make no Tcl calls."""
    def __init__(self, canvas):
        self.canvas = canvas
        self.items = getattr(canvas, "_cr_draw_items", [])
        self.index = 0

    def __getattr__(self, name):
        if name.startswith("create_"):
            return lambda *args, **options: self._create(name, args, options)
        return getattr(self.canvas, name)

    def delete(self, tag):
        if tag != "all":
            return self.canvas.delete(tag)
        self.index = 0

    def _create(self, method, coordinates, options):
        previous = self.items[self.index] if self.index < len(self.items) else None
        if previous is not None and previous[0] == method:
            _, item, old_coordinates, old_options = previous
            if coordinates != old_coordinates:
                self.canvas.coords(item, *coordinates)
            if options != old_options:
                self.canvas.itemconfigure(item, **options)
        else:
            if previous is not None:
                self.canvas.delete(previous[1])
            item = getattr(self.canvas, method)(*coordinates, **options)
        record = (method, item, coordinates, options)
        if previous is None:
            self.items.append(record)
        else:
            self.items[self.index] = record
        self.index += 1
        return item

    def finish(self):
        for record in self.items[self.index:]:
            self.canvas.delete(record[1])
        del self.items[self.index:]
        self.canvas._cr_draw_items = self.items


def json_detail_tail(document, limit=65536):
    """Exactly the suffix of indent=2 JSON, without formatting discarded rows."""
    if document:
        key = list(document)[-1]
        rows = document[key]
        if isinstance(rows, list) and len(rows) > 128:
            count = 128
            while count < len(rows):
                suffix = json.dumps({key: rows[-count:]}, indent=2, ensure_ascii=False)
                # Exclude the artificial object/list opening from the suffix.
                opening = len(json.dumps({key: []}, indent=2, ensure_ascii=False))
                if len(suffix) - opening >= limit:
                    return suffix[-limit:]
                count *= 2
    return json.dumps(document, indent=2, ensure_ascii=False)[-limit:]
