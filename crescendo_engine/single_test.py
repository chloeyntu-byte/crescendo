"""Single-Test projection with the complete saved catalogue retained for guards."""
import copy


def project_setup(setup, name):
    result = copy.deepcopy(setup)
    rows = result.get('_source_tests') or result.get('tests') or []
    matches = [row for row in rows if row.get('name') == name]
    if len(matches) != 1:
        raise ValueError('Selected Test is missing or ambiguous: %s' % name)
    result['_source_tests'] = rows
    for key in ('outputs', 'variables'):
        source_key = '_source_' + key
        if source_key not in result:
            result[source_key] = copy.deepcopy(result.get(key) or [])
    if '_source_global_variables' not in result:
        result['_source_global_variables'] = copy.deepcopy(result.get('global_variables') or [])
    selected = copy.deepcopy(matches[0])
    selected['enabled'] = True
    result['tests'] = [selected]
    result['selected_test'] = name
    globals_ = []
    for variable in copy.deepcopy(result['_source_global_variables']):
        dependencies = variable.get('dependent_tests') or []
        selected_dependencies = [row for row in dependencies if row.get('test') == name]
        if dependencies and not selected_dependencies:
            continue
        variable['dependent_tests'] = selected_dependencies
        for child in (variable.get('native_content') or {}).get('children') or []:
            if child.get('tag') == 'dependenttests':
                child['children'] = [item for item in child.get('children') or []
                                     if item.get('tag') != 'dependenttest' or item.get('text') == name]
        globals_.append(variable)
    result['global_variables'] = globals_
    for key in ('outputs', 'variables'):
        result[key] = [row for row in result.get('_source_' + key, [])
                       if not row.get('test') or row.get('test') == name]
    global_names = set(row.get('name') for row in globals_)
    result['variables'] = [row for row in result['variables']
                           if row.get('scope') != 'global' or row.get('name') in global_names]
    result['analysis_names'] = sorted(set(item.get('name') for item in selected.get('analyses', []) if item.get('name')))
    result['output_names'] = sorted(set(item.get('name') for item in result['outputs'] if item.get('name')))
    result['libraries'] = [selected['library']] if selected.get('library') else []
    result['cells'] = [selected['cell']] if selected.get('cell') else []
    return result


def source_setup(setup):
    result = copy.deepcopy(setup)
    if result.get('_source_tests') is not None:
        result['tests'] = result['_source_tests']
    for key in ('outputs', 'variables'):
        if result.get('_source_' + key) is not None:
            result[key] = result['_source_' + key]
    if result.get('_source_global_variables') is not None:
        result['global_variables'] = result['_source_global_variables']
    return result


def native_selection(setup):
    from .test_selection import selection_plan
    name = setup.get('selected_test')
    if not name:
        return None
    source = source_setup(setup)
    source['tests'] = [row for row in source.get('tests') or [] if row.get('name') == name]
    return selection_plan(source, {'execution_selection': {'tests': [name]}})


def native_selection_lines(plan):
    """Set Test flags and touch analyses only for the selected Test."""
    import json
    from .test_selection import selection_skill_lines
    if not plan:
        return []
    reduced = copy.deepcopy(plan)
    reduced['available'] = list(plan['selected'])
    lines = selection_skill_lines(reduced)
    # selection_skill_lines qualifies the selected set before other Tests are
    # disabled. Keep only its analysis operations, then set every Test flag.
    lines = lines[:-len(reduced['available']) - 1]
    quote = lambda value: json.dumps(value, ensure_ascii=True)
    for name in plan['available']:
        lines.append('unless(maeSetSetup(?tests list(%s) ?enabled %s ?session session) error("Crescendo Test selection setter failed\\n"))' % (quote(name), 't' if name in plan['selected'] else 'nil'))
    expected = 'list(%s)' % ' '.join(quote(name) for name in plan['selected'])
    lines.append('unless(equal(sort(copy(maeGetSetup(?typeName "tests" ?enabled t ?session session)) \'alphalessp) sort(%s \'alphalessp)) error("Crescendo final enabled Test set differs\\n"))' % expected)
    return lines


def project_native_files(setup_path, selected, manifest_path=None, selected_corners=None):
    """Project known per-Test nodes in an owned native copy before opening it."""
    import hashlib
    import json
    import os
    import tempfile
    import xml.etree.ElementTree as ET
    from .ade_setup import _active_node, _child, _children, _enabled, _local_name, _name
    removed = []
    file_hashes = []
    def record(file_name, kind, name, node):
        removed.append({'file': file_name, 'kind': kind, 'name': name,
                        'node_sha256': hashlib.sha256(ET.tostring(node, encoding='utf-8')).hexdigest()})
    paths = [setup_path, os.path.join(os.path.dirname(setup_path), 'active.state')]
    for path in paths:
        if not os.path.isfile(path):
            continue
        with open(path, 'rb') as stream:
            before_hash = hashlib.sha256(stream.read()).hexdigest()
        original_mode = os.stat(path).st_mode & 0o7777
        tree = ET.parse(path)
        root = tree.getroot()
        if os.path.basename(path) == 'active.state':
            for child in _children(root, 'Test'):
                if _name(child) != selected:
                    record('active.state', 'Test', _name(child), child)
                    root.remove(child)
        else:
            active = _active_node(root)
            if active is None:
                raise ValueError('Native setup projection requires an active setup')
            tests = _child(active, 'tests')
            if tests is None:
                raise ValueError('Native setup projection requires saved Tests')
            found = 0
            for child in _children(tests, 'test'):
                name = _name(child)
                if name == selected:
                    found += 1
                else:
                    record('setup', 'test', name, child)
                    tests.remove(child)
            if found != 1:
                raise ValueError('Native setup projection requires exactly one selected Test')
            corners = _child(active, 'corners')
            if corners is not None:
                for corner in _children(corners, 'corner'):
                    disabled_tests = _child(corner, 'disabledtests')
                    masked = disabled_tests is not None and any(
                        _name(child) == selected for child in _children(disabled_tests, 'test'))
                    name = _name(corner)
                    excluded = selected_corners is not None and name not in selected_corners
                    if selected_corners is not None and (not _enabled(corner) or masked or excluded):
                        record('setup', 'unselected_corner', name, corner)
                        corners.remove(corner)
            parents = []
            disabled = _child(active, 'disabledtests')
            if disabled is not None:
                parents.append(disabled)
            corners = _child(active, 'corners')
            if corners is not None:
                parents.extend(node for node in (_child(corner, 'disabledtests') for corner in _children(corners, 'corner')) if node is not None)
            variables = _child(active, 'vars')
            if variables is not None:
                parents.extend(node for node in (_child(var, 'dependentTests') for var in _children(variables, 'var')) if node is not None)
            emptied_dependencies = set()
            for parent in parents:
                previous = len(parent)
                for child in list(parent):
                    if _local_name(child) not in ('test', 'dependenttest'):
                        continue
                    name = _name(child)
                    if name != selected:
                        record('setup', parent.tag + '/' + child.tag, name, child)
                        parent.remove(child)
                if _local_name(parent) == 'dependenttests' and previous and not len(parent):
                    emptied_dependencies.add(id(parent))
            # A variable explicitly scoped only to other Tests must not become
            # global after its dependency list is projected to empty.
            if variables is not None:
                for var in list(variables):
                    deps = _child(var, 'dependentTests')
                    if deps is not None and id(deps) in emptied_dependencies:
                        record('setup', 'unselected_scoped_var', _name(var), var)
                        variables.remove(var)
        fd, temporary = tempfile.mkstemp(prefix='.crescendo-projection-', dir=os.path.dirname(path))
        os.close(fd)
        try:
            tree.write(temporary, encoding='utf-8', xml_declaration=True)
            os.chmod(temporary, original_mode)
            os.rename(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        with open(path, 'rb') as stream:
            after_hash = hashlib.sha256(stream.read()).hexdigest()
        file_hashes.append({'file': os.path.basename(path), 'before_sha256': before_hash,
                            'after_sha256': after_hash})
    if manifest_path:
        with open(manifest_path, 'w') as stream:
            json.dump({'schema_version': 'crescendo.native_test_projection.v1', 'selected_test': selected,
                       'selected_corners': selected_corners,
                       'removed': removed, 'files': file_hashes}, stream, indent=2, sort_keys=True)
    return removed
