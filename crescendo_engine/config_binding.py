"""Exact working config materialization from the observed HDB occurrence graph.

No source HDB object is opened for writing. Logical library names resolve to the
already verified working workspace copies; reference libraries remain read only.
"""
from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()


import ast
import hashlib
import os
import time

from .config_hierarchy import plain_switch_views


CONFIG_DEFAULT_LIST_MAX_BYTES = 3500


def _quote(value):
    return str(value).replace('\\', '\\\\').replace('"', '\\"')


def _string(value):
    return '"%s"' % _quote(value)


def _triple(row, prefix=''):
    values = tuple(row.get(prefix + name) for name in ('library', 'cell', 'view'))
    if not all(isinstance(value, str) and value for value in values):
        raise ValueError('config implementation identity is incomplete: %r' % (values,))
    return values


def config_designs(hierarchy):
    result = []
    for design in (hierarchy or {}).get('designs') or []:
        payload = design.get('hierarchy') or {}
        deps = design.get('config_dependencies') or payload.get('config_dependencies') or []
        if not deps:
            continue
        requested = design.get('design') or design
        if isinstance(requested, (list, tuple)):
            requested = dict(zip(('library', 'cell', 'view'), requested))
        result.append({
            'library': requested['library'], 'cell': requested['cell'], 'view': requested['view'],
            'resolved_top': design.get('resolved_top') or payload.get('resolved_top'),
            'config_bindings': design.get('config_bindings') or payload.get('config_bindings') or [],
            'config_dependencies': deps,
            'source_config': [requested[name] for name in ('library', 'cell', 'view')],
        })
    return result


def private_config_plan(workspace, clone_plan):
    # Local import avoids the workspace/config generator import cycle.
    from .workspace import private_design_identity, private_library_is_owned
    copied = workspace.get('copied_libraries') or {}
    clones = dict(((row['design_library'], row['design_cell'], row['design_view'],
                    row['occurrence_path']), row) for row in clone_plan)
    result = []
    tests = (workspace.get('setup') or {}).get('tests') or []
    for design in workspace.get('config_designs') or []:
        key = _triple(design)
        private_key = private_design_identity(workspace, key)
        if workspace.get('same_library_objects'):
            from .same_library_views import owns_view
            if not owns_view(workspace, private_key, role='config'):
                raise ValueError('requested config is not an exact owned new view: %r' % (private_key,))
        elif not private_library_is_owned(workspace, private_key[0]):
            raise ValueError('requested config is not in a working copied library: %s' % key[0])
        top = (_triple(design['resolved_top']) if workspace.get('copy_policy') == 'same_library_views'
               else private_design_identity(workspace, _triple(design['resolved_top'])))
        if workspace.get('copy_policy') != 'same_library_views' and not private_library_is_owned(workspace, top[0]):
            raise ValueError('config actual top is not in a working copied library: %s' % top[0])
        clone_paths = {index[3] for index in clones if index[:3] == key}
        top_clone = clones.get(key + ('/',))
        if top_clone and top_clone.get('same_library_view'):
            top = (top_clone['clone_library'], top_clone['clone_cell'], top_clone['target_view'])
            clone_paths.remove('/')
        rows = {}
        for source in design.get('config_bindings') or []:
            if source.get('netlister_ignored'):
                continue
            row = dict(source)
            path = row.get('hierarchy_path')
            parent = row.get('parent_path') or '/'
            name = row.get('name')
            if not path or path == '/' or not name or path != parent.rstrip('/') + '/' + name:
                raise ValueError('config occurrence path is inconsistent: %r' % path)
            if path in rows:
                raise ValueError('duplicate config occurrence: %s' % path)
            target = _triple(row, 'resolved_')
            clone = clones.get(key + (path,))
            if clone:
                if row.get('stop') or row.get('scan_traversable') is False:
                    raise ValueError('cannot clone below a config stop point or opaque view: %s' % path)
                target = (clone['clone_library'], clone['clone_cell'], clone.get('target_view') or clone['source_view'])
                clone_paths.remove(path)
            row['target'] = target
            if clone and not clone.get('same_library_view'):
                row['verification_master'] = (clone['clone_library'], clone['clone_cell'], clone['master_view'])
            rows[path] = row
        if clone_paths:
            raise ValueError('working config %s lacks bindings for optimized occurrences: %s' %
                             ('/'.join(key), ', '.join(sorted(clone_paths)[:10])))
        for path, row in rows.items():
            parent = row.get('parent_path') or '/'
            if parent != '/' and (parent not in rows or rows[parent].get('stop')
                                  or rows[parent].get('scan_traversable') is False):
                raise ValueError('config graph has a missing or stopped ancestor: %s' % path)
            ancestry = []
            current = row
            while current:
                parent_path = current.get('parent_path') or '/'
                owner = top if parent_path == '/' else rows[parent_path]['target']
                ancestry.insert(0, owner + (current['name'],))
                current = None if parent_path == '/' else rows[parent_path]
            row['specifier'] = ancestry
        generated = bool(design.get('generated_config'))
        backup_view = _contract_text('cr_source_') + hashlib.sha1('\x1f'.join(key).encode('utf-8')).hexdigest()[:12]
        # The copied-library path may have replaced its logical source mapping.
        # Use its preserved original; generated same-library configs use the
        # original source cellview (or have no source config at all).
        source_config = design.get('source_config')
        if not generated:
            source_config = private_key[:2] + (backup_view,)
        elif not source_config and design.get('config_dependencies'):
            source_config = key
        source_top = _triple(design['resolved_top'])[2]
        traversed = {row['resolved_view'] for row in rows.values() if not row.get('stop')}
        traversed.add(source_top)
        stopped = {row['resolved_view'] for row in rows.values() if row.get('stop')}
        source_key = tuple(design.get('source_config') or design.get('source_test_design') or key)
        matching_tests = [test for test in tests if test.get('enabled', True) and
                          (test.get('library'), test.get('cell'), test.get('view') or 'schematic') == source_key]
        candidate_tests = matching_tests or [test for test in tests if test.get('enabled', True)] or [{}]
        terminal_views = list(dict.fromkeys(view for test in candidate_tests
                                            for view in plain_switch_views(test) if view != 'schematic'))
        for view in ('alps', 'ahdl'):
            if view in stopped and view not in terminal_views:
                terminal_views.append(view)
        fallback_view_list = ' '.join(dict.fromkeys(terminal_views + [source_top]))
        fallback_stop_list = ' '.join(view for view in terminal_views
                                      if view in stopped and view not in traversed)
        fallback_stop_list = fallback_stop_list or _contract_text('cr_config_stop_sentinel')
        for kind, value in (('viewlist', fallback_view_list), ('stoplist', fallback_stop_list)):
            _validate_default_list(private_key, kind, value)
        result.append({'config': private_key, 'top': top, 'generated_config': generated,
                       'backup_view': backup_view, 'source_config': source_config,
                       'fallback_view_list': fallback_view_list,
                       'fallback_stop_list': fallback_stop_list,
                       'bindings': sorted(rows.values(), key=lambda row: (row['hierarchy_path'].count('/'), row['hierarchy_path']))})
        if workspace.get('same_library_objects'):
            result[-1]['owned_view'] = next(row for row in workspace['same_library_registry']
                                          if tuple(row['target']) == private_key and row['role'] in ('config', 'batch_config'))
    return result


def _validate_default_list(config, kind, value):
    size = len(value.encode('utf-8'))
    if size > CONFIG_DEFAULT_LIST_MAX_BYTES:
        raise ValueError('Working config %s %s is %d bytes (limit %d bytes)' %
                         ('/'.join(config), kind, size, CONFIG_DEFAULT_LIST_MAX_BYTES))


def _default_list_lines(design):
    source = design['source_config']
    lines = [_contract_text('crescSourceViewList = nil'), _contract_text('crescSourceStopList = nil')]
    if source:
        lines.extend([
            _contract_text('crescSourceConfig = hdbOpen(%s "r")') % ' '.join(_string(v) for v in source),
            _contract_text('unless(crescSourceConfig error("Crescendo cannot read source config default lists\\n"))'),
            _contract_text('crescSourceViewList = hdbGetDefaultViewListString(crescSourceConfig)'),
            _contract_text('crescSourceStopList = hdbGetDefaultStopListString(crescSourceConfig)'),
            _contract_text('hdbClose(crescSourceConfig)'),
        ])
    for kind, source_var, target_var, fallback in (
            ('viewlist', 'crescSourceViewList', 'crescDefaultViewList', design['fallback_view_list']),
            ('stoplist', 'crescSourceStopList', 'crescDefaultStopList', design['fallback_stop_list'])):
        source_var, target_var = _contract_text(source_var), _contract_text(target_var)
        lines.extend([
            '%s = if(%s && parseString(%s) then %s else %s)' %
            (target_var, source_var, source_var, source_var, _string(fallback)),
            _contract_text('when(strlen(%s) > %d error("Crescendo working config %%s %s is %%d bytes (limit %d bytes)\\n" %s strlen(%s)))') %
            (target_var, CONFIG_DEFAULT_LIST_MAX_BYTES, kind, CONFIG_DEFAULT_LIST_MAX_BYTES,
             _string('/'.join(design['config'])), target_var),
        ])
    return lines


def config_binding_lines(workspace, clone_plan, verification_path):
    """Build and reopen a private flattened config, verifying each real HDB binding."""
    plan = private_config_plan(workspace, clone_plan)
    if not plan:
        return [], plan
    lines = [_contract_text('crescConfigVerify = outfile(%s)') % _string(verification_path),
             _contract_text('unless(crescConfigVerify error("Crescendo cannot create config verification evidence\\n"))')]
    for design in plan:
        config = design['config']
        args = ' '.join(_string(value) for value in config)
        backup = _string(design['backup_view'])
        if not design.get('generated_config'):
            lines.extend([
            'unless(hdbIsConfig(%s %s %s)' % (_string(config[0]), _string(config[1]), backup),
            _contract_text('  crescSourceConfig = hdbOpen(%s "r")') % args,
            _contract_text('  unless(crescSourceConfig error("Crescendo cannot open copied original config\\n"))'),
            _contract_text('  unless(hdbSaveACopy(crescSourceConfig %s %s %s) error("Crescendo cannot preserve original copied config\\n"))') % (_string(config[0]), _string(config[1]), backup),
            _contract_text('  hdbClose(crescSourceConfig)'), ')',
            ])
        lines.extend(_default_list_lines(design))
        lines.extend([
            _contract_text('crescPrivateConfig = hdbOpen(%s "w")') % args,
            _contract_text('unless(crescPrivateConfig error("Crescendo cannot write working config\\n"))'),
            _contract_text('unless(hdbSetTopCellViewName(crescPrivateConfig %s) error("Crescendo cannot redirect working config top\\n"))') % ' '.join(_string(v) for v in design['top']),
            _contract_text('unless(hdbSetDefaultStopListString(crescPrivateConfig crescDefaultStopList) error("Crescendo cannot set working config stop list\\n"))'),
            _contract_text('unless(hdbSetDefaultViewListString(crescPrivateConfig crescDefaultViewList) error("Crescendo cannot set working config view list\\n"))'),
        ])
        for row in design['bindings']:
            spec = 'list(%s)' % ' '.join('list(%s)' % ' '.join(_string(value) for value in step) for step in row['specifier'])
            lines.extend([
                _contract_text('unless(hdbSetObjBindRule(crescPrivateConfig %s list(\'hdbcBindingRule list(%s))) error("Crescendo cannot set exact config occurrence %s\\n"))') % (spec, ' '.join(_string(v) for v in row['target']), _quote(row['hierarchy_path'])),
            ])
            if row.get('stop'):
                lines.append(_contract_text('unless(hdbSetObjBindRule(crescPrivateConfig %s list(\'hdbcStopRule t)) error("Crescendo cannot preserve config stop point\\n"))') % spec)
        lines.append(_contract_text('unless(hdbSave(crescPrivateConfig) error("Crescendo working config save failed\\n"))'))
        if design.get('owned_view'):
            from .same_library_views import ownership_marker_lines
            lines.extend(ownership_marker_lines(design['owned_view']))
        lines.extend([_contract_text('hdbClose(crescPrivateConfig)'), _contract_text('crescPrivateConfig = hdbOpen(%s "r")') % args,
                      _contract_text('unless(crescPrivateConfig error("Crescendo cannot reopen persisted working config\\n"))'),
                      _contract_text('unless(equal(list(hdbGetTopLibName(crescPrivateConfig) hdbGetTopCellName(crescPrivateConfig) hdbGetTopViewName(crescPrivateConfig)) list(%s)) error("Crescendo persisted config top mismatch\\n"))') % ' '.join(_string(v) for v in design['top']),
                      _contract_text('fprintf(crescConfigVerify "CONFIG_TOP\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" %s)') % ' '.join(_string(v) for v in config + design['top'])])
        lines.extend([
            _contract_text('crescSavedViewList = hdbGetDefaultViewListString(crescPrivateConfig)'),
            _contract_text('crescSavedStopList = hdbGetDefaultStopListString(crescPrivateConfig)'),
            _contract_text('unless(equal(crescSavedViewList crescDefaultViewList) && equal(crescSavedStopList crescDefaultStopList) error("Crescendo persisted config default lists mismatch\\n"))'),
            _contract_text('fprintf(crescConfigVerify "CONFIG_DEFAULTS\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" %s crescSourceViewList crescSourceStopList crescSavedViewList crescSavedStopList)') %
            ' '.join(_string(v) for v in config),
        ])
        bindings_by_path = dict((node['hierarchy_path'], node) for node in design['bindings'])
        for row in design['bindings']:
            lines.extend([_contract_text('crescConfigPath = hdbCreatePathVector(crescPrivateConfig)'),
                          _contract_text('unless(crescConfigPath error("Crescendo cannot create working config path vector\\n"))')])
            for index, step in enumerate(row['specifier'][:-1]):
                parent_path = '/' + '/'.join(s[3] for s in row['specifier'][:index+1])
                ancestor = bindings_by_path[parent_path]
                lines.append(_contract_text('unless(hdbPushCell(crescConfigPath %s %s) error("Crescendo cannot reconstruct working config path\\n"))') % (_string(step[3]), ' '.join(_string(v) for v in ancestor['target'])))
            raw = row.get('verification_master') or tuple(row.get('master_' + name) for name in ('library', 'cell', 'view'))
            if not all(raw):
                raise ValueError('config raw master identity is incomplete: %s' % row['hierarchy_path'])
            flags = ' '.join('t' if row.get('fixed_' + name) else 'nil' for name in ('library', 'cell', 'view'))
            lines.extend([
                _contract_text('crescConfigBound = hdbBind(crescConfigPath %s %s %s)') % (' '.join(_string(v) for v in raw), _string(row['name']), flags),
                _contract_text('unless(equal(list(car(crescConfigBound) cadr(crescConfigBound) caddr(crescConfigBound)) list(%s)) error("Crescendo persisted config binding mismatch at %s: %%L\\n" crescConfigBound))') % (' '.join(_string(v) for v in row['target']), _quote(row['hierarchy_path'])),
                _contract_text('unless(hdbPushCell(crescConfigPath %s %s) error("Crescendo cannot enter working config binding\\n"))') % (_string(row['name']), ' '.join(_string(v) for v in row['target'])),
                _contract_text('crescConfigStopped = if(hdbIsAtStopPoint(crescConfigPath) then t else nil)'),
                _contract_text('unless(equal(crescConfigStopped %s) error("Crescendo persisted config stop mismatch at %s\\n"))') % ('t' if row.get('stop') else 'nil', _quote(row['hierarchy_path'])),
                _contract_text('fprintf(crescConfigVerify "CONFIG_BINDING\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\t%%L\\n" %s %s car(crescConfigBound) cadr(crescConfigBound) caddr(crescConfigBound) crescConfigStopped)') % (' '.join(_string(v) for v in config), _string(row['hierarchy_path'])),
                _contract_text('hdbDestroyPathVector(crescConfigPath)'),
            ])
        lines.append(_contract_text('hdbClose(crescPrivateConfig)'))
    lines.extend([_contract_text('close(crescConfigVerify)'), _contract_text('crescConfigVerify = nil'), _contract_text('crescPrivateConfig = nil'), _contract_text('crescSourceConfig = nil'), _contract_text('crescConfigPath = nil')])
    return lines, plan


def validate_config_binding_records(plan, path):
    if not plan:
        return {'ok': True, 'configs': 0, 'bindings': 0}
    expected = []
    for design in plan:
        expected.append(('CONFIG_TOP', tuple(design['config']) + tuple(design['top'])))
        for row in design['bindings']:
            expected.append(('CONFIG_BINDING', tuple(design['config']) + (row['hierarchy_path'],) + tuple(row['target']) + (bool(row.get('stop')),)))
    actual = []
    defaults = {}
    by_config = {tuple(design['config']): design for design in plan}
    with open(path, 'r', encoding='utf-8') as stream:
        for line_number, line in enumerate(stream, 1):
            if line_number % 256 == 0:
                time.sleep(.001)
            fields = line.rstrip('\n').split('\t')
            values = tuple(False if value == 'nil' else True if value == 't' else ast.literal_eval(value) for value in fields[1:])
            if fields[0] != 'CONFIG_DEFAULTS':
                actual.append((fields[0], values))
                continue
            config = values[:3]
            design = by_config.get(config)
            if not design or config in defaults or len(values) != 7:
                raise ValueError('working config default list evidence is malformed or duplicated')
            source_views, source_stops, views, stops = values[3:]
            for kind, source, saved, fallback in (
                    ('viewlist', source_views, views, design['fallback_view_list']),
                    ('stoplist', source_stops, stops, design['fallback_stop_list'])):
                if source is not False and (not isinstance(source, str) or not design['source_config']):
                    raise ValueError('working config source %s evidence is invalid' % kind)
                wanted = source if source and source.split() else fallback
                if not isinstance(saved, str) or saved != wanted:
                    raise ValueError('working config persisted %s differs from expected defaults' % kind)
                _validate_default_list(config, kind, saved)
            defaults[config] = {'config': list(config), 'source_view_list': source_views or None,
                                'source_stop_list': source_stops or None, 'view_list': views, 'stop_list': stops}
    if actual != expected or set(defaults) != set(by_config):
        raise ValueError('working config binding evidence does not match observed graph and defaults')
    return {'ok': True, 'configs': len(plan), 'bindings': sum(len(row['bindings']) for row in plan),
            'default_lists': [defaults[tuple(design['config'])] for design in plan]}


def config_clone_cdf_lines(clone):
    """Preserve the declared module symbol's base CDF on a config clone."""
    if clone.get('same_library_view') or (not clone.get('config_bound') and not clone.get('source_reference')):
        return []
    source_library = clone.get('symbol_source_library') or clone['source_library']
    source_cell = clone.get('symbol_source_cell') or clone['source_cell']
    return [
        _contract_text('crescCloneSourceCDF = cdfGetBaseCellCDF(ddGetObj(%s %s))') % (_string(source_library), _string(source_cell)),
        _contract_text('when(crescCloneSourceCDF'),
        _contract_text('  crescCloneDestCell = ddGetObj(%s %s)') % (_string(clone['clone_library']), _string(clone['clone_cell'])),
        _contract_text('  unless(crescCloneDestCell error("Crescendo cannot obtain working clone CDF cell\\n"))'),
        _contract_text('  crescCloneDestCDF = cdfCopyCDF(crescCloneDestCell "baseCellData" crescCloneSourceCDF)'),
        _contract_text('  unless(crescCloneDestCDF error("Crescendo cloned module CDF is absent\\n"))'),
        _contract_text('  cdfSaveCDF(crescCloneDestCDF)'),
        _contract_text('  unless(length(crescCloneSourceCDF~>parameters) == length(crescCloneDestCDF~>parameters) error("Crescendo cloned module CDF parameter count mismatch\\n"))'),
        _contract_text('  foreach(crescSourceParam crescCloneSourceCDF~>parameters'),
        _contract_text('    crescDestParam = cdfFindParamByName(crescCloneDestCDF crescSourceParam~>name)'),
        _contract_text('    unless(crescDestParam && equal(list(crescSourceParam~>name crescSourceParam~>type crescSourceParam~>defValue crescSourceParam~>callback crescSourceParam~>parseAsNumber crescSourceParam~>parseAsCEL) list(crescDestParam~>name crescDestParam~>type crescDestParam~>defValue crescDestParam~>callback crescDestParam~>parseAsNumber crescDestParam~>parseAsCEL)) error("Crescendo cloned module CDF parameter mismatch\\n"))'),
        '  )',
        _contract_text('  unless(equal(crescCloneSourceCDF~>simInfo crescCloneDestCDF~>simInfo) error("Crescendo cloned module CDF simInfo mismatch\\n"))'),
        ')',
        _contract_text('crescCloneSourceCDF = nil'), _contract_text('crescCloneDestCDF = nil'), _contract_text('crescCloneDestCell = nil'), _contract_text('crescSourceParam = nil'), _contract_text('crescDestParam = nil'),
    ]
