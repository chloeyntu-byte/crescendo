"""Selective updates to an owned Maestro baseline from the saved source.

Only observed XML boundaries are modified. Source and previous baseline are
never written. The caller must perform native readback before publishing.
"""
from __future__ import print_function

import copy
import json
import os
import re
import shutil
import uuid
import xml.etree.ElementTree as ET

from .ade_setup import _active_node, _child, _children, _local_name, _name, inspect_setup
from .setup_baseline import (SCHEMA, _digest, _files, _view_relative,
                             origin_setup_path, verify_setup_baseline)
from .utils import atomic_write_json, sha256_file

SECTIONS = ('test_design', 'analyses', 'local_variables', 'global_variables',
            'outputs', 'corners', 'job_setup', 'save_options', 'test_settings', 'other_settings')


def _xml_semantics(path):
    def node(element):
        return (element.tag, tuple(sorted(element.attrib.items())),
                (element.text or '').strip(), tuple(node(child) for child in element))
    return node(ET.parse(path).getroot())


def _view_settings(path, state, selected_test):
    root = ET.parse(path).getroot()
    tests = _state_map(root) if state else _test_map(root)
    names = [selected_test] if selected_test else sorted(tests)
    result = []
    for name in names:
        test = tests.get(name)
        if test is None:
            continue
        for element in test.iter():
            key = (element.get('Name') or element.get('name') or _local_name(element)).lower().replace('_', '')
            if key in ('switchviewlist', 'stopviewlist'):
                result.append((name, key, (element.text or '').strip(), tuple(sorted(element.attrib.items()))))
    return sorted(result)


def _sim_settings(path, state, selected_test):
    root = ET.parse(path).getroot()
    tests = _state_map(root) if state else _test_map(root)
    rows = []
    for name in ([selected_test] if selected_test else sorted(tests)):
        test = tests.get(name)
        if test is None:
            continue
        if state:
            info = _component(test, 'adeinfo')
            fields = [item for item in _children(info, 'field')
                      if (item.get('Name') or item.get('name') or '').lower() == 'designinfo']
            if fields:
                values = re.findall(r'"(?:[^"\\]|\\.)*"', fields[0].text or '')
                rows.append((name, values[3] if len(values) > 3 else None))
        else:
            options = _one(test, 'tooloptions')
            values = [(str(_name(item)).lower(), ((_one(item, 'value').text or '').strip() if _one(item, 'value') is not None else None), tuple(sorted(item.attrib.items())))
                      for item in _children(options, 'option')
                      if (_name(item) or '').lower() in ('sim', 'simulator')]
            rows.append((name, tuple(values)))
    return rows


def _one(parent, tag):
    rows = _children(parent, tag)
    if len(rows) > 1:
        raise ValueError('Ambiguous native %s section' % tag)
    return rows[0] if rows else None


def _replace(parent, source, tag):
    old, new = _one(parent, tag), _one(source, tag)
    index = list(parent).index(old) if old is not None else len(parent)
    if old is not None:
        parent.remove(old)
    if new is not None:
        parent.insert(index, copy.deepcopy(new))


def _named(parent, tag):
    result = {}
    for node in _children(parent, tag):
        name = _name(node)
        if not name or name in result:
            raise ValueError('Ambiguous native %s identity' % tag)
        result[name] = node
    return result


def _test_map(root):
    active = _active_node(root)
    tests = _one(active, 'tests')
    if tests is None:
        raise ValueError('Native setup does not contain a Tests section')
    return _named(tests, 'test')


def _state_map(root):
    return _named(root, 'test')


def _component(test, name):
    rows = [n for n in _children(test, 'component')
            if (n.get('Name') or n.get('name') or '').lower() == name]
    if len(rows) > 1:
        raise ValueError('Ambiguous native state component: %s' % name)
    return rows[0] if rows else None


def _replace_component(test, source, name):
    old, new = _component(test, name), _component(source, name)
    index = list(test).index(old) if old is not None else len(test)
    if old is not None:
        test.remove(old)
    if new is not None:
        test.insert(index, copy.deepcopy(new))


def _design(test, source):
    old, new = _one(test, 'tooloptions'), _one(source, 'tooloptions')
    if old is None or new is None:
        raise ValueError('Test design update requires native tooloptions in both setups')
    names = ('lib', 'library', 'cell', 'view')
    def selected(parent):
        rows = [n for n in _children(parent, 'option') if (_name(n) or '').lower() in names]
        keys = [(_name(n) or '').lower() for n in rows]
        if len(keys) != len(set(keys)) or ('lib' in keys and 'library' in keys):
            raise ValueError('Ambiguous native Test design options')
        return rows
    old_rows, new_rows = selected(old), selected(new)
    for node in old_rows:
        old.remove(node)
    for node in new_rows:
        old.append(copy.deepcopy(node))


def _remaining(dest, src, excluded):
    # Replace the whole observed complement, retaining the explicitly owned sections.
    excluded = set(excluded)
    for node in list(dest):
        if _local_name(node) not in excluded:
            dest.remove(node)
    for node in src:
        if _local_name(node) not in excluded:
            dest.append(copy.deepcopy(node))


def _merge_sdb(old, new, selected, selected_test):
    a, b = _active_node(old), _active_node(new)
    ot, nt = _test_map(old), _test_map(new)
    names = [selected_test] if selected_test else sorted(ot)
    missing = [name for name in names if name not in ot or name not in nt]
    if missing:
        raise ValueError('Selected Test missing in saved source or baseline: %s' % ', '.join(missing))
    if 'other_settings' in selected:
        _remaining(a, b, ('tests', 'vars', 'corners', 'jobpolicyname', 'disabledtests'))
    if 'global_variables' in selected:
        _replace(a, b, 'vars')
    if 'corners' in selected:
        old_corners = _named(_one(a, 'corners'), 'corner') if _one(a, 'corners') is not None else {}
        _replace(a, b, 'corners')
        if selected_test:
            new_container = _one(a, 'corners')
            new_corners = _named(new_container, 'corner') if new_container is not None else {}
            if new_container is None and old_corners:
                new_container = ET.Element('corners')
                a.append(new_container)
            for corner_name in sorted(set(old_corners).difference(new_corners)):
                retained = copy.deepcopy(old_corners[corner_name])
                masks = _one(retained, 'disabledtests')
                if masks is None:
                    masks = ET.Element('disabledtests')
                    retained.append(masks)
                if not any(_name(x) == selected_test for x in _children(masks, 'test')):
                    row = ET.Element('test')
                    row.text = selected_test
                    masks.append(row)
                new_container.append(retained)
            # New corners inherit the source masks. Existing corners preserve
            # non-selected Test membership from the previous baseline.
            for corner_name, corner in new_corners.items():
                source_mask = _one(corner, 'disabledtests')
                if corner_name not in old_corners:
                    continue
                old_corner = old_corners.get(corner_name)
                old_mask = _one(old_corner, 'disabledtests') if old_corner is not None else None
                selected_rows = [copy.deepcopy(x) for x in _children(source_mask, 'test') if _name(x) == selected_test]
                if source_mask is not None:
                    corner.remove(source_mask)
                if old_mask is not None:
                    preserved = copy.deepcopy(old_mask)
                    for row in list(preserved):
                        if _local_name(row) == 'test' and _name(row) == selected_test:
                            preserved.remove(row)
                    for row in selected_rows:
                        preserved.append(row)
                    corner.append(preserved)
                elif source_mask is not None:
                    preserved = copy.deepcopy(source_mask)
                    for row in list(preserved):
                        if _local_name(row) == 'test' and _name(row) != selected_test:
                            preserved.remove(row)
                    corner.append(preserved)
        if 'other_settings' not in selected:
            source_groups_node, old_groups_node = _one(b, 'modelgroups'), _one(a, 'modelgroups')
            source_groups = _named(source_groups_node, 'modelgroup') if source_groups_node is not None else {}
            source_corners = _one(b, 'corners')
            referenced = set()
            for corner in _children(source_corners, 'corner'):
                group = _one(corner, 'modelgroupname')
                value = (group.text or '').strip() if group is not None else ''
                group_names = re.findall(r'"([^"\\]*(?:\\.[^"\\]*)*)"', value) if '"' in value else value.split()
                referenced.update(group_names)
            if referenced:
                if old_groups_node is None:
                    old_groups_node = ET.Element('modelgroups')
                    a.append(old_groups_node)
                existing = _named(old_groups_node, 'modelgroup')
                for name in sorted(referenced):
                    if name not in source_groups:
                        raise ValueError('Corner references unavailable native model group: %s' % name)
                    old_group = existing.get(name)
                    index = list(old_groups_node).index(old_group) if old_group is not None else len(old_groups_node)
                    if old_group is not None:
                        old_groups_node.remove(old_group)
                    old_groups_node.insert(index, copy.deepcopy(source_groups[name]))
        # The alternate observed mask encoding is stored on each Test.
        for name in names:
            for tag in ('disabledcorners', 'nominalcornerenabled'):
                _replace(ot[name], nt[name], tag)
    if 'test_settings' in selected:
        old_disabled, new_disabled = _one(a, 'disabledtests'), _one(b, 'disabledtests')
        if old_disabled is None and new_disabled is not None:
            old_disabled = ET.Element(new_disabled.tag, new_disabled.attrib)
            old_disabled.text = new_disabled.text
            a.append(old_disabled)
        if old_disabled is not None:
            for node in list(old_disabled):
                if _local_name(node) == 'test' and _name(node) in names:
                    old_disabled.remove(node)
            for node in _children(new_disabled, 'test'):
                if _name(node) in names:
                    old_disabled.append(copy.deepcopy(node))
    if 'job_setup' in selected:
        _replace(a, b, 'jobpolicyname')
    for name in names:
        dest, src = ot[name], nt[name]
        if 'other_settings' in selected:
            _remaining(dest, src, ('tooloptions', 'vars', 'analyses', 'outputs', 'jobpolicyname', 'disabledcorners', 'nominalcornerenabled'))
            old_opts, new_opts = _one(dest, 'tooloptions'), _one(src, 'tooloptions')
            if (old_opts is None) != (new_opts is None):
                raise ValueError('Native tooloptions presence differs')
            if old_opts is not None:
                owned = ('lib', 'library', 'cell', 'view', 'sim', 'simulator')
                for option in list(old_opts):
                    if _local_name(option) != 'option' or (_name(option) or '').lower() not in owned:
                        old_opts.remove(option)
                for option in new_opts:
                    if _local_name(option) != 'option' or (_name(option) or '').lower() not in owned:
                        old_opts.append(copy.deepcopy(option))
        if 'test_design' in selected:
            _design(dest, src)
        if 'test_settings' in selected:
            for key in ('enabled', 'Enabled'):
                dest.attrib.pop(key, None)
                if key in src.attrib:
                    dest.attrib[key] = src.attrib[key]
            old_opts, new_opts = _one(dest, 'tooloptions'), _one(src, 'tooloptions')
            if old_opts is None or new_opts is None:
                raise ValueError('Native Test simulator options unavailable')
            for node in list(old_opts):
                if _local_name(node) == 'option' and (_name(node) or '').lower() in ('sim', 'simulator'):
                    old_opts.remove(node)
            for node in new_opts:
                if _local_name(node) == 'option' and (_name(node) or '').lower() in ('sim', 'simulator'):
                    old_opts.append(copy.deepcopy(node))
        if 'local_variables' in selected:
            _replace(dest, src, 'vars')
        if 'analyses' in selected:
            _replace(dest, src, 'analyses')
        if 'outputs' in selected:
            _replace(dest, src, 'outputs')
        if 'job_setup' in selected:
            _replace(dest, src, 'jobpolicyname')


def _design_state(test, source, design=True, simulator=False):
    old, new = _component(test, 'adeinfo'), _component(source, 'adeinfo')
    if (old is None) != (new is None):
        raise ValueError('Native designInfo presence differs')
    if old is None:
        return
    def field(component):
        rows = [n for n in _children(component, 'field')
                if (n.get('Name') or n.get('name') or '').lower() == 'designinfo']
        if len(rows) > 1:
            raise ValueError('Ambiguous native designInfo')
        return rows[0] if rows else None
    a, b = field(old), field(new)
    if (a is None) != (b is None):
        raise ValueError('Native designInfo field presence differs')
    if a is None:
        return
    pattern = r'"(?:[^"\\]|\\.)*"'
    left, right = list(re.finditer(pattern, a.text or '')), list(re.finditer(pattern, b.text or ''))
    if len(left) < 3 or len(right) < 3:
        raise ValueError('Unrecognized native designInfo encoding')
    if simulator and (len(left) < 4 or len(right) < 4):
        raise ValueError('Unrecognized native simulator designInfo encoding')
    text = a.text or ''
    if simulator:
        text = text[:left[3].start()] + (b.text or '')[right[3].start():right[3].end()] + text[left[3].end():]
    if design:
        text = text[:left[0].start()] + (b.text or '')[right[0].start():right[2].end()] + text[left[2].end():]
    a.text = text


def _merge_state(old, new, selected, test_names):
    ot, nt = _state_map(old), _state_map(new)
    for name in test_names:
        if (name in ot) != (name in nt):
            raise ValueError('Native state for selected Test differs: %s' % name)
    for name in sorted(test_names.intersection(ot)):
        if 'other_settings' in selected:
            old_info, new_info = _component(ot[name], 'adeinfo'), _component(nt[name], 'adeinfo')
            if (old_info is None) != (new_info is None):
                raise ValueError('Native adeInfo presence differs')
            if old_info is not None:
                for node in list(old_info):
                    if _local_name(node) != 'field' or (node.get('Name') or node.get('name') or '').lower() not in ('designinfo', 'projectdir'):
                        old_info.remove(node)
                for node in new_info:
                    if _local_name(node) != 'field' or (node.get('Name') or node.get('name') or '').lower() not in ('designinfo', 'projectdir'):
                        old_info.append(copy.deepcopy(node))
            excluded = ('analyses', 'outputs', 'adeinfo')
            for node in list(ot[name]):
                if _local_name(node) == 'component' and (node.get('Name') or node.get('name') or '').lower() not in excluded:
                    ot[name].remove(node)
            for node in nt[name]:
                if _local_name(node) == 'component' and (node.get('Name') or node.get('name') or '').lower() not in excluded:
                    ot[name].append(copy.deepcopy(node))
        if selected.intersection(('test_design', 'test_settings')):
            _design_state(ot[name], nt[name], 'test_design' in selected, 'test_settings' in selected)
        for section, component in (('analyses', 'analyses'), ('outputs', 'outputs')):
            if section in selected:
                _replace_component(ot[name], nt[name], component)


def _merged_job_policy(old, incoming, selected_test):
    policy = copy.deepcopy(incoming)
    if not selected_test or not isinstance(old, dict):
        return policy
    old_tests, new_tests = old.get('tests') or [], incoming.get('tests') or []
    names = [row.get('name') for row in old_tests]
    new_names = [row.get('name') for row in new_tests]
    if len(names) != len(set(names)) or len(new_names) != len(set(new_names)):
        raise ValueError('Ambiguous native Job Setup Test identities')
    policy['tests'] = [copy.deepcopy(row) for row in old_tests if row.get('name') != selected_test]
    policy['tests'].extend(copy.deepcopy(row) for row in new_tests if row.get('name') == selected_test)
    options = policy.get('run_options')
    if isinstance(options, dict):
        old_limits = (old.get('run_options') or {}).get('JobLimit') or []
        new_limits = options.get('JobLimit') or []
        options['JobLimit'] = ([copy.deepcopy(row) for row in old_limits if row[0] != selected_test] +
                               [copy.deepcopy(row) for row in new_limits if row[0] == selected_test])
    return policy


def update_saved_sections(prepared, config, sections):
    """Build a new baseline, returning (setup, invalidation plan).

    Native verification and publishing the binding are the caller's responsibility.
    """
    selected = set(sections or [])
    requested = set(selected)
    if not selected or selected.difference(SECTIONS):
        raise ValueError('Select one or more known Maestro sections')
    original = prepared.get('setup') or {}
    baseline = verify_setup_baseline(original)
    if not baseline:
        raise ValueError('Selective update requires an existing saved Maestro baseline')
    with open(os.path.join(baseline['root'], 'setup.json')) as stream:
        previous_snapshot = json.load(stream)
    old_policy = previous_snapshot.get('job_policy')
    incoming_policy = prepared.get('_incoming_job_policy')
    warnings = []
    skipped = []
    if 'job_setup' in selected and not (incoming_policy and incoming_policy.get('available')):
        selected.remove('job_setup')
        skipped.append('job_setup')
        warnings.append('Source Job Setup unavailable; previous Job Setup retained')
    source = origin_setup_path(original)
    source_dir = os.path.realpath(os.path.dirname(source))
    before = _files(source_dir)
    old_dir = os.path.dirname(baseline['setup_path'])
    source_sdb = os.path.join(source_dir, os.path.basename(source))
    old_state, source_state = (os.path.join(old_dir, 'active.state'),
                               os.path.join(source_dir, 'active.state'))
    if selected.intersection(('test_design', 'test_settings', 'analyses', 'outputs', 'other_settings')) and os.path.isfile(old_state) != os.path.isfile(source_state):
        raise ValueError('Native active.state presence changed; cannot safely update analyses or outputs')
    runtime = config.get('runtime') or {}
    parent = os.path.join(runtime.get('design_root') or runtime['run_root'], 'setup_baselines')
    if not os.path.isdir(parent):
        os.makedirs(parent, mode=0o700)
    token = uuid.uuid4().hex
    root = os.path.realpath(os.path.join(parent, token))
    os.mkdir(root, 0o700)
    view = os.path.join(root, _view_relative(original))
    try:
        os.makedirs(os.path.dirname(view))
        from .workspace import _copy_private_file
        shutil.copytree(old_dir, view, copy_function=_copy_private_file)
        target = os.path.join(view, os.path.basename(source))
        tree, incoming = ET.parse(target), ET.parse(source_sdb)
        selected_test = original.get('selected_test')
        _merge_sdb(tree.getroot(), incoming.getroot(), selected, selected_test)
        tree.write(target, encoding='utf-8', xml_declaration=True)
        if os.path.isfile(old_state) and selected.intersection(('test_design', 'test_settings', 'analyses', 'outputs', 'other_settings')):
            state_path = os.path.join(view, 'active.state')
            state, new_state = ET.parse(state_path), ET.parse(source_state)
            _merge_state(state.getroot(), new_state.getroot(), selected, set([selected_test]) if selected_test else set(_test_map(tree.getroot())))
            state.write(state_path, encoding='utf-8', xml_declaration=True)
        if before != _files(source_dir):
            raise ValueError('Saved Maestro source changed during selective update')
        verify_setup_baseline(original)
        setup = inspect_setup(target, backend='maestro')
        # The baseline lives outside the source library; retain its actual identity.
        source_content = copy.deepcopy(setup['source'])
        setup['source'] = copy.deepcopy(original['source'])
        for key, value in source_content.items():
            if key == 'sha256' or key.endswith('_sha256'):
                setup['source'][key] = value
        setup['setup_locator'] = copy.deepcopy(original['setup_locator'])
        old_save = (original.get('save_options') or (old_policy or {}).get('save_options'))
        incoming_save = prepared.get('_incoming_save_options')
        if 'save_options' in selected and incoming_save and incoming_save.get('available'):
            save_options = copy.deepcopy(incoming_save)
        else:
            save_options = copy.deepcopy(old_save)
            if 'save_options' in selected:
                warnings.append('Source Save Options unavailable; previous values retained')
        if save_options is not None:
            setup['save_options'] = save_options
        policy = (_merged_job_policy(old_policy, incoming_policy, selected_test)
                  if 'job_setup' in selected else copy.deepcopy(old_policy))
        if isinstance(policy, dict):
            if save_options is not None:
                policy['save_options'] = copy.deepcopy(save_options)
            else:
                policy.pop('save_options', None)
        snapshot = {'setup': copy.deepcopy(setup), 'job_policy': policy}
        atomic_write_json(os.path.join(root, 'setup.json'), snapshot)
        manifest = {'schema_version': SCHEMA, 'baseline_id': token, 'owner_uid': os.getuid(),
                    'origin_canonical_path': os.path.realpath(source), 'origin_setup_path': source,
                    'view_relative_path': _view_relative(original), 'setup_filename': os.path.basename(source),
                    'files': _files(view), 'setup_snapshot_sha256': sha256_file(os.path.join(root, 'setup.json'))}
        atomic_write_json(os.path.join(root, 'manifest.json'), manifest)
        setup['baseline'] = {'schema_version': SCHEMA, 'baseline_id': token, 'root': root,
                             'setup_path': target, 'manifest_sha256': sha256_file(os.path.join(root, 'manifest.json')),
                             'fingerprint': _digest(manifest)}
        verify_setup_baseline(setup)
        setup['baseline_job_policy'] = copy.deepcopy(policy)
        hierarchy_changed = 'test_design' in selected
        if 'test_settings' in selected:
            hierarchy_changed = hierarchy_changed or (_sim_settings(baseline['setup_path'], False, selected_test) !=
                                                      _sim_settings(target, False, selected_test))
            if os.path.isfile(old_state) and os.path.isfile(os.path.join(view, 'active.state')):
                hierarchy_changed = hierarchy_changed or (_sim_settings(old_state, True, selected_test) !=
                                                          _sim_settings(os.path.join(view, 'active.state'), True, selected_test))
        if 'other_settings' in selected:
            hierarchy_changed = hierarchy_changed or (_view_settings(baseline['setup_path'], False, selected_test) !=
                                                      _view_settings(target, False, selected_test))
            if os.path.isfile(old_state) and os.path.isfile(os.path.join(view, 'active.state')):
                hierarchy_changed = hierarchy_changed or (_view_settings(old_state, True, selected_test) !=
                                                          _view_settings(os.path.join(view, 'active.state'), True, selected_test))
        plan = {'sections': sorted(requested), 'skipped_sections': skipped, 'requires_hierarchy': hierarchy_changed,
                'refresh_job_policy': bool(selected.intersection(('job_setup', 'save_options', 'other_settings'))),
                'refresh_variables': bool(selected.intersection(('local_variables', 'global_variables', 'corners', 'test_design', 'test_settings', 'other_settings'))),
                'refresh_outputs': bool(selected.intersection(('outputs', 'analyses', 'test_design', 'test_settings', 'other_settings'))),
                'invalidate_prepared': True, 'invalidate_private_session': True,
                'preserve_existing_checkpoints': True, 'native_readback_required': True, 'warnings': warnings,
                'no_changes': False}
        same_state = ((not os.path.isfile(old_state) and not os.path.isfile(os.path.join(view, 'active.state'))) or
                      (os.path.isfile(old_state) and os.path.isfile(os.path.join(view, 'active.state')) and
                       _xml_semantics(old_state) == _xml_semantics(os.path.join(view, 'active.state'))))
        if (_xml_semantics(baseline['setup_path']) == _xml_semantics(target) and same_state and
                previous_snapshot.get('job_policy') == snapshot.get('job_policy') and
                (previous_snapshot.get('setup') or {}).get('save_options') == setup.get('save_options')):
            from .single_test import source_setup
            retained = source_setup(original)
            retained.pop('selected_test', None)
            retained['baseline_job_policy'] = copy.deepcopy(old_policy)
            shutil.rmtree(root)
            plan.update(no_changes=True, native_readback_required=False,
                        invalidate_prepared=False, invalidate_private_session=False)
            return retained, plan
        return setup, plan
    except Exception:
        shutil.rmtree(root)
        raise
