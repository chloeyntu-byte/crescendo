"""A shared diagnostic population through the normal execution pipelines.

A test is diagnostic, never a resume/ownership/completion authority. Independent
observations continue after failures; native launches still obey normal leases,
source freshness, complete corner coverage and proven stop/cleanup barriers.
"""
from __future__ import print_function

import collections
import copy
import datetime
import hashlib
import json
import math
import os
import re
import queue
import shutil
import stat
import tarfile
import threading
import time
import uuid

from .diagnostic_plan import freeze_plan, digest as plan_digest
from .diagnostic_timing import DiagnosticTiming
from .environment_probes import run_environment_probes, query_scheduler_jobs, _redact
from .diagnosis_catalog import SCHEMA as DIAGNOSIS_SCHEMA, describe_check, format_section, format_evidence_section, observation_section, brief_success, format_ladder_summary
from .environment_diagnostics import collect_run_diagnostics, failure_summary, _windows, package_identity, pipeline_qualification, pipeline_summary, failure_layer

ENGINES = ('maestro_baseline', 'private_netlist', 'maestro_netlist')
LEGACY_ENGINES = {'persistent_maestro': 'maestro_netlist'}
EXECUTION_PROTOCOL = 'crescendo.execution_pipeline.v1'
LABELS = {'maestro_netlist': 'Maestro', 'private_netlist': 'PrivateNetlist', 'maestro_baseline': 'MaestroBaseline'}
SIMULATORS = ('spectre', 'alps')
SIMULATOR_LABELS = {'spectre': 'Spectre', 'alps': 'ALPS'}
TERMINAL = ('completed', 'failed', 'stopped')
LOG_TAIL = 65536


def _candidate_plan_observation(candidates, expected, state):
    """Separate observed identity mismatches from an unfinished plan."""
    actual = sorted(candidates, key=lambda item: (item.get('generation', -1), item.get('index', -1)))
    prefix_matches = len(actual) <= len(expected) and all(
        item.get('values') == plan['values'] and item.get('generation') == plan['generation'] and
        item.get('index') == plan['index'] and item.get('candidate_id') == 'g%03d_c%04d' % (plan['generation'], plan['index'])
        for item, plan in zip(actual, expected))
    if not prefix_matches:
        return 'FAIL'
    if len(actual) == len(expected):
        return 'PASS'
    return 'FAIL' if state == 'completed' else 'NOT_TESTED'


def _engine_deadline_budget(request_spec, runtime, candidate_spec):
    """Keep diagnostic time bounded without adding 180 to an unlimited sentinel."""
    candidate = float(candidate_spec.get('candidate_timeout_seconds', 600))
    count = int(candidate_spec.get('generations', 1)) * int(candidate_spec.get('population', 1))
    default = (candidate if math.isfinite(candidate) and candidate > 0 else 600) * count + 180
    budget = float(request_spec.get('engine_timeout_seconds',
                   runtime.get('environment_test_engine_timeout_seconds', default)))
    return budget if math.isfinite(budget) and budget > 0 else 780


def _report_truncation_paths(value, path='', result=None):
    """Summarize explicit producer bounds without treating absence as proof."""
    result = [] if result is None else result
    if len(result) >= 64:
        return result
    if isinstance(value, dict):
        for key, item in value.items():
            current = path + '/' + str(key)
            if (key in ('read_truncated', 'context_truncated', 'text_truncated', 'omitted_depth') and item is True
                    or key in ('rows_truncated', 'events_truncated', 'callback_input_lines_omitted')
                    and isinstance(item, int) and item > 0):
                result.append(current)
            elif isinstance(item, (dict, list)):
                _report_truncation_paths(item, current, result)
            if len(result) >= 64:
                break
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _report_truncation_paths(item, path + '/' + str(index), result)
            if len(result) >= 64:
                break
    return result


def _utc():
    return datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')


def _error(error):
    return '%s: %s' % (type(error).__name__, error)


def _diagnostic_value(value):
    if isinstance(value, float) and not math.isfinite(value):
        return {'nonfinite': str(value)}
    if isinstance(value, dict):
        return dict((key, _diagnostic_value(item)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return [_diagnostic_value(item) for item in value]
    return value


def _json(value):
    return json.dumps(_diagnostic_value(value), sort_keys=True, ensure_ascii=False, default=str, allow_nan=False)


def _compact_success_proof(proof):
    """A passed check needs its scope/counts, not every repeated receipt."""
    if not isinstance(proof, dict):
        return {'detail_sha256': hashlib.sha256(_json(proof).encode('utf-8')).hexdigest()}
    keep = {'ok', 'state', 'proof_kind', 'solver', 'submission', 'netlist_source',
            'source_mode', 'simulator', 'execution_simulator', 'cleanup_proven',
            'cleanup_pending', 'stop_proven', 'native_parameter_readback_verified',
            'private_oa_backannotation_proven', 'template_qualified',
            'native_generation_required', 'candidate_id', 'scope'}
    result = {key: value for key, value in proof.items()
              if (key in keep or key.endswith('_count')) and not isinstance(value, (dict, list))}
    # Scope prose is identical across every candidate and belongs in the UI.
    result.pop('scope', None)
    if isinstance(proof.get('points'), list):
        result['points_count'] = len(proof['points'])
    generation = proof.get('netlist_generation')
    if isinstance(generation, dict):
        result['netlist_generation'] = {key: generation[key] for key in
            ('source_mode', 'template_qualified', 'native_generation_required') if key in generation}
    return result


def _compact_report_context(context):
    """Keep a copyable context; large file inventories remain in owned files."""
    from .environment_diagnostics import concise_diagnostic_value
    result = {key: concise_diagnostic_value(value) for key, value in context.items()
              if key not in ('records', 'cdf_callback_failures', 'native_worker_diagnostics', 'artifacts', 'netlists')}
    selection = context.get('execution_selection')
    if isinstance(selection, dict):
        result['execution_selection'] = {
            'requested_tests': selection.get('requested_tests'),
            'tests': [{key: test.get(key) for key in ('name', 'selected', 'requested_analyses')}
                for test in selection.get('tests') or []]}
        for test, shown in zip(selection.get('tests') or [], result['execution_selection']['tests']):
            shown['enabled_corners'] = [corner.get('name') for corner in test.get('corners') or []
                                       if corner.get('enabled') is True]
    result['report_detail_mode'] = 'concise'
    for key in ('netlists', 'artifacts'):
        if key in context:
            rows = context.get(key) or []
            selected = rows if len(rows) <= 2 else rows[:1] + rows[-1:]
            result[key] = [{field: item[field] for field in ('path', 'kind', 'role', 'status', 'bytes')
                            if field in item} for item in selected if isinstance(item, dict)]
            result[key + '_count'] = len(rows)
            result[key + '_omitted'] = len(rows) - len(selected)
    return result


def _short_report_paths(raw, roots):
    """Abbreviate only exact root prefixes; each section has its root mapping."""
    for alias, root in sorted(roots.items(), key=lambda item: len(item[1]), reverse=True):
        for literal in sorted(set((root, _json(root)[1:-1])), key=len, reverse=True):
            raw = re.sub(re.escape(literal) + r'''(?=$|[/\s"')\],;])''', lambda unused: alias, raw)
    return raw


def _small(value, limit=65536):
    raw = _json(value).encode('utf-8')
    return value if len(raw) <= limit else {'truncated': True, 'original_bytes': len(raw),
        'text': raw[:limit].decode('utf-8', 'ignore')}


def _proof_brief(value):
    """Success proof index; failed proof details must remain complete."""
    if not isinstance(value, dict) or value.get('ok') is False:
        return value
    result = {}
    for key, item in value.items():
        if isinstance(item, (str, int, float, bool)) or item is None:
            result[key] = item
        elif isinstance(item, list):
            if key in ('tests', 'corners', 'enabled_tests') and len(item) <= 32 and all(isinstance(row, str) for row in item):
                result[key] = item
            else:
                result[key + '_count'] = len(item)
                result[key + '_sha256'] = hashlib.sha256(_json(item).encode('utf-8')).hexdigest()
        elif isinstance(item, dict):
            result[key] = dict((name, field) for name, field in item.items()
                               if isinstance(field, (str, int, float, bool)) or field is None)
    return result


def _native_failure_observations(primary):
    """Index explicit native observations without reordering producer errors."""
    if not isinstance(primary, dict):
        return []
    capture = primary.get('capture_diagnostics') or {}
    skill = (capture.get('skill') or {}) if isinstance(capture, dict) else {}
    if not isinstance(skill, dict):
        return []
    markers = [item for item in (skill.get('native_run_evidence') or [])[:24] if isinstance(item, dict)]
    errors = [item for item in (skill.get('errors') or [])[:32] if isinstance(item, dict)]
    rows, seen = [], set()
    for item in errors:
        message = str(item.get('message') or '')
        boundary = item.get('native_run_boundary') or {}
        identifier = None
        if isinstance(boundary, dict) and boundary.get('stage') in ('Run', 'Wait'):
            identifier = 'native.run_window'
        if 'Crescendo native Run returned invalid runId' in message:
            identifier = 'native.run_id'
        elif 'Crescendo native capture history unavailable' in message or 'Crescendo native history handle invalid' in message:
            identifier = 'native.history_lookup'
        elif 'Crescendo native history name lookup failed' in message or 'Crescendo native history name unavailable' in message:
            identifier = 'native.history_name'
        if identifier and identifier not in seen:
            seen.add(identifier)
            rows.append((identifier, {'error': item, 'native_run_evidence': markers,
                'scope': 'observed native error; cross-stream chronology and upstream cause are not inferred'}))
    return rows


def _successful_execution(status, candidates):
    return {'status': dict((key, status.get(key)) for key in
        ('state', 'message', 'total_candidates', 'completed_candidates', 'infrastructure_failed_candidates')),
        'candidates': [dict((key, item.get(key)) for key in ('candidate_id', 'status', 'values', 'source_test_map'))
                       for item in candidates]}


def _failed_execution(status, candidates):
    """Index the failure; detailed first cause and cleanup are separate records."""
    return {'status': dict((key, status.get(key)) for key in
        ('state', 'message', 'total_candidates', 'completed_candidates', 'failed_candidates',
         'infrastructure_failed_candidates')),
        'candidates': [dict((key, item.get(key)) for key in
            ('candidate_id', 'status', 'values', 'error', 'failure_class', 'failure_stage', 'source_test_map'))
            for item in candidates]}


def _schema_artifact_index(value, seen, prefix):
    """Repeat identical schema/hash evidence by index within one solver log."""
    if isinstance(value, dict):
        if value.get('same_as'):
            return value
        schema = value.get('schema_version') or value.get('schema')
        if isinstance(schema, str):
            raw = _json(value)
            if len(raw) >= 512:
                digest = hashlib.sha256(raw.encode('utf-8')).hexdigest()
                key = (schema, digest)
                if key in seen:
                    return {'same_as': seen[key], 'schema_version': schema, 'sha256': digest,
                        'scope': 'identical diagnostic structure; original evidence remains at the indexed case/path'}
                if len(seen) < 256:
                    seen[key] = prefix
        return {key: _schema_artifact_index(item, seen, prefix + '/' + str(key)) for key, item in value.items()}
    if isinstance(value, list):
        return [_schema_artifact_index(item, seen, prefix + '/' + str(index)) for index, item in enumerate(value)]
    return value


def selected_output_statistics(candidates, spec):
    selected = set(row.get('output_key') for field in ('objectives', 'constraints')
                   for row in spec.get(field) or [])
    rows = []
    for candidate in candidates:
        metrics = candidate.get('metrics') or {}
        statistics = metrics.get('selected_output_statistics') or metrics.get('output_statistics') or {}
        if isinstance(statistics, list):
            rows.extend(dict(row, candidate_id=candidate.get('candidate_id')) for row in statistics
                        if row.get('output_key') in selected or row.get('key') in selected)
        elif isinstance(statistics, dict) and statistics:
            rows.extend({'candidate_id': candidate.get('candidate_id'), 'output_key': key, 'statistics': statistics[key]}
                        for key in sorted(selected) if key in statistics)
        else:
            rows.extend({'candidate_id': candidate.get('candidate_id'), 'output_key': key, 'status': 'NOT_TESTED',
                         'reason': 'producer retained aggregation value; full selected point statistics unavailable'}
                        for key in sorted(selected))
    return rows


def _case_capability(config, prepared, simulator, engine):
    from .simulators import simulator_readiness
    cadence = config.get('cadence') or {}
    ocean = cadence.get('ocean_bin')
    environment = cadence.get('launch_environment') or {}
    resolved = shutil.which(ocean, path=environment.get('PATH', os.environ.get('PATH'))) if ocean else None
    if not resolved:
        return {'ready': False, 'reason': 'Common Ocean executable is unavailable', 'simulator': simulator}
    tests = [test for test in (prepared.get('setup') or {}).get('tests') or [] if test.get('enabled') is not False]
    capture_sources = {str(test.get('simulator') or '').strip().lower() for test in tests}
    capture_source = next(iter(capture_sources)) if len(capture_sources) == 1 else 'spectre'
    if engine == 'maestro_baseline':
        if capture_sources != {simulator}:
            return {'ready': False, 'simulator': simulator, 'failure_stage': 'native_simulator_identity',
                    'reason': 'Baseline uses the simulator saved in the selected Maestro Test; select that simulator without a CLI override',
                    'source_simulators': sorted(capture_sources)}
        return {'ready': True, 'simulator': simulator, 'execution_backend': engine,
                'message': 'Native Ocean entry available; Maestro Job Setup, Run and fresh result evidence are checked during the baseline candidate',
                'scope': 'native entry only; no CLI solver/model remapping preflight'}
    if capture_sources != {'spectre'}:
        return {'ready': False, 'simulator': simulator, 'capture_sources': sorted(capture_sources),
                'failure_stage': 'netlist_generation_preflight',
                'reason': 'Only saved Spectre-source native netlist capture is qualified; ALPS-source capture integration must be verified before it can safely intercept every solver invocation'}
    return simulator_readiness(config, {'simulator': simulator}, capture_source_simulator=capture_source)


def _causal_task(task):
    """Bounded producer metadata, never local-env guesses about compute nodes."""
    result = {key: task.get(key) for key in ('test', 'artifact_test', 'corner', 'point_id', 'state', 'job_id', 'scheduler_job_id',
        'simulator', 'returncode', 'submission_returncode', 'measurement_returncode', 'cwd',
        'compute_hostname', 'compute_uid', 'solver_started', 'execution_simulator', 'task_id') if task.get(key) is not None}
    for field in ('solver_command_plan', 'submission_command_plan'):
        value = task.get(field)
        if isinstance(value, dict):
            result[field] = {key: value.get(key) for key in ('schema_version', 'layer', 'argv', 'cwd',
                'template', 'fingerprint', 'remote', 'resource_cpu') if value.get(key) is not None}
    if isinstance(task.get('scheduler_observation'), dict):
        result['scheduler_observation'] = task['scheduler_observation']
    artifacts = task.get('solver_artifacts')
    if isinstance(artifacts, dict):
        # The full bounded excerpts are in the final pipeline snapshot once;
        # progress carries file/Job ID bindings without repeating log text.
        result['solver_artifacts'] = {key: artifacts.get(key) for key in
            ('schema_version', 'task_id', 'scheduler_job_id', 'execution_simulator')}
        result['solver_artifacts']['artifacts'] = [{key: artifact.get(key) for key in
            ('kind', 'path', 'status', 'reason', 'fresh', 'bytes', 'sha256', 'read_truncated')
            if artifact.get(key) is not None} for artifact in artifacts.get('artifacts') or []
            if isinstance(artifact, dict)]
    for field in ('submission', 'receipt', 'terminal_receipt', 'raw_terminal_receipt',
                  'started_receipt', 'compute_metadata', 'simulator_contract', 'measurement'):
        value = task.get(field)
        if not isinstance(value, dict):
            continue
        result[field] = {key: value.get(key) for key in ('state', 'returncode', 'job_id', 'cleanup_proven',
            'compute_hostname', 'compute_uid', 'cwd', 'execution_simulator', 'capture_source_simulator',
            'threads', 'allocated_cpu', 'result_reader', 'fingerprint') if value.get(key) is not None}
        executable = value.get('executable')
        if isinstance(executable, dict):
            result[field]['executable'] = {key: executable.get(key) for key in ('path', 'sha256')}
        presence = value.get('environment_presence')
        if isinstance(presence, dict):
            result[field]['environment_presence'] = {key: state for key, state in presence.items()
                if isinstance(state, bool) or state in ('set', 'empty', 'unset')}
    return result


def _report_context(row, scope=None):
    """Reuse already-captured identities; never inspect host/environment here."""
    identity = row.get('package_identity') or {}
    manifest = (identity.get('delivery_manifest') or {}).get('metadata') or {}
    package = {key: identity[key] for key in ('application_version', 'source_tree_sha256') if key in identity}
    for key in ('version', 'source_commit', 'tag'):
        if manifest.get(key) is not None:
            package[key] = manifest[key]
    setup = (row.get('prepared') or {}).get('setup') or {}
    source, locator = setup.get('source') or {}, setup.get('setup_locator') or {}
    selected = {key: source[key] for key in ('setup_db','maestro_sdb','data_sdb','active_state','setup_dir','sha256') if source.get(key) is not None}
    for name in ('library','cell','view'):
        value = source.get(name) or locator.get(name + '_name')
        if value is not None:
            selected[name] = value
    selected['active_state_path_scope'] = 'saved setup metadata; no new file probe'
    if not source.get('active_state'):
        selected['active_state_status'] = 'not_available_in_saved_metadata'
    logs = row.get('simulator_logs') or {}
    return {'test_id': row.get('test_id'), 'package': package, 'source_setup': selected,
            'execution_selection': _selection_context(setup, row.get('spec') or {}),
            'log_path': logs.get(scope if scope in SIMULATORS else row.get('simulator') or 'spectre')}


def _selection_context(setup, spec):
    """Selection evidence from inspected metadata only; no additional native work."""
    requested = spec.get('execution_selection')
    requested = requested if isinstance(requested, dict) else {}
    tests = [item for item in setup.get('tests') or [] if isinstance(item, dict)]
    selected = requested.get('tests')
    if selected is None:
        selected = [item.get('name') for item in tests if item.get('enabled', True)]
    analyses = requested.get('analyses') or {}
    if not isinstance(analyses, dict):
        analyses = {}
    summary = {'requested_tests': copy.deepcopy(selected), 'tests': []}
    from .ade_setup import test_corner_availability
    for test in tests:
        name = test.get('name')
        row = {'name': name, 'source_enabled': bool(test.get('enabled', True)),
               'selected': name in selected if isinstance(selected, list) else False,
               'requested_analyses': copy.deepcopy(analyses.get(name, [item.get('name')
                   for item in test.get('analyses') or [] if item.get('enabled', True)])),
               'available_analyses': [{'name': item.get('name'), 'enabled': item.get('enabled', True)}
                   for item in test.get('analyses') or []]}
        try:
            row['corners'] = test_corner_availability(setup, name)
        except (KeyError, TypeError, ValueError) as error:
            row['corner_summary_error'] = str(error)[:300]
        summary['tests'].append(row)
    return summary


def _metric_error_details(spec):
    """Retain the first input error for each selected row, with bounded text."""
    errors = spec.get('metric_definition_errors') or []
    if not isinstance(errors, list):
        return []
    return [{key: (value if isinstance(value, (int, bool)) else str(value)[:1024])
             for key, value in row.items() if key in ('kind', 'index', 'output_key', 'error')}
            for row in errors[:64] if isinstance(row, dict)]


def _request_gate_failure(row, reasons):
    """Describe the existing gate without changing its execution decision."""
    spec = row.get('spec') or {}
    labels = {'execution_selection_valid':'当前测试或仿真项选择为空或无效',
              'prepared_selection_valid':'当前所选模块尚未通过准备检查',
              'job_settings_valid':'仿真作业设置尚未通过检查',
              'storage_settings_valid':'仿真存放目录设置尚未通过检查',
              'command_settings_valid':'求解或提交命令设置尚未通过检查',
              'objectives_valid':'已选优化目标定义无效',
              'constraints_valid':'已选约束定义无效'}
    def text(value, limit):
        value = str(value)
        if len(value) <= limit:
            return value
        return {'text':value[:limit], 'characters':len(value), 'truncated':True,
                'omitted_characters':len(value)-limit,
                'sha256':hashlib.sha256(value.encode('utf-8')).hexdigest()}
    drafts = spec.get('ui_metric_drafts') or {}
    drafts = drafts if isinstance(drafts, dict) else {}
    warning_values = spec.get('ui_input_warnings') or []
    if not isinstance(warning_values,list):
        warning_values = [warning_values]
    warnings = [text(value,512) for value in warning_values[:8]]
    state = spec.get('ui_prepared_state') or {}
    state = state if isinstance(state,dict) else {}
    safe_state = {}
    for key in ('prepared_id','inspection_id','cell_scope_dirty','eda_settings_requires_revalidation',
                'pending_restore_same_setup','prepared_selection_matches','error'):
        if key not in state:
            continue
        value = state[key]
        safe_state[key] = value if value is None or isinstance(value,(bool,int,float)) else text(value,1024 if key == 'error' else 256)
    actions = []
    if 'execution_selection_valid' in reasons:
        actions.append('在 Objectives & Constraints 选择至少一个 Test，并为所选 Test 选择至少一个已有仿真项；诊断不会改回原 Maestro 勾选。')
    if 'prepared_selection_valid' in reasons or not row.get('prepared'):
        if state.get('eda_settings_requires_revalidation') or state.get('pending_restore_same_setup'):
            actions.append('当前 TB 的环境设置正在自动重新校验，请等待完成；原有编辑应保留，无需重填。若校验已结束仍阻断，请反馈本小节。')
        elif state.get('cell_scope_dirty'):
            actions.append('所选模块已变化，请 Apply 当前勾选模块；原有编辑应保留。')
        elif (not row.get('prepared') or not state.get('prepared_id') or
              state.get('prepared_selection_matches') is False):
            actions.append('当前设置尚未准备完成或路径不匹配，请对当前 TB 执行 Load & Inspect；原有编辑应保留。')
        else:
            actions.append('准备状态尚未通过检查，请保留当前编辑并反馈本小节中的原始原因。')
    if 'job_settings_valid' in reasons:
        actions.append('检查 Maestro Job Setup 中的提交方式、资源和时间设置。')
    if 'storage_settings_valid' in reasons or row.get('storage_error') or row.get('write_error'):
        actions.append('检查当前仿真存放目录及本轮报告中保留的目录错误。')
    if 'command_settings_valid' in reasons:
        actions.append('检查求解与集群提交命令模板中的原始输入错误。')
    if not spec.get('variables'):
        actions.append('选择至少一个可优化参数并填写合法范围。')
    if any(spec.get(key) is False for key in ('objectives_valid', 'constraints_valid')):
        actions.append('保留当前目标与约束选择，检查下列具体行错误；Corner 不可用时重新 Apply 当前模块以回读原生 Test/Corner 掩码，再核对原勾选。')
    elif not spec.get('objectives'):
        actions.append('选择至少一个优化目标。')
    metric_errors = _metric_error_details(spec)
    message = '开始仿真前的准备检查未通过；尚未生成网表或启动求解。'
    if metric_errors:
        first = metric_errors[0]
        message = '{} {}: {}'.format('目标' if first.get('kind') == 'objective' else '约束', first.get('index', ''), first.get('error', '定义无效'))[:400]
    return {'failure_stage':'diagnostic_request',
            'message':message,
            'blocking_reasons':list(reasons),
            'blocking_reason_labels':[labels.get(reason,reason) for reason in reasons],
            'input_flags':{key:spec.get(key) for key in ('execution_selection_valid','prepared_selection_valid','job_settings_valid',
                'storage_settings_valid','command_settings_valid','timeout_settings_valid','objectives_valid','constraints_valid')},
            'prepared_registration':{'requested_prepared_id':text(row.get('prepared_id'),256) if row.get('prepared_id') else None,
                                     'prepared_found':bool(row.get('prepared'))},
            'ui_prepared_state':safe_state,'ui_input_warnings':warnings,
            'metric_definition_errors': _metric_error_details(spec),
            'metric_draft_counts': {key: len(value) for key, value in drafts.items() if key in ('objectives', 'constraints') and isinstance(value, list)},
            'warning_scope':{'total_count':len(warning_values),'retained_count':len(warnings),
                'omitted_warning_count':max(0,len(warning_values)-len(warnings)),
                'truncated_warning_count':sum(isinstance(value,dict) for value in warnings),
                'max_warnings':8,'max_characters_per_warning':512},
            'recovery_actions':actions,'execution_started':False}


def _request_blocking_reasons(row, engine=None):
    flags = ('execution_selection_valid', 'prepared_selection_valid', 'storage_settings_valid', 'job_settings_valid', 'objectives_valid', 'constraints_valid')
    if engine != 'maestro_baseline':
        flags += ('command_settings_valid',)
    reasons = [key for key in flags
               if row['spec'].get(key) is False]
    if not row['prepared']:
        reasons.append('Load/Inspect and Apply selected cells first')
    if not row['spec'].get('variables'):
        reasons.append('Select at least one parameter with a legal range')
    if not row['spec'].get('objectives') and row['spec'].get('objectives_valid') is not False:
        reasons.append('Select at least one named output objective')
    if row['storage_error'] or row['write_error']:
        reasons.append('Requested storage could not be used')
    if row.get('candidate_error'):
        reasons.append('Frozen candidate invalid: ' + row['candidate_error'])
    return reasons


def _freeze_diagnostic_candidate(spec, prepared):
    """Prefer provably legal design values; sample each remaining value only once."""
    from .optimizers import PopulationGenerator, validate_candidate_values
    variables = spec.get('variables') or []
    sampled = PopulationGenerator(variables, 'random', 1, parameters={'population': 1, 'seed': 1}).ask(1)[0]
    parameters = {item.get('parameter_id'): item for item in prepared.get('parameters') or [] if isinstance(item, dict)}
    candidate, origin = {}, {}
    for variable in variables:
        key = variable['parameter_id']
        parameter = parameters.get(key) or {}
        value = parameter.get('numeric_value')
        if value is None:
            value = parameter.get('current_value')
        try:
            if value is None:
                raise ValueError('design current value unavailable')
            chosen = validate_candidate_values({key: value}, [variable])[key]
            origin[key] = {'source': 'design_current_value', 'reason': 'current value validated against selected range and declared grid/choices'}
        except (ValueError, TypeError, KeyError) as error:
            chosen = sampled[key]
            origin[key] = {'source': 'seed1_sample', 'reason': str(error)}
        candidate[key] = chosen
    return candidate, origin


def _simulator_completion_matches(proof, simulator):
    proof = proof or {}
    contract = proof.get('simulator_contract')
    contract = contract if isinstance(contract, dict) else {}
    values = [proof.get('simulator'), proof.get('execution_simulator'),
              contract.get('execution_simulator')]
    values = [str(value).lower() for value in values if value]
    matched = (not values and simulator == 'spectre') or bool(values and set(values) == {simulator})
    if simulator == 'alps':
        matched = (matched and proof.get('execution_simulator') == 'alps'
                   and proof.get('native_parameter_readback_verified') is True
                   and isinstance(proof.get('simulator_runs'), list) and bool(proof['simulator_runs'])
                   and proof.get('spectre_runs') == [])
    return matched


def _corner_source_untrusted(cause):
    """Only a proven private-clone-only mismatch permits another stage."""
    if not isinstance(cause, dict) or cause.get('failure_stage') != 'native_corner_setup_identity':
        return False
    detail = cause.get('corner_identity') or {}
    differences = detail.get('differences') if isinstance(detail, dict) else None
    return not (isinstance(differences, list) and differences and
                all(isinstance(item, dict) and item.get('reason') == 'private_setup_clone_changed_semantics'
                    for item in differences))


class EnvironmentTestManager(object):
    def __init__(self, service):
        self.service = service
        self.lock = threading.RLock()
        self.tests = {}

    def _timing(self, observer, method='snapshot', *args):
        try:
            return getattr(observer, method)(*args)
        except Exception:
            return {'available': False, 'status': 'unavailable', 'total_seconds': None,
                    'phases_seconds': None, 'active_phase': None} if method == 'snapshot' else None

    def start(self, prepared_id, spec):
        spec = copy.deepcopy(spec or {})
        with self.service.lock:
            if getattr(self.service, 'closing', False):
                raise RuntimeError('optimizer is closing')
            prepared = copy.deepcopy(self.service.prepared.get(prepared_id) or {})
            config = copy.deepcopy(self.service.config)
        with self.lock:
            if any(row['thread'].is_alive() or any(thread.is_alive() for thread in row['aux_threads']) for row in self.tests.values()):
                raise RuntimeError('an environment test is already active')
            identifier = 'EnvTest_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '_' + uuid.uuid4().hex[:6]
            storage_error = None
            try:
                frozen = self.service._freeze_run_config(spec, prepared)
                base = frozen['runtime']['run_root']
            except Exception as error:
                storage_error = _error(error)
                frozen = config
                from .runtime_layout import project_directory
                base = os.path.join(project_directory(config, (prepared.get('setup') or {}).get('source')),
                                    '.crescendo', 'environment_tests')
            root = os.path.join(base, '_environment_tests', identifier)
            from .simulators import selected_simulator
            selected = selected_simulator(frozen, spec)
            requested_engine = spec.get('execution_backend') or 'maestro_netlist'
            selected_engine = (requested_engine if requested_engine in ('private_netlist', 'maestro_baseline') else
                               'maestro_netlist' if requested_engine in
                               ('auto', 'maestro_netlist', 'persistent_maestro', 'legacy', 'adexl') else None)
            if selected_engine is None:
                raise ValueError('Unknown diagnostic netlist source: ' + str(requested_engine))
            spec['execution_backend'], spec['simulator'] = selected_engine, selected
            setup = prepared.get('setup') or {}
            try:
                from .test_selection import selection_plan, effective_setup
                effective = effective_setup(setup, selection_plan(setup, spec)) if spec.get('execution_selection') is not None else setup
                sources = sorted(set(str(t.get('simulator') or '').strip().lower() for t in effective.get('tests') or [] if t.get('enabled') is not False))
            except Exception:
                sources = []
            source_simulator = sources[0] if len(sources) == 1 and sources[0] in SIMULATORS else None
            candidate_error, candidate, candidate_origin = None, None, {}
            frozen_plan, shape = None, None
            timer = DiagnosticTiming()
            self._timing(timer, 'start', 'request')
            try:
                candidate, candidate_origin = _freeze_diagnostic_candidate(spec, prepared)
                frozen_plan, shape = freeze_plan(spec, candidate)
            except Exception as error:
                candidate_error = _error(error)
            main_log = os.path.join(root, 'environment_test.log')
            logs = {simulator: main_log for simulator in SIMULATORS}
            legacy_logs = {engine: logs[selected] for engine in ENGINES}
            legacy_logs.update({old: logs[selected] for old in LEGACY_ENGINES})
            write_error = None
            try:
                os.makedirs(root, mode=0o700)
                with open(main_log, 'x', encoding='utf-8'):
                    pass
            except Exception as error:
                write_error = _error(error)
            row = {'test_id': identifier, 'state': 'queued', 'report_root': root,
                   'ui_progress': {'revision': 0, 'checks_revision': 0, 'stage': 'queued', 'message': 'Waiting to start diagnostics',
                                   'gate_progress': {'scope': identifier, 'passed': 0, 'total': 3, 'current': 'Waiting to start diagnostics'}},
                   'main_log': logs['spectre'], 'engine_logs': legacy_logs, 'simulator_logs': logs,
                   'selected_simulator': selected, 'selected_engine': selected_engine,
                   'case_results': collections.OrderedDict(
                       (simulator + '.' + engine, {'case_id': simulator + '.' + engine, 'simulator': simulator, 'simulator_identity_known': engine != 'maestro_baseline' or source_simulator is not None, 'engine': engine, 'state': 'queued'})
                       for simulator, engine in [(source_simulator or selected, 'maestro_baseline'), (selected, 'private_netlist'), (selected, 'maestro_netlist')]),
                   'physical_log_tails': {main_log: ''},
                   'checks': collections.OrderedDict(), 'run_ids': {}, 'active_runs': [],
                   'log_bytes': {main_log: 0}, 'queued_log_bytes': {main_log: 0}, 'log_limited': set(),
                   'report_state': {'write_error': write_error}, 'report_ready': False,
                   'scheduler_queue': queue.Queue(maxsize=8), 'scheduler_capabilities': {}, 'scheduler_signatures': {}, 'scheduler_cancel': threading.Event(),
                   'log_queue': queue.Queue(maxsize=128), 'aux_threads': [],
                   'cancel': threading.Event(), 'prepared': prepared, 'prepared_id': prepared_id,
                   'config': frozen, 'spec': spec, 'storage_error': storage_error,
                   'source_simulator': source_simulator, 'source_simulators': sources,
                   'frozen_candidate': candidate, 'frozen_candidate_origin': candidate_origin, 'candidate_error': candidate_error,
                   'frozen_plan': frozen_plan, 'diagnostic_shape': shape, 'timing_observer': timer, 'case_timing_observers': {},
                   'record_signatures': set(), 'engine_outcomes': {},
                   'simulator_capabilities': {}, 'shared_schema_artifacts': {},
                   'case_contexts': {}, 'case_records': {}, 'case_record_bytes': {},
                   'write_error': write_error, 'started_at': _utc(), 'finished_at': None}
            thread = threading.Thread(target=self._run, args=(row,), name=identifier, daemon=True)
            row['thread'] = thread
            writer = threading.Thread(target=self._log_writer, args=(row,), name=identifier + '-report-writer', daemon=True)
            row['aux_threads'].append(writer)
            self.tests[identifier] = row
            writer.start()
            thread.start()
            return self.progress(identifier)

    def run_root(self, marker, engine):
        """Only a manager-owned active marker may redirect diagnostic run storage."""
        if not isinstance(marker, dict) or marker.get('schema_version') != 'crescendo.environment_test.run.v1':
            raise ValueError('invalid environment-test marker')
        with self.lock:
            row = self.tests.get(marker.get('test_id'))
            if row is None or row['cancel'].is_set() or row['state'] not in ('queued', 'running'):
                raise RuntimeError('environment test is unavailable or cancelled')
            if row['storage_error'] or row['write_error']:
                raise RuntimeError('environment test storage is unavailable')
            engine = LEGACY_ENGINES.get(engine, engine)
            if engine not in ENGINES:
                raise ValueError('invalid environment-test engine')
            simulator = marker.get('simulator', row['selected_simulator'])
            if simulator not in SIMULATORS:
                raise ValueError('invalid environment-test simulator')
            if marker.get('frozen_candidate') != row.get('frozen_candidate'):
                raise ValueError('diagnostic marker candidate differs from frozen candidate')
            if marker.get('frozen_plan') != row.get('frozen_plan'):
                raise ValueError('diagnostic marker plan differs from frozen plan')
            if simulator + '.' + engine != row.get('current_case'):
                raise ValueError('diagnostic marker does not match active stage')
            if simulator + '.' + engine not in row['case_results']:
                raise ValueError('netlist source/simulator was not selected for this diagnostic')
            return self._case_directory(row, simulator, engine)

    @staticmethod
    def _case_directory(row, simulator, engine):
        # Compatibility report aliases share one file; only the selected root runs.
        return os.path.join(row['report_root'], SIMULATOR_LABELS[simulator], LABELS[engine])

    def progress(self, identifier):
        with self.lock:
            row = self.tests[identifier]
            checks = copy.deepcopy(list(row['checks'].values()))
            contexts = copy.deepcopy(row.get('case_contexts') or {})
            for check in checks:
                case_id = ((check.get('simulator') + '.' + check.get('engine'))
                           if check.get('simulator') and check.get('engine') in ENGINES else check.get('simulator') or 'environment')
                check['presentation'] = describe_check(check, contexts.get(case_id))
            return {
                'test_id': row['test_id'], 'state': row['state'], 'report_root': row['report_root'],
                'revision': (row.get('ui_progress') or {}).get('revision', 0),
                'checks_revision': (row.get('ui_progress') or {}).get('checks_revision', 0),
                'stage': (row.get('ui_progress') or {}).get('stage', row['state']),
                'message': (row.get('ui_progress') or {}).get('message', ''),
                'gate_progress': copy.deepcopy((row.get('ui_progress') or {}).get('gate_progress')),
                'main_log': row['main_log'], 'engine_logs': dict(row['engine_logs']),
                'report_ready': row.get('report_ready', False), 'report_write_error': row.get('write_error'),
                'simulator_logs': dict(row['simulator_logs']),
                'simulator_log_tails': {simulator: row['physical_log_tails'][row['main_log']] for simulator in SIMULATORS},
                'engine_log_tails': {engine: row['physical_log_tails'][row['main_log']] for engine in tuple(ENGINES) + tuple(LEGACY_ENGINES)},
                'log_tail': row['physical_log_tails'][row['main_log']],
                'report_log_count': 1, 'physical_log_bytes': dict(row['log_bytes']),
                'pending_log_bytes': dict(row['queued_log_bytes']),
                'diagnostic_shape': copy.deepcopy(row.get('diagnostic_shape')),
                'timing': self._timing(row['timing_observer']),
                'case_results': [dict(copy.deepcopy(case), diagnostic_shape=copy.deepcopy(row.get('diagnostic_shape')),
                    timing=self._timing(row['case_timing_observers'][case_id]) if case_id in row['case_timing_observers'] else None)
                    for case_id, case in row['case_results'].items()],
                'simulator_capabilities': {key: {'ready': value['capability'].get('ready'),
                    'check_id': value['check_id'], 'sha256': value['sha256']}
                    for key, value in row.get('simulator_capabilities', {}).items()},
                'run_ids': dict(row['run_ids']),
                'case_run_ids': {key: row['run_ids'].get(key) for key in row['case_results']},
                'active_runs': list(row['active_runs']),
                'diagnosis_schema_version': DIAGNOSIS_SCHEMA, 'case_contexts': contexts,
                'checks': checks, 'summary': dict(collections.Counter(check['status'] for check in checks)),
                'started_at': row['started_at'], 'finished_at': row['finished_at']}

    def cancel_all(self):
        """Signal before parent Exit waits; it already sends STOP to owned runs."""
        with self.lock:
            for row in self.tests.values():
                if row['thread'].is_alive() or any(thread.is_alive() for thread in row['aux_threads']):
                    row['cancel'].set()

    def join(self, timeout):
        deadline = time.monotonic() + max(0, timeout)
        with self.lock:
            threads = [thread for row in self.tests.values() for thread in [row['thread']] + row['aux_threads']]
        for thread in threads:
            thread.join(max(0, deadline - time.monotonic()))
        with self.lock:
            current = [thread for row in self.tests.values() for thread in [row['thread']] + row['aux_threads']]
        for thread in current:
            if thread not in threads:
                thread.join(max(0, deadline - time.monotonic()))
        return not any(thread.is_alive() for thread in current)

    def stop(self, identifier):
        with self.lock:
            row = self.tests[identifier]
            row['cancel'].set()
            active = list(row['active_runs'])
        for name in active:
            try:
                self.service.stop(name)
            except Exception as error:
                self._check(row, 'cancel.' + name, 'WARN', _error(error), engine=None)
        return self.progress(identifier)

    def _redaction_environment(self, row):
        environment = dict(os.environ)
        cadence = row['config'].get('cadence') or {}
        snapshot = cadence.get('launch_environment')
        if isinstance(snapshot, dict):
            environment.update(snapshot)
        if cadence.get('license_file'):
            environment['CDS_LIC_FILE'] = cadence['license_file']
        return environment

    def _log(self, row, record, engine=None):
        record = _redact(record, self._redaction_environment(row))
        if record.get('observation') == 'execution.progress':
            payload = record.get('payload') or {}
            from .ui_gates import ladder_gate_progress, gate_progress
            with self.lock:
                progress = row.setdefault('ui_progress', {})
                native = gate_progress(payload.get('gate_progress')) or {}
                progress['gate_progress'] = ladder_gate_progress(row.get('case_results'),
                    str(row.get('test_id') or 'diagnostics'), native.get('current') or payload.get('message') or '')
                progress.update(stage=payload.get('execution_stage') or payload.get('state') or 'running',
                    message=str(payload.get('message') or '')[:400], revision=progress.get('revision', 0) + 1)
        # Raw observations remain owned evidence, but are emitted only in their
        # final problem section. Successful phases are summarized once.
        if record.get('observation') and 'case_records' in row:
            scope = row.get('case_id') or row.get('simulator') or 'environment'
            encoded = _json(record).encode('utf-8')
            size = len(encoded)
            signature = ('observation', scope, hashlib.sha256(encoded).hexdigest())
            with self.lock:
                if signature in row.setdefault('record_signatures', set()):
                    return set()
                used = row['case_record_bytes'].get(scope, 0)
                critical = record.get('observation') in ('diagnostic.producer_failure', 'diagnostic.first_cause',
                    'diagnostic.native_worker_diagnostics', 'diagnostic.collection')
                capacity = (8 if critical else 6) * 1024 * 1024
                if used + size <= capacity:
                    row['case_records'].setdefault(scope, []).append(record)
                    row['case_record_bytes'][scope] = used + size
                    row['record_signatures'].add(signature)
                else:
                    context = row.setdefault('case_contexts', {}).setdefault(scope, {})
                    context['report_omissions'] = 'Observation cache capacity reached; ordinary details limited, first-cause reserve and original owned files retained.'
                    if scope not in row['log_limited']:
                        row['log_limited'].add(scope)
                        self._check(row, 'log.capacity', 'WARN', context['report_omissions'], engine=engine)
            return {row.get('simulator', row.get('selected_simulator', 'spectre'))}
        if record.get('status') and record.get('id'):
            if record['status'] == 'PASS':
                # All successful checks are persisted once by functional category
                # at the end; progress/UI still receives the full check now.
                return set()
            if (record['status'] == 'NOT_TESTED' and
                    row.get('config', {}).get('runtime', {}).get('diagnostic_log_detail') != 'full'):
                # UI receives each check; the final checklist preserves every
                # untested ID without repeating a cascade in the live log.
                return set()
            presentation = record.get('presentation') or {}
            if presentation.get('summary'):
                record = dict(record, message=presentation['summary'])
            record = {key: record.get(key) for key in ('id', 'status', 'message', 'time')}
            if presentation.get('diagnostic_code'):
                record['diagnostic_code'] = presentation['diagnostic_code']
            record['scope'] = row.get('case_id') or row.get('simulator') or 'environment'
        limit = 196608 if str(record.get('observation') or '').startswith('diagnostic.') else 65536
        raw = _utc() + ' ' + _json(_small(record, limit)) + '\n'
        with self.lock:
            signature_record = dict(record)
            signature_record.pop('time', None)
            signature = (row['main_log'], _json(signature_record))
            if signature in row.get('record_signatures', set()):
                return set()
            row.setdefault('record_signatures', set()).add(signature)
            if record.get('observation') and row['log_bytes'][row['main_log']] + row['queued_log_bytes'][row['main_log']] >= 8 * 1024 * 1024:
                if row['main_log'] not in row['log_limited']:
                    row['log_limited'].add(row['main_log'])
                    self._check(row, 'log.capacity', 'WARN', 'Ordinary log sampling reached 8 MiB; core checks/errors still recorded and native evidence retained', engine=engine)
                return set()
            if not self._append_report(row, raw):
                return set()
            return {row.get('simulator', row.get('selected_simulator', 'spectre'))}

    def _append_report(self, row, raw, wait=False):
        # Reserve one physical queue entry. Waiting happens only in the owned
        # final drain thread, outside all control/progress locks.
        path, size = row['main_log'], len(raw.encode('utf-8'))
        with self.lock:
            if (row.get('report_state') or {}).get('write_error') or row['write_error']:
                return False
            row['queued_log_bytes'][path] += size
        try:
            if wait:
                row['log_queue'].put((path, raw))
            else:
                row['log_queue'].put_nowait((path, raw))
        except queue.Full:
            with self.lock:
                row['queued_log_bytes'][path] -= size
                row['checks']['report.queue_capacity'] = {'id': 'report.queue_capacity', 'status': 'WARN',
                    'category': 'storage', 'message': 'Report filesystem is slow; memory progress continues, some log observations were not persisted'}
            return False
        return True

    def _log_writer(self, row):
        # Only this writer changes the physical byte count and tail, so two
        # producers cannot reverse tail order relative to the actual file.
        while True:
            item = row['log_queue'].get()
            try:
                if item is None:
                    path = row['main_log']
                    raw = 'diagnostic_timing ' + _json({
                        'scope': 'observed through report drain; excludes this timing tail write',
                        'timing': self._timing(row.get('timing_observer')),
                        'diagnostic_shape': row.get('diagnostic_shape'),
                        'cases': {key: self._timing(timer) for key, timer in row.get('case_timing_observers', {}).items()}}) + '\n'
                else:
                    path, raw = item
                if row['write_error']:
                    continue
                if path != row['main_log']:
                    raise RuntimeError('diagnostic report queue refers to another file')
                fd = os.open(path, os.O_WRONLY | os.O_APPEND | getattr(os, 'O_NOFOLLOW', 0))
                with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                    info = os.fstat(stream.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        raise RuntimeError('diagnostic log has unsafe links')
                    stream.write(raw)
                with self.lock:
                    row['log_bytes'][path] += len(raw.encode('utf-8'))
                    row['physical_log_tails'][path] = (row['physical_log_tails'][path] + raw)[-LOG_TAIL:]
            except Exception as error:
                with self.lock:
                    row['write_error'] = _error(error)
                    row['report_state']['write_error'] = row['write_error']
                    row['checks']['report.write'] = {'id': 'report.write', 'status': 'FAIL',
                        'category': 'storage', 'message': row['write_error']}
            finally:
                if item is not None:
                    with self.lock:
                        row['queued_log_bytes'][row['main_log']] -= len(item[1].encode('utf-8'))
                row['log_queue'].task_done()
                if item is None:
                    self._timing(row.get('timing_observer'), 'finish', row.get('state', 'failed'))
                    with self.lock:
                        row['report_ready'] = not bool(row['write_error'])
                    return

    def _check(self, row, identifier, status, message, details=None, engine=None, candidate_id=None):
        prefix = ((row.get('case_id') + '.') if row.get('case_id') else (engine + '.' if engine else ''))
        native_parameters = (isinstance(details, dict) and
                             details.get('proof_kind') == 'native_run_device_parameters')
        check = {'id': prefix + (str(candidate_id) + '.' if candidate_id else '') + identifier, 'candidate_id': candidate_id, 'simulator': row.get('simulator'), 'engine': engine, 'check_id': identifier,
                 'name': identifier.replace('.', ' / '), 'category': LABELS.get(engine, identifier.split('.')[0]),
                 'status': status, 'message': message,
                 'details': details if native_parameters else _small(details or {}), 'time': _utc()}
        check = _redact(check, self._redaction_environment(row))
        scope = row.get('case_id') or row.get('simulator') or 'environment'
        check['presentation'] = describe_check(check, (row.get('case_contexts') or {}).get(scope))
        with self.lock:
            row['checks'][check['id']] = check
            progress = row.setdefault('ui_progress', {})
            from .ui_gates import ladder_gate_progress
            progress['gate_progress'] = ladder_gate_progress(row.get('case_results'),
                str(row.get('test_id') or 'diagnostics'), message)
            progress.update(revision=progress.get('revision', 0) + 1,
                checks_revision=progress.get('checks_revision', 0) + 1,
                stage=identifier, message=str(check.get('message') or '')[:400])
        self._log(row, check, engine)
        return check

    def _shared_case_capability(self, row, config, simulator, engine):
        """Static readiness once per fresh diagnostic, never a task contract."""
        cache = row.setdefault('simulator_capabilities', {})
        cache_key = engine + '.' + simulator
        if cache_key not in cache:
            prepared = row['prepared']
            if row.get('spec', {}).get('execution_selection') is not None:
                from .test_selection import selection_plan, effective_setup
                setup = prepared.get('setup') or {}
                prepared = dict(prepared, setup=effective_setup(setup, selection_plan(setup, row['spec'])))
            capability = _case_capability(config, prepared, simulator, engine)
            digest = hashlib.sha256(_json(capability).encode('utf-8')).hexdigest()
            identifier = cache_key + '.simulator.readiness'
            shared = dict(row, simulator=simulator, case_id=None)
            self._check(shared, identifier, 'PASS' if capability.get('ready') else 'BLOCKED',
                capability.get('message', 'Executable available; exact native model/section points, reader and candidate results still require qualification')
                if capability.get('ready') else capability.get('reason', 'Simulator unavailable'),
                dict(capability, evidence_scope='this fresh environment test; original setup unchanged; task contracts still verified per point',
                     capability_sha256=digest))
            with self.lock:
                cache[cache_key] = {'capability': copy.deepcopy(capability), 'check_id': identifier, 'sha256': digest}
        entry = cache[cache_key]
        return copy.deepcopy(entry['capability']), {'check_id': entry['check_id'], 'sha256': entry['sha256'],
            'scope': 'same diagnostic static capability; does not replace netlist generation, point or result proof'}

    def _run(self, row):
        with self.lock:
            row['state'] = 'running'
            self._timing(row['timing_observer'], 'transition', 'environment')
        self._append_report(row, 'diagnostic_start ' + _json({
            'test_id': row['test_id'], 'simulator': row['selected_simulator'],
            'netlist_source': row['selected_engine'], 'scope': 'three ordered diagnostic stages'}) + '\n')
        row['package_identity'] = package_identity()
        with self.lock:
            for scope in ('environment',) + SIMULATORS:
                row['case_contexts'][scope] = _report_context(row, scope)
        self._check(row, 'test.scope', 'PASS' if row.get('frozen_candidate') is not None else 'WARN',
                    'Three ordered stages share a frozen multi-generation candidate plan and selected tests/analyses/corners',
                    {'test_id': row['test_id'], 'selected_simulator': row['selected_simulator'],
                     'selected_engine': row['selected_engine'], 'execution_protocol': ('crescendo.maestro_baseline.v1' if row['selected_engine'] == 'maestro_baseline' else EXECUTION_PROTOCOL),
                     'gui_runtime': row['spec'].get('gui_runtime'),
                     'ui_input_warnings': row['spec'].get('ui_input_warnings'),
                     'source_simulator': row.get('source_simulator'), 'cli_simulator': row['selected_simulator'],
                     'numeric_equivalence': 'not_comparable_across_solvers' if row.get('source_simulator') != row['selected_simulator'] else 'requires_result_evidence',
                     'diagnostic_shape': row.get('diagnostic_shape'), 'candidate_source': 'first candidate uses validated original values where available; remaining legal seed-1 samples are frozen once for all stages', 'cannot_certify_parallel_throughput': True})
        if row['storage_error'] or row['write_error']:
            self._check(row, 'storage.requested', 'FAIL', 'Requested storage unavailable; native tests will not be redirected',
                        {'error': row['storage_error'] or row['write_error'], 'report_fallback': row['report_root']})
        def emit(check):
            if check.get('id') == 'runtime.summary' and check.get('status') == 'FAIL':
                check = dict(check, status='WARN', details=dict(check.get('details') or {}, observed_status='FAIL'))
            shared = dict(row, simulator=row['selected_simulator'])
            self._check(shared, check['id'], check['status'], check['message'], check.get('details'))
        try:
            # Current entry is a bounded in-memory session summary, not the
            # expanded tool and filesystem probe battery.
            run_environment_probes(row['config'], row['prepared'], row['report_root'], emit=emit,
                                   cancel=row['cancel'], spec=dict(row['spec'], _diagnostic_package_identity=row['package_identity']))
        except Exception as error:
            self._check(row, 'environment.probes', 'WARN', _error(error))
        def scheduler_worker():
            while True:
                task = row['scheduler_queue'].get()
                try:
                    if task is None:
                        return
                    self._scheduler_query(task[0], task[1], task[2])
                finally:
                    row['scheduler_queue'].task_done()
        scheduler_thread = threading.Thread(target=scheduler_worker, name=row['test_id'] + '-scheduler', daemon=True)
        row['aux_threads'].append(scheduler_thread)
        scheduler_thread.start()
        clean = True
        try:
            common_block = None
            for selected_case in list(row['case_results'].values()):
                simulator, engine = selected_case['simulator'], selected_case['engine']
                case_id = simulator + '.' + engine
                config = copy.deepcopy(row['config'])
                config.setdefault('cadence', {})['simulator'] = simulator
                case = dict(row, config=config, spec=dict(row['spec'], simulator=simulator),
                            simulator=simulator, case_id=case_id, engine_outcomes={})
                reasons = _request_blocking_reasons(row, engine)
                with self.lock:
                    self._timing(row['timing_observer'], 'transition', case_id)
                    case_timer = DiagnosticTiming()
                    self._timing(case_timer, 'start', 'readiness')
                    row['case_timing_observers'][case_id] = case_timer
                    case['case_timing_observer'] = case_timer
                    row['current_case'] = case_id
                    row['case_results'][case_id]['state'] = 'running'
                if row['cancel'].is_set():
                    self._check(case, 'execution', 'CANCELLED', 'Test cancelled before this case started', engine=engine)
                elif common_block:
                    self._check(case, 'execution', 'BLOCKED', common_block, engine=engine)
                elif reasons:
                    cause = _redact(_request_gate_failure(case,reasons),self._redaction_environment(case))
                    case['engine_outcomes'][engine] = {'first_cause':cause}
                    self._check(case, 'execution', 'BLOCKED', cause['message'],
                                {'first_cause':cause}, engine=engine)
                elif not row.get('source_simulator'):
                    self._check(case, 'execution', 'BLOCKED', 'Selected Test source simulator identity is unavailable or inconsistent; native capture cannot be qualified', {'source_simulators': row.get('source_simulators')}, engine=engine)
                elif not clean:
                    self._check(case, 'execution', 'BLOCKED', 'Previous native Stop/cleanup unproven; no later simulator/engine starts', engine=engine)
                else:
                    try:
                        capability, capability_reference = self._shared_case_capability(row, config, simulator, engine)
                        self._check(case, 'simulator.readiness', 'PASS' if capability.get('ready') else 'BLOCKED',
                            'Shared static readiness available; fresh native point qualification still required' if capability.get('ready') else capability.get('reason', 'Simulator unavailable'), capability_reference, engine)
                        if not capability.get('ready'):
                            case['engine_outcomes'][engine] = {'first_cause': {'failure_stage': capability.get('failure_stage', 'simulator_readiness'),
                                'message': capability.get('reason', 'Simulator unavailable; no fallback')}}
                            self._check(case, 'execution', 'BLOCKED', capability.get('reason', 'Simulator unavailable; no fallback'), engine=engine)
                        else:
                            clean = self._engine(case, engine)
                    except Exception as error:
                        self._check(case, 'execution', 'FAIL', _error(error), engine=engine)
                        clean = not row['active_runs']
                        for active in list(row['active_runs']):
                            try:
                                self.service.stop(active)
                            except Exception as stop_error:
                                self._check(case, 'stop_request', 'FAIL', _error(stop_error), engine=engine)
                checks = [value for key, value in row['checks'].items() if key.startswith(case_id + '.')]
                # Optional result roundtrip failure remains visible globally,
                # but cannot rewrite the native stage execution outcome.
                required_checks = [check for check in checks if check.get('check_id') != 'statistics_and_dataset']
                execution = row['checks'].get(case_id + '.execution') or {}
                state = ('cancelled' if execution.get('status') == 'CANCELLED' else
                         'blocked' if execution.get('status') == 'BLOCKED' else
                         'failed' if (any(value['status'] == 'FAIL' for value in required_checks)
                                      or execution.get('status') != 'PASS' or clean is not True) else 'completed')
                run_id = row['run_ids'].get(case_id)
                outcome = case['engine_outcomes'].get(engine) or {}
                cause = outcome.get('first_cause')
                if not cause:
                    failed_check = next((check for check in required_checks if check.get('status') == 'FAIL'), None)
                    if failed_check:
                        check_id = failed_check.get('check_id') or ''
                        cause = {'failure_stage': ('native_cleanup' if check_id in ('termination_and_cleanup', 'termination', 'stop_request') else check_id),
                                 'message': failed_check.get('message'), 'source': 'diagnostic_check',
                                 'check_id': failed_check.get('id')}
                        outcome['first_cause'] = cause
                        case['engine_outcomes'][engine] = outcome
                if not cause and clean is not True and execution.get('status') == 'PASS':
                    cause = {'failure_stage': 'termination_and_cleanup',
                             'message': 'Stage execution returned but owned task cleanup is not proven'}
                if not cause and execution.get('status') in ('BLOCKED', 'CANCELLED', 'FAIL'):
                    cause = {'failure_stage': 'diagnostic_request', 'message': execution.get('message')}
                with self.lock:
                    context = row['case_contexts'].setdefault(case_id, _report_context(case, simulator))
                    context.update(case_id=case_id, test_id=row['test_id'], run_id=run_id,
                        log_path=row['simulator_logs'][simulator], state=state, first_cause=cause)
                    context.setdefault('netlists', [])
                    context.setdefault('artifacts', [])
                    context.setdefault('retention_status', '案例未启动，尚未生成网表' if not run_id else '保留原始证据，不自动归档删除')
                stop_required = run_id is not None or execution.get('status') == 'FAIL' and not clean
                if outcome.get('shared_source_failure'):
                    common_block = 'Shared source guard failed; later stages cannot safely use the source setup'
                if cause and _corner_source_untrusted(cause):
                    common_block = 'Corner setup source identity is stale or unproven; later stages cannot safely use it'
                if cause and cause.get('failure_stage') in ('prepared_freshness', 'source_guard', 'source_freshness'):
                    common_block = 'Shared source or prepared setup identity failed; later stages cannot safely use it: ' + str(cause.get('message') or cause.get('failure_stage'))
                with self.lock:
                    self._timing(case_timer, 'finish', state)
                    row['case_results'][case_id].update(state=state, run_id=run_id, stop_required=stop_required,
                        first_cause=cause, skip_reason=execution.get('message') if state in ('blocked', 'cancelled') else None,
                        first_cause_layer=failure_layer(cause),
                        metric_summary=outcome.get('metric_summary', []) if state == 'completed' else [],
                        stop_proven=clean if stop_required else None)
                    row['engine_outcomes'][case_id] = case['engine_outcomes'].get(engine)
                    from .ui_gates import ladder_gate_progress
                    progress = row.setdefault('ui_progress', {})
                    progress['gate_progress'] = ladder_gate_progress(row['case_results'], row['test_id'],
                        execution.get('message') or state)
                    progress['revision'] = progress.get('revision', 0) + 1
        finally:
            self._timing(row['timing_observer'], 'transition', 'finalization')
            # Native Stop/deadlines never wait behind readonly scheduler commands.
            try:
                row['scheduler_queue'].put_nowait(None)
            except queue.Full:
                row['scheduler_cancel'].set()
                while True:
                    try:
                        row['scheduler_queue'].get_nowait()
                        row['scheduler_queue'].task_done()
                    except queue.Empty:
                        break
                row['scheduler_queue'].put_nowait(None)
            scheduler_thread.join(6)
            if scheduler_thread.is_alive():
                row['scheduler_cancel'].set()
                scheduler_thread.join(3)
                self._check(row, 'scheduler.observe_deadline', 'WARN', 'Outstanding read-only scheduler observations cancelled after qualification; unknown states remain NOT_TESTED')
            with self.lock:
                row['finished_at'] = _utc()
                statuses = [check['status'] for check in row['checks'].values()]
                row['state'] = 'cancelled' if row['cancel'].is_set() else ('failed' if 'FAIL' in statuses or 'BLOCKED' in statuses else 'completed')
            self._timing(row['timing_observer'], 'transition', 'report')
            self._check(row, 'test.summary', 'PASS' if row['state'] == 'completed' and 'NOT_TESTED' not in statuses else 'WARN',
                        'Qualification finished: ' + row['state'] + ('; some checks were not tested' if 'NOT_TESTED' in statuses else ''),
                        {'counts': dict(collections.Counter(statuses)), 'run_ids': row['run_ids'], 'engine_logs': row['engine_logs'],
                         'unavailable_checks_are_not_passes': True})
            def finish_reports():
                # Lossless final checklist is queued outside all control locks.
                # This survives an earlier observation queue overflow/slow NFS.
                with self.lock:
                    checks = copy.deepcopy(list(row['checks'].values()))
                compact = [dict((key, check.get(key)) for key in ('id', 'status')) for check in checks]
                full = row.get('config', {}).get('runtime', {}).get('diagnostic_log_detail') == 'full'
                from .environment_diagnostics import concise_case_records, concise_diagnostic_value
                roots = {'@run:' + scope: context['run_root'] for scope, context in
                         row.get('case_contexts', {}).items() if isinstance(context.get('run_root'), str)
                         and os.path.isabs(context['run_root'])}
                def emit(raw):
                    self._append_report(row, raw if full else _short_report_paths(raw, roots), wait=True)
                contexts = {}
                for scope in ('environment',) + tuple(row['case_results']):
                    context = dict(row.get('case_contexts', {}).get(scope) or {})
                    context.pop('records', None)
                    context.pop('cdf_callback_failures', None)
                    context.pop('native_worker_diagnostics', None)
                    contexts[scope] = context if full else _compact_report_context(context)
                # The small verdict and completed mode timings precede every
                # detail section. The writer still emits exact drain timing last.
                overview = {'test_id': row['test_id'], 'state': row['state'],
                    'summary': dict(collections.Counter(check['status'] for check in checks)),
                    'diagnostic_shape': row.get('diagnostic_shape'),
                    'package': (contexts.get('environment') or {}).get('package'),
                    'execution_selection': (contexts.get('environment') or {}).get('execution_selection'),
                    'cases': [{key: case.get(key) for key in ('case_id', 'engine', 'state', 'run_id',
                        'stop_required', 'stop_proven', 'first_cause', 'skip_reason')} for case in row['case_results'].values()],
                    'case_timings': {key: self._timing(timer) for key, timer in row.get('case_timing_observers', {}).items()},
                    'timing_scope': 'completed mode intervals; report drain timing follows at the end'}
                if not full:
                    selection = overview['execution_selection']
                    overview = concise_diagnostic_value(overview)
                    overview['execution_selection'] = selection
                    overview['path_roots'] = roots
                    overview['detail_mode'] = 'concise; original artifacts unchanged; diagnostic_log_detail=full expands report'
                self._append_report(row, 'diagnostic_overview ' + _json(overview) + '\n', wait=True)
                sections = collections.OrderedDict()
                successes = collections.OrderedDict()
                for check in checks:
                    scope = ((check.get('simulator') + '.' + check.get('engine'))
                        if check.get('simulator') and check.get('engine') in ENGINES else check.get('simulator') or 'environment')
                    context = row.get('case_contexts', {}).get(scope) or {
                        'test_id': row['test_id'], 'log_path': row['main_log']}
                    presentation = describe_check(check, context)
                    scope = presentation['group_id']
                    if check.get('status') == 'PASS':
                        details = check.get('details') or {}
                        # Full proof stays in the run dataset and UI. Successful
                        # diagnostic reports carry a compact verification index.
                        proof = (details if details.get('proof_kind') == 'native_run_device_parameters'
                                 else brief_success(details))
                        if check.get('check_id') == 'test.scope':
                            proof.update({key: details.get(key) for key in ('frozen_candidate', 'frozen_candidate_origin',
                                'source_simulator', 'cli_simulator', 'numeric_equivalence', 'candidate_source')})
                        elif check.get('check_id') == 'execution.pipeline':
                            proof = {key: value for key, value in details.items() if key != 'points'}
                            proof['points'] = [_causal_task(point) for point in details.get('points') or []]
                            if isinstance(proof.get('netlist_generation'), dict):
                                proof['netlist_generation'] = {key: value for key, value in
                                    proof['netlist_generation'].items() if key != 'parameter_application'}
                                proof['netlist_generation']['parameter_application_ref'] = 'parameter_application'
                        elif check.get('check_id') in ('metrics', 'statistics_and_dataset'):
                            proof = details
                        from .diagnosis_catalog import concise_multi_success
                        proof = concise_multi_success(check.get('check_id'), proof)
                        if not full:
                            proof = _compact_success_proof(proof)
                        successes.setdefault(presentation['section_id'], {
                            'category': presentation['section_label'], 'checks': []})['checks'].append(
                                dict({'id': check['id'], 'proof': proof},
                                     **({'title': presentation['title']} if full else {})))
                    else:
                        sections.setdefault((scope, presentation['section_id']), []).append(check)
                report_records = {}
                for scope, records in row.get('case_records', {}).items():
                    report_records[scope] = concise_case_records(records,
                        row['case_results'].get(scope) or {},
                        [check for check in checks if str(check.get('id', '')).startswith(scope + '.')],
                        full=full)
                emit(format_ladder_summary(row['case_results'], row.get('source_simulator'),
                    row.get('selected_simulator'), row.get('frozen_candidate'), row.get('frozen_candidate_origin'),
                    row.get('case_contexts')))
                for scope in ('environment',) + tuple(row['case_results']):
                    context = contexts[scope]
                    # Keep this section's absolute mapping for standalone Copy.
                    shown = context if full else json.loads(_short_report_paths(_json(context), roots))
                    if not full and context.get('run_root'):
                        shown.update(run_root=context['run_root'], path_alias='@run:' + scope)
                        shown['path_roots'] = roots
                    self._append_report(row, 'run_context ' + _json({'scope': scope, 'context': shown}) + '\n', wait=True)
                priority = {'FAIL': 0, 'BLOCKED': 0, 'WARN': 1, 'CANCELLED': 2, 'NOT_TESTED': 3, 'PASS': 4}
                ordered_sections = sorted(sections.items(), key=lambda item: min(
                    priority.get(check.get('status'), 3) for check in item[1]))
                seen_by_scope = {}
                for (scope, section), section_checks in ordered_sections:
                    context = dict(contexts.get(scope) or {},
                        test_id=row['test_id'], log_path=row['main_log'],
                        records=report_records.get(scope) or [])
                    emit(format_section(section_checks, context, compact=True,
                                        evidence_seen=seen_by_scope.setdefault(scope, set())))
                # Evidence is not conditional on a failed check in the same
                # category. In particular a baseline Ocean console is native
                # evidence even when no netlist check was reached.
                for scope, records in report_records.items():
                    seen = seen_by_scope.setdefault(scope, set())
                    for section in sorted(set(observation_section(record) for record in records)):
                        if (scope, section) not in sections:
                            emit(format_evidence_section(scope, section, records, seen))
                    truncated = _report_truncation_paths(records)
                    context = row.get('case_contexts', {}).get(scope) or {}
                    integrity = {
                        'scope': scope, 'record_count': len(records), 'rendered_unique_count': len(seen),
                        'rendering': 'queued_for_writer',
                        'collection_status_scope': 'collector only; writer completion uses report_ready',
                        'collection_omissions': context.get('omitted_or_truncated') or [],
                        'cache_omission': context.get('report_omissions'),
                        'bounded_evidence_paths': truncated,
                        'bounded_evidence_paths_limit': 64,
                        'status': 'PARTIAL' if (truncated or context.get('omitted_or_truncated') or
                                                context.get('report_omissions') or len(seen) < len(records)) else 'COMPLETE_FOR_QUEUED_RECORDS',
                    }
                    emit('report_evidence_integrity ' + _json(integrity if full else concise_diagnostic_value(integrity)) + '\n')
                for success in successes.values():
                    emit('success_summary ' + _json(success) + '\n')
                record = {'final_checklist': compact, 'state': row['state'], 'test_id': row['test_id'],
                          'case_results': [{key: case.get(key) for key in
                              ('case_id', 'simulator', 'simulator_identity_known', 'engine', 'state', 'run_id', 'stop_required', 'stop_proven', 'first_cause', 'skip_reason')}
                              for case in row['case_results'].values()],
                          'shared_summary': row['main_log'], 'run_ids': row['run_ids'],
                          'summary': dict(collections.Counter(check['status'] for check in checks))}
                if not full:
                    record['case_results'] = concise_diagnostic_value(record['case_results'])
                emit(_utc() + ' ' + _json(record) + '\n')
                row['log_queue'].put(None)
            sentinel_thread = threading.Thread(target=finish_reports, name=row['test_id'] + '-report-drain', daemon=True)
            row['aux_threads'].append(sentinel_thread)
            sentinel_thread.start()
            row['aux_threads'][0].join(2)
            if row['aux_threads'][0].is_alive():
                with self.lock:
                    row['checks']['report.flush'] = {'id': 'report.flush', 'status': 'WARN', 'category': 'storage',
                        'message': 'Report writes remain pending; Retry Exit waits for this owned writer'}

    def _engine_spec(self, row, engine):
        original = row['spec']
        spec = dict((key, copy.deepcopy(original[key])) for key in
                    ('variables', 'objectives', 'constraints', 'candidate_timeout_seconds', 'data_root_override', 'execution_commands', 'command_scheduler', 'execution_selection', 'selected_test', 'native_baseline_foreground') if key in original)
        overrides = copy.deepcopy(original.get('maestro_job_overrides') or {})
        if engine == 'maestro_baseline':
            spec.pop('execution_commands', None)
            spec.pop('command_scheduler', None)
        shape = row.get('diagnostic_shape') or {'generations': 1, 'samples_per_generation': 1}
        spec.update(algorithm='random', generations=shape['generations'], population=shape['samples_per_generation'], seed=1, evaluator='real',
                    algorithm_parameters={'population': shape['samples_per_generation'], 'seed': 1}, execution_backend=engine, simulator=row.get('simulator', row['selected_simulator']),
                    maestro_job_overrides=overrides, parallel_jobs=1, maestro_candidate_mode='batch',
                    display_name='ET_' + row['test_id'].split('_')[-1] + '_' + row.get('simulator', row['selected_simulator']) + ({'maestro_netlist': '_M', 'private_netlist': '_S', 'maestro_baseline': '_B'}[engine]),
                    environment_test={'schema_version': 'crescendo.environment_test.run.v1', 'test_id': row['test_id'],
                                      'simulator': row.get('simulator', row['selected_simulator']),
                                      'frozen_candidate': copy.deepcopy(row.get('frozen_candidate'))})
        if row.get('frozen_plan') is not None:
            spec['environment_test'].update(frozen_plan=copy.deepcopy(row['frozen_plan']),
                frozen_plan_sha256=plan_digest(row['frozen_plan']))
        return spec

    def _engine(self, row, engine):
        spec = self._engine_spec(row, engine)
        identity = row['package_identity']
        source = (row['prepared'].get('setup') or {}).get('source') or {}
        self._log(row, {'observation': 'diagnostic.identity', 'payload': {
            'test_id': row['test_id'], 'engine': engine, 'simulator': row['simulator'], 'source_tree_sha256': identity['source_tree_sha256'],
            'source_hash_scope': identity['scope'], 'delivery_manifest': identity.get('delivery_manifest'),
            'setup': dict((key, source.get(key)) for key in ('library', 'cell', 'view'))}}, engine)
        self._check(row, 'request', 'PASS', ('Shared candidate plan and native Maestro settings frozen; Run and exact history results retain selected Test/corners' if engine == 'maestro_baseline' else 'Shared candidate plan and current command templates frozen; selected tests/analyses/corners use the shared pipeline'),
                    {'spec': spec, 'original_job_overrides': row['spec'].get('maestro_job_overrides'),
                     'original_job_policy': row['prepared'].get('job_policy'), 'all_tests_corners_required': True,
                     'execution_selection': _selection_context(row['prepared'].get('setup') or {}, spec)}, engine)
        self._timing(row.get('case_timing_observer'), 'transition', 'registration')
        try:
            launch = self.service.start(row['prepared_id'], spec)
        except Exception as error:
            cause = {'failure_stage': 'run_registration', 'message': _error(error)}
            detail = getattr(error, 'detail', None)
            if isinstance(detail, dict) and detail.get('failure_stage') in ('prepared_freshness', 'source_freshness', 'source_guard', 'native_corner_setup_identity'):
                # Preserve an explicit source-identity failure from the producer.
                cause.update(copy.deepcopy(detail), error_type=type(error).__name__)
            source_guard = detail.get('source_guard') if isinstance(detail, dict) else None
            outcome = {'first_cause': cause}
            if isinstance(source_guard, dict) and source_guard.get('ok') is False:
                outcome['shared_source_failure'] = copy.deepcopy(source_guard)
                if cause.get('failure_stage') == 'run_registration':
                    cause['failure_stage'] = 'source_guard'
            row['engine_outcomes'][engine] = outcome
            self._check(row, 'execution', 'FAIL', _error(error), {'first_cause': cause}, engine=engine)
            self._collect_inline(row, engine, self._case_directory(row, row['simulator'], engine), spec, False)
            # An explicit native cleanup failure prevents any safe new launch even
            # when the launch failed before returning an owned run identifier.
            return not (getattr(error, 'cleanup_failed', False) or
                        getattr(error, 'unsafe_to_optimize', False) or
                        (isinstance(detail, dict) and (detail.get('cleanup_pending') is True or
                         detail.get('cleanup_proven') is False)))
        self._timing(row.get('case_timing_observer'), 'transition', 'execution')
        name = launch['run_id']
        with self.lock:
            row['run_ids'][row['case_id']] = name
            row['active_runs'].append(name)
        self._check(row, 'launch', 'PASS', 'Owned diagnostic run registered; normal output pages remain unchanged', launch, engine)
        runtime = row['config'].get('runtime') or {}
        budget = _engine_deadline_budget(row['spec'], runtime, spec)
        deadline = time.monotonic() + budget
        grace = float(runtime.get('environment_test_stop_timeout_seconds', runtime.get('optimizer_exit_timeout_seconds', 60)))
        grace = min(300, max(1, grace))
        self._check(row, 'deadline', 'PASS',
                    'Engine deadline covers queue and preparation; candidate deadline and native Job Setup timeouts are separate. Timeout requests owned Stop.',
                    {'engine_timeout_seconds': budget, 'candidate_timeout_seconds': spec.get('candidate_timeout_seconds', runtime.get('simulation_timeout_seconds', 900)),
                     'job_setup_timeouts_input': _json((spec.get('maestro_job_overrides') or {}).get('timeouts') or {}),
                     'job_setup_timeout_scope': 'native job policy; independent of candidate and diagnostic engine deadlines',
                     'stop_proof_wait_seconds': grace}, engine)
        stopping, timed_out, last, snapshot = False, False, None, {}
        observed_logs, log_offsets, owned_ids, queried_ids = set(), {}, {}, set()
        with self.service.lock:
            thread = self.service.threads.get(name)
        if thread is None:
            raise RuntimeError('registered native run has no lifecycle thread')
        while True:
            alive = thread.is_alive()
            try:
                snapshot = self.service.progress(name)
                status = snapshot.get('status') or {}
                candidates = snapshot.get('candidates') or []
                execution = snapshot.get('execution') or {}
                candidate_states = []
                for item in candidates:
                    progress = item.get('execution_progress') or {}
                    tasks = progress.get('tasks') or []
                    candidate_states.append({'candidate_id': item.get('candidate_id'), 'status': item.get('status'),
                        'stage': progress.get('stage'), 'tasks': [_causal_task(task) for task in tasks[:32]], 'task_states': dict(collections.Counter(str(task.get('state') or 'unknown') for task in tasks)),
                        'task_count': len(tasks)})
                phase = _json({'state': status.get('state'),
                    'execution_stage': execution.get('stage'), 'simulator': row['simulator'], 'engine': engine, 'candidates': candidate_states,
                    'gate_progress': execution.get('gate_progress'),
                    'submissions': [{key: command.get(key) for key in ('kind', 'state', 'invocation_id', 'returncode', 'job_ids')}
                                    for command in (execution.get('commands') or [])[-8:] if command.get('kind') in ('native_submission', 'submission', 'submit', 'solver', 'solve')],
                    'command_states': dict(collections.Counter(str(command.get('state') or command.get('kind') or 'unknown')
                        for command in execution.get('commands') or []))})
                if phase != last:
                    phase_payload = json.loads(phase)
                    phase_payload['message'] = status.get('message')
                    self._log(row, {'observation': 'execution.progress', 'run_id': name, 'payload': phase_payload}, engine)
                    last = phase
                for command in (snapshot.get('execution') or {}).get('commands') or []:
                    if command.get('kind') in ('native_submission', 'submission', 'submit', 'solver', 'solve'):
                        self._log(row, {'observation': 'execution.command', 'payload': {
                            key: command.get(key) for key in ('kind', 'invocation_id', 'argv', 'cwd', 'returncode',
                                'job_ids', 'scope', 'submission_mode', 'environment_export', 'execution_cwd',
                                'threads', 'allocated_cpu') if command.get(key) is not None}}, engine)
                        for identifier in command.get('job_ids') or []:
                            identifier = str(identifier)
                            owned_ids[identifier] = dict(job_id=identifier, candidate_id=None,
                                source='current_native_submission', shared=True, scope=command.get('scope') or 'shared_native_worker_pool')
                for candidate in candidates:
                    self._observe_candidate(row, engine, name, candidate, observed_logs, log_offsets, owned_ids)
            except Exception as error:
                signature = _error(error)
                if signature != last:
                    self._log(row, {'observation': 'execution.progress.unavailable', 'error': signature}, engine)
                    last = signature
            if not alive:
                break
            if (row['cancel'].is_set() or time.monotonic() >= deadline) and not stopping:
                timed_out = not row['cancel'].is_set()
                self._check(row, 'stop_request', 'WARN', 'Engine deadline reached' if timed_out else 'User cancellation', engine=engine)
                self.service.stop(name)
                stopping = True
                deadline = time.monotonic() + grace
            if stopping and time.monotonic() >= deadline:
                self._check(row, 'termination', 'FAIL', 'Native run has not proven termination within Stop deadline; no other run will start', engine=engine)
                row['engine_outcomes'][engine] = failure_summary(snapshot.get('status'), snapshot.get('candidates'), timed_out)
                self._collect_inline(row, engine, launch['run_root'], spec, False)
                return False
            # Query only IDs observed in current candidate/native submission evidence.
            new_ids = [value for key, value in owned_ids.items() if key not in queried_ids]
            if len(owned_ids) > 64:
                self._check(row, 'scheduler.id_capacity', 'WARN', 'Owned scheduler ID observation capacity exceeded; deferred IDs are not verified', {'observed_count': len(owned_ids), 'query_limit': 64}, engine)
                new_ids = new_ids[:max(0, 64 - len(queried_ids))]
            if new_ids:
                self._scheduler(row, engine, new_ids)
                queried_ids.update(value['job_id'] for value in new_ids)
            thread.join(.4)
        with self.lock:
            row['active_runs'].remove(name)
        if owned_ids:
            self._scheduler(row, engine, list(owned_ids.values()))
        self._timing(row.get('case_timing_observer'), 'transition', 'verification')
        candidates = snapshot.get('candidates') or []
        status = snapshot.get('status') or {}
        completed = [item for item in candidates if item.get('status') == 'completed' and (item.get('completion_evidence') or {}).get('ok') is True]
        pipelines = [pipeline_qualification(item, engine, row['simulator']) for item in completed]
        for item, pipeline in zip(completed, pipelines):
            self._check(row, 'execution.pipeline', 'PASS' if pipeline['ok'] else 'FAIL',
                ('Native Maestro Run/history/result protocol verified' if engine == 'maestro_baseline' else 'Selected source used the common solver/submission protocol') if pipeline['ok'] else '; '.join(pipeline['problems']),
                dict(pipeline_summary(pipeline['pipeline']), qualification_problems=pipeline['problems']), engine,
                candidate_id=item.get('candidate_id'))
        rejected_parameters = next((item.get('parameter_application') for item in candidates
            if isinstance(item.get('parameter_application'), dict)
            and item['parameter_application'].get('ok') is False), None)
        expected = row.get('frozen_plan') or [{'generation': 0, 'index': 0, 'values': row.get('frozen_candidate')}]
        identity_status = _candidate_plan_observation(candidates, expected, status.get('state'))
        candidate_matches = identity_status == 'PASS'
        if identity_status != 'PASS':
            incomplete = len(candidates) < len(expected)
            message = ('Frozen diagnostic plan was not fully registered' if incomplete and identity_status == 'NOT_TESTED' else
                       'Completed run did not register the full frozen diagnostic plan' if incomplete and status.get('state') == 'completed' else
                       'Actual candidate sequence differs from the frozen diagnostic plan')
            self._check(row, 'candidate_identity', identity_status, message,
                        {'expected_count': len(expected), 'actual_count': len(candidates),
                         'expected_plan_sha256': plan_digest(expected)}, engine)
        ok = (candidate_matches and status.get('state') == 'completed' and len(completed) == len(expected)
              and all(_simulator_completion_matches(item.get('completion_evidence'), row['simulator']) for item in completed)
              and len(pipelines) == len(expected) and all(pipeline['ok'] for pipeline in pipelines)
              and rejected_parameters is None)
        row['engine_outcomes'][engine] = failure_summary(status, candidates, timed_out)
        if ok:
            summary, seen = [], set()
            metrics = completed[0].get('metrics') or {}
            for kind in ('objectives', 'constraints'):
                for item in metrics.get(kind) or []:
                    identity = (item.get('output_key'), item.get('aggregation', 'mean'), tuple(item.get('corners') or []))
                    if identity not in seen:
                        seen.add(identity)
                        summary.append({'output_key': identity[0], 'aggregation': identity[1],
                            'corners': list(identity[2]), 'value': item.get('value'),
                            'scope': 'validated stage aggregate; not cross-stage point equivalence'})
            row['engine_outcomes'][engine]['metric_summary'] = summary
        source_failure = next((item.get('source_guard') for item in candidates
            if isinstance(item.get('source_guard'), dict) and item['source_guard'].get('ok') is False), None)
        if source_failure is not None:
            row['engine_outcomes'][engine]['shared_source_failure'] = source_failure
        if identity_status == 'FAIL' and not row['engine_outcomes'][engine].get('first_cause'):
            row['engine_outcomes'][engine]['first_cause'] = {'failure_stage': 'candidate_identity', 'message': message}
        if rejected_parameters is not None and not row['engine_outcomes'][engine].get('first_cause'):
            row['engine_outcomes'][engine]['first_cause'] = {
                'failure_stage': 'parameter_application',
                'message': rejected_parameters.get('error') or rejected_parameters.get('message') or
                           'The current native parameter application proof reports a mismatch',
                'source': 'current_candidate_parameter_application'}
        if any(not _simulator_completion_matches(item.get('completion_evidence'), row['simulator']) for item in completed):
            row['engine_outcomes'][engine]['first_cause'] = {'failure_stage': 'simulator_identity',
                'message': 'Native completion does not prove the explicitly selected simulator',
                'source': 'diagnostic_completion_gate', 'requested_simulator': row['simulator']}
        failed_pipeline = next((item for item in pipelines if not item['ok']), None)
        if failed_pipeline is not None and not row['engine_outcomes'][engine].get('first_cause'):
            row['engine_outcomes'][engine]['first_cause'] = {'failure_stage': 'execution_pipeline',
                'message': '; '.join(failed_pipeline['problems']), 'source': 'diagnostic_common_pipeline_gate'}
        if 'case_contexts' in row:
            with self.lock:
                row['case_contexts'].setdefault(row.get('case_id') or engine, {}).update(
                    case_id=row.get('case_id'), run_root=launch['run_root'],
                    log_path=row['simulator_logs'][row['simulator']],
                    first_cause=row['engine_outcomes'][engine].get('first_cause'))
        self._log(row, {'observation': 'diagnostic.first_cause', 'payload': row['engine_outcomes'][engine]}, engine)
        self._check(row, 'execution', 'PASS' if ok else ('CANCELLED' if row['cancel'].is_set() else 'FAIL'),
                    ('All planned candidates completed through native Maestro Run and exact history results' if engine == 'maestro_baseline' else 'All planned candidates completed with common CLI pipeline, private netlist and result proof') if ok else ((row['engine_outcomes'][engine].get('first_cause') or {}).get('message') or
                     (row['engine_outcomes'][engine].get('first_cause') or {}).get('error') or
                     'The selected run stopped before completion; see the first native failure'),
                    dict(_successful_execution(status, candidates), timed_out=timed_out) if ok else
                    dict(_failed_execution(status, candidates), timed_out=timed_out,
                         first_cause=row['engine_outcomes'][engine].get('first_cause')), engine)
        missing_stages = []
        for item in candidates:
            for key in ('completion_evidence', 'parameter_application', 'source_guard', 'raw_cleanup_proof', 'metrics'):
                evidence = item.get(key)
                if isinstance(evidence, dict) and key == 'parameter_application':
                    evidence = dict((name, value) for name, value in evidence.items() if name != 'export_snapshot')
                proof_status = ('PASS' if evidence and (key == 'metrics' or
                                isinstance(evidence, dict) and evidence.get('ok') is True) else
                                ('FAIL' if isinstance(evidence, dict) and evidence.get('ok') is False else 'NOT_TESTED'))
                if proof_status == 'PASS' and key not in ('parameter_application', 'metrics'):
                    evidence = _proof_brief(evidence)
                if not evidence:
                    missing_stages.append(key)
                    self._check(row, key, 'NOT_TESTED', 'Candidate producer evidence unavailable',
                        {'candidate_id': item.get('candidate_id')}, engine, candidate_id=item.get('candidate_id'))
                    continue
                self._check(row, key, proof_status, 'Native producer evidence', evidence, engine, candidate_id=item.get('candidate_id'))
        if missing_stages:
            self._check(row, 'execution.not_reached', 'NOT_TESTED',
                        'Later stages were not reached; see the first native failure',
                        {'stages': sorted(set(missing_stages)),
                         'first_cause_stage': (row['engine_outcomes'][engine].get('first_cause') or {}).get('failure_stage')},
                        engine)
        if ok and row['spec'].get('advanced_diagnostics') is not True:
            self._check(row, 'statistics_and_dataset', 'NOT_TESTED',
                        'Optional statistics, dataset readback and export/import roundtrip deferred', engine=engine)
        if ok and row['spec'].get('advanced_diagnostics') is True:
            try:
                from .run_data_store import load_dataset
                path = self.service.run_path(name, 'optimization.crescendo')
                dataset = load_dataset(path)
                exported = self.service.run_path(name, 'environment_export_probe.crescendo')
                if os.path.lexists(exported):
                    raise RuntimeError('export probe unexpectedly already exists')
                try:
                    self.service.export_results(name, exported)
                    imported = load_dataset(exported)
                    if imported.get('candidates') != dataset.get('candidates'):
                        raise RuntimeError('export/import candidate evidence differs')
                finally:
                    if os.path.isfile(exported) and not os.path.islink(exported):
                        os.unlink(exported)
                self._check(row, 'statistics_and_dataset', 'PASS' if dataset.get('candidates') == candidates else 'FAIL',
                            'Core dataset validation and optimization summary readback',
                            {'path': path, 'revision': dataset.get('revision'),
                             'dataset_candidate_sha256': hashlib.sha256(_json(dataset.get('candidates')).encode('utf-8')).hexdigest(),
                             'selected_output_statistics': selected_output_statistics(candidates, spec),
                             'pareto_count': len((dataset.get('result') or {}).get('pareto_front') or []),
                             'candidate_count': len(dataset.get('candidates') or []), 'export_import_roundtrip': True}, engine)
            except Exception as error:
                self._check(row, 'statistics_and_dataset', 'FAIL', _error(error), engine=engine)
        self._timing(row.get('case_timing_observer'), 'transition', 'cleanup')
        try:
            shutdown = self.service.read(name, 'worker_shutdown.json') or {}
            workers = shutdown.get('workers')
            final = status.get('final_cleanup') if 'final_cleanup' in status else shutdown.get('final_cleanup')
            from .shutdown_evidence import shutdown_proven
            clean = (shutdown_proven(shutdown)
                     and not (status.get('cleanup_failure') or {}).get('recovery_required'))
            if 'final_cleanup' in status:
                clean = clean and isinstance(final, dict) and all(final.get(key) is True for key in
                                    ('process_termination_proven', 'remote_stop_proven', 'artifact_cleanup_complete'))
            self._check(row, 'termination_and_cleanup', 'PASS' if clean else 'FAIL',
                        'Owned shutdown and retention proof verified' if clean else 'Shutdown, shared resource safety or retention remains unproven; inspect the separate evidence fields',
                        {'shutdown': _proof_brief(shutdown) if clean else shutdown,
                         'final_cleanup': _proof_brief(final) if clean else final,
                         'local_process_termination_proven': (final or {}).get('process_termination_proven'),
                         'native_termination_proven': (final or {}).get('native_termination_proven'),
                         'remote_stop_proven': (final or {}).get('remote_stop_proven'),
                         'source_integrity_proven': (final or {}).get('source_integrity_proven'),
                         'artifact_retention_or_cleanup_complete': (final or {}).get('artifact_cleanup_complete')}, engine)
        except Exception as error:
            clean = False
            self._check(row, 'termination_and_cleanup', 'FAIL',
                        '无法读取收尾证明；停止与清理尚未证实。旧记录可能超出读取上限或无效；保留原文件。 ' + _error(error),
                        {'artifact': 'worker_shutdown.json', 'stop_proven': False}, engine)
        self._collect_inline(row, engine, launch['run_root'], spec, clean)
        if clean:
            try:
                cleanup_test_runtime_lease(os.path.dirname(launch['run_root']))
                context = (row.get('case_contexts') or {}).get(row.get('case_id')) or {}
                self._check(row, 'files', 'PASS', '原始诊断文件和已生成网表保留，供手工检查；不自动归档删除',
                            {key: context.get(key) for key in ('run_root', 'netlists', 'artifacts', 'retention_status')}, engine)
            except Exception as error:
                self._check(row, 'files', 'WARN', '证据保留；释放已结束的诊断锁未完成：' + _error(error), engine=engine)
        return clean

    def _collect_inline(self, row, engine, root, spec, clean):
        try:
            from .environment_diagnostics import retained_artifact_index
            index = retained_artifact_index(root)
            index.update(_report_context(row))
            context = dict(index, case_id=row.get('case_id'), run_root=os.path.abspath(root),
                test_id=row.get('test_id'), log_path=(row.get('simulator_logs') or {}).get(row.get('simulator')),
                first_cause=(row['engine_outcomes'].get(engine) or {}).get('first_cause'),
                request_context={key: spec.get(key) for key in ('simulator', 'execution_backend', 'variables',
                    'objectives', 'constraints', 'execution_commands', 'command_scheduler') if key in spec})
            with self.lock:
                contexts = row.setdefault('case_contexts', {})
                previous = contexts.get(row.get('case_id') or engine) or {}
                if previous.get('report_omissions'):
                    context['report_omissions'] = previous['report_omissions']
                contexts[row.get('case_id') or engine] = context
            for record in collect_run_diagnostics(root, engine, row['config'], row['prepared'], spec,
                    stop_proven=clean, error_text=_json(row['engine_outcomes'].get(engine) or {}), cancel=row['cancel']):
                if record.get('observation') == 'diagnostic.native_stage_evidence':
                    payload = record.get('payload') or {}
                    observed = {'capture_diagnostics': {'skill': payload.get('skill') or {}}}
                    for identifier, evidence in _native_failure_observations(observed):
                        evidence['path'] = payload.get('path')
                        self._check(row, identifier, 'FAIL', '原生阶段记录到错误；完整上下文见证据，不能据此推断跨流首因。', evidence, engine)
                if record.get('observation') == 'diagnostic.native_worker_diagnostics':
                    report = (record.get('payload') or {}).get('native_worker_diagnostics')
                    if isinstance(report, dict):
                        with self.lock:
                            reports = context.setdefault('native_worker_diagnostics', [])
                            if report not in reports and len(reports) < 8:
                                reports.append(report)
                if record.get('observation') == 'diagnostic.cdf_callback_failure':
                    payload = record.get('payload') or {}
                    path = payload.get('path')
                    failures = payload.get('cdf_callback_failures') or []
                    if (isinstance(path, str) and not os.path.isabs(path)
                            and os.path.normpath(path) != '..'
                            and not os.path.normpath(path).startswith('..' + os.sep)
                            and os.path.basename(path) in ('ocean.stdout', 'ocean.stderr')
                            and isinstance(failures, list)):
                        with self.lock:
                            current = context.setdefault('cdf_callback_failures', [])
                            for failure in failures[:8]:
                                if (not isinstance(failure, dict)
                                        or failure.get('schema_version') != 'crescendo.cdf_callback_failure.v1'
                                        or failure.get('console_source') != path):
                                    continue
                                item = dict(failure, path=path)
                                if item not in current:
                                    if len(current) < 8:
                                        current.append(item)
                                    else:
                                        context['cdf_callback_failures_truncated'] = True
                            if payload.get('cdf_callback_failures_truncated') or len(failures) > 8:
                                context['cdf_callback_failures_truncated'] = True
                if record.get('observation') in ('diagnostic.artifact', 'diagnostic.artifact_section',
                                                'diagnostic.source_settings', 'diagnostic.settings'):
                    if 'case_records' in row:
                        self._log(row, record, engine)
                        continue
                    simulator = row.get('simulator', row['selected_simulator'])
                    cache = row.setdefault('shared_schema_artifacts', {}).setdefault(simulator, {})
                    trial = dict(cache)
                    safe_record = _redact(record, self._redaction_environment(row))
                    payload = safe_record.get('payload') or {}
                    location = str(payload.get('path', record['observation']))
                    if payload.get('field') is not None:
                        location += '#' + str(payload['field'])
                    indexed = dict(safe_record, payload=_schema_artifact_index(payload, trial,
                        row.get('case_id', engine) + ':' + location))
                    if simulator in self._log(row, indexed, engine) and len(_json(indexed)) <= 196608:
                        cache.update(trial)
                else:
                    self._log(row, record, engine)
                if record.get('observation') == 'diagnostic.result_reader_capability':
                    observation = record.get('payload') or {}
                    parameters = observation.get('native_parameter_readback') or {}
                    available = parameters.get('ok')
                    parameter_message = ('Current PSF parameter readback recorded' if available is True else
                        'Current PSF parameter identity mismatch; actual values retained' if parameters.get('identity_error') else
                        'Current PSF parameter metadata unavailable; solver completion and waveform access remain separate')
                    self._check(row, 'reader.parameter_metadata.' + str(observation.get('task_id') or 'unknown'),
                        'PASS' if available is True else 'FAIL' if available is False else 'NOT_TESTED',
                        parameter_message,
                        dict((key, observation.get(key)) for key in ('path', 'task_id', 'execution_simulator',
                            'result_reader', 'netlist_sha256', 'analysis_names', 'analysis_inventory_status',
                            'native_parameter_readback', 'candidate_qualification')), engine)
                    outputs = observation.get('output_diagnostics') or []
                    if outputs:
                        output_states = [value.get('status') for value in outputs if isinstance(value, dict)]
                        output_status = ('PASS' if output_states and all(value == 'PASS' for value in output_states) else
                            'FAIL' if 'FAIL' in output_states else 'WARN' if 'PASS' in output_states else 'NOT_TESTED')
                        self._check(row, 'reader.output_observations.' + str(observation.get('task_id') or 'unknown'),
                            output_status, 'Actual output expression observations; these do not replace candidate parameter qualification',
                            {'path': observation.get('path'), 'output_diagnostics': outputs,
                             'candidate_qualification': observation.get('candidate_qualification')}, engine)
                if record.get('observation') == 'diagnostic.identity_coverage':
                    self._coverage_diagnostic_checks(row, engine, record['payload'])
                if record.get('observation') == 'diagnostic.identity_coverage' and record['payload'].get('unverified_count'):
                    self._check(row, 'identity_numeric_verification', 'WARN',
                        'Some saved expressions have incomplete numeric identity verification; retained source/point evidence is not a full numeric PASS',
                        record['payload'], engine)
                if record.get('observation') == 'diagnostic.collection':
                    with self.lock:
                        context['collection_status'] = record['payload'].get('status')
                        context['collection_status_scope'] = 'collector only; final report delivery is separately tracked'
                        context['omitted_or_truncated'] = (record['payload'].get('omitted_or_truncated') or [])[:16]
                if record.get('observation') == 'diagnostic.collection' and record['payload'].get('status') == 'PARTIAL':
                    self._check(row, 'diagnostics.collection', 'WARN',
                        'Inline diagnostic evidence has explicitly listed omissions/truncation; native execution proof has separate scope',
                        {'omitted_or_truncated': record['payload'].get('omitted_or_truncated'),
                         'files_read': record['payload'].get('files_read'),
                         'discovered_file_count': record['payload'].get('discovered_file_count')}, engine)
        except Exception as error:
            self._check(row, 'diagnostics.collection', 'WARN', 'Inline diagnostic collection incomplete: ' + _error(error), engine=engine)

    def _coverage_diagnostic_checks(self, row, engine, payload):
        """Show bounded producer differences without changing qualification."""
        found = {}
        def add(key, test, reason, value):
            found.setdefault(key, []).append({'test': test, 'reason': reason, 'value': value})
        for contract in payload.get('contracts') or []:
            for field, key in (('unexpected_tests', 'test_selection.unexpected'),
                               ('duplicate_source_corners', 'test_selection.duplicate_corner'),
                               ('native_nominal_difference', 'corner_mask.nominal')):
                if contract.get(field):
                    add(key, None, field, contract[field])
            diff = contract.get('native_group_enabled_difference')
            if isinstance(diff, dict) and diff.get('expected') != diff.get('actual'):
                add('corner_mask.group', None, 'saved/native enabled groups', diff)
            schedule = contract.get('native_schedule')
            if isinstance(schedule, dict) and schedule.get('read_ok') is False:
                add('corner_mask.unreadable', None, 'native schedule read failed', schedule)
            if isinstance(schedule, dict) and schedule.get('members_state') == 'unavailable':
                add('unknown.members_unavailable', None, 'native member API unavailable; saved identity and native count fallback',
                    {'members_state': 'unavailable'})
        for test in payload.get('tests') or []:
            if not isinstance(test, dict):
                continue
            name = test.get('test')
            for field, key in (('missing', 'points.missing'), ('duplicate_points', 'points.duplicate'),
                               ('unexpected', 'points.unexpected'), ('mapping_error', 'identity.mapping'),
                               ('corner_identity_errors', 'identity.corner'),
                               ('corner_identity_failures', 'identity.corner'),
                               ('variable_mismatches', 'parameters.variable'),
                               ('sweep_differences', 'parameters.sweep'),
                               ('unverified_variable_check_count', 'unknown.numeric')):
                if test.get(field):
                    add(key, name, field, test[field])
            members = test.get('native_member_difference')
            if isinstance(members, dict):
                for field, key in (('missing', 'points.missing'), ('unexpected', 'points.unexpected')):
                    if members.get(field):
                        add(key, name, 'native_member_' + field, members[field])
            plan = test.get('execution_plan') or {}
            if isinstance(plan, dict) and plan.get('errors'):
                add('parameters.plan', name, 'execution_plan.errors', plan['errors'])
        for points in payload.get('point_coverage') or []:
            for field, key in (('missing_test_points', 'points.rdb_missing'),
                               ('duplicate_test_points', 'points.rdb_duplicate'),
                               ('unexpected_test_points', 'points.rdb_unexpected')):
                if points.get(field):
                    add(key, None, field, points[field])
        for key in sorted(found):
            examples = found[key][:3]
            # Short values make this useful even when a user cannot access logs.
            brief = _json(examples)
            if len(brief) > 450:
                brief = brief[:450] + '…'
            status = 'WARN' if key.startswith('unknown.') else 'FAIL'
            self._check(row, 'coverage.' + key, status, brief,
                        {'examples': examples, 'count': len(found[key]),
                         'scope': 'producer evidence; diagnostic presentation only'}, engine)

    def _scheduler(self, row, engine, ids):
        try:
            row['scheduler_queue'].put_nowait((row, engine, ids[:64]))
        except queue.Full:
            self._check(row, 'scheduler.observe_capacity', 'WARN', 'Readonly scheduler queue is full; native monitoring/Stop continue; these observations were not queried', {'job_ids': ids}, engine)

    def _scheduler_query(self, row, engine, ids):
        def emit(check):
            details = check.get('details') or {}
            observation = details.get('scheduler_observation') or {}
            signature = _json({'status': check['status'], 'message': check['message'],
                'state': observation.get('state'), 'not_found': observation.get('not_found'),
                'job_exit_code': observation.get('job_exit_code'), 'query_rc': details.get('query_returncode'),
                'metadata': observation.get('metadata')})
            key = (row.get('case_id'), check['id'])
            if row['scheduler_signatures'].get(key) == signature:
                return
            row['scheduler_signatures'][key] = signature
            self._check(row, check['id'], check['status'], check['message'], details, engine)
        try:
            query_scheduler_jobs(row['config'], row['prepared'], os.path.dirname(row['simulator_logs'][row.get('simulator', row['selected_simulator'])]), ids,
                                 emit=emit,
                                 cancel=lambda: row['cancel'].is_set() or row['scheduler_cancel'].is_set(), spec=row['spec'],
                                 capability_cache=row['scheduler_capabilities'])
        except Exception as error:
            self._check(row, 'scheduler.query', 'FAIL', _error(error), engine=engine)

    def _observe_candidate(self, row, engine, name, candidate, observed, offsets, owned_ids):
        cid = candidate['candidate_id']
        completion = candidate.get('completion_evidence') or {}
        pipeline = completion.get('execution_pipeline') or candidate.get('execution_pipeline') or {}
        tasks = list((candidate.get('execution_progress') or {}).get('tasks') or [])
        tasks.extend(pipeline.get('points') or pipeline.get('tasks') or [])
        for task in tasks:
            if task.get('scheduler_job_id') or task.get('job_id'):
                identifier = str(task.get('scheduler_job_id') or task['job_id'])
                owned_ids[identifier] = dict(job_id=identifier, candidate_id=cid, test=task.get('test'),
                    corner=task.get('corner'), point_id=task.get('point_id'), task_id=task.get('task_id'),
                    source='current_native_task', shared=bool(task.get('shared')), scope=task.get('scope'))
        try:
            listing = self.service.candidate_logs(name, cid)
            for task in listing.get('tasks') or []:
                if task.get('job_id'):
                    identifier = str(task['job_id'])
                    owned_ids[identifier] = dict(job_id=identifier, candidate_id=cid, test=task.get('test'), corner=task.get('corner'), source='current_candidate_log_task')
            for descriptor in listing.get('logs') or []:
                key = (cid, descriptor['log_id'])
                if key not in observed:
                    observed.add(key)
                    self._log(row, {'observation': 'native.log.discovered', 'candidate_id': cid, 'descriptor': descriptor}, engine)
                if str(descriptor.get('kind') or '').startswith(('spectre', 'alps', 'simulator')):
                    chunk = self.service.read_candidate_log_chunk(name, cid, descriptor['log_id'], offset=offsets.get(key, 0), max_bytes=16384)
                    if chunk.get('text'):
                        context = _windows(chunk['text'])
                        if context['error_count']:
                            self._log(row, {'observation': 'simulator.log.increment', 'candidate_id': cid,
                                'descriptor': descriptor, 'context': context, 'offset': offsets.get(key, 0)}, engine)
                        proof_key = (cid, descriptor['log_id'], 'read_proof')
                        if proof_key not in observed:
                            observed.add(proof_key)
                            self._check(row, 'simulator_log_read', 'PASS', 'Read native simulator log; capture-only status does not certify a solver run',
                                        {'candidate_id': cid, 'descriptor': descriptor}, engine)
                    offsets[key] = chunk.get('next_offset', offsets.get(key, 0))
        except Exception as error:
            key = (cid, _error(error))
            if key not in observed:
                observed.add(key)
                self._log(row, {'observation': 'native.log.not_available', 'candidate_id': cid, 'reason': _error(error)}, engine)


def compact_diagnostic_run(root, run_id, cancel=None, timeout=30):
    """Archive verified owned inactive diagnostics; never follow or remove sources.

    The caller must first consume authoritative native shutdown evidence. This
    keeps the status/shutdown/core needed by normal service resource fences.
    An interrupted archive never causes source or unarchived evidence removal.
    """
    root = os.path.abspath(root)
    deadline = time.monotonic() + timeout
    def budget():
        if time.monotonic() > deadline or cancel and cancel.is_set():
            raise RuntimeError('diagnostic archive deadline/cancellation; evidence retained')
    if os.path.basename(root) != run_id or os.path.islink(root):
        raise RuntimeError('diagnostic run root identity differs')
    keep = {'status.json', 'worker_shutdown.json', 'optimization.crescendo', 'evidence.tar.gz'}
    files, dirs, total = [], [], 0
    for directory, children, names in os.walk(root, followlinks=False):
        budget()
        for child in list(children):
            path = os.path.join(directory, child)
            if os.path.islink(path):
                if os.path.commonpath((root, os.path.realpath(path))) != root:
                    raise RuntimeError('external linked diagnostic directory retained: ' + path)
                files.append((path, os.path.relpath(path, root), os.lstat(path)))
                children.remove(child)
                continue
            dirs.append(path)
        for name in names:
            path = os.path.join(directory, name)
            relative = os.path.relpath(path, root)
            if relative in keep:
                continue
            info = os.lstat(path)
            if stat.S_ISLNK(info.st_mode):
                if os.path.commonpath((root, os.path.realpath(path))) != root:
                    raise RuntimeError('external linked diagnostic file retained: ' + path)
            elif not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise RuntimeError('unexpected linked/special diagnostic file retained: ' + path)
            total += info.st_size
            if len(files) >= 50000 or total > 512 * 1024 * 1024:
                raise RuntimeError('diagnostic evidence exceeds bounded archive budget; retained')
            files.append((path, relative, info))
    checkpoint_root = os.path.join(os.path.dirname(root), '.continuation_checkpoints')
    extra_dirs = []
    if os.path.isdir(checkpoint_root) and not os.path.islink(checkpoint_root):
        for digest in os.listdir(checkpoint_root):
            directory = os.path.join(checkpoint_root, digest)
            if not re.match(r'^[0-9a-f]{64}$', digest) or os.path.islink(directory):
                raise RuntimeError('unexpected diagnostic continuation checkpoint retained')
            names = os.listdir(directory)
            if names != ['checkpoint.json']:
                raise RuntimeError('unexpected diagnostic checkpoint files retained')
            path = os.path.join(directory, 'checkpoint.json')
            info = os.lstat(path)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise RuntimeError('linked diagnostic checkpoint retained')
            with open(path, encoding='utf-8') as stream:
                document = json.load(stream)
            manifest_doc = document.get('manifest') or {}
            if manifest_doc.get('origin_run_id') != run_id or os.path.abspath(manifest_doc.get('origin_run_root') or '') != root:
                raise RuntimeError('diagnostic checkpoint origin differs')
            files.append((path, '_runtime_checkpoints/' + digest + '/checkpoint.json', info))
            extra_dirs.append(directory)
        extra_dirs.append(checkpoint_root)
    target = os.path.join(root, 'evidence.tar.gz')
    temporary = target + '.tmp'
    if os.path.lexists(target) or os.path.lexists(temporary):
        raise RuntimeError('diagnostic archive already exists; retained without overwrite')
    manifest = {}
    try:
        with tarfile.open(temporary, 'x:gz', compresslevel=4, dereference=False) as archive:
            for path, relative, info in files:
                budget()
                digest = hashlib.sha256()
                if stat.S_ISLNK(info.st_mode):
                    link = os.readlink(path)
                    archive.addfile(archive.gettarinfo(path, arcname=relative))
                    manifest[relative] = hashlib.sha256(('link:' + link).encode('utf-8')).hexdigest()
                    continue
                fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
                with os.fdopen(fd, 'rb') as stream:
                    if not os.path.samestat(info, os.fstat(stream.fileno())):
                        raise RuntimeError('diagnostic file changed before archive')
                    metadata = archive.gettarinfo(path, arcname=relative)
                    archive.addfile(metadata, stream)
                with open(path, 'rb') as stream:
                    for chunk in iter(lambda: stream.read(65536), b''):
                        digest.update(chunk)
                manifest[relative] = digest.hexdigest()
        # Verify archive contents and current files before deleting any evidence.
        with tarfile.open(temporary, 'r:gz') as archive:
            if set(archive.getnames()) != set(manifest):
                raise RuntimeError('archive file set differs')
            for member in archive:
                budget()
                digest = hashlib.sha256()
                if member.issym():
                    if hashlib.sha256(('link:' + member.linkname).encode('utf-8')).hexdigest() != manifest[member.name]:
                        raise RuntimeError('archive link target differs')
                    continue
                with archive.extractfile(member) as stream:
                    for chunk in iter(lambda: stream.read(65536), b''):
                        digest.update(chunk)
                if digest.hexdigest() != manifest[member.name]:
                    raise RuntimeError('archive SHA256 differs')
        for path, _relative, info in files:
            current = os.lstat(path)
            if not os.path.samestat(current, info) or current.st_mtime_ns != info.st_mtime_ns or current.st_size != info.st_size:
                raise RuntimeError('inactive evidence changed; no deletion performed')
        os.rename(temporary, target)
        for path, _relative, _info in files:
            os.unlink(path)
        for path in sorted(dirs + extra_dirs, key=len, reverse=True):
            os.rmdir(path)
        return {'run_root': root, 'archive': target, 'archived_files': len(files), 'archived_source_bytes': total,
                'retained_run_files': sorted(os.listdir(root)), 'report_plus_run_file_count': len(os.listdir(root)) + 1}
    except Exception:
        if os.path.isfile(temporary) and not os.path.islink(temporary):
            os.unlink(temporary)
        raise


def cleanup_test_runtime_lease(directory):
    """Remove only this process's released lock in its unique diagnostic root."""
    import fcntl
    namespace = os.path.join(directory, '.continuation_leases')
    if not os.path.isdir(namespace) or os.path.islink(namespace):
        return
    for name in os.listdir(namespace):
        if not re.match(r"^[0-9a-f]{64}\.lock$", name):
            raise RuntimeError('unexpected diagnostic lease retained')
        path = os.path.join(namespace, name)
        fd = os.open(path, os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0))
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != os.getuid():
                raise RuntimeError('diagnostic runtime lease ownership differs')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            document = json.loads(os.read(fd, 16384).decode('utf-8'))
            root = document.get('run_root')
            if (document.get('schema_version') != 'crescendo.continuation_lease.v1' or
                    document.get('lease_kind') not in ('runtime_real_eda_optimization', 'continuation') or
                    document.get('state') != 'released' or document.get('pid') != os.getpid() or
                    root and os.path.dirname(os.path.abspath(root)) != os.path.abspath(directory) or
                    not os.path.samestat(before, os.lstat(path))):
                raise RuntimeError('diagnostic lease was not released by this process')
            os.unlink(path)
        finally:
            os.close(fd)
    os.rmdir(namespace)
