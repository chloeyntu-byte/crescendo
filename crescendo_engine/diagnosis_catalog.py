# -*- coding: utf-8 -*-
"""User-facing diagnostic names and self-contained sections; no run authority.

All locations identify the responsible implementation, not a proven traceback.
Reading this presentation never qualifies a solver, result or stopped process.
"""
from __future__ import print_function
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

import collections
import hashlib
import json
import os

SCHEMA = _contract_text('crescendo.diagnosis.presentation.v1')
ENGINE_NAMES = {'maestro_netlist': '私有 Maestro 生成网表', 'private_netlist': '私有网表改参', 'maestro_baseline': '私有 Maestro（Baseline）'}
STATUS_NAMES = {'PASS': '通过', 'FAIL': '失败', 'BLOCKED': '前置阻塞', 'WARN': '需要关注',
                'NOT_TESTED': '尚未验证', 'CANCELLED': '已停止'}
SECTIONS = collections.OrderedDict([
    ('environment', '运行环境与移植配置'), ('models', '仿真器与模型'),
    ('netlist', '模型继承与网表生成'), ('solver', '求解器执行'),
    ('scheduler', '集群提交与状态'), ('results', '结果读取与目标计算'),
    ('cleanup', '停止与文件保留'), ('report', '诊断报告与证据'), ('request', '本次案例与整体结果')])
# Prefixes are longest-first; raw producer IDs remain in every check.
CATALOG = [
 ('suite.lease', 'cleanup', '私有套件租约占用', 'setup_private_session.py', '_acquire_lease'),
 ('suite.lock', 'cleanup', '私有视图被原生编辑器占用', 'private_view_access.py', 'check_private_view_edit_locks'),
 ('suite.snapshot', 'environment', '私有套件快照不可用', 'setup_private_session.py', '_read_suite'),
 ('suite.recovery', 'cleanup', '私有套件需要恢复', 'setup_private_session.py', 'reopen_setup_private_session'),
 ('suite.rollback', 'cleanup', '私有套件刷新回滚失败', 'setup_private_session.py', 'refresh_setup_private_session'),
 ('suite.permission', 'cleanup', '私有视图写权限恢复提示', 'setup_private_session.py', 'close_setup_private_session'),
 ('suite.release', 'cleanup', '私有套件租约释放提示', 'setup_private_session.py', 'close_setup_private_session'),
 ('coverage.test_selection', 'netlist', 'Test 选择与启用', 'private_netlist.py', 'validate_capture_corners'),
 ('coverage.corner_mask', 'netlist', '原生与保存态 Corner 启用', 'private_netlist.py', 'validate_capture_corners'),
 ('coverage.identity', 'netlist', 'Test / Corner 身份匹配', 'private_netlist.py', 'validate_capture_corners'),
 ('coverage.parameters', 'netlist', '有效参数与扫参数量', 'private_netlist.py', 'validate_capture_corners'),
 ('coverage.points', 'netlist', '缺失、重复与意外点', 'private_netlist.py', 'validate_capture_corners'),
 ('coverage.unknown', 'netlist', '可选语义尚未验证', 'private_netlist.py', 'validate_capture_corners'),
 ('scheduler.query_capability', 'scheduler', '识别集群状态查询命令', 'environment_probes.py', '_scheduler_capability'),
 ('scheduler.cli_capability', 'scheduler', '识别批处理调度器提交与取消接口', 'environment_probes.py', '_scheduler_capability'),
 ('scheduler.current_policy', 'scheduler', '继承 Maestro Job Setup 与手动覆盖', 'environment_probes.py', 'run_environment_probes'),
 ('scheduler.auth_context', 'scheduler', '继承集群账号环境', 'environment_probes.py', 'query_scheduler_jobs'),
 ('scheduler.probe_home_cleanup', 'cleanup', '清理集群查询的临时工作目录', 'environment_probes.py', '_cleanup_native_home'),
 ('scheduler.context', 'scheduler', '建立集群只读查询环境', 'environment_probes.py', 'query_scheduler_jobs'),
 ('scheduler.jobs', 'scheduler', '确认本次已提交的 Job ID', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('scheduler.job', 'scheduler', '查询本次批处理作业状态', 'environment_probes.py', 'query_scheduler_jobs'),
 ('scheduler.invalid_job_ids', 'scheduler', '拒绝无效的 Job ID', 'environment_probes.py', 'query_scheduler_jobs'),
 ('scheduler.query_capacity', 'scheduler', '限制本次作业查询数量', 'environment_probes.py', 'query_scheduler_jobs'),
 ('scheduler.id_capacity', 'scheduler', '记录本次已发现的作业数量', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('scheduler.observe_capacity', 'scheduler', '集群查询队列容量', 'environment_test.py', 'EnvironmentTestManager._scheduler'),
 ('scheduler.observe_deadline', 'scheduler', '结束剩余集群只读查询', 'environment_test.py', 'EnvironmentTestManager._run'),
 ('native.run_window', 'netlist', '原生 Run / Wait 错误窗口', 'private_netlist.py', '_capture_phase'),
 ('native.run_id', 'netlist', '原生 Run ID 资格', 'private_netlist.py', '_capture_phase'),
 ('native.history_lookup', 'netlist', '原生 Run Data / History 资格', 'private_netlist.py', '_capture_phase'),
 ('native.history_name', 'netlist', '原生 History 名称读取', 'storage_policy.py', 'native_storage_capture_definitions'),
 ('native.probe_home_cleanup', 'cleanup', '清理环境检查的临时工作目录', 'environment_probes.py', '_cleanup_native_home'),
 ('native.environment_isolation', 'environment', '继承 Virtuoso 启动环境', 'environment_probes.py', 'run_environment_probes'),
 ('native.version', 'environment', '读取 EDA 工具版本', 'environment_probes.py', 'run_environment_probes'),
 ('host.runtime', 'environment', '操作系统与当前 Python 版本', 'environment_probes.py', 'run_environment_probes'),
 ('storage.filesystem', 'environment', '检查仿真目录可写性', 'environment_probes.py', 'run_environment_probes'),
 ('storage.policy', 'environment', '继承 Maestro 仿真结果目录', 'environment_probes.py', 'run_environment_probes'),
 ('report.queue_capacity', 'report', '诊断日志写入队列已满', 'environment_test.py', 'EnvironmentTestManager._log'),
 ('report.write', 'report', '写入诊断日志文件', 'environment_test.py', 'EnvironmentTestManager._log_writer'),
 ('report.flush', 'report', '等待诊断日志写完', 'environment_test.py', 'EnvironmentTestManager._run'),
 ('log.capacity', 'report', '诊断证据容量已满', 'environment_test.py', 'EnvironmentTestManager._log'),
 ('environment.probe_join', 'environment', '结束独立环境检查', 'environment_test.py', 'EnvironmentTestManager._run'),
 ('test.scope', 'report', '三级诊断与共同检查范围', 'environment_test.py', 'EnvironmentTestManager._run'),
 ('test.summary', 'report', '本次诊断总结果', 'environment_test.py', 'EnvironmentTestManager._run'),
 ('reader.parameter_metadata', 'results', '仿真参数回读', 'netlist_measurement.py', 'verify_actual_parameters'),
 ('reader.output_observations', 'results', '目标与约束表达式读取', 'netlist_measurement.py', 'evaluate_task_outputs'),
 ('statistics_and_dataset', 'results', '附加数据导出往返与统计检查（默认暂缓）', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('termination_and_cleanup', 'cleanup', '结束任务并核验清理', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('identity_numeric_verification', 'netlist', '网表参数与角点一致性', 'private_netlist.py', 'native_netlist_identity'),
 ('simulator.readiness', 'models', '求解器及模型可用性', 'simulators.py', 'simulator_readiness'),
 ('execution.pipeline', 'request', '统一求解与提交链路', 'environment_diagnostics.py', 'pipeline_qualification'),
 ('execution.not_reached', 'request', '前置失败后尚未到达的阶段', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('parameter_application', 'netlist', '核验原生 Run 的候选参数', 'netlist_optimizer.py', 'PrivateNetlistOptimizer.evaluate'),
 ('source_guard', 'netlist', '核验原电路未被修改', 'workspace.py', 'verify_sources_unchanged'),
 ('completion_evidence', 'results', '核验本次候选仿真完成', 'netlist_execution.py', 'NetlistExecutionContext.evaluate'),
 ('raw_cleanup_proof', 'cleanup', '核验原始仿真任务已结束', 'runner.py', 'run_optimization'),
 ('simulator_log_read', 'solver', '读取本次求解器日志', 'environment_test.py', 'EnvironmentTestManager._observe_candidate'),
 ('scheduler', 'scheduler', '批处理作业提交与状态核验', 'environment_probes.py', 'query_scheduler_jobs'),
 ('metrics', 'results', '计算优化目标与约束', 'metrics.py', 'evaluate_metrics'),
 ('runtime', 'environment', '当前 Python、界面和工具配置', 'environment_probes.py', 'run_environment_probes'),
 ('python', 'environment', 'Python 与界面运行环境', 'environment_probes.py', 'run_environment_probes'),
 ('native', 'environment', 'Virtuoso 启动环境与工具', 'environment_probes.py', 'run_environment_probes'),
 ('dependencies', 'environment', '运行依赖', 'environment_probes.py', 'run_environment_probes'),
 ('package', 'environment', _contract_text('Crescendo 安装包与插件资源'), 'environment_probes.py', 'run_environment_probes'),
 ('storage', 'environment', '仿真与工作目录', 'environment_probes.py', 'run_environment_probes'),
 ('setup', 'environment', '当前 Maestro 设置', 'environment_probes.py', 'run_environment_probes'),
 ('pdk', 'models', 'PDK 与器件参数配置', 'environment_probes.py', 'run_environment_probes'),
 ('diagnostics', 'report', '收集错误证据', 'environment_test.py', 'EnvironmentTestManager._collect_inline'),
 ('report', 'report', '写入诊断报告', 'environment_test.py', 'EnvironmentTestManager._log_writer'),
 ('termination', 'cleanup', '等待仿真停止', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('stop_request', 'cleanup', '请求停止当前仿真', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('cancel', 'cleanup', '停止诊断任务', 'environment_test.py', 'EnvironmentTestManager.stop'),
 ('files', 'cleanup', '保留可手工检查的网表', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('deadline', 'request', '本次案例的诊断期限', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('launch', 'request', '建立本次独立仿真任务', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('request', 'request', '固定本次候选与优化设置', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('execution', 'request', '本次案例结果', 'environment_test.py', 'EnvironmentTestManager._engine'),
 ('test', 'report', '诊断范围与汇总', 'environment_test.py', 'EnvironmentTestManager._run'),
 ('environment', 'environment', '运行环境检查', 'environment_test.py', 'EnvironmentTestManager._run')]


# Codes identify a stable diagnostic category, not a run, test name or job ID.
# Hash inputs are permanent catalog IDs; never recycle or rename published IDs.
_CODE_STAGE_KEYS = ('execution.native_corner_coverage', 'execution.native_capture_point_coverage',
                    'execution.native_corner_setup_identity', 'execution.diagnostic_request',
                    'execution.native_history_path_readback')
_PRODUCER_CODE_KEYS = ('coverage.corner_mask', 'coverage.identity', 'coverage.parameters', 'coverage.points', 'coverage.test_selection', 'coverage.unknown', 'deadline', 'dependencies.alps_bin', 'dependencies.ocean_bin', 'dependencies.python', 'dependencies.spectre_bin', 'dependencies.tkinter', 'dependencies.virtuoso_bin', 'diagnostics.collection', 'environment.probe_join', 'environment.probes', 'execution', 'execution.not_reached', 'execution.pipeline', 'files', 'host.runtime', 'identity_numeric_verification', 'launch', 'log.capacity', 'native.environment_isolation', 'native.probe_home_cleanup', 'native.version.alps_bin', 'native.version.ocean_bin', 'native.version.spectre_bin', 'native.version.virtuoso_bin', 'package.resources', 'pdk.cdf_metadata', 'python.dependencies_and_tk', 'reader.output_observations', 'reader.parameter_metadata', 'request', 'runtime.summary', 'scheduler.auth_context', 'scheduler.cli_capability', 'scheduler.context', 'scheduler.current_policy', 'scheduler.id_capacity', 'scheduler.invalid_job_ids', 'scheduler.jobs', 'scheduler.observe_capacity', 'scheduler.observe_deadline', 'scheduler.probe_home_cleanup', 'scheduler.query', 'scheduler.query_capability', 'scheduler.query_capacity', 'setup.snapshot', 'simulator.readiness', 'simulator_log_read', 'statistics_and_dataset', 'stop_request', 'storage.filesystem.local_cache', 'storage.filesystem.run', 'storage.filesystem.simulation', 'storage.filesystem.state', 'storage.filesystem.work', 'storage.policy', 'storage.requested', 'termination', 'termination_and_cleanup', 'test.scope', 'test.summary')
_CODE_KEYS = tuple(sorted(set(row[0] for row in CATALOG) | set(_PRODUCER_CODE_KEYS) | set(_CODE_STAGE_KEYS) | set(('other',))))
def _code_for_key(key):
    return _contract_text('CRESCENDO-') + hashlib.sha256(key.encode('utf-8')).hexdigest()[:10].upper()

_COVERAGE_CODES = {
    'coverage.points.missing': _contract_text('CRESCENDO-COV-101'),
    'coverage.points.duplicate': _contract_text('CRESCENDO-COV-102'),
    'coverage.points.unexpected': _contract_text('CRESCENDO-COV-103'),
    'coverage.points.rdb_missing': _contract_text('CRESCENDO-COV-111'),
    'coverage.points.rdb_duplicate': _contract_text('CRESCENDO-COV-112'),
    'coverage.points.rdb_unexpected': _contract_text('CRESCENDO-COV-113'),
    'coverage.identity.mapping': _contract_text('CRESCENDO-COV-201'),
    'coverage.identity.corner': _contract_text('CRESCENDO-COV-202'),
    'coverage.parameters.variable': _contract_text('CRESCENDO-COV-301'),
    'coverage.parameters.sweep': _contract_text('CRESCENDO-COV-302'),
    'coverage.parameters.plan': _contract_text('CRESCENDO-COV-303'),
    'coverage.test_selection.unexpected': _contract_text('CRESCENDO-COV-401'),
    'coverage.test_selection.duplicate_corner': _contract_text('CRESCENDO-COV-402'),
    'coverage.corner_mask.group': _contract_text('CRESCENDO-COV-501'),
    'coverage.corner_mask.nominal': _contract_text('CRESCENDO-COV-502'),
    'coverage.corner_mask.unreadable': _contract_text('CRESCENDO-COV-503'),
    'coverage.unknown.numeric': _contract_text('CRESCENDO-COV-601'),
    'coverage.unknown.members_unavailable': _contract_text('CRESCENDO-COV-602'),
}
_SUITE_CODES = {
    'suite.lease': _contract_text('CRESCENDO-SUITE-LEASE'),
    'suite.lock': _contract_text('CRESCENDO-SUITE-LOCK'),
    'suite.snapshot': _contract_text('CRESCENDO-SUITE-SNAPSHOT'),
    'suite.recovery': _contract_text('CRESCENDO-SUITE-RECOVERY'),
    'suite.rollback': _contract_text('CRESCENDO-SUITE-ROLLBACK'),
    'suite.permission': _contract_text('CRESCENDO-SUITE-PERMISSION'),
    'suite.release': _contract_text('CRESCENDO-SUITE-RELEASE'),
}
_SUITE_EXPLANATIONS = {
    'suite.lease': '另一进程持有套件租约；等待其释放后重试。',
    'suite.lock': '私有视图存在原生锁；由所属编辑器关闭后重试，保留锁及视图。',
    'suite.snapshot': '持久快照缺失或不兼容；不要按名称猜测成员或作为可运行套件重开。',
    'suite.recovery': '此前原生活动或刷新状态尚未证明安全；保留现场并检查恢复记录。',
    'suite.rollback': '刷新回滚未完成；保留登记视图和备份，检查证据后恢复。',
    'suite.permission': '退出时未能恢复私有视图的所有者写权限；检查具体路径和原生锁。',
    'suite.release': '套件租约释放报告异常；检查所属进程及释放证据。',
}


def _suite_code_in_check(check, primary):
    """Inspect only explicit diagnostic fields, never arbitrary snapshots."""
    details = check.get('details') or {}
    if not isinstance(details, dict):
        details = {}
    values = [check.get('diagnostic_code'), check.get('message'),
              primary.get('message'), primary.get('error'),
              details.get('message'), details.get('error')]
    for warning in (check.get('warnings'), details.get('warnings')):
        if isinstance(warning, list):
            values.extend(warning[:16])
    for value in values:
        if not isinstance(value, str):
            continue
        text = value[:8192]
        # These legacy close warnings do not yet carry explicit producer codes.
        if text.startswith(('Private view permissions could not be restored:',
                            'Working view permissions could not be restored:',
                            'Working-copy view permissions could not be restored:')):
            return 'suite.permission'
        if text.startswith('Suite lease could not be released:'):
            return 'suite.release'
        for key, code in _SUITE_CODES.items():
            if code in text:
                return key
    return None


_CODE_LOOKUP = dict((_code_for_key(key), key) for key in _CODE_KEYS)
_CODE_LOOKUP.update(dict((code, key) for key, code in _COVERAGE_CODES.items()))
_CODE_LOOKUP.update(dict((code, key) for key, code in _SUITE_CODES.items()))

def resolve_diagnostic_code(code):
    """Return the stable category ID, or None for an unknown code."""
    return _CODE_LOOKUP.get(str(code).upper())

def diagnostic_code(identifier, status, primary=None):
    if status not in ('FAIL', 'WARN', 'BLOCKED'):
        return None
    if identifier in _SUITE_CODES:
        return _SUITE_CODES[identifier]
    stage_key = 'execution.' + str((primary or {}).get('failure_stage') or '')
    if identifier == 'execution' and stage_key in _CODE_STAGE_KEYS:
        return _code_for_key(stage_key)
    for prefix in ('reader.parameter_metadata.', 'reader.output_observations.', 'scheduler.job.'):
        if identifier.startswith(prefix):
            identifier = prefix[:-1]
            break
    if identifier in _COVERAGE_CODES:
        return _COVERAGE_CODES[identifier]
    return _code_for_key(identifier if identifier in _CODE_KEYS else 'other')


def diagnostic_run(spec):
    """Only the existing validated one-candidate marker changes retention."""
    from .runner import _environment_test_run
    try:
        return _environment_test_run(spec or {})
    except (ValueError, TypeError):
        return False


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _identifier(check):
    name = str(check.get('check_id') or check.get('id') or '')
    for simulator in ('spectre', 'alps'):
        for engine in ENGINE_NAMES:
            prefix = simulator + '.' + engine + '.'
            if name.startswith(prefix):
                return name[len(prefix):]
        if name.startswith(simulator + '.'):
            name = name[len(simulator) + 1:]
    return name


def _scope(check, context):
    simulator, engine = check.get('simulator'), check.get('engine')
    case = context.get('case_id') or check.get('case_id')
    if not case and simulator and engine in ENGINE_NAMES:
        case = simulator + '.' + engine
    if not case:
        for sim in ('spectre', 'alps'):
            for source in ENGINE_NAMES:
                if str(check.get('id', '')).startswith(sim + '.' + source + '.'):
                    case, simulator, engine = sim + '.' + source, sim, source
    if case:
        simulator, engine = case.split('.', 1) if '.' in case else (simulator, engine)
        return case, '%s · %s' % ((simulator or '').upper() if simulator == 'alps' else (simulator or '').title(), ENGINE_NAMES.get(engine, engine)), case
    solver_scope = simulator if _identifier(check).startswith('simulator.readiness') else None
    return solver_scope or 'environment', (solver_scope.upper() if solver_scope == 'alps' else solver_scope.title()) + ' · 工具配置' if solver_scope else '运行环境', None


def _evidence(value, root=None, output=None, depth=0, _state=None):
    output = [] if output is None else output
    _state = {'remaining': 512, 'seen': set()} if _state is None else _state
    if depth > 12 or len(output) >= 64 or _state['remaining'] <= 0:
        return output
    _state['remaining'] -= 1
    if isinstance(value, (dict, list)) and id(value) in _state['seen']:
        return output
    _state['seen'].add(id(value))
    if isinstance(value, dict):
        for key, item in value.items():
            if len(output) >= 64 or _state['remaining'] <= 0:
                break
            _state['remaining'] -= 1
            if key in ('prepared', 'prepared_snapshot'):
                continue
            if isinstance(key, str) and isinstance(item, str) and (key in ('path', 'diagnostic_path', 'details_path', 'history_readback_path', 'console_source', 'reported_file', 'script_path', 'source_setup_path', 'netlist_path', 'scs_path') or key.endswith('_log')) and item:
                attribution = 'reported_literal' if key in ('reported_file', 'console_source') and not os.path.isabs(item) else 'owned_artifact'
                path = os.path.join(root, item) if root and not os.path.isabs(item) and attribution == 'owned_artifact' else item
                record = {'path': path, 'role': key, 'locator': value.get('xml_path') or value.get('field') or value.get('line') or value.get('reported_line'), 'attribution': attribution}
                if record not in output:
                    output.append(record)
            elif isinstance(item, (dict, list)):
                _evidence(item, root, output, depth + 1, _state)
    elif isinstance(value, list):
        for item in value[:64]:
            _evidence(item, root, output, depth + 1, _state)
    return output


def stage_section(primary):
    stage = str((primary or {}).get('failure_stage') or '')
    if stage == 'native_corner_coverage' and (primary or {}).get('phase') == 'completed_native_point_identity':
        return 'results'
    if stage == 'native_point_parameter_readback':
        return 'results'
    if stage.startswith(('model', 'native_corner_coverage', 'native_capture', 'native_user_script', 'native_template',
                         'native_parameter', 'native_device_parameter', 'native_run', 'native_history',
                         'netlist', 'capture', 'workspace', 'parameter', 'maestro_startup',
                         'source_guard', 'source_freshness', 'prepared_freshness', 'native_file_input')):
        return 'netlist'
    if stage.startswith(('measurement', 'output', 'result_reader')):
        return 'results'
    if stage.startswith(('submit', 'submission', 'scheduler')):
        return 'scheduler'
    if stage.startswith(('stop', 'cleanup', 'termination')):
        return 'cleanup'
    if stage.startswith(('simulator', 'command_render', 'solver_configuration')):
        return 'models'
    if stage.startswith(('solve', 'spectre', 'alps')):
        return 'solver'
    return 'request'


def _current_cdf_failures(value, output=None, depth=0, _state=None):
    """Presentation consumes structured current evidence, never searches prose."""
    output = [] if output is None else output
    _state = {'remaining': 256, 'seen': set()} if _state is None else _state
    if depth > 12 or len(output) >= 8 or _state['remaining'] <= 0:
        return output
    _state['remaining'] -= 1
    if isinstance(value, (dict, list)) and id(value) in _state['seen']:
        return output
    _state['seen'].add(id(value))
    if isinstance(value, dict):
        if value.get('observation') and value['observation'] != 'diagnostic.cdf_callback_failure':
            return output
        rows = value.get('cdf_callback_failures')
        if isinstance(rows, list):
            for row in rows[:8]:
                if (isinstance(row, dict) and row.get('schema_version') == _contract_text('crescendo.cdf_callback_failure.v1')
                        and row.get('callback_error') and row.get('property') and row not in output):
                    output.append(row)
                    if len(output) >= 8:
                        return output
        for key, child in value.items():
            if _state['remaining'] <= 0:
                break
            _state['remaining'] -= 1
            if key not in ('cdf_callback_failures', 'history', 'examples', 'previous_run', 'snapshot'):
                if isinstance(child, (dict, list)):
                    _current_cdf_failures(child, output, depth + 1, _state)
    elif isinstance(value, list):
        for child in value[:64]:
            _current_cdf_failures(child, output, depth + 1, _state)
    return output


def _cdf_display_value(value):
    if not isinstance(value, (str, int, float, bool, type(None))):
        return '<unsupported diagnostic value>'
    text = _json(value)
    return text if len(text) <= 256 else text[:256] + '…（界面截断）'


def _native_worker_explanation(reports):
    """Display producer-reported worker locations, never scan/qualify logs."""
    if reports and all(isinstance(report, dict) and report.get('schema_version') == _contract_text('crescendo.native_worker_diagnostics.v1')
                       and report.get('logs') == [] and not (report.get('parent_capture_failed') is True
                           and report.get('expected_wrapper_aftermath_samples')) for report in reports):
        return '尚无原生 worker 日志；采集完成不代表 worker 或网表生成已通过。'
    paths, rows, seen = [], [], set()
    aftermath = []
    for report in reports[:8]:
        if not isinstance(report, dict) or report.get('parent_capture_failed') is not True:
            continue
        for sample in (report.get('expected_wrapper_aftermath_samples') or [])[:2]:
            if not isinstance(sample, dict) or len(aftermath) >= 2:
                continue
            path, number = sample.get('console_source'), sample.get('line')
            message = sample.get('message')
            if not isinstance(message, str) or not message:
                continue
            location = path if isinstance(path, str) else '原生 worker 日志'
            if isinstance(path, str) and path not in paths:
                paths.append(path)
            if isinstance(number, int) and not isinstance(number, bool) and number > 0:
                location += ':' + str(number)
            item = location + ' · ' + message[:512]
            if item not in aftermath:
                aftermath.append(item)
    pending = collections.deque((report, None, 0) for report in reports[:8] if isinstance(report, dict))
    inspected = 0
    while pending and inspected < 256 and len(rows) < 12:
        value, parent_path, depth = pending.popleft()
        if depth > 10 or not isinstance(value, (dict, list)) or id(value) in seen:
            continue
        seen.add(id(value))
        inspected += 1
        if isinstance(value, dict):
            if value.get('fresh') is False or value.get('ownership_proven') is False:
                continue
            path = value.get('path') or value.get('log_path') or value.get('source_path') or parent_path
            if isinstance(path, str) and path not in paths and len(paths) < 8:
                paths.append(path)
            text = value.get('text') or value.get('error')
            number = value.get('line') if value.get('line') is not None else value.get('line_number')
            if isinstance(text, str) and text:
                location = path if isinstance(path, str) else '原生 worker 日志'
                if isinstance(number, int) and not isinstance(number, bool) and number > 0:
                    location += ':' + str(number)
                row = location + ' · ' + text[:512]
                if row not in rows:
                    rows.append(row)
            pending.extend((child, path, depth + 1) for key, child in list(value.items())[:64]
                           if key != 'expected_wrapper_aftermath_samples'
                           if isinstance(child, (dict, list)))
        else:
            pending.extend((child, parent_path, depth + 1) for child in value[:64]
                           if isinstance(child, (dict, list)))
    if aftermath:
        rows += ['捕获器主动返回后的预期提示（辅助证据，不作为根因）：'] + aftermath
    return '\n'.join(['日志：' + path for path in paths] + rows) or '原生执行器已记录 worker 证据；具体结构见本小节总日志。'


def _cdf_failure_explanation(row):
    prop = str(row.get('property') or '')[:64]
    selected = row.get('selected_parameter_names') or [prop]
    selected = {name for name in selected[:32] if isinstance(name, str)} if isinstance(selected, list) else {prop}
    lines = ['选中参数 %s：请求值 %s，回调入口 OA 值 %s，CDF 值 %s。' % (
        prop, _cdf_display_value(row.get('requested_value')),
        _cdf_display_value(row.get('db_property_value')), _cdf_display_value(row.get('cdf_value')))]
    fields = row.get('expression_fields') or []
    fields = [field for field in fields[:16]
              if isinstance(field, dict) and isinstance(field.get('name'), str)
              and field.get('name') not in selected
              and field.get('name') != prop] if isinstance(fields, list) else []
    if fields:
        lines.append('失败现场的未选参数字符串：' + '；'.join(
            '%s=%s%s' % (str(field.get('name'))[:64], _cdf_display_value(field.get('value')),
                         '（已截断）' if field.get('value_truncated') else '')
            for field in fields) + _contract_text('。这些字段未被 Crescendo 替换成常数。'))
    if row.get('native_error'):
        lines.append('本轮原始错误：' + str(row['native_error'])[:2048])
    matching = [str(field.get('name'))[:64] for field in fields
                if field.get('name') in (row.get('matching_expression_fields') or [])]
    if matching and row.get('native_error'):
        lines.append('原错误中的 int 字符串与未选字段 %s 相同，可能有关；'
                     '尚未证明厂商回调实际读取哪个字段，也不表示 Maestro 变量应被改写。' % '、'.join(matching))
    else:
        lines.append('这些字符串仅是失败现场的上下文，不能据此认定它们导致了回调错误。')
    if (row.get('expression_fields_truncated') or row.get('remaining_cdf_fields_uninspected')
            or row.get('expression_fields_status') == 'unavailable'):
        lines.append('伴随字段采集不完整；完整原始错误保留在本轮日志中。')
    if (any(row.get(key) for key in ('callback_truncated', 'selected_parameter_names_truncated',
            'selected_cdf_metadata_truncated', 'form_init_proc_truncated', 'done_proc_truncated'))
            or any(field.get('name_truncated') or field.get('metadata_truncated') for field in fields)):
        lines.append('部分字段名或 CDF 元数据已截断；这些记录不证明回调上下文完整。')
    source = row.get('path') or row.get('console_source')
    if source:
        lines.append('本轮证据：%s，第 %s 行；生成补丁及其原生报错位置见本小节证据。' %
                     (str(source)[:2048], str(row.get('console_line', '?'))[:32]))
    return '\n'.join(lines)


def describe_check(check, case_context=None):
    context = case_context or {}
    identifier = _identifier(check)
    row = next((row for row in CATALOG if identifier == row[0] or identifier.startswith(row[0] + '.')), None)
    section, title, filename, function = row[1:] if row else ('report', '其他诊断观察：' + identifier, 'environment_test.py', 'EnvironmentTestManager._check')
    if check.get('engine') == 'maestro_baseline' and identifier in ('execution.pipeline', 'parameter_application', 'completion_evidence'):
        title = {'execution.pipeline': '原生 Maestro 执行链路', 'parameter_application': '原生 Device Parameters 生效验证', 'completion_evidence': '精确 history 与本次仿真完成'}[identifier]
        filename, function = 'native_baseline.py', 'evaluate_native_baseline'
    if identifier.startswith(('native.version.', 'dependencies.')):
        title += '：' + identifier.rsplit('.', 1)[-1].replace('_bin', '')
    elif identifier.startswith('scheduler.job.'):
        title += ' · Job ' + identifier.rsplit('.', 1)[-1]
    primary = context.get('first_cause') or (check.get('details') or {}).get('first_cause') or {}
    if identifier == 'files' and context.get('retention_status') == 'no_generated_scs_observed_check_capture_evidence':
        title = '保留诊断证据（尚未生成 SCS）'
    if identifier == 'execution' and primary:
        section = stage_section(primary)
        title = '本次案例首个阻塞：' + SECTIONS[section]
    group, group_label, case_id = _scope(check, context)
    status = check.get('status') or 'NOT_TESTED'
    message = str(check.get('message') or '')
    primary_message = str(primary.get('message') or primary.get('error') or '')
    blocked_by = None
    if status == 'NOT_TESTED' and primary and not identifier.startswith('reader.'):
        blocked_by = {'failure_stage': primary.get('failure_stage'), 'message': primary_message,
                      'section_id': stage_section(primary)}
        summary = '未执行：前面的“%s”未完成。' % SECTIONS[stage_section(primary)]
        explanation = summary + (' 首个原因：' + primary_message if primary_message else '')
        status_label = '前置阻塞，未执行'
    elif identifier == 'execution' and primary_message and status != 'PASS':
        summary = primary_message
        explanation = '本案例首先停在“%s”；后续未到达的阶段不表示求解器自身失败。\n原始原因：%s' % (SECTIONS[section], primary_message)
        status_label = STATUS_NAMES.get(status, status)
    else:
        summary = message
        explanation = message
        status_label = STATUS_NAMES.get(status, status)
        if status == 'NOT_TESTED' and identifier.startswith('reader.'):
            summary = '本次结果读取尚未提供这项可核验信息。'
            explanation = summary + '\n读取器说明：' + message + ('\n本案例首个阻塞：' + primary_message if primary_message else '')
        elif message == 'No native evidence available for this stage':
            summary = '当前阶段没有产生可核验记录。'
            explanation = summary + '请查看本案例的首个阻塞原因；这不等同于该功能自身报错。'
        elif message == 'Native producer evidence':
            summary = ('此项原生证据通过核验。' if status == 'PASS' else
                       '此项原生证据未通过核验；请查看具体证明字段。' if status == 'FAIL' else
                       '此项原生证据尚未验证。')
            explanation = summary
    stage = str(primary.get('failure_stage') or '')
    if stage == 'diagnostic_request' and status != 'PASS':
        if identifier == 'execution':
            title = '开始仿真前的准备检查'
            filename, function = 'environment_test.py', 'EnvironmentTestManager._run'
        explanation = primary_message
        for reason in primary.get('blocking_reason_labels') or []:
            explanation += '\n未满足：' + str(reason)
        raw_state = primary.get('ui_prepared_state') or {}
        raw_error = raw_state.get('error')
        if raw_error:
            explanation += '\n原始界面说明：' + (str(raw_error.get('text')) + '（已截断，完整身份摘要见证据）' if isinstance(raw_error,dict) else str(raw_error))
        for action in primary.get('recovery_actions') or []:
            explanation += '\n下一步：' + str(action)
    stage_explanations = {
        'native_corner_coverage': '原设置与实际生成网表的测试/角点覆盖不一致，尚未求解。',
        'native_capture_point_coverage': '部分测试/角点的网表未生成；请核对证据中的 missing / duplicate。',
        'native_history_path_readback': '原生 history 路径回读资格检查失败；请查看具体路径及原始错误。',
        'native_capture_skill_error': '原生捕获报告错误；此分组不证明模型继承失败。stdout 和 stderr 的扫描顺序不代表跨流时间顺序。'}
    completed_point_identity = (stage == 'native_corner_coverage' and
                                primary.get('phase') == 'completed_native_point_identity')
    if completed_point_identity:
        stage_explanations[stage] = '已进入原生结果核验，测试/角点或模型身份未匹配；请对照模型解析证据，不能据此判定求解器失败。'
    missing_expression = any('missing_output_expression' in str(primary.get(key) or '')
                             for key in ('message', 'error'))
    if stage in ('measurement_preflight', 'missing_output_expression') and missing_expression:
        stage_explanations[stage] = '本次未从保存设置读到所选目标/约束表达式，求解前停止；这不代表用户的表达式为空。'
    if identifier == 'execution' and primary and status != 'PASS':
        if stage.startswith(('native_parameter', 'native_device_parameter', 'native_run')):
            filename, function = 'private_netlist.py', 'prepare_maestro_netlists'
            title = '原生 Maestro Run / Device Parameters 未完成'
        elif completed_point_identity:
            filename, function = 'completion.py', 'validate_candidate_completion'
            title = '本次结果的角点与模型身份核验'
        elif stage in ('native_corner_coverage', 'native_capture_point_coverage', 'native_capture_skill_error'):
            filename, function = 'private_netlist.py', '_capture_phase'
        elif stage == 'native_history_path_readback':
            filename, function = 'storage_policy.py', 'qualify_native_history_paths'
        elif stage in ('measurement_preflight', 'missing_output_expression') and missing_expression:
            filename, function = 'netlist_measurement.py', 'qualify_expressions'
    if primary and status != 'PASS' and stage in stage_explanations:
        explanation = stage_explanations[stage] + '\n' + explanation
        if identifier == 'execution' and stage != 'native_capture_skill_error':
            summary = stage_explanations[stage]
    if identifier == 'execution' and stage == 'native_capture_skill_error' and status != 'PASS':
        title = '本次案例原生捕获错误'
        explanation = (stage_explanations[stage] + '\n原始错误：' + primary_message +
                       '\n后续未到达的阶段不表示求解器自身失败。')
    if identifier == 'parameter_application' and (check.get('details') or {}).get('proof_kind') == 'native_run_device_parameters':
        title = '原生 Maestro Run 的实际器件参数'
        summary = ('所选参数已在各测试/角点的实际网表中核验。' if status == 'PASS' else
                   '原生 Run 参数未通过核验；请查看具体测试/角点及 SCS。')
        explanation = summary + '\n证明来自本轮 Run 的 Device Parameters / DPAR 与 SCS；不代表私有 schematic 已回写参数。'
        parameters = (check.get('details') or {}).get('parameters') or []
        for parameter in parameters[:16]:
            if not isinstance(parameter, dict):
                continue
            explanation += '\n参数 %s：请求 %s' % (parameter.get('parameter_id'), _cdf_display_value(parameter.get('requested')))
            points = parameter.get('points') or []
            for point in points[:32]:
                if isinstance(point, dict):
                    explanation += '\n  Test=%s · Corner=%s · Point=%s · effective=%s · DPAR=%s\n  SCS=%s' % (
                        point.get('test'), point.get('corner'), point.get('point_id'),
                        _cdf_display_value(point.get('effective')), point.get('dpar_status'), point.get('scs_path'))
            if len(points) > 32:
                explanation += '\n  其余点见本轮总日志；本轮仍按全部测试/角点核验。'
        if len(parameters) > 16:
            explanation += '\n其余参数见本轮总日志。'
    callbacks = []
    if status in ('FAIL', 'BLOCKED', 'WARN') and identifier in ('execution', 'parameter_application'):
        callbacks = _current_cdf_failures(check.get('details') or {})
        _current_cdf_failures(context.get('cdf_callback_failures') and {
            'cdf_callback_failures': context['cdf_callback_failures']} or {}, callbacks)
        if not callbacks:
            _current_cdf_failures(context.get('records') or [], callbacks)
        if callbacks:
            callback_summary = '选中参数 %s 的 PDK 回调报错。' % str(callbacks[0]['property'])[:64]
            if not primary:
                section = 'netlist'
                title = 'PDK 回调未完成：将候选参数写入私有电路'
                filename, function = 'workspace.py', 'write_schematic_patch'
                summary = callback_summary
            explanation += '\n\n本轮回调失败上下文：' + callback_summary + '\n' + '\n\n'.join(
                _cdf_failure_explanation(row) for row in callbacks[:3])
            if len(callbacks) > 3 or context.get('cdf_callback_failures_truncated'):
                explanation += '\n界面仅展示部分回调失败；其余见本轮总日志的本小节。'
    workers = []
    if status in ('FAIL', 'BLOCKED', 'WARN') and identifier in ('execution', 'parameter_application'):
        from .environment_diagnostics import native_worker_reports
        workers = list(context.get('native_worker_diagnostics') or [])[:8]
        for report in native_worker_reports(check.get('details') or {}):
            if report not in workers and len(workers) < 8:
                workers.append(report)
        if workers:
            explanation += '\n\n本轮原生 worker 记录（资格由原生执行器核验）：\n' + _native_worker_explanation(workers)
    evidence = _evidence(check.get('details') or {}, context.get('run_root'))
    if callbacks:
        _evidence({'cdf_callback_failures': callbacks}, context.get('run_root'), evidence)
    if workers:
        _evidence({'native_worker_diagnostics': workers}, context.get('run_root'), evidence)
    if context.get('log_path'):
        evidence.append({'path': context['log_path'], 'role': '分类诊断日志', 'locator': 'SECTION ' + group + '/' + section})
    suite_key = _suite_code_in_check(check, primary) if status in ('FAIL', 'WARN', 'BLOCKED') else None
    if suite_key:
        suite_row = next(row for row in CATALOG if row[0] == suite_key)
        section, title, filename, function = suite_row[1:]
        explanation = _SUITE_EXPLANATIONS[suite_key] + ('\n原始说明：' + summary if summary else '')
    code = _SUITE_CODES[suite_key] if suite_key else diagnostic_code(identifier, status, primary)
    if code:
        status_label += ' · ' + code
    result = {'diagnostic_code': code, 'schema_version': SCHEMA, 'check_id': check.get('id') or identifier, 'case_id': case_id,
              'group_id': group, 'group_label': group_label, 'section_id': section, 'section_label': SECTIONS[section],
              'title': title, 'status_label': status_label, 'summary': summary, 'explanation': explanation,
              'component': {'label': title, 'file': _contract_text('crescendo_engine/') + filename, 'function': function,
                            'scope': '功能实现归属；只有原始错误明确给出文件/行号才构成故障定位证据'},
              'evidence': evidence}
    if blocked_by:
        result['blocked_by'] = blocked_by
    return result


def observation_section(record):
    kind = str(record.get('observation') or '')
    value = record.get('payload') or {}
    path = str(value.get('path') or '')
    if kind.endswith(('producer_failure', 'first_cause')):
        primary = value.get('first_cause') or value.get('data') or value
        return stage_section(primary.get('detail') or primary)
    name = os.path.basename(path)
    if '/native_point_parameters/' in '/' + path or name in (_contract_text('reader_request.json'), _contract_text('reader_status.json')):
        return 'results'
    if kind.endswith(('source_settings', 'settings', 'netlist_points', 'capture_semantics', 'identity_coverage', 'script_context', 'native_run_context', 'setup_private_session', 'cdf_callback_failure', 'native_worker_diagnostics', 'source_guard', 'native_file_inputs')) or name in (_contract_text('corner_identity.json'),_contract_text('capture_diagnostics.json'),_contract_text('capture_generation.json'),_contract_text('private_netlist.json'),_contract_text('private_netlist_candidate.json'),_contract_text('netlist_generation.json'),'capture.il','bootstrap.il',_contract_text('source_guard.json'),_contract_text('native_file_inputs.json')):
        return 'netlist'
    if kind.endswith('missing_output_source') or 'reader' in kind or name in (_contract_text('result_reader_capability.json'),_contract_text('measurement_error.json'),_contract_text('measurement.json'),_contract_text('completion_evidence.json'),_contract_text('cli_results.json')):
        return 'results'
    if 'cleanup' in kind or name in (_contract_text('worker_shutdown.json'),_contract_text('cleanup_failure.json'),_contract_text('cleanup_diagnostics.json'),_contract_text('guardian_state.json')):
        return 'cleanup'
    if 'scheduler' in kind or name in (_contract_text('native_submission.json'),_contract_text('submission.json'),_contract_text('native_dispatch.jsonl')) or name.startswith('scheduler.'):
        return 'scheduler'
    if name.startswith(('ocean.', 'capture.')):
        return 'netlist'
    if name.startswith(('spectre.', 'alps.', 'simulator.')) or name.endswith((_contract_text('.psf.log'), '.psf.warn')):
        return 'solver'
    if kind.endswith(('collection','scope','identity','matrix_basis')):
        return 'report'
    return 'request'



def _brief_tool_paths(key, value):
    """Keep the small path allowlist readable without exposing launch state."""
    def literal(item):
        if item is None or not isinstance(item, str):
            return None
        raw = item.encode('utf-8')
        limit = 4096 if key == 'tools' else 144
        return raw[:limit].decode('utf-8', 'ignore') + '...[truncated]' if len(raw) > limit else item
    if key == 'tools':
        return {name: literal(value.get(name)) for name in
                ('ocean_bin', 'spectre_bin', 'alps_bin', 'spectre_model_dir', 'alps_model_dir')}
    fields = ('alps_root','alps_bin','root_source','binary_source','resolution_status','resolution_message')
    result = {name: literal(value.get(name)) for name in fields}
    for name in ('candidate_count','omitted_candidate_count'):
        if isinstance(value.get(name), int):
            result[name] = value[name]
    candidates = value.get('candidates') or []
    result['candidates'] = [literal(item) for item in candidates[:4]] if isinstance(candidates, list) else []
    truncated = value.get('truncated_fields') or []
    result['truncated_fields'] = [literal(item) for item in truncated[:16]] if isinstance(truncated, list) else []
    return result

def brief_success(value):
    """Summarize successful evidence; retain identity, paths and useful scalars."""
    if not isinstance(value, dict):
        return {'sha256': __import__('hashlib').sha256(_json(value).encode('utf-8')).hexdigest()}
    if value.get('proof_kind') == 'native_run_device_parameters':
        return value
    scalars = {key: item for key, item in value.items() if item is None or isinstance(item, (str, bool, int, float))}
    if value.get('schema_version') == _contract_text('crescendo.native_worker_diagnostics.v1') and value.get('logs') == []:
        scalars['worker_evidence_status'] = 'no_worker_logs_observed; worker/netlist success not proven'
    for key, item in value.items():
        if key == 'python_version':
            scalars[key] = item
        elif key in ('tools', 'alps_discovery') and isinstance(item, dict):
            scalars[key] = _brief_tool_paths(key, item)
        elif isinstance(item, (dict, list)):
            scalars[key + '_count'] = len(item)
            scalars[key + '_sha256'] = __import__('hashlib').sha256(_json(item).encode('utf-8')).hexdigest()
    return scalars


def _redact_prepared(value):
    """Do not copy complete prepared snapshots into human diagnostic sections."""
    if isinstance(value, dict):
        return {key: ('[prepared snapshot omitted]' if key in ('prepared', 'prepared_snapshot') else _redact_prepared(item))
                for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_prepared(item) for item in value]
    return value


def _problem_record(record):
    def contains_problem(value):
        if isinstance(value, dict):
            if (value.get('ok') is False or value.get('parent_capture_failed') is True
                    or value.get('status') in ('FAIL', 'FAILED', 'BLOCKED', 'INVALID', 'NOT_VERIFIED') or value.get('error')
                    or value.get('same_as') or value.get('evidence_ref')):
                # An interned subtree can point to a failure. Preserve its
                # small reference instead of replacing that reference by a
                # success hash; independently successful siblings still shrink.
                return True
            return any(contains_problem(item) for item in value.values() if isinstance(item, (dict,list)))
        if isinstance(value, list):
            return any(contains_problem(item) for item in value)
        return False
    def focus(value):
        if isinstance(value, dict):
            if value.get('ok') is True and not contains_problem(value):
                return dict(brief_success(value), successful_subtree_summary=True)
            return {key: item if key == 'native_reader_call' else focus(item) for key,item in value.items()}
        if isinstance(value,list):
            return [focus(item) for item in value]
        return value
    return focus(_redact_prepared(record))


def worker_error_observation(context):
    """Select one owned, fresh structured generation error as auxiliary evidence."""
    reports = (context or {}).get('native_worker_diagnostics') if isinstance(context, dict) else None
    if not isinstance(reports, list):
        return None
    for report in reports[:8]:
        if not isinstance(report, dict) or report.get('schema_version') != _contract_text('crescendo.native_worker_diagnostics.v1') or report.get('ok') is not False:
            continue
        owner, logs = report.get('owned_workspace'), report.get('logs')
        if (not isinstance(owner, str) or not os.path.isabs(owner) or len(owner) > 4096 or
                any(ord(char) < 32 for char in owner) or not isinstance(logs, list)):
            continue
        for log in logs[:32]:
            if not isinstance(log, dict) or log.get('fresh') is not True or log.get('ownership_proven') is False:
                continue
            path, errors = log.get('path'), log.get('error_lines')
            if (not isinstance(path, str) or not os.path.isabs(path) or len(path) > 4096 or
                    any(ord(char) < 32 for char in path) or not isinstance(errors, list)):
                continue
            try:
                if os.path.commonpath((os.path.normpath(owner), os.path.normpath(path))) != os.path.normpath(owner):
                    continue
            except (TypeError, ValueError):
                continue
            for error in errors[:32]:
                if not isinstance(error, dict):
                    continue
                line, text = error.get('line'), error.get('text')
                if type(line) is not int or line < 1 or not isinstance(text, str) or not text.strip():
                    continue
                # Wrapper aftermath is not an actual netlist-generation error.
                if 'ASSEMBLER-5031' in text.upper() or error.get('primary_failure') is False:
                    continue
                message = ' '.join(text[:1024].split())
                message = message[:320] + ('…' if len(message) > 320 or len(text) > 1024 else '')
                location = path if len(path) <= 600 else '…' + path[-599:]
                return {'path': location, 'line': line, 'text': message}
    return None


def worker_error_text(context):
    observation = worker_error_observation(context)
    if not observation:
        return ''
    return '原生 worker 观察（辅助证据）：%s:%s · %s' % (
        observation['path'], observation['line'], observation['text'])


def assess_ladder(cases):
    """Describe observed stage patterns without asserting a precise root cause."""
    rows = list(cases.values()) if isinstance(cases, dict) else list(cases or [])
    by_engine = {row.get('engine'): row for row in rows if isinstance(row, dict)}
    order = ('maestro_baseline', 'private_netlist', 'maestro_netlist')
    names = {'maestro_baseline': '原生 Baseline', 'private_netlist': '私有网表 CLI',
             'maestro_netlist': '每候选原生生成网表 CLI'}
    good = [name for name in order if str((by_engine.get(name) or {}).get('state') or '').lower() in ('completed', 'pass')]
    bad = [name for name in order if str((by_engine.get(name) or {}).get('state') or '').lower() in ('failed', 'fail', 'blocked', 'skipped', 'cancelled')]
    conclusions, actions, recovery = [], [], []
    if len(good) == 3:
        conclusions.append('三个阶段均通过本次检查；可使用当前配置先进行小规模优化，再核验实际结果。')
    elif len(good) == 2 and len(bad) == 1:
        failed = bad[0]
        case = by_engine[failed]
        cause = case.get('first_cause') if isinstance(case.get('first_cause'), dict) else {}
        layer = case.get('first_cause_layer') or ''
        if failed == 'private_netlist' and layer == 'netlist_generation':
            conclusions.append('仅私有网表 CLI 阶段未通过，已观测首因属于网表生成层；优先检查模板准备、捕获及改参链。该范围判断不等于精确根因。')
        elif failed == 'maestro_baseline':
            conclusions.append('原生 Baseline 未通过，两条 CLI 阶段通过本次检查；不能据此判定整个环境不可用。')
        else:
            conclusions.append('仅%s阶段未通过；另外两阶段通过本次检查，差异可帮助缩小范围，但不单独证明根因。' % names[failed])
        conclusions.append('本次通过的路径：%s。' % '、'.join(names[name] for name in good))
    elif good:
        conclusions.append('本次通过的路径：%s；其余阶段需结合首因或跳过原因判断。' % '、'.join(names[name] for name in good))
    else:
        conclusions.append('目前没有阶段通过；未执行或阻塞不等于相应功能自身失败。')
    for name in bad:
        case = by_engine[name]
        cause = case.get('first_cause') if isinstance(case.get('first_cause'), dict) else {}
        layer = str(case.get('first_cause_layer') or '').lower()
        suggested = cause.get('recovery_actions') or []
        if isinstance(suggested, str):
            suggested = [suggested]
        stage = str(cause.get('failure_stage') or '').lower()
        reason = str(case.get('skip_reason') or cause.get('message') or '').lower()
        if layer == 'termination_and_cleanup' or stage.startswith(('cleanup', 'termination', 'stop')) or ('stop' in reason and ('unproven' in reason or 'unknown' in reason)):
            action = '核实本轮所属任务和集群作业已停止，保留现场；停止状态明确后重试。'
        elif stage.startswith(('source_', 'prepared_', 'native_file_input')) or 'source guard' in reason:
            action = '核对当前源 setup、所选 Test 及文件身份，重新 Load/Inspect 和 Apply 后重试。'
        elif stage == 'diagnostic_request':
            action = '核对所选参数范围、目标、Test/角点和 CLI 输入，修正首个无效项后重试。'
        elif layer == 'netlist_generation':
            if name == 'private_netlist':
                action = '核对模板捕获的 Test/角点、模型包含和生成 SCS；对照候选参数在网表中的实际改写。'
            else:
                action = '核对私有 Maestro 捕获、Test/角点覆盖、Device Parameters 和生成的 SCS。'
        elif layer == 'solver_command':
            action = '核对该阶段求解器身份、可执行路径、命令模板、模型目录及展开后的参数。'
        elif layer == 'submission':
            action = '核对本次提交命令、队列、作业 ID 和集群返回的拒绝或状态。'
        elif layer == 'solve':
            action = '检查本次求解器日志中的首个错误、模型 section 和实际网表。'
        elif layer == 'measurement':
            action = '核对所选输出表达式与相同 Test/角点结果，检查 reader 和实际 PSF/RDB 路径。'
        elif layer == 'pipeline_proof':
            action = '核对本次候选、Test/角点、求解和结果证据是否完整且属于同一运行。'
        else:
            action = '核对%s阶段首因、所属 Test/角点及其生成、求解或读取证据。' % names[name]
        if action not in actions:
            actions.append(action)
        for suggestion in suggested:
            suggestion = ' '.join(str(suggestion).split())[:350]
            if suggestion and suggestion not in recovery:
                recovery.append(suggestion)
    actions.extend(item for item in recovery if item not in actions)
    if len(good) == 3:
        actions.append('先以少量候选运行优化，并检查各 Test/角点的指标和停止结果。')
    if not actions:
        actions.append('待阶段完成后核对本次候选、Test/角点和实际结果。')
    return {'conclusions': conclusions, 'actions': actions[:3], 'passed_engines': good, 'problem_engines': bad}


def format_ladder_summary(cases, source_simulator, cli_simulator, frozen_candidate=None, frozen_candidate_origin=None, case_contexts=None):
    """Human-readable triage; statuses and equivalence require actual evidence."""
    stages = (('maestro_baseline', '原生 Baseline'), ('private_netlist', '私有网表 CLI'),
              ('maestro_netlist', '每候选原生生成网表 CLI'))
    names = {'queued': '待执行', 'pending': '待执行', 'running': '运行中',
             'completed': '通过', 'pass': '通过', 'failed': '失败', 'fail': '失败',
             'blocked': '阻塞', 'skipped': '跳过', 'cancelled': '已停止'}
    def short(value, limit=350):
        return ' '.join(str(value or '').split())[:limit]
    rows = list(cases.values()) if isinstance(cases, dict) else list(cases or [])
    source, cli = short(source_simulator), short(cli_simulator)
    lines = ['=== 三级诊断摘要 ===',
             '对照范围：请求使用相同冻结候选序列及相同 Test/角点；仅对实际完成且身份核验的点进行比较。',
             '原生 Test 求解器：%s；CLI 求解器：%s。' % (source or '未知', cli or '未知')]
    if frozen_candidate is None:
        lines.append('冻结候选：未知或未生成；不能进行候选对照。')
    else:
        lines.append('冻结候选序列已生成；样本规模见 test.scope，具体参数见对应运行数据。')
    if not source or not cli:
        lines.append('数值对照：求解器身份未知，不能判定数值等价。')
    elif source.lower() != cli.lower():
        lines.append('数值对照：求解器不同，阶段通过不证明跨求解器数值等价。')
    else:
        lines.append('数值对照：即使各阶段通过，也需相同点的实际结果及容差证据才能判定数值等价。')
    assessment = assess_ladder(rows)
    lines.extend('结论：' + item for item in assessment['conclusions'])
    for number, (engine, label) in enumerate(stages, 1):
        case = next((item for item in rows if isinstance(item, dict) and item.get('engine') == engine), {})
        state = str(case.get('state') or 'pending').lower()
        cause = case.get('first_cause') or {}
        cause = cause if isinstance(cause, dict) else {'message': str(cause)}
        reason = case.get('skip_reason') or cause.get('message') or cause.get('error') or cause.get('failure_stage')
        line = '%d. %s：%s' % (number, label, names.get(state, state))
        simulator = source if engine == 'maestro_baseline' else case.get('simulator')
        if simulator:
            line += ' [%s]' % short(simulator, 50).upper()
        elif engine == 'maestro_baseline':
            line += ' [求解器未知]'
        if reason:
            line += ' — ' + short(reason)
        lines.append(line)
        contexts = case_contexts if isinstance(case_contexts, dict) else {}
        if state in ('failed', 'fail', 'blocked'):
            observation = worker_error_text(contexts.get(case.get('case_id')))
            if observation:
                lines.append('   ' + observation)
    lines.append('优化曲线、候选表和具体指标请在 Live Results 中按诊断模式查看；日志不重复列出优化数据。')
    lines.append('建议下一步：')
    lines.extend('%d. %s' % (index, item) for index, item in enumerate(assessment['actions'], 1))
    lines.append('=== 三级诊断摘要结束 ===')
    return '\n'.join(lines) + '\n'


def _compact_section(checks, context, evidence_seen):
    """Persist each cause once; rich standalone/UI presentation stays separate."""
    import hashlib
    presentation = describe_check(checks[0], context)
    scope, section = presentation['group_id'], presentation['section_id']
    lines = ['=== SECTION BEGIN %s/%s ===' % (scope, section),
             '%s / %s' % (presentation['group_label'], presentation['section_label']),
             _contract_text('report_format crescendo.diagnosis.compact.v1')]
    concise = context.get('report_detail_mode') == 'concise'
    not_reached_groups = {}
    for check in checks:
        if check.get('status') == 'PASS':
            continue
        p = describe_check(check, context)
        entry = {'id': check.get('id'), 'status': check.get('status'), 'title': p['title'],
                 'summary': p['summary'], 'diagnostic_code': p['diagnostic_code'], 'component': p['component']}
        details = check.get('details') or {}
        if _identifier(check) == 'execution':
            entry['cause_ref'] = scope + '/first_cause'
        elif p.get('blocked_by'):
            entry['blocked_by'] = scope + '/first_cause'
            if details.get('stages'):
                entry['not_reached'] = details['stages']
        elif details:
            if concise:
                from .environment_diagnostics import concise_diagnostic_value
                entry['details'] = _problem_record(concise_diagnostic_value(details))
            else:
                entry['details'] = _problem_record(details)
        if concise:
            entry.pop('component', None)
            entry.pop('title', None)
            if check.get('status') == 'NOT_TESTED' and entry.get('blocked_by'):
                group = dict(entry)
                group.pop('id', None)
                key = _json(group)
                if key in not_reached_groups:
                    not_reached_groups[key]['ids'].append(entry['id'])
                else:
                    not_reached_groups[key] = dict(group, ids=[entry['id']])
                continue
        lines.append('check ' + _json(entry))
    for group in not_reached_groups.values():
        lines.append('check_group ' + _json(group))
    primary_section = stage_section(context.get('first_cause'))
    if section in ('netlist', primary_section):
        if context.get('netlists') is not None:
            lines.append('netlists ' + _json(context['netlists']))
        if context.get('artifacts'):
            lines.append('capture_evidence ' + _json(context['artifacts']))
    relevant = [record for record in context.get('records') or []
                if observation_section(record) == section or
                section == primary_section and str(record.get('observation') or '').endswith('producer_failure')]
    for record in relevant:
        compact = _problem_record(record)
        signature = hashlib.sha256(_json(compact).encode('utf-8')).hexdigest()
        if signature in evidence_seen:
            continue
        evidence_seen.add(signature)
        lines.append('evidence ' + _json(compact))
    lines.append('=== SECTION END %s/%s ===' % (scope, section))
    return '\n'.join(lines) + '\n'


def format_evidence_section(scope, section, records, evidence_seen):
    """Emit observed evidence even when no check produced a problem section."""
    lines = ['=== SECTION BEGIN %s/%s ===' % (scope, section),
             '%s / %s' % (scope, SECTIONS.get(section, section)),
             _contract_text('report_format crescendo.diagnosis.compact.v1')]
    for record in records:
        if observation_section(record) != section:
            continue
        compact = _problem_record(record)
        signature = hashlib.sha256(_json(compact).encode('utf-8')).hexdigest()
        if signature in evidence_seen:
            continue
        evidence_seen.add(signature)
        lines.append('evidence ' + _json(compact))
    lines.append('=== SECTION END %s/%s ===' % (scope, section))
    return '\n'.join(lines) + '\n'


def format_section(checks, case_context=None, compact=False, evidence_seen=None):
    """Format one same-scope/same-section slice; standalone copy includes cause."""
    checks, context = list(checks), case_context or {}
    if not checks:
        return ''
    if compact:
        return _compact_section(checks, context, set() if evidence_seen is None else evidence_seen)
    presentation = describe_check(checks[0], context)
    scope, section = presentation['group_id'], presentation['section_id']
    lines = ['=== SECTION BEGIN %s/%s ===' % (scope, section),
             '%s / %s' % (presentation['group_label'], presentation['section_label'])]
    basis = {key: context[key] for key in ('test_id','case_id','run_id','run_root','log_path','state','first_cause','request_context','retention_status','package','source_setup','report_omissions','index_complete','index_status','collection_status','omitted_or_truncated') if context.get(key) is not None}
    lines.append('context ' + _json(_redact_prepared(basis)))
    if context.get('netlists') is not None and (section == 'netlist' or section == stage_section(context.get('first_cause'))):
        lines.append('netlists ' + _json(context['netlists']))
        lines.append('capture_evidence ' + _json(context.get('artifacts') or []))
    passes = [row for row in checks if row.get('status') == 'PASS']
    if passes:
        lines.append('success_summary ' + _json([{'id': row.get('id'), 'title': describe_check(row,context)['title'],
            'proof': brief_success(row.get('details') or {})} for row in passes]))
    for check in checks:
        if check.get('status') == 'PASS':
            continue
        p = describe_check(check, context)
        lines.append('[%s] %s — %s' % (p['status_label'], p['title'], p['summary'].replace('\r', r'\r').replace('\n', r'\n')))
        lines.append('check ' + _json(_redact_prepared(dict(check, presentation=p))))
    if any(row.get('status') != 'PASS' for row in checks):
        relevant = [record for record in context.get('records') or [] if observation_section(record) == section]
        # The complete primary producer failure is a prerequisite for every
        # problem slice, even when the stage itself was never reached.
        primary_records = [record for record in context.get('records') or [] if str(record.get('observation') or '').endswith('producer_failure')]
        included = []
        for record in primary_records + relevant:
            if record not in included:
                included.append(record)
                lines.append('evidence ' + _json(_problem_record(record)))
    lines.append('=== SECTION END %s/%s ===' % (scope, section))
    return '\n'.join(lines) + '\n'


def read_section(log_path, group_id, section_id, max_bytes=8 * 1024 * 1024):
    """Read the latest complete exact section, or None; never partial success.

    Intended for the existing optional background worker. A linked/special file,
    changed identity, oversized report or incomplete section uses UI fallback.
    """
    import stat
    if not isinstance(log_path, str) or not isinstance(group_id, str) or section_id not in SECTIONS:
        return None
    if any(char in group_id for char in '\r\n/'):
        return None
    marker = group_id + '/' + section_id
    begin = ('=== SECTION BEGIN ' + marker + ' ===').encode('utf-8')
    end = ('=== SECTION END ' + marker + ' ===').encode('utf-8')
    try:
        descriptor = os.open(log_path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        with os.fdopen(descriptor, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > max_bytes:
                return None
            active, latest, read = None, None, 0
            shared_context, primary_begin, primary_end = None, None, None
            primary_active, primary_latest = None, None
            copied_success = []
            while True:
                raw = stream.readline(min(1024 * 1024, max_bytes - read + 1))
                if not raw:
                    break
                read += len(raw)
                if read > max_bytes or len(raw) >= 1024 * 1024 and not raw.endswith(b'\n'):
                    return None
                line = raw.rstrip(b'\r\n')
                if line.startswith(b'run_context '):
                    try:
                        shared = json.loads(line[len(b'run_context '):].decode('utf-8'))
                        if shared.get('scope') == group_id:
                            shared_context = raw
                            primary_marker = group_id + '/' + stage_section((shared.get('context') or {}).get('first_cause'))
                            primary_begin = ('=== SECTION BEGIN ' + primary_marker + ' ===').encode('utf-8')
                            primary_end = ('=== SECTION END ' + primary_marker + ' ===').encode('utf-8')
                    except (ValueError, TypeError, AttributeError):
                        pass
                if line.startswith(b'success_summary '):
                    try:
                        success = json.loads(line[len(b'success_summary '):].decode('utf-8'))
                        if success.get('category') == SECTIONS[section_id]:
                            selected = [check for check in success.get('checks') or []
                                if isinstance(check, dict) and (
                                    str(check.get('id') or '').startswith(group_id + '.') or
                                    group_id == 'environment' and not str(check.get('id') or '').startswith(('spectre.', 'alps.')))]
                            if selected:
                                copied_success.append('success_summary ' + _json(dict(success, checks=selected)) + '\n')
                    except (ValueError, TypeError, AttributeError):
                        pass
                if primary_begin and line == primary_begin:
                    primary_active = [raw]
                elif primary_active is not None:
                    if line.startswith(b'=== SECTION BEGIN '):
                        primary_active = None
                    else:
                        primary_active.append(raw)
                        if line == primary_end:
                            primary_latest, primary_active = b''.join(primary_active), None
                if line == begin:
                    active = [raw]
                elif active is not None:
                    if line.startswith(b'=== SECTION BEGIN '):
                        active = None
                    else:
                        active.append(raw)
                        if line == end:
                            latest, active = b''.join(active), None
            after = os.fstat(stream.fileno())
            current = os.lstat(log_path)
            if (not os.path.samestat(before, after) or not os.path.samestat(after, current)
                    or stat.S_ISLNK(current.st_mode) or current.st_nlink != 1):
                return None
            if latest is not None and _contract_text('report_format crescendo.diagnosis.compact.v1').encode('utf-8') in latest:
                # A copied section remains self-contained although the one
                # physical report stores its shared context and first cause once.
                prefix = shared_context or b''
                if primary_latest and primary_latest != latest:
                    prefix += primary_latest
                latest = prefix + latest + ''.join(copied_success).encode('utf-8')
            return latest.decode('utf-8', 'replace') if latest is not None else None
    except (OSError, ValueError, TypeError):
        return None


def concise_multi_success(check_id, details):
    """Project success-only report data; actual optimization data stays in runs."""
    import hashlib
    hidden = {'values', 'value', 'effective', 'requested', 'applied', 'parameters',
              'objectives', 'constraints', 'selected_output_statistics', 'metric_summary',
              'frozen_plan', 'frozen_candidate', 'frozen_candidate_origin',
              'candidates', 'history', 'result', 'text', 'export_snapshot'}
    def project(value):
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                if key in hidden:
                    result[key + '_sha256'] = hashlib.sha256(_json(item).encode('utf-8')).hexdigest()
                    if isinstance(item, (dict, list)):
                        result[key + '_count'] = len(item)
                elif key == 'source_guard' and isinstance(item, dict):
                    result[key] = {name: item[name] for name in
                        ('ok', 'state', 'fingerprint', 'schema_version', 'verification_policy') if name in item}
                else:
                    result[key] = project(item)
            return result
        if isinstance(value, list):
            return [project(item) for item in value]
        return value
    return project(details)
