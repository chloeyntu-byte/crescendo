"""Service orchestration for durable private suites, without native work at startup."""
from __future__ import print_function

import copy
import functools
import os
import re
import uuid

from . import setup_private_session as sessions
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

from .utils import atomic_write_json
from .settings_state import REVISION_UNCHECKED


def suite_command(method):
    @functools.wraps(method)
    def invoke(self, *args, **kwargs):
        from .cadence import command_cancellation_scope
        with command_cancellation_scope(self.prepare_cancel_event):
            return method(self, *args, **kwargs)
    return invoke


def suite_reference(prepared):
    ref = (prepared or {}).get('setup_private_session') or {}
    if not ref.get('session_id'):
        return None
    return {'suite_id': ref['session_id'], 'revision': ref.get('baseline_id'),
            'source_path': (((prepared.get('setup') or {}).get('source') or {}).get('setup_db') or
                            ((prepared.get('setup') or {}).get('source') or {}).get('maestro_sdb'))}


class PrivateSuiteServiceMixin(object):
    def _suite_row(self, reference):
        members = []
        for row in reference.get('views') or []:
            member = dict(zip(('library', 'cell', 'view'), row.get('target') or []))
            member.update(path=row.get('path'), role=row.get('role'))
            members.append(member)
        return dict(reference, id=reference.get('session_id') or reference.get('suite_id'),
                    name=reference.get('label') or 'Private suite', members=members)

    def list_private_suites(self):
        rows = sessions.list_setup_private_sessions(self.config)
        result, deleted = [], []
        for row in rows:
            if row.get('status') == 'deleted':
                deleted.append(self._suite_row(dict(row, can_load=False, can_delete=False, can_recover=False)))
                continue
            owned = self.setup_private_sessions.get(row.get('session_id') or row.get('suite_id'))
            if owned:
                row = dict(row, busy=False, owned_by_window=True)
            result.append(self._suite_row(row))
        return {'suites': result, 'deleted_suites': deleted, 'warnings': []}

    def _suite_reference_for_id(self, suite_id):
        if not isinstance(suite_id, str) or not re.match(r'^[a-f0-9]{32}$', suite_id):
            raise ValueError('Invalid private suite identifier')
        if suite_id in self.setup_private_sessions:
            return self.setup_private_sessions[suite_id]
        rows = sessions.list_setup_private_sessions(self.config)
        found = next((row for row in rows if row.get('suite_id') == suite_id), None)
        if not found or found.get('status') in ('unavailable', 'deleted'):
            raise ValueError('Private suite is unavailable: %s' % (', '.join((found or {}).get('warnings') or []) or suite_id))
        return found

    def _obtain_suite(self, suite_id, for_delete=False):
        reference = self._suite_reference_for_id(suite_id)
        if suite_id in self.setup_private_sessions:
            current = sessions.private_session_info(reference)
        else:
            current = sessions.reopen_setup_private_session(reference, self.config, for_delete=for_delete)
            current.pop('prepared', None)
        self._remember_setup_private_session(current)
        return current

    def _publish_suite(self, reference, prepared, selection=None, warnings=None):
        from .hierarchy import cell_scope_records
        from .runner import configuration_binding_identity
        from .workspace import config_dependency_identity
        from .setup_baseline import origin_setup_path
        self._set_activity('suite_publish', 'Restoring selected cells, parameters and settings')
        prepared = copy.deepcopy(prepared)
        prepared['setup_private_session'] = sessions.private_session_info(reference)
        source = origin_setup_path(prepared['setup'])
        self.config['_setup_baseline'] = {'origin_setup_path': source,
            'baseline': copy.deepcopy(prepared['setup']['baseline'])}
        scopes = [row for row in cell_scope_records(prepared['hierarchy'], setup=prepared['setup'],
                                                   config=self.config) if row.get('selectable')]
        preview = {'schema_version': _contract_text('crescendo.cell_inspection.v1'), 'setup': prepared['setup'],
            'setup_path': source, 'setup_locator': prepared['setup_locator'],
            'hierarchy': prepared['hierarchy'], 'cell_scopes': scopes,
            'job_policy': prepared.get('job_policy'), 'eda_context': prepared.get('eda_context'),
            'setup_private_session': prepared['setup_private_session']}
        inspection_id, prepared_id = uuid.uuid4().hex, uuid.uuid4().hex
        ready = (isinstance(prepared.get('parameters'), list) and bool(prepared.get('prepared_freshness'))
                 and not prepared.get('suite_selection_required'))
        record = {'preview': preview, 'identity': self._inspection_setup_identity(prepared['setup']),
            'max_depth': 32, 'epoch': self.inspection_epoch, 'prepared_ids': [prepared_id] if ready else [],
            'config_identity': config_dependency_identity(prepared['setup'], prepared['hierarchy'], config=self.config),
            'binding_identity': configuration_binding_identity(prepared['hierarchy']),
            'config_dependencies': prepared['hierarchy']}
        result = {'suite': self._suite_row(prepared['setup_private_session']),
            'selection': selection, 'setup_path': source, 'inspection_id': inspection_id,
            'inspection': preview, 'prepared_id': prepared_id if ready else None,
            'prepared': prepared if ready else None, 'ready_for_settings': ready,
            'selected_cell_scope_ids': list((prepared.get('cell_selection') or {}).get('scope_ids') or []),
            'warnings': list(warnings or []), 'eda_setup': self.eda_setup(prepared['setup'])}
        path = os.path.join(reference['root'], _contract_text('configuration.json'))
        if os.path.isfile(path):
            try:
                from .unified_configuration import read_configuration, match_configuration
                read_result = read_configuration(path, return_result=True)
                document = read_result['document']
                result['suite_configuration_revision'] = read_result['revision']
                result['saved_configuration'] = document
                result['document'] = document
                result['settings'] = document['settings']
                result['match'] = match_configuration(document,
                    self._configuration_match_context(prepared if ready else {}, preview, source))
                result['warnings'].extend(result['match'].get('warnings') or [])
            except Exception as error:
                result['warnings'].append('Suite settings could not be restored; current draft retained: ' + str(error))
        if not os.path.lexists(path):
            result['suite_configuration_revision'] = None
        with self.lock:
            self.inspections.clear()
            self.prepared.clear()
            self.inspections[inspection_id] = record
            if ready:
                self.prepared[prepared_id] = prepared
        self._remember_setup_private_session(prepared['setup_private_session'])
        for warning in result['warnings']:
            self._set_activity('suite_warning', warning, level='WARN')
        self._set_activity('suite_ready', 'Private suite loaded; saved cells and parameters reused', 'complete')
        return result

    def _load_private_suite_registered(self, suite_id):
        from .setup_baseline import origin_setup_path
        from .prepared_freshness import verify_prepared_freshness
        self._require_idle_setup_resources('load a private suite')
        self._set_activity('suite_open', 'Opening saved private suite')
        reference = self._obtain_suite(suite_id)
        prepared = sessions.setup_private_session_snapshot(reference)
        source = origin_setup_path(prepared['setup'])
        selection = self._select_setup_registered(source)
        # Session config follows this explicitly selected source, never another TB.
        sessions._controller(reference).config = copy.deepcopy(self.config)
        warnings = []
        self._set_activity('suite_compare', 'Checking saved suite and source differences')
        try:
            from .ade_setup import inspect_setup
            from .unified_configuration import maestro_settings_snapshot, _snapshot_differences
            current = inspect_setup(source, backend='maestro')
            from .single_test import project_setup
            current = project_setup(current, prepared['setup']['selected_test'])
            differences = _snapshot_differences(maestro_settings_snapshot(prepared['setup']),
                                                maestro_settings_snapshot(current))
            if differences:
                warnings.append('Source Maestro differs in %s. Saved suite retained; select sections to update.' %
                                ', '.join(row['field'] for row in differences[:12]))
        except Exception as error:
            warnings.append('Source settings comparison unavailable; saved suite retained: ' + str(error))
        freshness = None
        if prepared.get('prepared_freshness'):
            freshness = verify_prepared_freshness(prepared, self.config)
            if not freshness.get('ok'):
                warnings.append('Electrical dependencies changed; update Test design before running: ' +
                                '; '.join(freshness.get('reasons') or []))
        setup = prepared.get('setup') or {}
        mask = (setup.get('test_corner_masks') or {}).get(setup.get('selected_test')) or {}
        corners = setup.get('corners') or []
        named_corners = any(row.get('name') != '_default' for row in corners)
        missing_mask = ((named_corners and mask.get('known') is not True) or
                        (not corners and mask.get('nominal_enabled') is None) or
                        (any(row.get('name') == '_default' for row in corners) and
                         mask.get('nominal_enabled') is None))
        if (missing_mask and freshness and freshness.get('ok') and
                not prepared.get('suite_selection_required') and isinstance(prepared.get('parameters'), list)):
            self._set_activity('suite_corner_readback', 'Refreshing unavailable native Test corner masks')
            try:
                updated = sessions.apply_setup_private_cells(reference, prepared, self.config,
                    cancel_check=self.prepare_cancel_event.is_set)
                updated_masks = updated.get('test_corner_masks') or {}
                new_mask = updated_masks.get(setup.get('selected_test')) or {}
                if ((named_corners and new_mask.get('known') is not True) or
                        ((not corners or any(row.get('name') == '_default' for row in corners)) and
                         new_mask.get('nominal_enabled') is None)):
                    raise ValueError('Native Test corner mask readback is still unavailable: %s' %
                                     '; '.join(updated.get('metadata_warnings') or []))
                prepared['setup']['test_corner_masks'] = copy.deepcopy(updated_masks)
                from .prepared_freshness import capture_prepared_freshness
                from .setup_baseline import persist_prepared_baseline
                prepared['prepared_freshness'] = capture_prepared_freshness(prepared)
                snapshot = dict(prepared)
                snapshot.pop('setup_private_session', None)
                prepared['setup_baseline_snapshot'] = persist_prepared_baseline(snapshot)
                sessions.update_setup_private_session_snapshot(updated, prepared)
                reference = updated
            except Exception as error:
                from .cadence import CommandCancelled
                failed_session = getattr(error, 'setup_private_session', None) or {}
                if (isinstance(error, CommandCancelled) or getattr(error, 'cleanup_failed', False)
                        or getattr(error, 'unsafe_to_optimize', False)
                        or (isinstance(failed_session, dict) and failed_session.get('unsafe'))):
                    raise
                failure = RuntimeError('Saved suite native Test corner readback failed; draft retained: %s' % error)
                failure.suite_corner_readback_failed = True
                raise failure from error
        return self._publish_suite(reference, prepared, selection, warnings)

    @suite_command
    def load_private_suite(self, suite_id, source_path=None):
        self._begin_lifecycle_operation('load a private suite', exclusive=True)
        previous_config = copy.deepcopy(self.config)
        previous = (self.prepared.copy(), self.inspections.copy(), self.test_inspections.copy(), self.inspection_epoch)
        previous_suites = set(self.setup_private_sessions)
        try:
            if source_path:
                self._select_setup_registered(source_path)
            return self._load_private_suite_registered(suite_id)
        except Exception as error:
            self.config = previous_config
            with self.lock:
                self.prepared, self.inspections, self.test_inspections, self.inspection_epoch = previous
            for token in set(self.setup_private_sessions).difference(previous_suites):
                try:
                    close_result = sessions.close_setup_private_session(self.setup_private_sessions[token])
                    if close_result.get('cleanup_proven') is True:
                        self.setup_private_sessions.pop(token, None)
                    else:
                        error.cleanup_failed = True
                        error.suite_cleanup_result = close_result
                except Exception as close_error:
                    # Keep exact authority for Exit to retry; never delete on failed load.
                    error.cleanup_failed = True
                    error.suite_cleanup_result = {'cleanup_proven': False, 'error': str(close_error)}
            self._set_activity('suite_load_failed', str(error), 'error')
            raise
        finally:
            self._end_lifecycle_operation(exclusive=True)

    def private_suite_configuration_revision(self, suite_id):
        from .unified_configuration import configuration_revision
        reference = self._suite_reference_for_id(suite_id)
        sessions._controller(reference)
        return configuration_revision(os.path.join(reference['root'], _contract_text('configuration.json')))

    def save_private_suite_configuration(self, suite_id, document, expected_revision=REVISION_UNCHECKED):
        from .unified_configuration import normalize_configuration, save_configuration
        from .setup_baseline import origin_setup_path
        reference = self._suite_reference_for_id(suite_id)
        controller = sessions._controller(reference)
        cleaned = normalize_configuration(document)['document']
        if not controller.lock.acquire(timeout=0.1):
            raise RuntimeError('Suite settings are being updated; suite draft was not overwritten')
        try:
            if controller.closed or controller.unsafe:
                raise RuntimeError('Suite is no longer available for saving preferences')
            saved_source = (cleaned.get('source_setup') or {}).get('path')
            if not saved_source or os.path.realpath(saved_source) != os.path.realpath(origin_setup_path(controller.prepared['setup'])):
                raise ValueError('Configuration belongs to another source setup')
            result = save_configuration(os.path.join(reference['root'], _contract_text('configuration.json')), cleaned,
                                        retain_backups=0, expected_revision=expected_revision, return_result=True)
            result['saved'] = True
            return result
        finally:
            controller.lock.release()

    @suite_command
    def rename_private_suite(self, suite_id, name):
        self._begin_lifecycle_operation('rename a private suite', exclusive=True)
        try:
            self._require_idle_setup_resources('rename a private suite')
            reference = self._obtain_suite(suite_id)
            self._set_activity('suite_rename', 'Rebinding all views under the new suite name')
            active = any((item.get('setup_private_session') or {}).get('session_id') == suite_id
                         for item in self.prepared.values()) or any(
                         ((item.get('preview') or {}).get('setup_private_session') or {}).get('session_id') == suite_id
                         for item in self.inspections.values())
            from .ade_setup import resolve_setup_locator
            from .maestro_context import selected_setup_config
            from .setup_baseline import origin_setup_path
            owned_prepared = sessions.setup_private_session_snapshot(reference)
            suite_config = selected_setup_config(self.invoking_config, self.config,
                resolve_setup_locator(origin_setup_path(owned_prepared['setup']), backend='maestro'))
            replacement = sessions.rename_setup_private_session(reference, name, suite_config,
                cancel_check=self.prepare_cancel_event.is_set)
            self.setup_private_sessions.pop(suite_id, None)
            self._remember_setup_private_session(replacement)
            prepared = sessions.setup_private_session_snapshot(replacement)
            old_config = os.path.join(reference['root'], _contract_text('configuration.json'))
            warnings = []
            if os.path.isfile(old_config):
                try:
                    from .unified_configuration import read_configuration
                    document = read_configuration(old_config)
                    document['private_suite'] = suite_reference(dict(prepared, setup_private_session=replacement))
                    atomic_write_json(os.path.join(replacement['root'], _contract_text('configuration.json')), document)
                except Exception as error:
                    warnings.append('Renamed suite is ready; configuration draft copy failed: ' + str(error))
            if active:
                return self._publish_suite(replacement, prepared, warnings=warnings)
            return {'suite': self._suite_row(replacement), 'warnings': warnings}
        except Exception as error:
            replacement = getattr(error, 'replacement_setup_private_session', None) or locals().get('replacement')
            failed = replacement or getattr(error, 'setup_private_session', None)
            if failed:
                self._remember_setup_private_session(failed)
            valid = True
            if 'reference' in locals():
                try:
                    current = sessions.private_session_info(reference)
                    valid = current.get('status') == 'ready' and not current.get('unsafe')
                except Exception:
                    valid = False
            if not valid:
                error.private_suite_invalidated = True
                error.replacement_setup_private_session = replacement
                self._invalidate_suite_authority(suite_id)
            raise
        finally:
            self._end_lifecycle_operation(exclusive=True)

    def _invalidate_suite_authority(self, suite_id):
        with self.lock:
            self.prepared = {key: value for key, value in self.prepared.items()
                if (value.get('setup_private_session') or {}).get('session_id') != suite_id}
            self.inspections = {key: value for key, value in self.inspections.items()
                if ((value.get('preview') or {}).get('setup_private_session') or {}).get('session_id') != suite_id}

    def recover_private_suite(self, suite_id):
        self._begin_lifecycle_operation('recover an interrupted private suite update', exclusive=True)
        try:
            self._require_idle_setup_resources('recover an interrupted private suite update')
            reference = self._suite_reference_for_id(suite_id)
            result = sessions.recover_setup_private_session(reference, self.config)
            self._invalidate_suite_authority(suite_id)
            with self.lock:
                self.setup_private_sessions.pop(suite_id, None)
            return result
        finally:
            self._end_lifecycle_operation(exclusive=True)

    def delete_private_suite(self, suite_id):
        self._begin_lifecycle_operation('delete a private suite', exclusive=True)
        try:
            self._require_idle_setup_resources('delete a private suite')
            reference = self._obtain_suite(suite_id, for_delete=True)
            result = sessions.delete_setup_private_session(reference)
            if not result.get('cleanup_proven'):
                self._invalidate_suite_authority(suite_id)
                error = RuntimeError('Suite deletion is incomplete; retained members: %s' % result)
                error.private_suite_invalidated = True
                raise error
            with self.lock:
                self.setup_private_sessions.pop(suite_id, None)
                self.prepared = {key: value for key, value in self.prepared.items()
                    if (value.get('setup_private_session') or {}).get('session_id') != suite_id}
                self.inspections = {key: value for key, value in self.inspections.items()
                    if ((value.get('preview') or {}).get('setup_private_session') or {}).get('session_id') != suite_id}
            return dict(result, warnings=[], retained_metadata_root=reference['root'],
                        retention_note='Private views deleted; configuration, history and audit metadata retained')
        finally:
            self._end_lifecycle_operation(exclusive=True)

    @suite_command
    def update_private_suite(self, suite_id, sections):
        self._begin_lifecycle_operation('update selected Maestro sections', exclusive=True)
        saved_config = self.config
        saved_prepared, saved_inspections, saved_tests = self.prepared.copy(), self.inspections.copy(), self.test_inspections.copy()
        saved_epoch = self.inspection_epoch
        updated = None
        try:
            self._require_idle_setup_resources('update a private suite')
            reference = self._obtain_suite(suite_id)
            old = sessions.setup_private_session_snapshot(reference)
            from .setup_baseline import origin_setup_path
            selection = self._select_setup_registered(origin_setup_path(old['setup']))
            sessions._controller(reference).config = copy.deepcopy(self.config)
            from .prepared_freshness import require_prepared_freshness, capture_prepared_freshness
            from .setup_baseline import origin_setup_path, persist_prepared_baseline
            from .suite_updates import update_saved_sections
            from .runner import parameter_catalog, output_catalog
            from .workspace import source_design_identity
            if 'test_design' not in sections:
                require_prepared_freshness(old, self.config, action='update saved settings; select Test design if electrical dependencies changed')
            self._set_activity('suite_merge', 'Updating only selected native Maestro sections')
            source_config = copy.deepcopy(self.config)
            source_config.pop('_setup_baseline', None)
            incoming = copy.deepcopy(old)
            if set(sections).intersection(('job_setup', 'save_options')):
                from .ade_setup import inspect_setup
                from .job_policy import capture_setup_job_policy
                current = inspect_setup(origin_setup_path(old['setup']), backend='maestro')
                policy = capture_setup_job_policy(current, source_config)
                incoming['_incoming_job_policy'] = copy.deepcopy(policy)
                if 'job_setup' in sections and policy.get('available'):
                    incoming['job_policy'] = policy
                elif policy.get('save_options'):
                    incoming.setdefault('job_policy', {})['save_options'] = policy['save_options']
                incoming['_incoming_save_options'] = current.get('save_options') or policy.get('save_options')
            setup, plan = update_saved_sections(incoming, source_config, sections)
            if plan.get('no_changes'):
                self._set_activity('suite_unchanged', 'Selected settings are unchanged; reusing saved suite')
                result = self._publish_suite(reference, old, selection=selection, warnings=plan.get('warnings'))
                result['update_plan'] = plan
                self._set_activity('suite_reuse_ready', 'Selected source settings are unchanged; saved suite reused', 'complete')
                return result
            from .single_test import project_setup
            setup = project_setup(setup, old['setup']['selected_test'])
            prepared = copy.deepcopy(old)
            prepared.pop('setup_private_session', None)
            prepared.pop('prepared_freshness', None)
            prepared.pop('setup_baseline_snapshot', None)
            prepared['setup'] = setup
            from .ade_setup import build_setup_reference
            prepared['setup_locator'] = build_setup_reference(setup)
            prepared['job_policy'] = copy.deepcopy(setup.get('baseline_job_policy', incoming.get('job_policy')))
            if setup.get('save_options') and isinstance(prepared['job_policy'], dict):
                prepared['job_policy']['save_options'] = copy.deepcopy(setup['save_options'])
            if plan.get('requires_hierarchy'):
                from .hierarchy import probe_hierarchy, cell_scope_records
                self._set_activity('suite_hierarchy', 'Checking changed Test design and selected cells')
                # Discover once, match previous selections, then read parameters only there.
                hierarchy = probe_hierarchy(setup, source_config, max_depth=32, cells_only=True)
                choices = [row for row in cell_scope_records(hierarchy, setup=setup, config=source_config) if row.get('selectable')]
                from .unified_configuration import match_configuration_cells
                from .settings_state import _cell_evidence
                saved = [{'scope_id': row.get('scope_id'), 'selected': True, 'evidence': _cell_evidence(row)}
                         for row in (old.get('cell_selection') or {}).get('scopes') or []]
                match = match_configuration_cells({'schema_version': _contract_text('crescendo.configuration.v1'),
                    'settings': {'cells': saved}, 'selection': {'provided': True}}, choices)
                selected = [row for row in choices if row.get('scope_id') in match['scope_ids']]
                if selected:
                    hierarchy = probe_hierarchy(setup, source_config, max_depth=32, selected_cell_paths=selected)
                prepared['hierarchy'] = hierarchy
                prepared['cell_selection'] = {'scopes': selected, 'scope_ids': [row['scope_id'] for row in selected]}
                prepared['suite_selection_required'] = not bool(selected)
                if not selected:
                    plan.setdefault('warnings', []).append('Previous selected cells no longer match this Test design. Select cells in Setup; all settings remain available.')
            prepared['parameters'] = parameter_catalog(setup, prepared['hierarchy'])
            prepared['outputs'] = output_catalog(setup)
            prepared['source_identity'] = source_design_identity(setup, prepared['hierarchy'], config=self.config)
            if plan.get('requires_hierarchy') or set(sections).intersection(('other_settings', 'test_settings', 'corners')):
                from .cadence import detect_pdk_context
                from .runner import collect_external_eda_environment, _simulation_submission_metadata
                pdk = detect_pdk_context(setup, prepared['hierarchy'], self.config.get('cadence') or {})
                if pdk.get('blocking'):
                    raise ValueError('Updated Test environment cannot be resolved: %s' % pdk.get('recommendations'))
                submission = _simulation_submission_metadata(self.config, controller='maestro')
                external = collect_external_eda_environment(setup, self.config.get('cadence') or {}, pdk, submission=submission)
                prepared['eda_context'] = dict(prepared.get('eda_context') or {}, pdk=pdk, submission=submission,
                    external_environment=external['identity'], external_environment_diagnostics=external['diagnostics'])
            prepared['suite_update'] = {'sections': list(plan.get('sections') or []),
                'previous_baseline': (old['setup'].get('baseline') or {}).get('baseline_id'),
                'baseline': (setup.get('baseline') or {}).get('baseline_id'),
                'hierarchy_reloaded': bool(plan.get('requires_hierarchy')), 'warnings': list(plan.get('warnings') or [])}
            self._set_activity('suite_native_check', 'Verifying updated private Maestro bindings')
            if plan.get('requires_hierarchy'):
                updated = sessions.create_setup_private_session(prepared, self.config,
                    cancel_check=self.prepare_cancel_event.is_set, label=reference.get('label'))
                self._remember_setup_private_session(updated)
                if (prepared.get('cell_selection') or {}).get('scope_ids'):
                    updated = sessions.apply_setup_private_cells(updated, prepared, self.config,
                        cancel_check=self.prepare_cancel_event.is_set)
                # Structural replacement keeps the previous suite available as a version.
            else:
                updated = sessions.refresh_setup_private_session(reference, prepared, self.config,
                    cancel_check=self.prepare_cancel_event.is_set)
            prepared['setup_private_session'] = updated
            if isinstance(updated.get('test_corner_masks'), dict):
                prepared['setup']['test_corner_masks'] = copy.deepcopy(updated['test_corner_masks'])
            prepared['prepared_freshness'] = capture_prepared_freshness(prepared)
            snapshot = dict(prepared)
            snapshot.pop('setup_private_session', None)
            prepared['setup_baseline_snapshot'] = persist_prepared_baseline(snapshot)
            sessions.update_setup_private_session_snapshot(updated, prepared)
            result = self._publish_suite(updated, prepared, selection=selection, warnings=plan.get('warnings'))
            result['update_plan'] = plan
            return result
        except Exception as error:
            failed = getattr(error, 'setup_private_session', None)
            if failed:
                self._remember_setup_private_session(failed)
            invalidated = updated is not None
            if updated is None and 'old' in locals():
                try:
                    observed = sessions.private_session_info(reference)
                    old_baseline = (old['setup'].get('baseline') or {}).get('baseline_id')
                    invalidated = (observed.get('unsafe') or observed.get('status') != 'ready' or
                                   observed.get('baseline_id') != old_baseline)
                except Exception:
                    invalidated = True
            self.config = saved_config
            with self.lock:
                self.prepared, self.inspections, self.test_inspections = saved_prepared, saved_inspections, saved_tests
                self.inspection_epoch = saved_epoch
                if invalidated:
                    self.prepared = {key: value for key, value in self.prepared.items()
                        if (value.get('setup_private_session') or {}).get('session_id') != suite_id}
                    self.inspections = {key: value for key, value in self.inspections.items()
                        if ((value.get('preview') or {}).get('setup_private_session') or {}).get('session_id') != suite_id}
                    error.private_suite_invalidated = True
            self._set_activity('suite_update_failed', str(error), 'error')
            raise
        finally:
            self._end_lifecycle_operation(exclusive=True)
