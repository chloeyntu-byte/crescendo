from __future__ import print_function

import copy
import os
import threading
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from tests import test_gui_cell_selection as fixture


@unittest.skipUnless(os.environ.get("DISPLAY"), "Real Tk tests require a DISPLAY")
class PrivateSuiteGuiTests(unittest.TestCase):
    setUp = fixture.GuiCellSelectionTests.setUp
    _close = fixture.GuiCellSelectionTests._close
    _wait = fixture.GuiCellSelectionTests._wait
    _open = fixture.GuiCellSelectionTests._open
    _load_cells = fixture.GuiCellSelectionTests._load_cells
    _toggle = fixture.GuiCellSelectionTests._toggle
    _apply = fixture.GuiCellSelectionTests._apply
    def _suite_window(self):
        return next(w for w in self.root.winfo_children()
                    if isinstance(w, gui.tk.Toplevel) and w.title() == u'私有套件管理')

    @staticmethod
    def _widgets(widget):
        result = [widget]
        for child in widget.winfo_children():
            result.extend(PrivateSuiteGuiTests._widgets(child))
        return result

    def test_list_members_and_load_are_responsive(self):
        app = self._open()
        self.service.list_private_suites = lambda: {'suites': [{'id': 's1', 'name': 'A',
            'selected_test': 'VCO', 'members': [{'role': 'schematic', 'library': 'L',
                'cell': 'C', 'view': 'schematic', 'path': '/fixture/view'}]}]}
        app.open_private_suites()
        window = self._suite_window()
        tree = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Treeview))
        self._wait(lambda: len(tree.get_children()) == 1)
        tree.selection_set(tree.get_children()[0])
        tree.event_generate('<<TreeviewSelect>>')
        self.root.update()
        details = next(w for w in self._widgets(window) if isinstance(w, gui.tk.Text))
        self.assertIn('L/C/schematic', details.get('1.0', 'end'))
        with mock.patch.object(app, 'load_configuration') as load:
            button = next(w for w in self._widgets(window)
                          if isinstance(w, gui.ttk.Button) and w.cget('text') == u'加载')
            button.invoke()
            load.assert_called_once_with(suite_id='s1')
        self.assertEqual(self.service.inspect_calls, [])

    def test_update_passes_selected_sections(self):
        app = self._open()
        self.service.list_private_suites = lambda: {'suites': [{'id': 's1', 'name': 'A'}]}
        app.open_private_suites()
        window = self._suite_window()
        tree = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Treeview))
        self._wait(lambda: len(tree.get_children()) == 1)
        tree.selection_set(tree.get_children()[0])
        button = next(w for w in self._widgets(window)
                      if isinstance(w, gui.ttk.Button) and w.cget('text') == u'更新选中部分…')
        button.invoke()
        dialog = next(w for w in window.winfo_children() if isinstance(w, gui.tk.Toplevel))
        checks = [w for w in self._widgets(dialog) if isinstance(w, gui.ttk.Checkbutton)]
        checks[0].invoke()
        checks[4].invoke()
        with mock.patch.object(app, 'load_configuration') as load:
            apply = next(w for w in self._widgets(dialog)
                         if isinstance(w, gui.ttk.Button) and w.cget('text') == u'更新并加载')
            apply.invoke()
            load.assert_called_once_with(suite_id='s1', suite_sections=['test_design', 'global_variables'])
        self.assertEqual(self.service.inspect_calls, [])

    def test_rename_and_delete_run_in_background(self):
        app = self._open()
        calls = []
        main_thread = threading.current_thread().ident
        self.service.list_private_suites = lambda: {'suites': [{'id': 's1', 'name': 'A'}]}
        self.service.rename_private_suite = lambda suite_id, name: (calls.append(('rename', suite_id, name, threading.current_thread().ident)) or {})
        self.service.delete_private_suite = lambda suite_id: (calls.append(('delete', suite_id, threading.current_thread().ident)) or {})
        app.open_private_suites()
        window = self._suite_window()
        tree = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Treeview))
        self._wait(lambda: len(tree.get_children()) == 1)
        tree.selection_set(tree.get_children()[0])
        with mock.patch.object(gui.simpledialog, 'askstring', return_value='B'):
            button = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Button) and w.cget('text') == u'重命名…')
            button.invoke()
        self._wait(lambda: len(calls) == 1 and len(tree.get_children()) == 1 and str(button.cget('state')) == 'normal')
        tree.selection_set(tree.get_children()[0])
        with mock.patch.object(gui.messagebox, 'askyesno', return_value=True):
            button = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Button) and w.cget('text') == u'删除…')
            button.invoke()
        self._wait(lambda: len(calls) == 2)
        self.assertEqual(calls[0][:3], ('rename', 's1', 'B'))
        self.assertEqual(calls[1][:2], ('delete', 's1'))
        self.assertTrue(all(call[-1] != main_thread for call in calls))

    def test_rename_active_suite_reloads_replacement(self):
        app = self._open()
        app.active_private_suite = {'id': 's1'}
        self.service.list_private_suites = lambda: {'suites': [{'id': 's1', 'name': 'A'}]}
        self.service.rename_private_suite = lambda suite_id, name: {'suite': {'id': 's2'}}
        app.open_private_suites()
        window = self._suite_window()
        tree = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Treeview))
        self._wait(lambda: len(tree.get_children()) == 1)
        tree.selection_set(tree.get_children()[0])
        with mock.patch.object(gui.simpledialog, 'askstring', return_value='B'), \
                mock.patch.object(app, 'load_configuration') as load:
            button = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Button) and w.cget('text') == u'重命名…')
            button.invoke()
            self._wait(lambda: load.called)
            load.assert_called_once_with(suite_id='s2', preserve_suite_draft=True)

    def test_rename_failure_with_invalidated_identity_disables_prepared(self):
        app = self._open()
        self._load_cells()
        self._toggle('/CORE_A')
        self._apply()
        app.active_private_suite = {'id': 's1'}
        self.service.list_private_suites = lambda: {'suites': [{'id': 's1', 'name': 'A'}]}
        def rename(*_args):
            error = RuntimeError('replacement requires recovery')
            error.private_suite_invalidated = True
            raise error
        self.service.rename_private_suite = rename
        app.open_private_suites()
        window = self._suite_window()
        tree = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Treeview))
        self._wait(lambda: len(tree.get_children()) == 1)
        tree.selection_set(tree.get_children()[0])
        with mock.patch.object(gui.simpledialog, 'askstring', return_value='B'):
            button = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Button) and w.cget('text') == u'重命名…')
            button.invoke()
            self._wait(lambda: self.showerror.called)
        self.assertIsNone(app.prepared_id)
        self.assertIsNone(app.active_private_suite)
        self.assertIsNotNone(app.pending_settings_state)

    def test_other_suite_delete_failure_does_not_revoke_current_prepared(self):
        app = self._open()
        self._load_cells()
        self._toggle('/CORE_A')
        self._apply()
        active_prepared = app.prepared_id
        app.active_private_suite = {'id': 's1'}
        self.service.list_private_suites = lambda: {'suites': [{'id': 's2', 'name': 'other'}]}
        def delete(*_args):
            error = RuntimeError('partial deletion')
            error.private_suite_invalidated = True
            raise error
        self.service.delete_private_suite = delete
        app.open_private_suites()
        window = self._suite_window()
        tree = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Treeview))
        self._wait(lambda: len(tree.get_children()) == 1)
        tree.selection_set(tree.get_children()[0])
        with mock.patch.object(gui.messagebox, 'askyesno', return_value=True):
            button = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Button) and w.cget('text') == u'删除…')
            button.invoke()
            self._wait(lambda: self.showerror.called)
        self.assertEqual(app.prepared_id, active_prepared)
        self.assertEqual(app.active_private_suite['id'], 's1')

    def test_old_suite_can_be_deleted_but_not_loaded(self):
        app = self._open()
        self.service.list_private_suites = lambda: {'suites': [{'id': 's1', 'name': 'old', 'can_load': False, 'can_delete': True}]}
        calls = []
        self.service.delete_private_suite = lambda suite_id: (calls.append(suite_id) or {})
        app.open_private_suites()
        window = self._suite_window()
        tree = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Treeview))
        self._wait(lambda: len(tree.get_children()) == 1)
        tree.selection_set(tree.get_children()[0])
        with mock.patch.object(app, 'load_configuration') as load:
            button = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Button) and w.cget('text') == u'加载')
            button.invoke()
            load.assert_not_called()
        with mock.patch.object(gui.messagebox, 'askyesno', return_value=True):
            button = next(w for w in self._widgets(window) if isinstance(w, gui.ttk.Button) and w.cget('text') == u'删除…')
            button.invoke()
            self._wait(lambda: calls == ['s1'])

    def test_async_load_and_update_restore_prepared_without_reinspection(self):
        app = self._open()
        self._load_cells()
        self._toggle('/CORE_A')
        self._apply()
        preview = copy.deepcopy(app.cell_inspection)
        prepared = copy.deepcopy(app.prepared)
        scope_ids = list(app.applied_cell_scope_ids)
        calls = []
        raw, context = app._snapshot_configuration()
        old_document = gui.build_configuration(raw, context['parameter_by_id'], context['cell_scopes'], context['outputs'])
        main_thread = threading.current_thread().ident
        def result(kind, sections=None):
            calls.append((kind, sections, threading.current_thread().ident))
            return {'suite': {'id': 's1', 'name': 'A'}, 'document': copy.deepcopy(old_document),
                    'inspection_id': 'restored-inspection', 'inspection': copy.deepcopy(preview),
                    'prepared_id': 'restored-prepared', 'prepared': copy.deepcopy(prepared),
                    'selected_cell_scope_ids': scope_ids, 'warnings': []}
        self.service.load_private_suite = lambda suite_id: result('load')
        self.service.update_private_suite = lambda suite_id, sections: result('update', sections)
        inspected_before = len(self.service.inspect_calls)
        app.load_configuration(suite_id='s1')
        self._wait(lambda: not app.configuration_loading)
        self.assertEqual(app.prepared_id, 'restored-prepared', app.settings_status_var.get())
        self.assertEqual(set(scope_ids), app.applied_cell_scope_ids)
        self.assertEqual(app.test_choice_var.get(), 'VCO')
        app.generations_var.set('99')
        app.load_configuration(suite_id='s1', suite_sections=['outputs'])
        self._wait(lambda: not app.configuration_loading)
        self.assertEqual(app.prepared_id, 'restored-prepared', app.settings_status_var.get())
        self.assertEqual(calls[1][1], ['outputs'])
        self.assertEqual(app.generations_var.get(), '99')
        self.assertTrue(all(call[2] != main_thread for call in calls))
        self.assertEqual(len(self.service.inspect_calls), inspected_before)

    def test_configuration_suite_keeps_current_source(self):
        app = self._open()
        source = self.paths['lib_B']
        document = {'schema_version': 'crescendo.configuration.v1', 'source_setup': {'path': source},
                    'settings': {}, 'private_suite': {'suite_id': 'a' * 32, 'source_path': source}}
        path = os.path.join(self.directory.name, 'suite.json')
        gui.save_configuration(path, document)
        self.service.load_configuration = mock.Mock(side_effect=RuntimeError('fixture stop after path capture'))
        with mock.patch.object(gui.filedialog, 'askopenfilename', return_value=path), \
                mock.patch.object(gui, 'read_configuration', wraps=gui.read_configuration) as read:
            app.load_configuration()
            self._wait(lambda: not app.configuration_loading)
        self.assertTrue(read.call_args[1]['return_result'])
        self.assertEqual(self.service.load_configuration.call_args[1]['path'], self.paths['lib_A'])
        self.assertEqual(self.service.load_configuration.call_args[0][0]['private_suite']['source_path'], source)

    def test_unsafe_update_clears_prepared_authority_and_preserves_draft(self):
        app = self._open()
        self._load_cells()
        self._toggle('/CORE_A')
        self._apply()
        app.active_private_suite = {'id': 's1'}
        def update(*_args):
            error = RuntimeError('unsafe refresh')
            error.private_suite_invalidated = True
            raise error
        self.service.update_private_suite = update
        app.load_configuration(suite_id='s1', suite_sections=['outputs'])
        self._wait(lambda: not app.configuration_loading)
        self.assertIsNone(app.prepared_id)
        self.assertIsNone(app.cell_inspection_id)
        self.assertIsNone(app.active_private_suite)
        self.assertIn('unsafe refresh', app.settings_status_var.get())
        self.assertIsNotNone(app.pending_settings_state)
        self.assertTrue(any(row.get('selected') and '/CORE_A' in row.get('scope_id', '')
                            for row in app.pending_settings_state.get('cells', [])))

    def test_exit_saves_snapshot_and_failure_does_not_block_exit(self):
        app = self._open()
        app.active_private_suite = {'id': 's1', 'baseline_id': 'revision1'}
        calls = []
        main_thread = threading.current_thread().ident
        app.suite_configuration_revisions = {'s1': 'read-revision'}
        self.service.private_suite_configuration_revision = mock.Mock(side_effect=AssertionError('must use loaded revision'))
        def save(suite_id, document, expected_revision=None):
            calls.append((suite_id, document, threading.current_thread().ident, expected_revision))
            raise ValueError('fixture snapshot failed')
        self.service.save_private_suite_configuration = save
        self.service.exit_optimizer = lambda: {'warnings': ['release warning']}
        with mock.patch.object(gui.messagebox, 'showwarning') as warning:
            app.request_exit(confirm=False)
            self._wait(lambda: self.destroyed)
            self.assertEqual(warning.call_count, 1)
            self.assertIn('fixture snapshot failed', warning.call_args[0][1])
            self.assertIn('release warning', warning.call_args[0][1])
        self.assertEqual(calls[0][0], 's1')
        self.assertEqual(calls[0][1]['private_suite']['source_path'], self.paths['lib_A'])
        self.assertEqual(calls[0][1]['private_suite']['revision'], 'revision1')
        self.assertNotEqual(calls[0][2], main_thread)
        self.assertEqual(calls[0][3], 'read-revision')
        self.assertFalse(self.service.private_suite_configuration_revision.called)

    def test_exit_displays_warning_once_then_closes(self):
        app = self._open()
        self.service.exit_optimizer = lambda: {'warnings': ['release warning']}
        with mock.patch.object(gui.messagebox, 'showwarning') as warning:
            app.request_exit(confirm=False)
            self._wait(lambda: self.destroyed)
            self.assertEqual(warning.call_count, 1)
            self.assertIn('release warning', warning.call_args[0][1])

    def test_successful_exit_does_not_wait_for_queue_lock(self):
        app = self._open()
        locked = threading.Event()
        release = threading.Event()
        def hold_lock():
            with app.tasks.state_lock:
                locked.set()
                release.wait(2)
        def hold_during_callback(_snapshot):
            thread = threading.Thread(target=hold_lock)
            thread.daemon = True
            thread.start()
            self.assertTrue(locked.wait(1))
        try:
            with mock.patch.object(app, '_write_exit_launch_async', side_effect=hold_during_callback):
                app.request_exit(confirm=False)
                start = time.monotonic()
                self._wait(lambda: self.destroyed, timeout=1)
                self.assertLess(time.monotonic() - start, 1)
                self.assertTrue(locked.is_set())
                self.assertFalse(release.is_set())
        finally:
            release.set()


if __name__ == '__main__':
    unittest.main()
