import os
import time
import unittest
from unittest import mock

from crescendo_engine import gui
from tests import test_gui_activity as activity_fixture


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Existing DISPLAY required')
class ResponsiveHelpTkTests(unittest.TestCase):
    setUp = activity_fixture.ActivityGuiTests.setUp
    close = activity_fixture.ActivityGuiTests.close
    wait = activity_fixture.ActivityGuiTests.wait

    def settle(self):
        end = time.monotonic() + .25
        while time.monotonic() < end:
            self.root.update()
            self.assertEqual(self.errors, [])
            time.sleep(.005)

    def label(self, text):
        pending = [self.app.settings_sections['eda']]
        while pending:
            widget = pending.pop()
            pending.extend(widget.winfo_children())
            if isinstance(widget, gui.ttk.Label) and str(widget.cget('text')).startswith(text):
                return widget
        self.fail('Label not found: ' + text)

    def test_eda_help_shrinks_and_expands_with_actual_field_width(self):
        app = self.app
        app.show_page('Settings')
        app.settings_notebook.select(app.settings_sections['eda'])
        self.root.deiconify()
        self.root.geometry('1080x860')
        self.settle()
        label = self.label('Ocean used to read the saved setup')
        minimum = int(label.cget('wraplength'))
        self.assertLessEqual(minimum, label.master.winfo_width())
        self.assertLess(minimum, 470)
        self.assertGreater(label.winfo_height(), 15)
        self.root.geometry('1380x860')
        self.settle()
        maximum = int(label.cget('wraplength'))
        self.assertGreater(maximum, minimum)
        self.assertLessEqual(maximum, min(470, label.master.winfo_width()))
        # After settling, wrapping does not keep scheduling or reconfiguring.
        with mock.patch.object(label, 'configure', wraps=label.configure) as configured:
            self.settle()
            configured.assert_not_called()

    def test_full_card_and_heading_notes_fit_at_minimum_window_width(self):
        app = self.app
        app.show_page('Settings')
        app.settings_notebook.select(app.settings_sections['crescendo'])
        self.root.deiconify()
        self.root.geometry('1080x860')
        self.settle()
        labels = []
        pending = [app.settings_sections['crescendo']]
        while pending:
            widget = pending.pop()
            pending.extend(widget.winfo_children())
            state = getattr(widget, '_cr_help_wrap_state', None)
            if state:
                labels.extend(label for label, _maximum in state['labels'])
        self.assertGreaterEqual(len(labels), 10)
        for label in labels:
            if label.winfo_ismapped():
                self.assertLessEqual(int(label.cget('wraplength')), max(40, label.master.winfo_width()), str(label.cget('text')))

    def test_analysis_checkboxes_scroll_inner_canvas_without_moving_the_page(self):
        app = self.app
        panel = app.execution_selection_panel
        panel.set_setup({'tests': [{'name': 'Only', 'enabled': True, 'analyses': [
            {'name': 'analysis_%d' % index, 'enabled': True} for index in range(12)]}]})
        app.show_page('Objectives & Constraints')
        self.root.deiconify()
        self.root.geometry('1080x860')
        self.settle()
        page = app.pages[app.current_page]
        checkbox = next(widget for widget in panel.analysis_frame.winfo_children() if isinstance(widget, gui.ttk.Checkbutton))
        event = type('Event', (), {'num': 5, 'delta': 0, 'x_root': 1, 'y_root': 1})()
        before = panel.canvas.yview()
        with mock.patch.object(page, 'winfo_containing', return_value=checkbox), \
                mock.patch.object(page, 'scroll_units') as outer:
            self.assertEqual(app._page_mousewheel(event), 'break')
            outer.assert_not_called()
        self.assertGreater(panel.canvas.yview()[0], before[0])
        # Treeviews embedded on the page retain their own wheel handling.
        tree = gui.ttk.Treeview(page.body)
        try:
            with mock.patch.object(page, 'winfo_containing', return_value=tree):
                self.assertIsNone(app._page_mousewheel(event))
        finally:
            tree.destroy()
