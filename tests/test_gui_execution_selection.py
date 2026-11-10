import os
import tkinter as tk
import unittest
from crescendo_engine.gui_execution_selection import ExecutionSelectionPanel


@unittest.skipUnless(os.environ.get('DISPLAY'), 'Real Tk requires DISPLAY')
class ExecutionSelectionPanelTests(unittest.TestCase):
    def setUp(self):
        self.root = tk.Tk(); self.root.withdraw()
        self.calls = []
        self.panel = ExecutionSelectionPanel(self.root, lambda: self.calls.append(True))

    def tearDown(self):
        self.panel.destroy(); self.root.destroy()

    def test_only_current_test_and_inherited_analysis_state(self):
        self.panel.set_setup({'tests': [{'name': 'B', 'enabled': False, 'analyses': [
            {'name': 'sp', 'enabled': False}, {'name': 'dc', 'enabled': True}]}]})
        self.assertEqual(self.panel.collect(), {'tests': ['B'], 'analyses': {'B': ['dc']}})
        self.assertEqual([(key, value.get()) for key, value in self.panel.analysis_variables], [('sp', False), ('dc', True)])
        self.panel.analysis_variables[0][1].set(True); self.panel._changed()
        self.assertEqual(self.panel.collect()['analyses']['B'], ['sp', 'dc'])
        self.assertTrue(self.calls)

    def test_restore_requires_same_single_test_and_filters_analyses(self):
        self.panel.set_setup({'tests': [{'name': 'B', 'analyses': [{'name': 'dc'}, {'name': 'sp'}]}]})
        with self.assertRaisesRegex(ValueError, 'exactly'):
            self.panel.restore({'tests': ['A', 'B']})
        self.panel.restore({'tests': ['B'], 'analyses': {'B': ['sp', 'removed']}})
        self.assertEqual(self.panel.collect()['analyses'], {'B': ['sp']})
        self.panel.set_setup({'tests': [{'name': 'B', 'analyses': [{'name': 'dc'}, {'name': 'sp'}]}]}, preserve=True)
        self.assertEqual(self.panel.collect()['analyses'], {'B': ['sp']})

    def test_empty_analysis_selection_is_rejected(self):
        self.panel.set_setup({'tests': [{'name': 'B', 'analyses': [{'name': 'dc'}]}]})
        self.panel.restore({'tests': ['B'], 'analyses': {'B': []}})
        with self.assertRaisesRegex(ValueError, 'analysis'):
            self.panel.collect()
        self.panel.set_setup({'tests': [{'name': 'B', 'analyses': []}]})
        self.assertEqual(self.panel.collect(), {'tests': ['B'], 'analyses': {}})

    def test_multiple_tests_cannot_enter_analysis_panel(self):
        with self.assertRaisesRegex(ValueError, 'exactly one'):
            self.panel.set_setup({'tests': [{'name': 'A'}, {'name': 'B'}]})
