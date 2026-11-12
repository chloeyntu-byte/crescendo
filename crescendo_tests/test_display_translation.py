# -*- coding: utf-8 -*-
"""Asynchronous window labels keep their English presentation after mapping."""
import os
import unittest
from unittest import mock

from crescendo import app


@unittest.skipUnless(os.environ.get('DISPLAY'), 'DISPLAY required')
class DynamicLabelTranslationTests(unittest.TestCase):
    def setUp(self):
        self.root = app.tk.Tk()
        self.root.withdraw()
        self.addCleanup(self.root.destroy)

    def test_async_suite_count_and_warnings_are_translated_without_editing_inputs(self):
        label = app.ttk.Label(self.root, text='正在读取套件列表…')
        entry = app.ttk.Entry(self.root)
        entry.insert(0, '用户输入')
        window = app.CrescendoGui.__new__(app.CrescendoGui)
        window._translate_static_texts(self.root)
        label.configure(text='共 2 个套件记录。')
        self.assertEqual(label.cget('text'), '2 suite records in total. ')
        label.config({'text': '已重命名。'})
        self.assertEqual(label.cget('text'), 'Renamed.')
        self.assertEqual(entry.get(), '用户输入')

    def test_kernel_wording_mode_keeps_dynamic_label_text(self):
        label = app.ttk.Label(self.root, text='正在读取套件列表…')
        window = app.CrescendoGui.__new__(app.CrescendoGui)
        with mock.patch.object(app.i18n, 'ENABLED', False):
            window._translate_static_texts(self.root)
            label.configure(text='共 2 个套件记录。')
        self.assertEqual(label.cget('text'), '共 2 个套件记录。')
