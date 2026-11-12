# -*- coding: utf-8 -*-
import ast
import glob
import os
import re
import unittest

from crescendo import i18n_en_data

CJK = re.compile(u"[一-鿿　-〿＀-￯]")
PH = re.compile(r"\{[^{}]*\}|%[sd]")


class I18nEnDataTest(unittest.TestCase):
    def test_imports_and_nonempty(self):
        self.assertTrue(i18n_en_data.PAIRS)

    def test_english_has_no_cjk(self):
        for zh, en in i18n_en_data.PAIRS:
            self.assertIsNone(CJK.search(en), (zh, en))

    def test_keys_contain_cjk(self):
        for zh, en in i18n_en_data.PAIRS:
            self.assertTrue(CJK.search(zh), zh)
            self.assertTrue(en.strip(), zh)

    def test_placeholders_match(self):
        for zh, en in i18n_en_data.PAIRS:
            self.assertEqual(PH.findall(zh), PH.findall(en), (zh, en))

    def test_all_engine_string_literals_translate_to_english(self):
        from crescendo.i18n import tr
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        untranslated = []
        for path in sorted(glob.glob(os.path.join(root, "crescendo_engine", "*.py"))):
            with open(path, encoding="utf-8") as source:
                tree = ast.parse(source.read(), filename=path)
            for node in ast.walk(tree):
                value = node.value if isinstance(node, getattr(ast, "Constant", ())) else getattr(node, "s", None)
                if isinstance(value, str) and CJK.search(tr(value)):
                    untranslated.append((os.path.basename(path), node.lineno, value))
        self.assertEqual(untranslated, [])

    def test_specific_format_precedes_generic_format_on_every_python(self):
        from crescendo.i18n import tr
        self.assertEqual(tr("安全退出未完成：owned stop pending"),
                         "Safe exit not completed: owned stop pending")

    def test_no_duplicate_keys(self):
        keys = [zh for zh, _ in i18n_en_data.PAIRS]
        self.assertEqual(len(keys), len(set(keys)))

    def test_sorted_by_length(self):
        lens = [len(zh) for zh, _ in i18n_en_data.PAIRS]
        self.assertEqual(lens, sorted(lens, reverse=True))


if __name__ == "__main__":
    unittest.main()
