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

    def test_display_terminology_excludes_retired_words(self):
        retired = re.compile(r"\b(?:baseline|private|privatization)\b", re.I)
        for zh, en in i18n_en_data.PAIRS:
            self.assertIsNone(retired.search(en), (zh, en))

    def test_mode_names_keep_original_translation_keys(self):
        from crescendo.i18n import tr
        expected = {
            "私有 Maestro（Baseline）": "Native Run",
            "私有网表 CLI": "Template Netlist CLI",
            "私有 Maestro 生成网表": "Maestro Netlist",
            "私有套件": "Working Copies",
        }
        for original, display in expected.items():
            self.assertEqual(tr(original), display)

    def test_selection_notice_matches_runtime_wording(self):
        from crescendo.i18n import tr
        from crescendo_engine.variable_selection_notice import variable_selection_notice
        chinese = ("已选 51 个变量。变量较多时，Template Netlist 的参考捕获在部分环境中可能导致 Virtuoso 意外退出。"
                   "如果遇到这种情况，请减少选中的变量，或改用 Maestro Netlist 或 Native Run。")
        self.assertEqual(tr(chinese), variable_selection_notice("private_netlist", 51))
        chinese = ("已选 51 个变量。变量较多时，每代耗时和达到收敛所需的代数都可能明显增加。"
                   "建议先收窄变量选择范围。")
        for mode in ("maestro_baseline", "maestro_netlist"):
            self.assertEqual(tr(chinese), variable_selection_notice(mode, 51))

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
