# -*- coding: utf-8 -*-
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

    def test_no_duplicate_keys(self):
        keys = [zh for zh, _ in i18n_en_data.PAIRS]
        self.assertEqual(len(keys), len(set(keys)))

    def test_sorted_by_length(self):
        lens = [len(zh) for zh, _ in i18n_en_data.PAIRS]
        self.assertEqual(lens, sorted(lens, reverse=True))


if __name__ == "__main__":
    unittest.main()
