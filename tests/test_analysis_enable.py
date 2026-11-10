import unittest
import xml.etree.ElementTree as ET
from crescendo_engine.ade_setup import _analysis_rows


class AnalysisEnableTests(unittest.TestCase):
    def rows(self,text):
        return _analysis_rows(ET.fromstring('<test>'+text+'</test>'))

    def test_native_append_of_nil_skilldpl_disables_previously_enabled_analysis(self):
        rows=self.rows('<analysis Name="tran"><Field Name="enable" Type="list">(t)</Field><Field Name="enable" Type="skillDpl"></Field></analysis>')
        self.assertFalse(rows[0]['enabled'])
        self.assertEqual(rows[0]['fields']['enable'],'')

    def test_explicit_nil_is_not_mistaken_for_missing_field(self):
        for value in ('nil','(nil)','()','false','0'):
            rows=self.rows('<analysis Name="ac"><Field Name="enable">'+value+'</Field></analysis>')
            self.assertFalse(rows[0]['enabled'],value)

    def test_absent_field_respects_attribute_and_native_list_true(self):
        rows=self.rows('<analysis Name="dc"><Field Name="enable" Type="list">(t)</Field></analysis><analysis Name="noise" enabled="false"/><analysis Name="ac"/>')
        self.assertEqual([(r['name'],r['enabled']) for r in rows],[('dc',True),('noise',False),('ac',True)])

    def test_latest_true_override_enables_previously_disabled_analysis(self):
        rows=self.rows('<analysis Name="dc"><Field Name="enable">nil</Field><Field Name="enable" Type="skillDpl">t</Field></analysis>')
        self.assertTrue(rows[0]['enabled'])
