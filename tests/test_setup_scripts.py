import hashlib
import json
import os
import tempfile
import unittest
import xml.etree.ElementTree as ET

from crescendo_engine.private_netlist import PrivateNetlistUnsupported, _qualify_setup_capture
from crescendo_engine.setup_scripts import inspect_setup_script_hooks


class SavedScriptHookQualificationTests(unittest.TestCase):
    def test_native_scalar_prerun_switch_off_is_not_a_script_payload(self):
        text = '<setupdb><active><tests><test Name="C"><preRunScript>0</preRunScript>' \
               '<preRunScriptFile/></test></tests></active></setupdb>'
        evidence = self.observe(text)
        self.assertFalse(evidence['blocked'])
        self.assertEqual(evidence['checks'][0]['status'], 'native_disabled_switch')
        self.assertEqual(evidence['checks'][0]['payload_field_count'], 0)
        self.qualify(text)

    def test_native_disabled_switch_does_not_hide_a_companion_script_file(self):
        with self.assertRaises(PrivateNetlistUnsupported):
            self.qualify('<setupdb><active><preRunScript>0</preRunScript>'
                         '<preRunScriptFile>actual.il</preRunScriptFile></active></setupdb>')

    def test_zero_is_not_a_general_empty_script_or_unknown_metadata_rule(self):
        observed = self.observe('<setupdb><active><preRunScript type="file">0</preRunScript></active></setupdb>')
        self.assertEqual(observed['checks'][0]['attribute_names'], ['type'])
        self.assertEqual(observed['checks'][0]['attribute_count'], 1)
        for hook in ('<preRunScript type="file">0</preRunScript>',
                     '<preRunScript file="actual.il">0</preRunScript>',
                     '<preRunScript><code>0</code></preRunScript>',
                     '<preRunScript>"0"</preRunScript>',
                     '<postRunScript>0</postRunScript>',
                     '<preRunScript>1</preRunScript>',
                     '<preRunScript>0; actual()</preRunScript>'):
            with self.subTest(hook=hook), self.assertRaises(PrivateNetlistUnsupported):
                self.qualify('<setupdb><active>' + hook + '</active></setupdb>')

    def observe(self, text):
        return inspect_setup_script_hooks(ET.fromstring(text))

    def qualify(self, text):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'maestro.sdb')
            with open(path, 'w', encoding='utf-8') as stream:
                stream.write(text)
            setup = {'source': {'setup_db': path}, 'tests': [{'simulator': 'spectre'}],
                     'mode': 'Single Run, Sweeps and Corners'}
            _qualify_setup_capture(setup)

    def test_empty_script_containers_do_not_require_a_hook_adapter(self):
        cases = ('<preRunScript/>', '<postRunScript>nil</postRunScript>',
                 '<userScripts><scriptFile>""</scriptFile></userScripts>',
                 '<evalScripts><language>skill</language><count>0</count></evalScripts>',
                 '<preRunScriptEnabled>t</preRunScriptEnabled><preRunScript/>')
        for hook in cases:
            with self.subTest(hook=hook):
                self.assertFalse(self.observe('<setupdb><active>' + hook + '</active></setupdb>')['blocked'])

    def test_explicitly_disabled_payloads_are_preserved_without_blocking(self):
        cases = ('<postRunScript enabled="false">run()</postRunScript>',
                 '<userScripts><enabled>nil</enabled><scriptFile>startup.il</scriptFile></userScripts>',
                 '<tests><test enabled="nil"><preRunScript>run()</preRunScript></test></tests>',
                 '<preRunScripts><script enabled="false"><file>startup.il</file></script></preRunScripts>')
        for hook in cases:
            with self.subTest(hook=hook):
                self.assertFalse(self.observe('<setupdb><active>' + hook + '</active></setupdb>')['blocked'])

    def test_historical_scripts_do_not_change_current_execution_requirements(self):
        self.assertFalse(self.observe('<setupdb><active><history><historyentry><checkpoint>'
                     '<postRunScript>old_run()</postRunScript></checkpoint></historyentry></history>'
                     '<preRunScript/></active></setupdb>')['blocked'])
        self.assertFalse(self.observe('<setupdb><active/><history><postRunScript>old_run()</postRunScript>'
                     '</history></setupdb>')['blocked'])

    def test_real_payload_remains_blocked_with_precise_source_evidence(self):
        text = '<setupdb><active><tests><test Name="C"><postRunScript>' \
               'SECRET_VALUE_DO_NOT_LOG()</postRunScript></test></tests></active></setupdb>'
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, 'maestro.sdb')
            with open(path, 'w', encoding='utf-8') as stream:
                stream.write(text)
            with self.assertRaises(PrivateNetlistUnsupported) as caught:
                _qualify_setup_capture({'source': {'setup_db': path}, 'tests': [{'simulator': 'spectre'}]})
            detail = caught.exception.detail
            self.assertEqual(detail['failure_stage'], 'native_user_scripts')
            self.assertEqual(detail['source_setup_sha256'], hashlib.sha256(text.encode()).hexdigest())
            self.assertFalse(detail['execution_started'])
            self.assertTrue(detail['cleanup_proven'])
            self.assertEqual(detail['script_checks'][0]['xml_path'], '/active/tests[1]/test[1]/postrunscript[1]')
            self.assertTrue(detail['script_checks'][0]['blocking'])
            self.assertNotIn('SECRET_VALUE_DO_NOT_LOG', str(caught.exception) + json.dumps(detail))

    def test_file_attribute_is_a_real_execution_dependency(self):
        with self.assertRaisesRegex(PrivateNetlistUnsupported, 'scripts require'):
            self.qualify('<setupdb><active><postRunScript file="post.il" enabled="true"/></active></setupdb>')

    def test_unknown_enabled_value_does_not_authorize_skipping_nonempty_code(self):
        with self.assertRaises(PrivateNetlistUnsupported):
            self.qualify('<setupdb><active><postRunScript enabled="unknown">run()</postRunScript></active></setupdb>')

    def test_disabled_sibling_cannot_mask_an_enabled_script(self):
        with self.assertRaises(PrivateNetlistUnsupported):
            self.qualify('<setupdb><active><preRunScripts><script enabled="false"><file>old.il</file>'
                         '</script><script enabled="true"><file>actual.il</file></script></preRunScripts></active></setupdb>')

    def test_namespaces_and_quote_empty_literals_are_supported(self):
        self.assertFalse(self.observe('<s:setupdb xmlns:s="urn:test"><s:active><s:pre_Run_Script>""</s:pre_Run_Script>'
                     '</s:active></s:setupdb>')['blocked'])

    def test_literal_uppercase_nil_is_not_guessed_to_be_empty(self):
        with self.assertRaises(PrivateNetlistUnsupported):
            self.qualify('<setupdb><active><postRunScript>NIL</postRunScript></active></setupdb>')

    def test_unobserved_hook_tail_is_blocked_and_counted(self):
        root = ET.fromstring('<setupdb><active>' + '<preRunScript/>' * 65 + '</active></setupdb>')
        evidence = inspect_setup_script_hooks(root)
        self.assertTrue(evidence['blocked'])
        self.assertEqual(evidence['hook_count'], 65)
        self.assertEqual(evidence['omitted_hooks'], 1)
        self.assertEqual(len(evidence['checks']), 64)

    def test_control_subtrees_do_not_hide_actual_payloads(self):
        cases = ('<enabled><scriptFile>actual.il</scriptFile></enabled>',
                 '<language file="actual.il"/>', '<type><code>run()</code></type>')
        for nested in cases:
            with self.subTest(nested=nested), self.assertRaises(PrivateNetlistUnsupported):
                self.qualify('<setupdb><active><preRunScript>' + nested + '</preRunScript></active></setupdb>')

    def test_text_after_control_child_is_a_script_payload(self):
        with self.assertRaises(PrivateNetlistUnsupported):
            self.qualify('<setupdb><active><preRunScript><enabled>true</enabled>run()'
                         '</preRunScript></active></setupdb>')

    def test_conflicting_flags_do_not_prove_disabled(self):
        cases = ('enabled="false" disabled="false"', 'enabled="false" enable="unknown"')
        for attributes in cases:
            with self.subTest(attributes=attributes), self.assertRaises(PrivateNetlistUnsupported):
                self.qualify('<setupdb><active><preRunScript ' + attributes + '>run()</preRunScript></active></setupdb>')
        with self.assertRaises(PrivateNetlistUnsupported):
            self.qualify('<setupdb><active><preRunScript enabled="false"><enabled>true</enabled>'
                         '<file>actual.il</file></preRunScript></active></setupdb>')

    def test_unknown_nonmetadata_attributes_remain_unqualified(self):
        for attributes in ('script-file="actual.il"', 'src="actual.il"', 'unknown="run()"'):
            with self.subTest(attributes=attributes), self.assertRaises(PrivateNetlistUnsupported):
                self.qualify('<setupdb><active><preRunScript ' + attributes + '/></active></setupdb>')

    def test_checkpoint_inside_a_hook_cannot_be_skipped_as_history(self):
        with self.assertRaises(PrivateNetlistUnsupported):
            self.qualify('<setupdb><active><preRunScript><checkpoint>run()</checkpoint>'
                         '</preRunScript></active></setupdb>')

    def test_script_file_expression_is_not_logged_as_a_file_reference(self):
        root = ET.fromstring('<setupdb><active><preRunScript><scriptFile>'
                             'system("SECRET_VALUE_DO_NOT_LOG")</scriptFile></preRunScript></active></setupdb>')
        evidence = inspect_setup_script_hooks(root)
        self.assertTrue(evidence['blocked'])
        self.assertNotIn('SECRET_VALUE_DO_NOT_LOG', json.dumps(evidence))
        self.assertNotIn('file_reference', evidence['checks'][0]['payload_metadata'][0])

    def test_empty_or_disabled_observations_do_not_override_saved_native_guard(self):
        for hook in ('<preRunScript>nil</preRunScript>',
                     '<preRunScript enabled="false">run()</preRunScript>',
                     '<preRunScript><language>skill</language></preRunScript>'):
            text = '<setupdb><active>' + hook + '</active></setupdb>'
            self.assertFalse(self.observe(text)['blocked'])
            with self.subTest(hook=hook), self.assertRaises(PrivateNetlistUnsupported) as caught:
                self.qualify(text)
            self.assertEqual(caught.exception.detail['qualification_policy'], 'preserved_guard_pending_site_evidence')
            self.assertTrue(caught.exception.detail['legacy_guard_hits'])

    def test_plain_empty_hook_keeps_existing_capture_behavior(self):
        self.qualify('<setupdb><active><preRunScript/></active></setupdb>')


if __name__ == '__main__':
    unittest.main()
