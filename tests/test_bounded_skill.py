"""Large generated bodies preserve all operations and a native failure fence."""
import os
import tempfile
import unittest

from crescendo_engine.workspace import bounded_skill_body_lines
from tests.test_persistent_maestro import PersistentMaestroTest


class BoundedSkillTests(unittest.TestCase):
    def test_large_body_keeps_every_expression_once_in_order(self):
        body = ['crescAuditCounter = %d' % index for index in range(30000)]
        rendered, generation = bounded_skill_body_lines(body, 'crescAuditResult')
        self.assertEqual(generation['expression_count'], 30000)
        self.assertEqual(generation['block_count'], 235)
        operations = [line.strip() for line in rendered if 'crescAuditCounter =' in line]
        self.assertEqual(operations, body)
        self.assertNotIn('progn(\n' + '\n'.join(body), '\n'.join(rendered))

    def test_balanced_callbacks_and_escaped_string_are_never_split(self):
        body = ['x = "quoted \\"(\\")"', 'foreach(item list(1 2)',
                '  when(item ; a comment with ( )', '    printf("%L\\n" item)',
                '  )', ')', 'last = t']
        rendered, generation = bounded_skill_body_lines(body, 'crescAuditResult', max_forms=1)
        self.assertEqual(generation['expression_count'], 3)
        text = '\n'.join(rendered)
        self.assertIn('\n'.join(body[1:6]), text)

    def test_failure_latch_is_cleared_before_each_native_block(self):
        rendered, generation = bounded_skill_body_lines(['x = 1', 'error("failure")', 'x = 2'],
                                                       'crescAuditResult', max_forms=1)
        text = '\n'.join(rendered)
        self.assertEqual(text.count('crescAuditResult = nil'), 3)
        self.assertTrue(text.index('crescAuditResult = nil') < text.index('crescAuditResult = errset'))
        self.assertEqual(text.count('when(crescAuditResult_allowed'), 3)

    def test_unbalanced_source_is_rejected_without_emitting_partial_body(self):
        for body in (['foreach(item list(1 2)'], ['foo)'], ['x = "unclosed']):
            with self.assertRaises(ValueError):
                bounded_skill_body_lines(body)

    def test_real_bootstrap_has_bounded_generation_manifest(self):
        with tempfile.TemporaryDirectory() as root:
            worker, _factory, _clock = PersistentMaestroTest()._worker(root)
            path = worker._write_bootstrap({'epoch': worker.epoch, 'sequence': 0,
                                          'nonce': 'bounded', 'candidate_id': 'startup'})
            with open(path) as stream:
                text = stream.read()
            self.assertIn('CRESCENDO_SKILL_BLOCK bootstrapResult', text)
            self.assertNotIn('bootstrapResult = errset(progn(\n  unless', text)
            self.assertIn('crescBootstrapStage = "occurrence_clone"', text)
            self.assertIn('crescBootstrapStage = "maestro_setup_open"', text)


if __name__ == '__main__':
    unittest.main()
