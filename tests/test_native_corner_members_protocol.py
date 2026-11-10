import os
import tempfile
import unittest
from crescendo_engine.private_netlist import (PrivateNetlistUnsupported, native_corner_schedule_skill,
                                                parse_native_corner_schedule)


class NativeCornerMembersProtocolTests(unittest.TestCase):
    def parse(self, text):
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as stream:
            stream.write(text)
            path = stream.name
        self.addCleanup(lambda: os.unlink(path))
        return parse_native_corner_schedule(path)

    def header(self):
        return ('GROUP\t"A"\t1\t2\nGROUP\t"B"\t1\t1\n'
                'DISABLED\t"B"\t"T2"\nNOMINAL\t0\n'
                'NOMINAL_TEST\t"T"\t0\nNOMINAL_TEST\t"T2"\t0\n')

    def test_members_preserve_test_group_and_label(self):
        result = self.parse(self.header() +
            'MEMBER_TEST\t"A"\t"T"\nMEMBER_TEST\t"A"\t"T2"\n'
            'MEMBER_TEST\t"B"\t"T"\nMEMBER_TEST\t"B"\t"T2"\n'
            'MEMBER\t"A"\t"T"\t"one"\nMEMBER\t"A"\t"T"\t"two"\n'
            'MEMBER\t"A"\t"T2"\t"one"\nMEMBER\t"B"\t"T"\t"three"\n'
            'MEMBERS_DONE\nREAD_OK\n')
        self.assertEqual(result['members_state'], 'available')
        self.assertEqual(result['groups']['A']['test_members']['T'], ['one', 'two'])
        self.assertEqual(result['groups']['B']['test_members']['T2'], [])

    def test_unavailable_and_legacy(self):
        self.assertEqual(self.parse(self.header() + 'MEMBERS_UNAVAILABLE\nREAD_OK\n')['members_state'], 'unavailable')
        self.assertEqual(self.parse(self.header() + 'READ_OK\n')['members_state'], 'not_requested')

    def test_reject_duplicate_conflict_partial_or_disabled(self):
        for suffix in (
            'MEMBER_TEST\t"A"\t"T"\nMEMBER_TEST\t"A"\t"T"\n',
            'MEMBER_TEST\t"A"\t"T"\nMEMBER_TEST\t"B"\t"T"\nMEMBER\t"A"\t"T"\t"one"\nMEMBER\t"B"\t"T"\t"one"\n',
            'MEMBER_TEST\t"A"\t"T"\nMEMBERS_DONE\n',
            'MEMBER_TEST\t"A"\t"T"\n',
            'MEMBER_TEST\t"A"\t"T"\nMEMBERS_UNAVAILABLE\n',
            'MEMBER_TEST\t"A"\t"T"\nMEMBER_TEST\t"A"\t"T2"\nMEMBER_TEST\t"B"\t"T"\nMEMBER_TEST\t"B"\t"T2"\nMEMBER\t"B"\t"T2"\t"z"\nMEMBERS_DONE\n'):
            with self.subTest(suffix=suffix), self.assertRaises(PrivateNetlistUnsupported):
                self.parse(self.header() + suffix + 'READ_OK\n')

    def test_default_does_not_mutate_and_opt_in_protects_restore(self):
        setup = {'tests': [{'name': 'T'}]}
        baseline = '\n'.join(native_corner_schedule_skill(setup, '/tmp/meta'))
        enabled = '\n'.join(native_corner_schedule_skill(setup, '/tmp/meta', include_members=True))
        self.assertNotIn('maeSetSetup', baseline)
        self.assertIn('unwindProtect', enabled)
        self.assertIn('restore failed; Run blocked', enabled)
        self.assertIn('MEMBERS_UNAVAILABLE', enabled)
        self.assertIn('MEMBERS_DONE', enabled)
