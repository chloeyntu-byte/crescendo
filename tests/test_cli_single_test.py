import unittest
from unittest import mock
from crescendo_engine import __main__ as cli


class SingleTestCliTest(unittest.TestCase):
    def test_selection(self):
        one = {'tests': [{'name': 'A'}]}
        many = {'tests': [{'name': 'A'}, {'name': 'B'}]}
        self.assertEqual(cli._choose_test(one, None), 'A')
        self.assertEqual(cli._choose_test(many, 'B'), 'B')
        with self.assertRaisesRegex(ValueError, 'exactly one'):
            cli._choose_test(many, None)
        with self.assertRaisesRegex(ValueError, 'Unknown Test'):
            cli._choose_test(many, 'C')

    def test_prepare_projects_selected_test(self):
        with mock.patch.object(cli, 'load_config', return_value={}), \
             mock.patch.object(cli, '_read_bound_test', return_value='B') as read, \
             mock.patch.object(cli, 'prepare_setup', return_value={}) as prepare:
            self.assertEqual(cli.main(['prepare', '/a', '--test', 'B']), 0)
        self.assertEqual(read.call_args[0][-1], 'B')
        self.assertEqual(prepare.call_args[1]['selected_test'], 'B')

    def test_run_spec_propagates_selected_test(self):
        with mock.patch.object(cli, 'load_json', return_value={'setup_path': '/a'}), \
             mock.patch.object(cli, 'load_config', return_value={}), \
             mock.patch.object(cli, '_read_bound_test', return_value='B'), \
             mock.patch.object(cli, 'run_optimization', return_value={}) as run:
            self.assertEqual(cli.main(['run', '/spec', '--test', 'B']), 0)
        self.assertEqual(run.call_args[0][0]['selected_test'], 'B')


if __name__ == '__main__':
    unittest.main()
