"""Per-Test native includePath survives both solver builders and producers."""
import copy
import os
import shlex
import tempfile
import unittest

from crescendo_engine.execution_commands import (
    ExecutionCommandError, build_solver_command_plan, normalize_execution_commands,
    validate_solver_command_plan, _fingerprint,
)
from crescendo_engine.native_includes import freeze_native_includes, resolve_include_file
from crescendo_engine.netlist_providers import _freeze_task
from crescendo_engine.private_netlist import materialize_candidate, _manifest_digest
from crescendo_engine.simulators import execution_contract
from crescendo_engine.utils import sha256_file
from tests import test_private_netlist as template_fixtures


def include_dirs(argv):
    result, index = [], 0
    while index < len(argv):
        if argv[index] == '-I':
            result.append(argv[index + 1])
            index += 2
        elif argv[index].startswith('-I'):
            result.append(argv[index][2:])
            index += 1
        else:
            index += 1
    return result


class NativeIncludeExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='crescendo per test include ')
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.models = {name: os.path.join(self.root, 'models', name) for name in ('spectre', 'alps')}
        self.explicit = os.path.join(self.root, 'edited paths')
        self.common = os.path.join(self.root, 'shared data')
        for path in list(self.models.values()) + [self.explicit, self.common]:
            os.makedirs(path)
        self.config = {'cadence': {'spectre_bin': '/bin/true', 'alps_bin': '/bin/true',
            'spectre_model_dir': self.models['spectre'], 'alps_model_dir': self.models['alps']}}

    def native_task(self, test, corner):
        cwd = os.path.join(self.root, 'native', test, corner)
        data = os.path.join(cwd, 'spectre data files')
        os.makedirs(data, exist_ok=True)
        filename = os.path.join(data, 'shared.tbl')
        with open(filename, 'w') as stream:
            stream.write(test + '/' + corner)
        return {'test': test, 'corner': corner, 'native_cwd': cwd,
            'argv': ['/bin/true', 'input.scs', '-I', 'spectre data files', '-I' + self.common]}, filename

    def settings(self, simulator, edited):
        template = ''
        if edited:
            template = ('{simulator} {netlist} +mt={threads} -raw {psf} +log {log}'
                if simulator == 'spectre' else
                '{simulator} {netlist} -format ps -o {psf} -I {model_dir} -mt {threads}')
            template += ' -I ' + shlex.quote(self.explicit)
        return normalize_execution_commands({}, {'execution_commands': {'solver_templates': {simulator: template}}})

    def check_paths(self, argv, filename, native, simulator, edited):
        directories = include_dirs(argv)
        expected = [os.path.dirname(filename), self.common, self.models['spectre']]
        self.assertEqual([path for path in directories if path in expected], expected)
        self.assertEqual(len(directories), len(set(directories)))
        self.assertEqual(resolve_include_file('shared.tbl', self.root, directories, {}), filename)
        if simulator == 'alps':
            self.assertEqual(directories[0], self.models['alps'])
            self.assertTrue(all(value == '-I' or not value.startswith('-I') for value in argv))
        else:
            self.assertNotIn('-I', argv)
        if edited:
            self.assertLess(directories.index(self.explicit), directories.index(os.path.dirname(filename)))
        self.assertEqual(native['argv'][native['argv'].index('-I') + 1], 'spectre data files')

    def test_two_tests_and_corners_keep_their_own_same_named_file_in_both_command_modes(self):
        for test in ('TestA', 'TestB'):
            for corner in ('TT', 'SS'):
                native, filename = self.native_task(test, corner)
                _text, argv, proof = freeze_native_includes('', native, self.config, 'spectre')
                self.assertEqual(proof['include_dirs'][0], os.path.dirname(filename))
                for simulator in ('spectre', 'alps'):
                    contract = execution_contract(self.config, spec={'simulator': simulator}, template_argv=argv)
                    for edited in (False, True):
                        with self.subTest(test=test, corner=corner, simulator=simulator, edited=edited):
                            plan = build_solver_command_plan(contract, argv, '/private/input.scs',
                                '/private/psf', '/private/log', '/private/ahdl', self.settings(simulator, edited))
                            self.check_paths(plan['argv'], filename, native, simulator, edited)
                            self.assertEqual(plan['native_argv'], argv)

    def test_explicit_template_directory_order_survives_native_deduplication(self):
        native, filename = self.native_task('TestA', 'TT')
        _text, argv, _proof = freeze_native_includes('', native, self.config, 'spectre')
        for simulator in ('spectre', 'alps'):
            contract = execution_contract(self.config, spec={'simulator': simulator}, template_argv=argv)
            settings = self.settings(simulator, True)
            template = settings['solver_templates'][simulator]
            template += ' -I ' + shlex.quote(self.common) + ' -I' + shlex.quote(self.explicit)
            settings = normalize_execution_commands({}, {'execution_commands': {'solver_templates': {simulator: template}}})
            plan = build_solver_command_plan(contract, argv + ['-I', self.common], '/private/input.scs',
                '/private/psf', '/private/log', '/private/ahdl', settings)
            directories = include_dirs(plan['argv'])
            explicit_order = [self.explicit, self.common]
            self.assertEqual([p for p in directories if p in explicit_order], explicit_order)
            self.assertEqual(len(directories), len(set(directories)))
            self.assertIn(os.path.dirname(filename), directories)

    def test_malformed_native_include_is_rejected_even_with_an_edited_command(self):
        for simulator in ('spectre', 'alps'):
            contract = execution_contract(self.config, spec={'simulator': simulator})
            for edited in (False, True):
                with self.subTest(simulator=simulator, edited=edited), self.assertRaises(ExecutionCommandError):
                    build_solver_command_plan(contract, ['/bin/true', 'input.scs', '-I'],
                        '/private/input.scs', '/private/psf', '/private/log', '/private/ahdl',
                        self.settings(simulator, edited))

    def test_old_frozen_plan_missing_native_paths_is_rejected_without_mutation(self):
        native, filename = self.native_task('TestA', 'TT')
        _text, argv, _proof = freeze_native_includes('', native, self.config, 'spectre')
        for simulator in ('spectre', 'alps'):
            contract = execution_contract(self.config, spec={'simulator': simulator}, template_argv=argv)
            task = {'netlist_path': '/private/input.scs', 'psf_dir': '/private/psf',
                    'spectre_log': '/private/log', 'ahdl_dir': '/private/ahdl'}
            plan = build_solver_command_plan(contract, argv, task['netlist_path'], task['psf_dir'],
                task['spectre_log'], task['ahdl_dir'], self.settings(simulator, True))
            missing = {os.path.dirname(filename), self.common}
            original, plan['argv'], index = plan['argv'], [], 0
            while index < len(original):
                value = original[index]
                if value == '-I' and original[index + 1] in missing:
                    index += 2
                elif value.startswith('-I') and value[2:] in missing:
                    index += 1
                else:
                    plan['argv'].append(value)
                    index += 1
            plan['fingerprint'] = _fingerprint(plan)
            task['argv'] = list(plan['argv'])
            before = copy.deepcopy((plan, task))
            with self.assertRaisesRegex(ExecutionCommandError, 'differs'):
                validate_solver_command_plan(plan, contract, task)
            self.assertEqual((plan, task), before)

    def test_numeric_provider_retains_per_task_paths_for_spectre_and_alps(self):
        for simulator in ('spectre', 'alps'):
            for edited in (False, True):
                for test, corner in (('TestA', 'TT'), ('TestB', 'SS')):
                    native, filename = self.native_task(test, corner)
                    candidate = os.path.join(self.root, 'numeric-' + simulator + str(edited), test)
                    capture = os.path.join(candidate, 'capture')
                    os.makedirs(capture)
                    netlist = os.path.join(capture, 'input.scs')
                    with open(netlist, 'w') as stream:
                        stream.write('simulator lang=spectre\nparameters P=1\nR0 (p 0) resistor r=P\n')
                    task = dict(native, task_id='p1_' + test, cwd=capture, netlist_path=netlist,
                        template_files=[{'path': netlist, 'relative_path': 'input.scs', 'sha256': sha256_file(netlist)}])
                    task = _freeze_task(task, self.config, 'spectre', self.settings(simulator, edited),
                        1, {'simulator': simulator}, candidate)
                    self.check_paths(task['argv'], filename, native, simulator, edited)
                    self.assertTrue(validate_solver_command_plan(task['solver_command_plan'], task['simulator_contract'], task))

    def test_template_materialization_preserves_test_corner_paths_in_each_candidate(self):
        for simulator in ('spectre', 'alps'):
            for edited in (False, True):
                parent = os.path.join(self.root, 'template-' + simulator + str(edited))
                os.makedirs(parent)
                manifest = template_fixtures.PrivateNetlistCandidateTest().template(parent)
                prototype = manifest['tasks'][0]
                manifest['tasks'] = []
                expected = {}
                for index, (test, corner) in enumerate((('TestA', 'TT'), ('TestB', 'SS'))):
                    native, filename = self.native_task(test, corner)
                    _text, argv, resolution = freeze_native_includes('', native, self.config, 'spectre')
                    task = copy.deepcopy(prototype)
                    task.update(task_id='t' + str(index), test=test, corner=corner,
                        native_solver_argv=argv, argv=argv, include_resolution=resolution,
                        execution_commands=self.settings(simulator, edited),
                        simulator_contract=execution_contract(self.config, spec={'simulator': simulator}, template_argv=argv))
                    manifest['tasks'].append(task)
                    expected[test] = (filename, native)
                manifest['manifest_sha256'] = _manifest_digest(manifest)
                for identifier, value in (('first', 1000), ('second', 2000)):
                    candidate = materialize_candidate(manifest, {'p': value}, os.path.join(parent, identifier), identifier)
                    for task in candidate['tasks']:
                        filename, native = expected[task['test']]
                        self.check_paths(task['argv'], filename, native, simulator, edited)
                        self.assertEqual(task['corner'], native['corner'])
                        self.assertTrue(validate_solver_command_plan(task['solver_command_plan'], task['simulator_contract'], task))


if __name__ == '__main__':
    unittest.main()
