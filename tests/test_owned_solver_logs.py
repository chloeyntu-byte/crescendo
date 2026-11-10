"""ALPS's PSF companion links never expand the owned read boundary."""
import os
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine.owned_solver_logs import OwnedSolverLogError, owned_solver_log_reader
from crescendo_engine.native_log_io import _reader_snapshot


class OwnedSolverLogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name
        self.task_root = os.path.join(self.root, 'candidate', 'tasks', 'owned')
        self.psf = os.path.join(self.task_root, 'psf')
        os.makedirs(self.psf)
        self.task = {'execution_simulator': 'alps', 'psf_dir': self.psf, 'task_root': self.task_root}
        self.epoch = time.time() - 1
        self.target = self.write(os.path.join(self.psf, 'alps.log'), 'current ALPS log\n')
        self.alias = self.psf + '.log'

    def tearDown(self):
        self.temp.cleanup()

    def write(self, path, text):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as stream:
            stream.write(text)
        return path

    def read(self, path=None, task=None):
        with owned_solver_log_reader(path or self.alias, self.root, self.epoch, task=task or self.task) as (fd, metadata):
            return os.read(fd, 1024), metadata

    def test_relative_and_absolute_leaf_aliases_capture_both_identities(self):
        for link in ('psf/alps.log', self.target):
            os.symlink(link, self.alias)
            content, meta = self.read()
            self.assertEqual(content, b'current ALPS log\n')
            self.assertEqual(meta['source_path'], self.alias)
            self.assertEqual(meta['resolved_source_path'], self.target)
            self.assertTrue(meta['leaf_link'])
            self.assertNotEqual(meta['source_inode'], meta['target_inode'])
            os.unlink(self.alias)

    def test_regular_file_contract_and_reader_barrier_are_preserved(self):
        with owned_solver_log_reader(self.target, self.root, self.epoch) as (fd, meta):
            self.assertFalse(meta['leaf_link'])
            self.assertEqual(os.read(fd, 1024), b'current ALPS log\n')
            self.assertEqual(_reader_snapshot()['active_reader_count'], 1)
        self.assertEqual(_reader_snapshot()['active_reader_count'], 0)

    def test_spectre_or_missing_simulator_never_enables_companion_exception(self):
        os.symlink(self.target, self.alias)
        for selected in ('spectre', None):
            task = dict(self.task, execution_simulator=selected)
            with self.assertRaisesRegex(OwnedSolverLogError, 'unowned_or_linked_path'):
                self.read(task=task)

    def test_companion_alias_without_owned_launch_freshness_is_rejected(self):
        os.symlink(self.target, self.alias)
        with self.assertRaisesRegex(OwnedSolverLogError, 'companion_freshness_unavailable'):
            with owned_solver_log_reader(self.alias, self.root, task=self.task):
                self.fail('alias read without a launch timestamp')

    def test_another_leaf_name_and_wrong_task_identity_are_rejected(self):
        os.symlink(self.target, self.alias)
        with self.assertRaises(OwnedSolverLogError):
            self.read(task=dict(self.task, task_root=os.path.join(self.root, 'another')))
        alternate = os.path.join(self.task_root, 'solver.stdout')
        os.symlink(self.target, alternate)
        with self.assertRaises(OwnedSolverLogError):
            self.read(path=alternate)

    def test_external_cross_task_and_cross_candidate_targets_are_rejected(self):
        targets = [self.write(os.path.join(self.root, 'external.log'), 'unowned'),
            self.write(os.path.join(self.root, 'candidate', 'tasks', 'other', 'psf', 'log'), 'other task'),
            self.write(os.path.join(self.root, 'other_candidate', 'psf', 'log'), 'other candidate')]
        with tempfile.TemporaryDirectory() as outside:
            targets.append(self.write(os.path.join(outside, 'log'), 'external root'))
            for target in targets:
                os.symlink(target, self.alias)
                with self.subTest(target=target), self.assertRaises(OwnedSolverLogError):
                    self.read()
                os.unlink(self.alias)

    def test_chain_and_linked_target_ancestor_are_rejected_even_inside_psf(self):
        chain = os.path.join(self.psf, 'chain')
        os.symlink(self.target, chain)
        os.symlink(chain, self.alias)
        with self.assertRaisesRegex(OwnedSolverLogError, 'linked_or_nonregular_log'):
            self.read()
        os.unlink(self.alias)
        directory = os.path.join(self.psf, 'linked_directory')
        os.symlink(self.psf, directory)
        os.symlink(os.path.join(directory, 'alps.log'), self.alias)
        with self.assertRaisesRegex(OwnedSolverLogError, 'unowned_or_linked_path'):
            self.read()

    def test_source_ancestor_link_and_root_alias_are_rejected(self):
        alias_root = os.path.join(self.root, 'aliased_candidate')
        os.symlink(os.path.dirname(os.path.dirname(self.task_root)), alias_root)
        source = os.path.join(alias_root, 'tasks', 'owned', 'psf', 'alps.log')
        with self.assertRaises(OwnedSolverLogError):
            self.read(path=source)
        with self.assertRaises(OwnedSolverLogError):
            with owned_solver_log_reader(source, alias_root, self.epoch, task=self.task):
                self.fail('linked root was opened')

    def test_hardlink_fifo_directory_and_dangling_alias_are_rejected_without_blocking(self):
        hard = os.path.join(self.psf, 'hard')
        os.link(self.target, hard)
        fifo = os.path.join(self.psf, 'fifo')
        os.mkfifo(fifo)
        for target in (hard, fifo, self.psf):
            os.symlink(target, self.alias)
            with self.subTest(target=target), self.assertRaises(OwnedSolverLogError):
                self.read()
            os.unlink(self.alias)
        os.symlink(os.path.join(self.psf, 'missing'), self.alias)
        with self.assertRaises(FileNotFoundError):
            self.read()

    def test_new_alias_mtime_does_not_make_stale_target_fresh(self):
        os.utime(self.target, (self.epoch - 20, self.epoch - 20))
        os.symlink(self.target, self.alias)
        with self.assertRaisesRegex(OwnedSolverLogError, 'stale_log'):
            self.read()

    def test_alias_changed_during_read_discards_the_snapshot(self):
        os.symlink(self.target, self.alias)
        alternate = self.write(os.path.join(self.psf, 'other.log'), 'other bytes')
        with self.assertRaisesRegex(OwnedSolverLogError, 'log_alias_changed_during_read'):
            with owned_solver_log_reader(self.alias, self.root, self.epoch, task=self.task) as (fd, _meta):
                self.assertEqual(os.read(fd, 1024), b'current ALPS log\n')
                os.unlink(self.alias)
                os.symlink(alternate, self.alias)
        self.assertEqual(_reader_snapshot()['active_reader_count'], 0)

    def test_target_path_changed_during_read_is_rejected(self):
        os.symlink(self.target, self.alias)
        with self.assertRaisesRegex(OwnedSolverLogError, 'log_target_changed_during_read'):
            with owned_solver_log_reader(self.alias, self.root, self.epoch, task=self.task) as (fd, _meta):
                self.assertEqual(os.read(fd, 1024), b'current ALPS log\n')
                os.unlink(self.target)
                self.write(self.target, 'replacement')

    def test_target_swapped_to_external_alias_before_open_is_never_read(self):
        os.symlink(self.target, self.alias)
        real_open = os.open
        with tempfile.TemporaryDirectory() as outside:
            secret = self.write(os.path.join(outside, 'secret'), 'DO_NOT_READ')
            def swapping_open(path, flags, *args, **kwargs):
                if path == 'alps.log' and not flags & os.O_DIRECTORY:
                    os.unlink(self.target)
                    os.symlink(secret, self.target)
                return real_open(path, flags, *args, **kwargs)
            with mock.patch('crescendo_engine.owned_solver_logs.os.open', side_effect=swapping_open):
                with self.assertRaisesRegex(OwnedSolverLogError, 'log_changed_during_open'):
                    self.read()


if __name__ == '__main__':
    unittest.main()
