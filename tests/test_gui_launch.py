from __future__ import print_function

import unittest
from unittest import mock

from crescendo_engine import gui


class FakeRoot(object):
    def __init__(self, error=None):
        self.error = error
        self.destroy_count = 0

    def mainloop(self):
        if self.error is not None:
            raise self.error

    def destroy(self):
        self.destroy_count += 1


class FakeTasks(object):
    def __init__(self):
        self.close_count = 0
        self.abandon_count = 0

    def close(self):
        self.close_count += 1

    def abandon(self):
        self.abandon_count += 1


class GuiLaunchTests(unittest.TestCase):
    def _launch(self, mainloop_error=None, cleanup_error=None, cleanup_confirmed=False):
        root = FakeRoot(mainloop_error)
        app = mock.Mock()
        app.cleanup_confirmed = cleanup_confirmed
        app.force_exit_requested = False
        app.tasks = FakeTasks()
        service = mock.Mock()
        if cleanup_error is not None:
            service.exit_optimizer.side_effect = cleanup_error
        with mock.patch.object(gui.tk, "Tk", return_value=root), mock.patch.object(
                gui, "OptimizerGui", return_value=app), mock.patch.object(
                gui.signal, "signal", return_value=object()):
            result = gui.launch_gui({}, service=service)
        return result, app, service

    def test_normal_mainloop_performs_cleanup_and_closes_tasks(self):
        result, app, service = self._launch()
        self.assertEqual(result, 0)
        service.exit_optimizer.assert_called_once_with()
        self.assertEqual(app.tasks.close_count, 1)

    def test_confirmed_cleanup_is_not_repeated(self):
        result, app, service = self._launch(cleanup_confirmed=True)
        self.assertEqual(result, 0)
        service.exit_optimizer.assert_not_called()
        self.assertEqual(app.tasks.close_count, 0)
        self.assertEqual(app.tasks.abandon_count, 1)

    def test_cleanup_failure_is_not_reported_as_success(self):
        with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
            self._launch(cleanup_error=RuntimeError("cleanup failed"))

    def test_mainloop_and_cleanup_failures_are_both_reported(self):
        with self.assertRaisesRegex(
                RuntimeError, "main loop failed.*window failed.*cleanup also failed.*cleanup failed"):
            self._launch(
                mainloop_error=ValueError("window failed"),
                cleanup_error=RuntimeError("cleanup failed"),
            )

    def test_mainloop_failure_is_preserved_when_cleanup_succeeds(self):
        with self.assertRaisesRegex(ValueError, "window failed"):
            self._launch(mainloop_error=ValueError("window failed"))

    def test_constructor_failure_closes_service_and_root(self):
        root = FakeRoot()
        service = mock.Mock()
        with mock.patch.object(gui.tk, "Tk", return_value=root), mock.patch.object(
                gui, "OptimizerGui", side_effect=ValueError("build failed")):
            with self.assertRaisesRegex(ValueError, "build failed"):
                gui.launch_gui({}, service=service)
        service.exit_optimizer.assert_called_once_with()
        self.assertEqual(root.destroy_count, 1)

    def test_constructor_and_cleanup_failures_are_both_reported(self):
        root = FakeRoot()
        service = mock.Mock()
        service.exit_optimizer.side_effect = RuntimeError("cleanup failed")
        with mock.patch.object(gui.tk, "Tk", return_value=root), mock.patch.object(
                gui, "OptimizerGui", side_effect=ValueError("build failed")):
            with self.assertRaisesRegex(
                    RuntimeError, "initialization failed.*build failed.*cleanup also failed"):
                gui.launch_gui({}, service=service)
        self.assertEqual(root.destroy_count, 1)


if __name__ == "__main__":
    unittest.main()
