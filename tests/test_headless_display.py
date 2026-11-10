from __future__ import print_function

import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock

from crescendo_engine.headless_display import (
    HeadlessDisplayUnavailable, _display_ready, owned_headless_display,
)


class OwnedHeadlessDisplayTest(unittest.TestCase):
    def _process(self):
        process = mock.Mock(pid=31415, returncode=None)
        process.poll.return_value = None

        def terminate():
            process.returncode = -15
            process.poll.return_value = -15

        process.terminate.side_effect = terminate
        return process

    def test_provided_display_is_reused_without_starting_or_stopping_it(self):
        with tempfile.TemporaryDirectory() as root, mock.patch(
                "crescendo_engine.headless_display.subprocess.Popen") as launch:
            with owned_headless_display({"CDS_NOGRAPH_DISPLAY": ":0"}, root) as (env, manifest):
                self.assertEqual(env["CDS_NOGRAPH_DISPLAY"], ":0")
                self.assertFalse(manifest["owned"])
            launch.assert_not_called()

    def test_success_and_probe_exception_both_cleanup_only_owned_process(self):
        for fail in (False, True):
            process = self._process()
            with tempfile.TemporaryDirectory() as root, mock.patch(
                    "crescendo_engine.headless_display.shutil.which", return_value="/usr/bin/Xvnc"), \
                    mock.patch("crescendo_engine.headless_display.os.path.lexists", return_value=False), \
                    mock.patch("crescendo_engine.headless_display._display_ready", return_value=True), \
                    mock.patch("crescendo_engine.headless_display.time.sleep"), \
                    mock.patch("crescendo_engine.headless_display.subprocess.Popen", return_value=process) as launch:
                try:
                    with owned_headless_display({"PATH": "/usr/bin"}, root) as (env, manifest):
                        self.assertTrue(manifest["owned"])
                        self.assertEqual(manifest["pid"], process.pid)
                        self.assertNotIn(env["CDS_NOGRAPH_DISPLAY"], (":0", ":80"))
                        if fail:
                            raise ValueError("probe failed")
                except ValueError:
                    self.assertTrue(fail)
                process.terminate.assert_called_once_with()
                process.kill.assert_not_called()
                argv = launch.call_args[0][0]
                self.assertIn("-inetd", argv)
                self.assertIn("-nolisten", argv)
                self.assertEqual(launch.call_args[1]["stdin"], subprocess.DEVNULL)
                with open(os.path.join(root, "headless_display.json")) as stream:
                    self.assertEqual(json.load(stream)["status"], "stopped")

    def test_missing_capability_fails_explicitly_without_launching(self):
        with tempfile.TemporaryDirectory() as root, mock.patch(
                "crescendo_engine.headless_display.shutil.which", return_value=None), \
                mock.patch("crescendo_engine.headless_display.subprocess.Popen") as launch:
            with self.assertRaises(HeadlessDisplayUnavailable):
                with owned_headless_display({}, root):
                    self.fail("unavailable display yielded")
            launch.assert_not_called()

    def test_failed_owned_start_is_bounded_and_cleaned_up(self):
        process = self._process()
        with tempfile.TemporaryDirectory() as root, mock.patch(
                "crescendo_engine.headless_display.shutil.which", return_value="/usr/bin/Xvnc"), \
                mock.patch("crescendo_engine.headless_display.os.path.lexists", return_value=False), \
                mock.patch("crescendo_engine.headless_display.subprocess.Popen", return_value=process) as launch:
            with self.assertRaises(HeadlessDisplayUnavailable):
                with owned_headless_display({}, root, startup_timeout_seconds=0, max_attempts=1):
                    self.fail("failed display yielded")
            launch.assert_called_once()
            process.terminate.assert_called_once_with()

    def test_x_socket_from_another_process_is_not_ready(self):
        process = self._process()
        with mock.patch("builtins.open", mock.mock_open(read_data=" 27182\n")), \
                mock.patch("crescendo_engine.headless_display.os.path.exists", return_value=True):
            self.assertFalse(_display_ready(181, process))


if __name__ == "__main__":
    unittest.main()
