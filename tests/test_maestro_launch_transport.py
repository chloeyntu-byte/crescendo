"""Exercise the shell operations emitted by the installed Maestro SKILL menu."""
import json
from pathlib import Path
import re
import shlex
import stat
import subprocess
import tempfile
import unittest


CADENCE = Path(__file__).resolve().parents[1] / "cadence"
MENU = (CADENCE / "crescendo_maestro_menu.il").read_text(encoding="utf-8")


def procedure(name):
    return MENU.split("procedure(" + name + "(", 1)[1].split("\nprocedure(", 1)[0]


def run_shell_from_skill(name, path, arguments):
    source = procedure(name)
    quoted = re.search(r'system\(sprintf\(nil\s+("(?:[^"\\]|\\.)*")', source)
    command = json.loads(quoted.group(1)) % ((shlex.quote(str(path)),) * arguments)
    return subprocess.run(["/bin/sh", "-c", "umask 000; " + command]).returncode


class MaestroLaunchTransportTests(unittest.TestCase):
    def test_truncated_cadence_name_is_outside_ipc_contract(self):
        for source in CADENCE.rglob("*.il"):
            self.assertNotIn("makeTempFileName", source.read_text(encoding="utf-8"))
        allocator = procedure("crescMaestroCreateTransport")
        validator = procedure("crescMaestroTransportDirectoryP")
        self.assertIn('ipcBeginProcess("/usr/bin/mktemp -d /tmp/crescendo-ipc.XXXXXXXXXX")', allocator)
        self.assertIn("[ ! -L /tmp ] && [ -k /tmp ]", allocator)
        self.assertIn("ipcWait(alloc)\n    output = ipcReadProcess(alloc)", allocator)
        self.assertIn("ipcGetExitStatus(alloc) == 0", allocator)
        self.assertIn("crescMaestroTransportDirectoryP(directory)", allocator)
        pattern = re.search(r'rexMatchp\("([^"]+)" directory\)', validator).group(1)
        self.assertIn('strlen(directory) == strlen("/tmp/crescendo-ipc.") + 10', validator)
        self.assertIsNotNone(re.fullmatch(pattern, "/tmp/crescendo-ipc.A1b2C3d4E5"))
        self.assertIsNone(re.fullmatch(pattern, "/tmp/crescendp29996"))
        self.assertIsNone(re.fullmatch(pattern, "/tmp/crescendo-ipc.A1b2C3d4E5/$x"))
        launch = procedure("crescMaestroOpenCurrent")
        self.assertIn('ipcBeginProcess(strcat("/bin/bash " launchScript)', launch)
        for name in ("context", "launch.sh"):
            self.assertIn('crescMaestroCreateTransportFile(directory "{}")'.format(name), launch)
        self.assertIn('outfile(context "a")', procedure("crescMaestroWriteContext"))
        self.assertIn('outfile(script "a")', procedure("crescMaestroWriteLaunchScript"))

    def test_existing_entries_and_symlinks_are_preserved(self):
        name = "crescMaestroCreateTransportFile"
        self.assertIn("set -C", procedure(name))
        self.assertIn("[ ! -e %s ] && [ ! -L %s ]", procedure(name))
        with tempfile.TemporaryDirectory() as root:
            entry = Path(root) / "context"
            entry.write_text("previous", encoding="utf-8")
            self.assertNotEqual(run_shell_from_skill(name, entry, 8), 0)
            self.assertEqual(entry.read_text(encoding="utf-8"), "previous")
            for target in (entry, Path(root) / "missing"):
                alias = Path(root) / "launch.sh"
                alias.symlink_to(target)
                self.assertNotEqual(run_shell_from_skill(name, alias, 8), 0)
                self.assertTrue(alias.is_symlink())
                alias.unlink()
            self.assertEqual(entry.read_text(encoding="utf-8"), "previous")
            self.assertFalse((Path(root) / "missing").exists())

    def test_mode_owner_and_directory_type(self):
        validator = "crescMaestroTransportDirectoryP"
        for name in (validator, "crescMaestroCreateTransportFile"):
            self.assertIn("[ -O %s ]", procedure(name))
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root) / "transport"
            directory.mkdir(0o700)
            self.assertEqual(run_shell_from_skill(validator, directory, 4), 0)
            alias = Path(root) / "alias"
            alias.symlink_to(directory, target_is_directory=True)
            self.assertNotEqual(run_shell_from_skill(validator, alias, 4), 0)
            path = directory / "context"
            self.assertEqual(run_shell_from_skill("crescMaestroCreateTransportFile", path, 8), 0)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertEqual(path.stat().st_nlink, 1)
            directory.chmod(0o755)
            self.assertNotEqual(run_shell_from_skill(validator, directory, 4), 0)


if __name__ == "__main__":
    unittest.main()
