from __future__ import print_function

import atexit
import os
import shutil
import tempfile
import unittest

from crescendo_engine.maestro import inspect_maestro, resolve_setup_path


FIXTURE_SOURCE = os.path.join(os.path.dirname(__file__), "fixtures")
# The shipped fixtures carry text extensions; the engine looks for the Maestro
# file names, so materialise them in a scratch directory.
FIXTURE = tempfile.mkdtemp(prefix="crescendo-maestro-fixture-")
atexit.register(shutil.rmtree, FIXTURE, True)
shutil.copyfile(os.path.join(FIXTURE_SOURCE, "maestro.sdb.xml"), os.path.join(FIXTURE, "maestro.sdb"))
shutil.copyfile(os.path.join(FIXTURE_SOURCE, "active.state.xml"), os.path.join(FIXTURE, "active.state"))


class MaestroInspectionTest(unittest.TestCase):
    def test_resolves_directory_and_state_file(self):
        expected = os.path.join(FIXTURE, "maestro.sdb")
        self.assertEqual(resolve_setup_path(FIXTURE), expected)
        self.assertEqual(resolve_setup_path(os.path.join(FIXTURE, "active.state")), expected)

    def test_extracts_generic_setup(self):
        data = inspect_maestro(FIXTURE)
        self.assertEqual(data["schema_version"], "crescendo.maestro_setup.v1")
        self.assertEqual(data["libraries"], ["DEMO"])
        self.assertEqual(data["cells"], ["TOP"])
        self.assertEqual(data["analysis_names"], ["ac", "dc"])
        self.assertEqual(data["output_names"], ["bandwidth", "gain"])
        self.assertEqual(data["tests"][0]["variables"][0], {"name": "BIAS", "value": "1m"})
        self.assertEqual(data["tests"][0]["outputs"][0]["expression"], 'value(mag(v("/out")) 1)')
        self.assertEqual(len(data["corners"]), 2)


if __name__ == "__main__":
    unittest.main()
