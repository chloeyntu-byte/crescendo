import os
import stat
import tempfile
import unittest
from unittest import mock

from crescendo_engine import resources


class PackageResourcesTest(unittest.TestCase):
    def test_source_checkout_resolves_required_resources(self):
        self.assertEqual(resources.resource_origin("web")["origin"], "source")
        self.assertTrue(os.path.isfile(os.path.join(resources.web_root(), "index.html")))
        self.assertTrue(os.path.isfile(resources.tool_path("submit_spectre_job.sh")))
        self.assertTrue(os.path.isfile(resources.guardian_script_path()))
        self.assertTrue(os.path.isfile(
            resources.pdk_adapter_path("cadence_pas_transaction.il")
        ))

    def test_resource_paths_reject_traversal_and_unknown_tools(self):
        with self.assertRaises(ValueError):
            resources.resource_path("web", "../setup.py")
        with self.assertRaises(ValueError):
            resources.resource_path("../web", "index.html")
        with self.assertRaises(ValueError):
            resources.tool_path("unlisted.py")
        with self.assertRaises(ValueError):
            resources.pdk_adapter_path("../unlisted.il")

    def test_non_executable_tool_is_materialized_outside_package(self):
        source = resources.tool_path("submit_spectre_job.sh")
        with tempfile.TemporaryDirectory() as cache_root:
            with mock.patch("crescendo_engine.resources.os.access", return_value=False):
                result = resources.executable_tool_path(
                    "submit_spectre_job.sh", cache_root=cache_root,
                )
            self.assertNotEqual(os.path.realpath(result), os.path.realpath(source))
            self.assertEqual(stat.S_IMODE(os.stat(result).st_mode), 0o700)
            with open(source, "rb") as source_stream, open(result, "rb") as result_stream:
                self.assertEqual(source_stream.read(), result_stream.read())


if __name__ == "__main__":
    unittest.main()
