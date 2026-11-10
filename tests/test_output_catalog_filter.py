import copy
import unittest

from crescendo_engine.runner import output_catalog, resolve_metric_bindings


class OutputCatalogFilterTests(unittest.TestCase):
    def test_saved_nodes_and_unnamed_outputs_are_not_metric_candidates(self):
        names = ["gain", "/out", "  /bias  ", "", " \t\n", None, 42]
        outputs = [{"name": name, "expression": "original-%s" % index}
                   for index, name in enumerate(names)]
        outputs.append({"expression": "no name"})
        setup = {"tests": [{"name": "main", "outputs": outputs}]}
        original = copy.deepcopy(setup)

        self.assertEqual(output_catalog(setup), [{
            "key": "main::gain", "test": "main", "name": "gain",
            "expression": "original-0",
        }])
        self.assertEqual(setup, original)

    def test_real_measurement_names_and_expressions_are_preserved_exactly(self):
        names = ["频率", "power (mW)", "phase/noise", "  gain margin  ", "A::B"]
        setup = {"tests": [
            {"name": "vco", "outputs": [
                {"name": name, "expression": "/signal/path"} for name in names
            ]},
            {"name": "other", "outputs": [{"name": "频率"}]},
            {"name": "empty"},
        ]}

        catalog = output_catalog(setup)
        self.assertEqual([row["name"] for row in catalog], names + ["频率"])
        self.assertEqual([row["key"] for row in catalog],
                         ["vco::" + name for name in names] + ["other::频率"])
        self.assertTrue(all(row["expression"] == "/signal/path" for row in catalog[:-1]))

    def test_same_catalog_rejects_signal_paths_for_metric_binding(self):
        setup = {"tests": [{"name": "main", "outputs": [
            {"name": "gain"}, {"name": "power"}, {"name": "/out"}, {"name": ""},
        ]}], "corners": [{"name": "tt"}]}
        prepared = {"setup": setup, "outputs": output_catalog(setup)}

        accepted = resolve_metric_bindings(
            prepared, [{"output_key": "main::gain", "corners": ["tt"]}],
            [{"output_key": "main::power", "corners": ["tt"]}],
        )
        self.assertEqual(accepted, {"outputs": [], "corners": []})
        excluded = resolve_metric_bindings(
            prepared, [{"output_key": "main::/out"}], [{"output_key": "main::"}],
        )
        self.assertEqual(excluded, {"outputs": ["main::", "main::/out"], "corners": []})
        self.assertEqual(len(prepared["setup"]["tests"][0]["outputs"]), 4)


if __name__ == "__main__":
    unittest.main()
