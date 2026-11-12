"""Native alias and lexical-scope qualification regressions."""
import os
import tempfile
import unittest
from crescendo_engine.netlist_scope import NetlistScope, BindingError
from crescendo_engine.native_maestro_parameters import read_native_parameter_points
from crescendo_engine.private_netlist import qualify_symbolic_netlist, _symbolize_native_dpar_phase, PrivateNetlistUnsupported


class ScopedTemplateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name

    def qualify(self, text, transform=None):
        references = []
        for value in (1, 2):
            native = text.replace("DPAR_1=OWN", "DPAR_1=" + str(value))
            if transform:
                native = transform(native, value)
            references.append({"name": str(value), "text": native, "values": {"p": value}})
        return qualify_symbolic_netlist(text, references, {"p": "OWN"})

    def test_audit_direct_alias(self):
        self.assertTrue(self.qualify("parameters DPAR_1=OWN\nM0 (d g 0 0) nch w=DPAR_1\n")["ok"])

    def test_audit_indirect_alias_readback_symbolization_and_qualification(self):
        text = "// Library name: L\n// Cell name: C\n// View name: schematic\nparameters DPAR_1=2 aliasW=DPAR_1\nM0 (d g 0 0) nch w=aliasW\n"
        self.roundtrip(text)

    def roundtrip(self, text):
        path = os.path.join(self.root, "input.scs")
        with open(path, "w") as stream:
            stream.write(text)
        task = {"task_id": "t", "test": "T", "point_id": 1, "corner": "C0", "netlist_path": path, "template_files": []}
        application = read_native_parameter_points([{"parameter_id": "p", "library": "L", "cell": "C", "view": "schematic", "instance": "M0", "property": "w"}], [task])
        self.assertEqual(application[0]["points"][0]["effective"], 2)
        self.assertEqual(application[0]["points"][0]["dpar_values"]["DPAR_1"], 2)
        phase = _symbolize_native_dpar_phase({"parameter_application": {"parameters": application}, "tasks": [task]}, {"p": "OWN"}, os.path.join(self.root, "symbolic"))
        with open(phase["tasks"][0]["netlist_path"]) as stream:
            symbolic = stream.read()
        self.assertTrue(self.qualify(symbolic)["ok"])
        return application

    def test_audit_sibling_local_names_readback_and_qualification(self):
        self.roundtrip("parameters DPAR_1=2\n// Library name: L\n// Cell name: C\n// View name: schematic\nsubckt a (d g)\nparameters ww=DPAR_1\nM0 (d g 0 0) nch w=ww\nends a\n// Cell name: D\n// View name: schematic\nsubckt b (d g)\nparameters ww=5\nM1 (d g 0 0) nch w=ww\nends b\nX0 (d g) a\nX1 (d g) b\n")

    def test_audit_numeric_companion_retains_native_fallback(self):
        with self.assertRaises(PrivateNetlistUnsupported) as caught:
            self.qualify("parameters DPAR_1=OWN DPAR_2=2\nM0 (d g 0 0) nch w=DPAR_1 ad=DPAR_2\n", lambda text, value: text.replace("DPAR_2=2", "DPAR_2=" + str(value * 2)))
        self.assertEqual(caught.exception.detail["reason"], "numeric_companion")
        self.assertTrue(caught.exception.detail["native_generation_required"])

    def test_audit_unsupported_expression_preserves_text_and_reason(self):
        expression = "pow(DPAR_1,2)"
        with self.assertRaises(PrivateNetlistUnsupported) as caught:
            self.qualify("parameters DPAR_1=OWN DPAR_2=" + expression + "\nM0 (d g 0 0) nch w=DPAR_1 ad=DPAR_2\n", lambda text, value: text.replace("DPAR_2=" + expression, "DPAR_2=" + str(value * value)))
        self.assertEqual(caught.exception.detail["reason"], "unsupported_expression")
        self.assertEqual(caught.exception.detail["expression"], expression)
        self.assertNotIn("native_generation_required", caught.exception.detail)

    def test_cycle_and_complexity_limit(self):
        for text in ("parameters a=b b=a\nM0 (d g) nch w=a\n", "parameters " + " ".join("a%d=a%d" % (i, i + 1) for i in range(70)) + " a70=DPAR_1 DPAR_1=2\n"):
            scope = NetlistScope(text)
            with self.assertRaises(BindingError) as caught:
                scope.trace("a" if "a=b" in text else "a0", ())
            self.assertEqual(caught.exception.detail["reason"], "binding_unresolved")

    def test_same_valued_multiple_dpars_are_never_inferred(self):
        with self.assertRaises(PrivateNetlistUnsupported) as caught:
            self.roundtrip("// Library name: L\n// Cell name: C\n// View name: schematic\nparameters DPAR_1=2 DPAR_2=2 aliasW=(DPAR_1+DPAR_2)/2\nM0 (d g) nch w=aliasW\n")
        self.assertEqual(caught.exception.detail["reason"], "binding_unresolved")

    def test_scope_escape_instance_override_and_duplicate_declarations(self):
        text = "parameters DPAR_1=OWN\nsubckt a (d g)\nparameters ww=DPAR_1\nM0 (d g) nch w=ww\nends a\nX0 (d g) a ww=7\n"
        with self.assertRaises(PrivateNetlistUnsupported) as caught:
            self.qualify(text)
        self.assertEqual(caught.exception.detail["reason"], "scope_shadow")
        with self.assertRaises(BindingError):
            NetlistScope("parameters x=1 x=2\n")
        with self.assertRaises(PrivateNetlistUnsupported) as caught:
            qualify_symbolic_netlist("parameters DPAR_1=OWN\nM0 (d g) nch w=DPAR_1\nM1 (d g) nch w=DPAR_1\n", [{"values": {"p": 2}, "text": ""}], {"p": "OWN"}, {"p": (None, None, "M0")})
        self.assertEqual(caught.exception.detail["reason"], "scope_shadow")

    def test_topology_change_has_machine_reason(self):
        with self.assertRaises(PrivateNetlistUnsupported) as caught:
            self.qualify("parameters DPAR_1=OWN\nM0 (d g) nch w=DPAR_1\n", lambda text, value: text.replace("nch", "pch"))
        self.assertEqual(caught.exception.detail["reason"], "topology_change")
