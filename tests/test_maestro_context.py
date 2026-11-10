import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.cadence import child_environment, detect_pdk_context, write_readonly_cds_context
from crescendo_engine.config import DEFAULTS
from crescendo_engine.maestro_context import (
    SCHEMA, MAX_CONTEXT_BYTES, bind_launch_config, load_launch_context,
    parse_launch_records, selected_history_record, validate_bound_setup,
)
from crescendo_engine.workspace import parse_cds_defines, resolve_copy_libraries


def record(tag, *values):
    return tag + "\t" + "\t".join("nil" if value is None else "t" if value is True else json.dumps(value) for value in values)


class MaestroContextTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        # Physical directory deliberately differs from the logical OA library.
        self.library = os.path.join(self.root, "physical alias")
        self.view = os.path.join(self.library, "VCO", "maestro")
        os.makedirs(self.view)
        with open(os.path.join(self.view, "maestro.sdb"), "w") as stream:
            stream.write('<setupdb type="maestro"/>')
        self.cds = os.path.join(self.root, "cds.lib")
        with open(self.cds, "w") as stream:
            stream.write('DEFINE LogicalLib "%s"\n' % self.library)
        self.tools = os.path.join(self.root, "tools")
        os.makedirs(self.tools)
        for name in ("ocean", "virtuoso", "spectre"):
            with open(os.path.join(self.tools, name), "w") as stream:
                stream.write("#!/bin/sh\nexit 0\n")
            os.chmod(os.path.join(self.tools, name), 0o755)
        self.rows = [SCHEMA, record("C", "LogicalLib", "VCO", "maestro", self.root,
                     self.view, self.cds, os.path.join(self.tools, "ocean")),
                     record("L", "LogicalLib", self.library), record("M", None),
                     record("R", "/native/ld"), record("E", "PATH", self.tools),
                     record("E", "CDS_LIC_FILE", "27000@native-license"),
                     record("T", "test1", "LogicalLib", "TB", "schematic", "spectre")]

    def parse(self, extra=(), rows=None):
        return parse_launch_records(("\n".join((rows or self.rows) + list(extra)) + "\n").encode("utf-8"))

    def load(self, extra=(), name="input.context"):
        path = os.path.join(self.root, name)
        with open(path, "w") as stream:
            stream.write("\n".join(self.rows + list(extra)) + "\n")
        with mock.patch.dict(os.environ, {"CRESCENDO_RUN_ROOT": os.path.join(self.root, "runtime")}):
            return load_launch_context(path), path

    def test_logical_alias_and_saved_view_have_exact_identity(self):
        context = self.parse()
        self.assertEqual(context["source"]["library"], "LogicalLib")
        self.assertEqual(context["libraries"]["LogicalLib"], self.library)
        self.assertEqual(validate_bound_setup(context, self.view), os.path.join(self.view, "maestro.sdb"))

    def test_another_maestro_view_is_rejected(self):
        other = os.path.join(self.library, "Other", "maestro")
        os.makedirs(other)
        with open(os.path.join(other, "maestro.sdb"), "w") as stream:
            stream.write("<setupdb/>")
        with self.assertRaisesRegex(ValueError, "bound"):
            validate_bound_setup(self.parse(), other)

    def test_unsaved_setup_cannot_prepare_or_run(self):
        rows = [row for row in self.rows if not row.startswith("M\t")] + [record("M", True)]
        with self.assertRaisesRegex(ValueError, "unsaved"):
            validate_bound_setup(self.parse(rows=rows), self.view)

    def test_unknown_modification_state_remains_unknown(self):
        rows = [row for row in self.rows if not row.startswith("M\t")] + [record("M", "unknown")]
        self.assertIsNone(self.parse(rows=rows)["source"]["modified"])

    def test_loaded_mapping_conflict_is_rejected(self):
        rows = [row for row in self.rows if not row.startswith("L\t")] + [record("L", "LogicalLib", self.root)]
        with self.assertRaisesRegex(ValueError, "mapping"):
            self.parse(rows=rows)

    def test_view_identity_conflict_is_rejected(self):
        rows = list(self.rows)
        rows[1] = record("C", "LogicalLib", "Wrong", "maestro", self.root, self.view, self.cds, os.path.join(self.tools, "ocean"))
        with self.assertRaisesRegex(ValueError, "identity"):
            self.parse(rows=rows)

    def test_quoted_literals_do_not_expand_shell_or_environment(self):
        context = self.parse([record("H", "$(touch bad)", "$HOME/results", "$HOME/result.rdb")])
        self.assertEqual(context["histories"][0]["results_dir"], os.path.join(self.root, "$HOME/results"))
        self.assertFalse(os.path.exists(os.path.join(self.root, "bad")))

    def test_executable_alias_invocation_name_is_preserved(self):
        alias = os.path.join(self.tools, "ocean_alias")
        os.symlink("ocean", alias)
        rows = list(self.rows)
        rows[1] = record("C", "LogicalLib", "VCO", "maestro", self.root, self.view, self.cds, alias)
        self.assertEqual(self.parse(rows=rows)["source"]["ocean"], alias)

    def test_malformed_or_executable_tokens_are_rejected(self):
        for line in ('S\tgetWorkingDir()', 'S\t"unterminated', 'S\t"ok" trailing', 'S\t"bad\\q"'):
            with self.subTest(line=line), self.assertRaises(ValueError):
                self.parse([line])

    def test_unknown_and_duplicate_records_are_rejected(self):
        for line in (record("Z", None), self.rows[1], record("R", None), record("E", "PATH", "new"), self.rows[-1]):
            with self.subTest(line=line), self.assertRaises(ValueError):
                self.parse([line])

    def test_control_characters_and_escape_identity_are_rejected(self):
        for line in (record("H", "bad\nname", None, None), record("L", "../bad", self.root)):
            with self.subTest(line=line), self.assertRaises(ValueError):
                self.parse([line])

    def test_only_runtime_whitelist_is_accepted(self):
        with self.assertRaises(ValueError):
            self.parse([record("E", "UNSAFE_ENV", "/unsafe")])

    def test_invalid_history_extension_is_rejected(self):
        with self.assertRaises(ValueError):
            self.parse([record("H", "run", self.root, "source.oa")])

    def test_size_limit_rejects_before_parsing(self):
        with self.assertRaisesRegex(ValueError, "size"):
            parse_launch_records(b"x" * (MAX_CONTEXT_BYTES + 1))

    def test_explicit_missing_history_never_switches_to_other_results(self):
        context = {"selected_history": "missing", "histories": [{"name": "valid", "rdb_file": self.cds}]}
        self.assertEqual(selected_history_record(context)["name"], "missing")
        self.assertIsNone(selected_history_record(context)["rdb_file"])

    def test_native_catalog_order_selects_last_readable_history(self):
        context = {"histories": [{"name": "zeta", "rdb_file": self.cds},
                                {"name": "alpha", "rdb_file": self.cds},
                                {"name": "unavailable", "rdb_file": "missing.rdb"}]}
        self.assertEqual(selected_history_record(context)["name"], "alpha")

    def test_no_registered_history_has_no_fallback_scan(self):
        self.assertIsNone(selected_history_record({"histories": []}))

    def test_each_launch_has_private_mapping_and_state(self):
        first, path = self.load()
        second, unused = self.load()
        self.assertNotEqual(first["state_path"], second["state_path"])
        self.assertNotEqual(first["launch_id"], second["launch_id"])
        self.assertNotEqual(first["probe_root"], second["probe_root"])
        self.assertEqual(first["runtime_root"], second["runtime_root"])
        self.assertEqual(first["effective_cds_lib"], second["effective_cds_lib"])
        self.assertEqual(parse_cds_defines(first["effective_cds_lib"])["LogicalLib"], self.library)
        self.assertTrue(os.path.isfile(path))
        self.assertEqual(os.stat(os.path.dirname(first["context_path"])).st_mode & 0o777, 0o700)
        self.assertEqual(os.stat(first["context_path"]).st_mode & 0o777, 0o600)

    def test_reopened_same_source_accepts_existing_checkpoint_origin(self):
        from crescendo_engine.server import _checkpoint_origin_path
        first, unused = self.load()
        origin = os.path.join(first["runtime_root"], "optimization_saved")
        os.makedirs(origin)
        before = os.stat(first["effective_cds_lib"])
        second, unused = self.load()
        after = os.stat(second["effective_cds_lib"])
        self.assertEqual((before.st_ino, before.st_mtime_ns), (after.st_ino, after.st_mtime_ns))
        checkpoint = {"manifest": {"origin_run_root": origin, "origin_run_id": "optimization_saved"}}
        self.assertEqual(_checkpoint_origin_path(checkpoint, second["runtime_root"])[0], origin)

    def test_successful_native_temp_is_consumed_but_saved_view_is_retained(self):
        descriptor, path = tempfile.mkstemp(prefix="cr_ctx_")
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        with os.fdopen(descriptor, "w") as stream:
            stream.write("\n".join(self.rows) + "\n")
        with mock.patch.dict(os.environ, {"CRESCENDO_RUN_ROOT": os.path.join(self.root, "runtime")}):
            context = load_launch_context(path)
        self.assertFalse(os.path.exists(path))
        self.assertTrue(os.path.isfile(context["source"]["setup_path"]))

    def test_invalid_temp_input_is_not_deleted(self):
        descriptor, path = tempfile.mkstemp(prefix="cr_maestro_")
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        with os.fdopen(descriptor, "w") as stream:
            stream.write("invalid")
        with self.assertRaises(ValueError):
            load_launch_context(path)
        self.assertTrue(os.path.exists(path))

    def test_symlink_context_cannot_be_consumed(self):
        unused, path = self.load()
        link = os.path.join(self.root, "link.context")
        os.symlink(path, link)
        with self.assertRaises(OSError):
            load_launch_context(link)

    def test_binding_uses_invoking_tools_and_does_not_mutate_config(self):
        context, unused = self.load()
        config = copy.deepcopy(DEFAULTS)
        config["cadence"]["user_config_path"] = "/global/config.json"
        before = copy.deepcopy(config)
        environment = dict(os.environ)
        bound = bind_launch_config(config, context)
        self.assertEqual(config, before)
        self.assertEqual(dict(os.environ), environment)
        self.assertEqual(bound["cadence"]["ocean_bin"], os.path.join(self.tools, "ocean"))
        self.assertEqual(bound["cadence"]["spectre_bin"], os.path.join(self.tools, "spectre"))
        self.assertEqual(bound["cadence"]["cds_lib"], context["effective_cds_lib"])
        self.assertEqual(bound["runtime"]["run_root"], context["runtime_root"])
        self.assertEqual(bound["runtime"]["hierarchy_probe_root"], context["probe_root"])
        self.assertNotEqual(bound["cadence"]["user_config_path"], "/global/config.json")
        self.assertEqual(bound["cadence"]["user_config_path"], os.path.join(context["settings_root"], "eda.json"))
        self.assertFalse(bound["cadence"]["user_config_path"].startswith(os.path.dirname(context["context_path"])))

    def test_other_install_environment_cannot_replace_native_tool_roots(self):
        context, unused = self.load()
        other = os.path.join(self.root, "other-install")
        os.makedirs(os.path.join(other, "tools.lnx86", "lib", "64bit"))
        config = copy.deepcopy(DEFAULTS)
        config["cadence"].update(cadence_root=other, spectre_root=other, search_roots=[other])
        with mock.patch.dict(os.environ, {"CDSHOME": other, "MMSIMHOME": other,
                                         "CRESCENDO_CADENCE_ROOT": other, "CRESCENDO_SPECTRE_ROOT": other}):
            bound = bind_launch_config(config, context)
        self.assertEqual(bound["cadence"]["cadence_root"], self.root)
        self.assertEqual(bound["cadence"]["spectre_root"], self.root)
        self.assertFalse(any(path.startswith(other) for path in bound["cadence"]["extra_library_paths"]))

    def _other_spectre(self, directory):
        tool_root = os.path.join(self.root, directory)
        binary_directory = os.path.join(tool_root, "bin")
        os.makedirs(binary_directory)
        binary = os.path.join(binary_directory, "spectre")
        with open(binary, "w") as stream:
            stream.write("#!/bin/sh\nexit 0\n")
        os.chmod(binary, 0o755)
        return tool_root, binary

    def test_declared_manual_spectre_binary_wins_over_invoking_path(self):
        context, unused = self.load()
        unused_root, binary = self._other_spectre("manual solver")
        config = copy.deepcopy(DEFAULTS)
        config["cadence"].update(spectre_bin=binary, _manual_overrides={"spectre_bin": binary})
        before = copy.deepcopy(config)
        bound = bind_launch_config(config, context)
        self.assertEqual(bound["cadence"]["spectre_bin"], binary)
        self.assertEqual(bound["cadence"]["discovery"]["sources"]["spectre_bin"], "manual")
        self.assertEqual(bound["cadence"]["ocean_bin"], os.path.join(self.tools, "ocean"))
        self.assertEqual(config, before)

    def test_declared_file_spectre_override_keeps_priority_over_manual(self):
        context, unused = self.load()
        unused_root, manual_binary = self._other_spectre("manual solver")
        unused_root, file_binary = self._other_spectre("file solver")
        config = copy.deepcopy(DEFAULTS)
        config["cadence"].update(spectre_bin=file_binary,
            _manual_overrides={"spectre_bin": manual_binary}, _file_overrides={"spectre_bin": file_binary})
        bound = bind_launch_config(config, context)
        self.assertEqual(bound["cadence"]["spectre_bin"], file_binary)
        self.assertEqual(bound["cadence"]["discovery"]["sources"]["spectre_bin"], "manual")

    def test_declared_spectre_root_wins_over_invoking_path(self):
        context, unused = self.load()
        root, binary = self._other_spectre("selected solver")
        config = copy.deepcopy(DEFAULTS)
        config["cadence"].update(spectre_root=root, spectre_bin=binary,
                               _manual_overrides={"spectre_root": root})
        bound = bind_launch_config(config, context)
        self.assertEqual(bound["cadence"]["spectre_root"], root)
        self.assertEqual(bound["cadence"]["spectre_bin"], binary)
        self.assertEqual(bound["cadence"]["discovery"]["sources"]["spectre_root"], "manual")

    def test_invalid_declared_spectre_binary_does_not_use_invoking_path(self):
        context, unused = self.load()
        missing = os.path.join(self.root, "missing", "spectre")
        config = copy.deepcopy(DEFAULTS)
        # Initial discovery represents an invalid executable as None. Keep
        # the declaration so binding cannot turn it into an automatic choice.
        config["cadence"].update(spectre_bin=None, _manual_overrides={"spectre_bin": missing})
        bound = bind_launch_config(config, context)
        self.assertIsNone(bound["cadence"]["spectre_bin"])
        self.assertIn("spectre_bin", bound["cadence"]["discovery"]["missing"])
        self.assertEqual(bound["cadence"]["discovery"]["sources"]["spectre_bin"], "manual")

    def test_empty_spectre_override_fields_retain_native_automatic_discovery(self):
        context, unused = self.load()
        for value in (None, "", []):
            config = copy.deepcopy(DEFAULTS)
            config["cadence"].update(_manual_overrides={"spectre_bin": value},
                                   _file_overrides={"spectre_root": value})
            with self.subTest(value=value):
                bound = bind_launch_config(config, context)
                self.assertEqual(bound["cadence"]["spectre_bin"], os.path.join(self.tools, "spectre"))
                self.assertEqual(bound["cadence"]["discovery"]["sources"]["spectre_bin"], "invoking Maestro PATH")

    def test_automatic_spectre_cache_is_replaced_by_current_invoking_path(self):
        context, unused = self.load()
        root, binary = self._other_spectre("old automatic solver")
        config = copy.deepcopy(DEFAULTS)
        config["cadence"].update(spectre_bin=binary, spectre_root=root,
                               _manual_overrides={}, _file_overrides={})
        bound = bind_launch_config(config, context)
        self.assertEqual(bound["cadence"]["spectre_bin"], os.path.join(self.tools, "spectre"))
        self.assertEqual(bound["cadence"]["discovery"]["sources"]["spectre_bin"], "invoking Maestro PATH")

    def _alps(self, name):
        root = os.path.join(self.root, name)
        binary = os.path.join(root, "bin", "alps")
        os.makedirs(os.path.dirname(binary))
        with open(binary, "w") as stream:
            stream.write("#!/bin/sh\nexit 0\n")
        os.chmod(binary, 0o755)
        return root, binary

    def _alps_context(self, binary):
        context, unused = self.load()
        context["runtime_env"]["PATH"] = os.path.dirname(binary) + os.pathsep + self.tools
        config = copy.deepcopy(DEFAULTS)
        config["cadence"]["alps_search_roots"] = [os.path.join(self.root, "no common ALPS")]
        return context, config

    def test_current_native_alps_replaces_stale_automatic_root_and_binary(self):
        root, binary = self._alps("native ALPS")
        context, config = self._alps_context(binary)
        stale_root, stale_binary = self._alps("old automatic ALPS")
        for previous in (None, stale_binary):
            config["cadence"].update(alps_bin=previous, alps_root=stale_root)
            with mock.patch.dict(os.environ, {}, clear=True):
                bound = bind_launch_config(config, context)
            self.assertEqual(bound["cadence"]["alps_bin"], binary)
            self.assertEqual(bound["cadence"]["alps_root"], root)
            self.assertEqual(bound["cadence"]["discovery"]["sources"]["alps_bin"], "invoking Maestro PATH")

    def test_explicit_alps_home_root_is_not_replaced_by_path_launcher(self):
        root, binary = self._alps("native ALPS")
        context, config = self._alps_context(binary)
        internal = os.path.join(root, "tools", "alps")
        os.makedirs(internal)
        with mock.patch.dict(os.environ, {"ALPS_HOME": internal}, clear=True):
            bound = bind_launch_config(config, context)
        # ALPS_HOME can name tools/alps while the launcher lives at the
        # installation sibling bin/alps. Preserve root provenance and discover
        # that executable, including whitespace in the installation path.
        self.assertEqual(bound["cadence"]["alps_bin"], binary)
        self.assertEqual(bound["cadence"]["alps_root"], root)
        self.assertEqual(bound["cadence"]["discovery"]["sources"]["alps_root"], "environment:ALPS_HOME")

    def test_declared_alps_file_binary_retains_priority_over_manual_and_path(self):
        unused, binary = self._alps("native ALPS")
        context, config = self._alps_context(binary)
        unused, manual_binary = self._alps("manual ALPS")
        file_root, file_binary = self._alps("file ALPS")
        config["cadence"].update(alps_bin=file_binary, alps_root=file_root,
            _manual_overrides={"alps_bin": manual_binary}, _file_overrides={"alps_bin": file_binary})
        with mock.patch.dict(os.environ, {}, clear=True):
            bound = bind_launch_config(config, context)
        self.assertEqual(bound["cadence"]["alps_bin"], file_binary)
        self.assertEqual(bound["cadence"]["discovery"]["sources"]["alps_bin"], "manual")

    def test_declared_alps_root_does_not_borrow_old_effective_binary(self):
        unused, binary = self._alps("native ALPS")
        context, config = self._alps_context(binary)
        root, manual_binary = self._alps("manual ALPS")
        config["cadence"].update(alps_bin=binary, alps_root=root,
                                _manual_overrides={"alps_root": root})
        with mock.patch.dict(os.environ, {}, clear=True):
            bound = bind_launch_config(config, context)
        self.assertEqual(bound["cadence"]["alps_bin"], manual_binary)
        self.assertEqual(bound["cadence"]["alps_root"], root)

    def test_invalid_declared_alps_binary_stays_unavailable(self):
        unused, binary = self._alps("native ALPS")
        context, config = self._alps_context(binary)
        missing = os.path.join(self.root, "missing", "alps")
        config["cadence"].update(alps_bin=None, _manual_overrides={"alps_bin": missing})
        with mock.patch.dict(os.environ, {}, clear=True):
            bound = bind_launch_config(config, context)
        self.assertIsNone(bound["cadence"]["alps_bin"])
        self.assertEqual(bound["cadence"]["discovery"]["sources"]["alps_bin"], "manual")

    def test_explicit_alps_environment_binary_stays_authoritative(self):
        unused, binary = self._alps("native ALPS")
        context, config = self._alps_context(binary)
        missing = os.path.join(self.root, "missing", "alps")
        with mock.patch.dict(os.environ, {"CRESCENDO_ALPS_BIN": missing}, clear=True):
            bound = bind_launch_config(config, context)
        self.assertIsNone(bound["cadence"]["alps_bin"])
        self.assertEqual(bound["cadence"]["discovery"]["sources"]["alps_bin"], "environment:CRESCENDO_ALPS_BIN")

    def test_captured_runtime_only_enters_children(self):
        context, unused = self.load()
        config = bind_launch_config(copy.deepcopy(DEFAULTS), context)
        with mock.patch.dict(os.environ, {"LD_LIBRARY_PATH": "/python/ld", "PATH": "/python/path"}):
            child = child_environment(config, self.root)
            self.assertEqual(os.environ["LD_LIBRARY_PATH"], "/python/ld")
            self.assertIn("/native/ld", child["LD_LIBRARY_PATH"].split(os.pathsep))
            self.assertNotIn("/python/ld", child["LD_LIBRARY_PATH"].split(os.pathsep))
            self.assertTrue(child["PATH"].startswith(self.tools))
            self.assertEqual(child["CDS_LIC_FILE"], "27000@native-license")

    def test_workspace_and_pdk_use_native_mapping_over_nearest_file(self):
        other = os.path.join(self.root, "wrong")
        os.makedirs(other)
        native = os.path.join(self.root, "native.cds.lib")
        with open(native, "w") as stream:
            stream.write('DEFINE LogicalLib "%s"\nDEFINE helper "%s"\n' % (self.library, other))
        with open(self.cds, "w") as stream:
            stream.write('DEFINE LogicalLib "%s"\nDEFINE helper /missing\n' % self.library)
        config = copy.deepcopy(DEFAULTS)
        config["cadence"].update(cds_lib=self.cds, source_cds_lib_override=native)
        setup = {"source": {"library_root": self.library}, "tests": [{"library": "LogicalLib"}]}
        hierarchy = {"designs": [{"hierarchy": {"cells": [{"library": "LogicalLib"}, {"library": "helper"}]}}]}
        name, cds, libraries = resolve_copy_libraries(setup, hierarchy, config)
        self.assertEqual(cds, native)
        self.assertEqual(libraries["helper"], other)
        self.assertEqual(name, "LogicalLib")
        self.assertEqual(detect_pdk_context(setup, configured=config["cadence"])["source_cds_lib"], native)
        probe = write_readonly_cds_context(os.path.join(self.root, "probe"), name, self.library,
                                          source_cds_lib_override=native)
        self.assertEqual(probe["source_cds_lib"], native)
        self.assertEqual(parse_cds_defines(probe["local_cds_lib"])["helper"], other)


if __name__ == "__main__":
    unittest.main()
