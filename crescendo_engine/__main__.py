from __future__ import print_function

import argparse
import json
import os
import sys

from . import __version__
from .ade_setup import inspect_setup
from .config import doctor, load_config
from .hierarchy import probe_hierarchy
from .maestro import inspect_maestro
from .rdb import parse_rdb
from .runner import prepare_maestro, prepare_setup, run_optimization
from .server import OptimizerService, serve
from .utils import load_json


def _close_optimizer_service(service):
    result = service.exit_optimizer()
    if not isinstance(result, dict) or result.get("validation_session_closed") is not True:
        raise RuntimeError("optimizer service did not confirm resident validation cleanup")
    return result


def _choose_test(setup, requested):
    from .test_selection import catalogue
    names = [row['name'] for row in catalogue(setup)]
    if requested is None:
        if len(names) != 1:
            raise ValueError('Select exactly one Test with --test; available: ' + ', '.join(names))
        return names[0]
    if requested not in names:
        raise ValueError('Unknown Test: ' + str(requested))
    return requested


def _read_bound_test(path, backend, config, requested):
    from .setup_baseline import load_bound_setup
    return _choose_test(load_bound_setup(path, config, backend=backend), requested)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Standalone Cadence ADE circuit optimizer")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--config", default=None)
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("doctor")
    install_plugin_parser = sub.add_parser("install-maestro-menu")
    install_plugin_parser.add_argument("loader", help="new project-local SKILL loader file")
    install_plugin_parser.add_argument("--python", default=None, help="existing Python 3.6+ executable")
    inspect_parser = sub.add_parser("inspect-maestro")
    inspect_parser.add_argument("path")
    inspect_setup_parser = sub.add_parser("inspect-setup")
    inspect_setup_parser.add_argument("path")
    inspect_setup_parser.add_argument("--backend", choices=("auto", "maestro", "adexl"), default="auto")
    probe_parser = sub.add_parser("probe-hierarchy")
    probe_parser.add_argument("path")
    probe_parser.add_argument("--test", default=None)
    probe_parser.add_argument("--max-depth", type=int, default=32)
    probe_parser.add_argument("--backend", choices=("auto", "maestro", "adexl"), default="auto")
    prepare_parser = sub.add_parser("prepare")
    prepare_parser.add_argument("path")
    prepare_parser.add_argument("--test", default=None)
    prepare_parser.add_argument("--max-depth", type=int, default=32)
    prepare_parser.add_argument("--backend", choices=("auto", "maestro", "adexl"), default="auto")
    run_parser = sub.add_parser("run")
    run_parser.add_argument("spec")
    run_parser.add_argument("--run-root", default=None)
    run_parser.add_argument("--test", default=None)
    resume_parser = sub.add_parser("resume")
    resume_parser.add_argument("checkpoint")
    resume_parser.add_argument("--setup-path", default=None)
    resume_parser.add_argument("--maestro-path", default=None)
    rdb_parser = sub.add_parser("parse-rdb")
    rdb_parser.add_argument("path")
    serve_parser = sub.add_parser("serve")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8877)
    gui_parser = sub.add_parser("gui")
    gui_parser.add_argument("--maestro-context-file", "--context", dest="maestro_context_file", default=None)
    args = parser.parse_args(argv)
    if args.command == "install-maestro-menu":
        from .maestro_plugin import install_maestro_menu
        print(json.dumps(install_maestro_menu(args.loader, python=args.python), indent=2, sort_keys=True))
        return 0
    if args.command == "doctor":
        print(json.dumps(doctor(load_config(args.config)), indent=2, sort_keys=True))
        return 0
    if args.command == "inspect-maestro":
        print(json.dumps(inspect_maestro(args.path), indent=2, sort_keys=True))
        return 0
    if args.command == "inspect-setup":
        print(json.dumps(inspect_setup(args.path, backend=args.backend), indent=2, sort_keys=True))
        return 0
    if args.command == "probe-hierarchy":
        config = load_config(args.config)
        from .setup_baseline import load_bound_setup
        from .single_test import project_setup
        setup = load_bound_setup(args.path, config, backend=args.backend)
        selected = _choose_test(setup, args.test)
        print(json.dumps(probe_hierarchy(project_setup(setup, selected), config, args.max_depth), indent=2, sort_keys=True))
        return 0
    if args.command == "prepare":
        config = load_config(args.config)
        selected = _read_bound_test(args.path, args.backend, config, args.test)
        print(json.dumps(prepare_setup(args.path, config, args.max_depth, backend=args.backend, selected_test=selected), indent=2, sort_keys=True))
        return 0
    if args.command == "run":
        spec = load_json(os.path.abspath(args.spec))
        from .ade_setup import normalize_setup_reference
        config = load_config(args.config)
        reference = normalize_setup_reference(spec.get('setup_locator') or spec.get('setup_path') or spec.get('maestro_path'))
        if args.test and spec.get('selected_test') and args.test != spec['selected_test']:
            raise ValueError('--test conflicts with spec selected_test')
        spec['selected_test'] = _read_bound_test(reference['path'], reference['backend'], config,
                                                  args.test or spec.get('selected_test'))
        print(json.dumps(run_optimization(spec, config, run_root=args.run_root), indent=2, sort_keys=True))
        return 0
    if args.command == "resume":
        checkpoint_path = os.path.abspath(os.path.expanduser(args.checkpoint))
        checkpoint = load_json(checkpoint_path)
        service = OptimizerService(load_config(args.config))
        try:
            launch = service.resume(checkpoint, path=args.setup_path or args.maestro_path)
        except BaseException:
            try:
                _close_optimizer_service(service)
            except BaseException:
                pass
            raise
        interrupted = False
        try:
            terminal = service.wait_for_terminal(launch["run_id"])
        except KeyboardInterrupt:
            interrupted = True
            try:
                try:
                    service.stop(launch["run_id"])
                except ValueError as error:
                    if "not active" not in str(error):
                        raise
                terminal = service.wait_for_terminal(launch["run_id"])
            except KeyboardInterrupt:
                interrupted_payload = {
                    "checkpoint": checkpoint_path,
                    "checkpoint_resumable": True,
                    "cleanup_confirmed": False,
                    "interrupt_requested": True,
                    "optimizer_cleanup_attempted": True,
                    "run_id": launch["run_id"],
                    "run_root": launch["run_root"],
                    "state": "interrupted",
                }
                try:
                    interrupted_payload["optimizer_cleanup"] = _close_optimizer_service(service)
                except BaseException as cleanup_error:
                    interrupted_payload["cleanup_error"] = "%s: %s" % (
                        cleanup_error.__class__.__name__, cleanup_error,
                    )
                print(json.dumps(interrupted_payload, indent=2, sort_keys=True))
                return 130
            except BaseException:
                try:
                    _close_optimizer_service(service)
                except BaseException:
                    pass
                raise
        except BaseException:
            try:
                _close_optimizer_service(service)
            except BaseException:
                pass
            raise
        payload = {
            "checkpoint": checkpoint_path,
            "run_id": launch["run_id"],
            "run_root": launch["run_root"],
            "status": terminal["status"],
        }
        if interrupted:
            payload["interrupt_requested"] = True
        if terminal.get("result") is not None:
            payload["result"] = terminal["result"]
        try:
            payload["optimizer_cleanup"] = _close_optimizer_service(service)
            payload["cleanup_confirmed"] = True
        except Exception as cleanup_error:
            payload["cleanup_confirmed"] = False
            payload["cleanup_error"] = "%s: %s" % (
                cleanup_error.__class__.__name__, cleanup_error,
            )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if (
            payload["cleanup_confirmed"] and
            terminal["status"].get("state") in ("completed", "stopped")
        ) else 1
    if args.command == "parse-rdb":
        print(json.dumps(parse_rdb(args.path), indent=2, sort_keys=True))
        return 0
    if args.command == "serve":
        serve(load_config(args.config), args.host, args.port)
        return 0
    if args.command == "gui":
        from .gui import launch_gui
        if args.maestro_context_file:
            from .maestro_context import bind_launch_config, load_launch_context
            config = load_config(args.config, persist_discovery=False, validate_discovery=False)
            context = load_launch_context(args.maestro_context_file,
                                          storage_base_root=config["runtime"].get("storage_base_override"))
            config = bind_launch_config(config, context)
            return launch_gui(config, launch_context=context, force_exit_process=True)
        return launch_gui(load_config(args.config), force_exit_process=True)
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
