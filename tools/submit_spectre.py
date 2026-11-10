#!/usr/bin/env python3
from __future__ import print_function

"""JSON CLI for the opt-in local Spectre submission backend."""

import argparse
import json
import os
import sys


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from crescendo_engine.submission import execute_request, load_request, write_result


SUCCESS_STATUSES = ("available", "planned", "completed")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Probe, plan, or run a local Spectre request from JSON.")
    parser.add_argument("--request", required=True, help="crescendo.simulation_request.v1 JSON file")
    parser.add_argument("--output", help="write normalized crescendo.simulation_result.v1 JSON here")
    parser.add_argument("--action", choices=("probe", "plan", "run"), help="override the request action")
    parser.add_argument(
        "--enable-local-spectre",
        action="store_true",
        help="explicitly enable direct local Spectre execution for this request",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    request = load_request(args.request)
    request["backend"] = "local_spectre"
    if args.action:
        request["action"] = args.action
    if args.enable_local_spectre:
        request.setdefault("backend_options", {})["enabled"] = True
    result = execute_request(request)
    if args.output:
        write_result(args.output, result)
    else:
        sys.stdout.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return 0 if result.get("status") in SUCCESS_STATUSES else 2


if __name__ == "__main__":
    sys.exit(main())
