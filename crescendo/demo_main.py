"""Launch the optimizer GUI against the synthetic DemoService.

    python3 -m crescendo.demo_main [--scale small|large|huge] [--speed N] [--latency F]

Nothing touches the real ~/.config: HOME, run roots and settings live in a
temporary directory that is removed on exit (use --keep to retain it).
"""
from __future__ import print_function

import argparse
import os
import shutil
import sys
import tempfile


def prepare_environment(base=None):
    """Point HOME / run roots at a scratch directory and build a GUI config."""
    base = base or tempfile.mkdtemp(prefix="crescendo-demo-home-")
    home = os.path.join(base, "home")
    runs = os.path.join(base, "runs")
    for folder in (home, runs):
        os.makedirs(folder, exist_ok=True)
    os.environ["HOME"] = home
    os.environ["XDG_CONFIG_HOME"] = os.path.join(home, ".config")
    os.environ["CRESCENDO_RUN_ROOT"] = runs
    os.environ.pop("CRESCENDO_CONFIG", None)
    setup_dir = os.path.join(base, "demo_lib", "OTA_TOP", "maestro")
    os.makedirs(setup_dir, exist_ok=True)
    setup_path = os.path.join(setup_dir, "maestro.sdb")
    with open(setup_path, "w") as stream:
        stream.write('<setupdb application="maestro"/>')
    return base, setup_path


def build_config(base):
    from crescendo_engine.config import load_config
    return load_config(None, persist_discovery=False, validate_discovery=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the optimizer GUI on synthetic demo data.")
    parser.add_argument("--scale", choices=("small", "large", "huge"), default="small")
    parser.add_argument("--speed", type=float, default=1.0,
                        help="optimisation speed multiplier (1 = about 2 min for 12 x 16)")
    parser.add_argument("--latency", type=float, default=1.0,
                        help="Ocean/Virtuoso round-trip factor for inspect/prepare (0 = none)")
    parser.add_argument("--tests", type=int, default=3)
    parser.add_argument("--cells", type=int, default=None)
    parser.add_argument("--outputs", type=int, default=None)
    parser.add_argument("--corners", type=int, default=None)
    parser.add_argument("--generations", type=int, default=12, help="0 follows the GUI value")
    parser.add_argument("--population", type=int, default=16, help="0 follows the GUI value")
    parser.add_argument("--seed", type=int, default=2045)
    parser.add_argument("--autoload", action="store_true", help="click Load and Inspect on start")
    parser.add_argument("--keep", action="store_true", help="keep the scratch directory")
    parser.add_argument("--ui", choices=("crescendo", "classic"), default="crescendo",
                        help="window class: the Crescendo window (default) or the engine's classic window")
    args = parser.parse_args(argv)

    base, setup_path = prepare_environment()
    try:
        from crescendo_engine import gui
        from crescendo.demo_service import DemoService
        if args.ui == "crescendo":
            from crescendo import app
            app.install()
        config = build_config(base)
        service = DemoService(
            scale=args.scale, tests=args.tests, cells=args.cells, outputs=args.outputs,
            corners=args.corners, latency=args.latency, speed=args.speed,
            generations=args.generations, population=args.population, seed=args.seed,
            workdir=os.path.join(base, "service"), config=config)
        original = gui.OptimizerGui.__init__

        def patched(self, root, service_object, *a, **k):
            original(self, root, service_object, *a, **k)
            self.setup_path_var.set(setup_path)
            if args.autoload:
                root.after(300, self.load_setup)

        gui.OptimizerGui.__init__ = patched
        print("Demo scale={} cells={} setup={}".format(args.scale, service.cells, setup_path))
        return gui.launch_gui(config, service=service)
    finally:
        if not args.keep:
            shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
