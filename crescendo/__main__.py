"""``python -m crescendo [args]``: the Crescendo command line.

Sub-commands (gui, doctor, run, resume, parse-rdb, ...) are the engine's;
``gui`` opens the Crescendo window.
"""
import sys


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else list(argv)
    if arguments == ["--version"]:
        from . import __version__
        from crescendo_engine import __version__ as engine_version
        print("Crescendo {} (engine {})".format(__version__, engine_version))
        return 0
    from crescendo_engine.__main__ import main as kernel_main
    return kernel_main(arguments)


if __name__ == "__main__":
    sys.exit(main())
