"""Test runner for the vendored kernel tests (no tests/__init__.py upstream).

  python3 scripts/run_tests.py [--crescendo] [glob]
--crescendo installs the Crescendo window class first, so kernel GUI tests
exercise CrescendoGui (the presentation layer must keep kernel behaviour).
"""
import faulthandler
import fnmatch
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(1, os.path.join(ROOT, "tests"))  # tests import siblings by bare name
os.chdir(ROOT)

faulthandler.enable()  # a native crash prints every thread's stack

# GUI fixtures join their worker threads with a short timeout. On a slow host a
# worker can outlive its test and drop the last reference to the window; Tcl
# then deletes its interpreter from that thread and aborts the whole process
# ("Tcl_AsyncDelete: async handler deleted by the wrong thread"). Keep every
# interpreter referenced here so it is only ever released by the main thread.
try:
    import tkinter
except ImportError:  # headless Python without Tk: GUI tests skip themselves
    tkinter = None
if tkinter is not None:
    _interpreters = []
    _tk_init = tkinter.Tk.__init__

    def _pinned_tk_init(self, *args, **kwargs):
        _tk_init(self, *args, **kwargs)
        _interpreters.append(self.tk)

    tkinter.Tk.__init__ = _pinned_tk_init

args = [a for a in sys.argv[1:] if not a.startswith("--")]
if "--crescendo" in sys.argv:
    from crescendo import app
    app.install()
pattern = args[0] if args else "test_*.py"
loader = unittest.defaultTestLoader
suite = unittest.TestSuite()
import_errors = []
for name in sorted(os.listdir("tests")):
    if not (name.endswith(".py") and fnmatch.fnmatch(name, pattern)):
        continue
    module = "tests." + name[:-3]
    try:
        suite.addTests(loader.loadTestsFromName(module))
    except unittest.SkipTest as skip:
        print("skip module", module, "-", skip)
    except Exception as error:  # report, keep going
        import_errors.append((module, error))
result = unittest.TextTestRunner(verbosity=1).run(suite)
for module, error in import_errors:
    print("IMPORT ERROR", module, type(error).__name__, error)
sys.exit(0 if result.wasSuccessful() and not import_errors else 1)
