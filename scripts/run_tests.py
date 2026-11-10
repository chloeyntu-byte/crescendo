"""Test runner for the vendored kernel tests (no tests/__init__.py upstream).

  python3 scripts/run_tests.py [--crescendo] [glob]
--crescendo installs the Crescendo window class first, so kernel GUI tests
exercise CrescendoGui (the presentation layer must keep kernel behaviour).
"""
import fnmatch
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(1, os.path.join(ROOT, "tests"))  # tests import siblings by bare name
os.chdir(ROOT)
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
