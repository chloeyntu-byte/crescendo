"""Automatic runtime selection contracts using owned, bounded fixtures only."""
import contextlib
import copy
import json
import os
import signal
import sys
import tempfile
import time
import unittest
from unittest import mock

from crescendo_engine import python_runtime
from crescendo_engine.native_environment import GUI_ENV_NAMES


class AutomaticPythonDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="crescendo auto python ' $ ")
        self.addCleanup(self.temp.cleanup)
        self.root = self.temp.name
        self.cwd = os.path.join(self.root, "caller workspace")
        os.makedirs(self.cwd)

    def executable(self, relative, text="#!/bin/sh\nexit 0\n"):
        path = os.path.join(self.root, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as stream:
            stream.write(text)
        os.chmod(path, 0o700)
        return path

    @contextlib.contextmanager
    def system_fallback_catalog(self):
        # Catalog-only identity fixture: no host /usr/bin Python is required or
        # executed, so this boundary test also runs on alternate installations.
        real_isfile=python_runtime.os.path.isfile
        real_access=python_runtime.os.access
        with mock.patch.object(python_runtime.os.path,'isfile',side_effect=lambda path:
                path=='/usr/bin/python3' or real_isfile(path)),mock.patch.object(
                python_runtime.os,'access',side_effect=lambda path,mode:
                path=='/usr/bin/python3' or real_access(path,mode)):
            yield

    def fake_runtime(self, relative, mode="success", capture=None):
        # Execute the real product probe code in this owned fixture interpreter.
        # Mocked Tk certifies capability/classification logic, never real X11.
        source = "#!" + sys.executable + "\n" + """
import json,os,sys,types
mode=MODE
capture=CAPTURE
if capture:
    json.dump({'cwd':os.getcwd(),'env':dict(os.environ),'argv':sys.argv},open(capture,'w'))
if mode=='old_python':
    sys.version_info=(3,5,10)
if mode=='missing_tk':
    sys.modules['tkinter']=None
else:
    class TclError(Exception): pass
    class Tcl:
        def call(self,*args):
            if args[:2]==('info','patchlevel'): return '8.5.19'
            if args[:3]==('package','require','Tk'): return '8.4.20' if mode=='actual_old_tk' else '8.5.19'
            if args[:2]==('info','commands'): return 'ttk::treeview'
            return '8.5.19'
        def eval(self,code): return '8.5.19'
    class Root:
        def __init__(self):
            if mode=='display_error': raise TclError('could not connect to display :owned')
            if mode=='tk_data_error': raise TclError('cannot find a usable tk.tcl')
            self.tk=Tcl()
        def withdraw(self): pass
        def update_idletasks(self): pass
        def destroy(self): pass
    class Widget:
        def __init__(self,*args,**kwargs):
            if mode=='broken_widget': raise TclError('invalid command name ttk::treeview')
        def destroy(self): pass
        def insert(self,*args,**kwargs): return 'owned-item'
        def selection_set(self,*args): pass
        def set(self,*args): return ''
    tk=types.ModuleType('tkinter')
    tk.TkVersion=8.4 if mode=='old_tk' else 8.5
    tk.TclVersion=8.5
    tk.Tcl=Tcl;tk.Tk=Root;tk.TclError=TclError
    ttk=types.ModuleType('tkinter.ttk')
    for name in ('Frame','Label','Button','Scrollbar','Treeview','Combobox','Notebook','Style','Checkbutton','Entry','Menubutton','Separator'):
        setattr(ttk,name,Widget)
    if mode=='missing_treeview': del ttk.Treeview
    tk.ttk=ttk
    sys.modules['tkinter']=tk;sys.modules['tkinter.ttk']=ttk
if '-c' not in sys.argv: raise RuntimeError('Fixture requires a bounded -c probe')
code=sys.argv[sys.argv.index('-c')+1]
exec(compile(code,'<owned-probe>','exec'),{'__name__':'__main__'})
"""
        source = source.replace("mode=MODE", "mode=" + repr(mode)).replace("capture=CAPTURE", "capture=" + repr(capture))
        return self.executable(relative, source)

    def test_catalog_contains_every_path_python3_and_versioned_candidate(self):
        first = self.executable("first bin/python3")
        versioned = self.executable("first bin/python3.12")
        second = self.executable("second bin/python3")
        environ = {"PATH": os.pathsep.join((os.path.dirname(first), os.path.dirname(second))), "HOME": self.root}
        before = dict(environ)
        catalog = python_runtime.discover_candidates(caller_dir=self.cwd,module_root=self.root,environ=environ)
        paths = [row["path"] for row in catalog["candidates"]]
        self.assertIn(first, paths)
        self.assertIn(versioned, paths)
        self.assertIn(second, paths)
        self.assertLess(paths.index(first), paths.index(second))
        self.assertEqual(environ, before)

    def test_relative_path_entries_anchor_to_original_caller_directory(self):
        path = self.executable("caller workspace/relative bin/python3")
        catalog = python_runtime.discover_candidates(caller_dir=self.cwd,module_root=self.root,
                                                     environ={"PATH": "relative bin", "HOME": self.root})
        self.assertIn(path, [row["path"] for row in catalog["candidates"]])

    def test_venv_symlink_invocation_and_base_are_both_preserved(self):
        base = self.executable("base/bin/python3")
        venv = os.path.join(self.cwd, ".venv", "bin", "python")
        os.makedirs(os.path.dirname(venv))
        os.symlink(base, venv)
        catalog = python_runtime.discover_candidates(caller_dir=self.cwd,module_root=self.root,
                    environ={"PATH":os.path.dirname(base), "HOME":self.root})
        paths = [row["path"] for row in catalog["candidates"]]
        self.assertIn(venv, paths)
        self.assertIn(base, paths)
        self.assertNotEqual(venv, os.path.realpath(venv))

    def test_candidate_limit_reports_incomplete_discovery(self):
        first = self.executable("limit first/python3")
        second = self.executable("limit second/python3")
        third = self.executable("limit third/python3")
        catalog = python_runtime.discover_candidates(caller_dir=self.cwd,module_root=self.root,
                    environ={"PATH":os.pathsep.join(os.path.dirname(p) for p in (first,second,third)),"HOME":self.root},
                    max_candidates=2)
        self.assertLessEqual(len(catalog["candidates"]),2)
        self.assertTrue(catalog["limited"])

    def test_probe_cleans_gui_three_states_without_mutating_original_environment(self):
        capture = os.path.join(self.root,"capture.json")
        candidate = self.fake_runtime("probe interpreter ' $",capture=capture)
        for value in (None,"","GUI_OVERRIDE"):
            environ={"PATH":os.defpath,"LC_ALL":"C","DISPLAY":":owned"}
            if value is not None: environ.update(dict((key,value) for key in GUI_ENV_NAMES))
            before=copy.deepcopy(environ)
            result=python_runtime.probe_candidate(candidate,environ=environ,timeout=2)
            self.assertEqual(result["state"],"eligible",result)
            self.assertEqual(environ,before)
            with open(capture) as stream: observed=json.load(stream)
            self.assertFalse(any(key in observed["env"] for key in GUI_ENV_NAMES))
            self.assertIn('-I',observed['argv'])
            self.assertIn('-B',observed['argv'])

    def test_headless_python_with_tk_and_tcl_remains_eligible_without_tk_pass(self):
        candidate=self.fake_runtime("headless python")
        result=python_runtime.probe_candidate(candidate,environ={"PATH":os.defpath,"LC_ALL":"C"},timeout=2)
        self.assertEqual(result["state"],"eligible",result)
        self.assertEqual(result["tk_init"],"NOT TESTED")
        self.assertTrue(result["display_unavailable"])

    def test_display_connection_failure_is_classified_separately_from_tk_capability(self):
        candidate=self.fake_runtime("display error python",mode="display_error")
        result=python_runtime.probe_candidate(candidate,environ={"PATH":os.defpath,"LC_ALL":"C","DISPLAY":":owned"},timeout=2)
        self.assertEqual(result["state"],"eligible",result)
        self.assertEqual(result["tk_init"],"NOT TESTED")
        self.assertTrue(result["display_unavailable"])

    def test_missing_tk_old_python_old_tk_and_missing_required_interface_are_rejected(self):
        for mode in ('missing_tk','old_python','old_tk','missing_treeview','tk_data_error','actual_old_tk','broken_widget'):
            candidate=self.fake_runtime("rejected "+mode,mode=mode)
            result=python_runtime.probe_candidate(candidate,environ={"PATH":os.defpath,"LC_ALL":"C","DISPLAY":":owned"},timeout=2)
            self.assertEqual(result["state"],"rejected",(mode,result))
            self.assertNotEqual(result.get("tk_init"),"PASS")

    def test_many_user_environments_keep_system_fallback_and_report_catalog_limit(self):
        # Bound user scans without spending all slots before system fallback.
        for parent in ('.venvs','.virtualenvs'):
            for index in range(16):
                for name in ('python','python3','python3.6'):
                    self.executable(parent+'/env%02d/bin/'%index+name)
        with self.system_fallback_catalog():
            catalog=python_runtime.discover_candidates(caller_dir=self.cwd,module_root=self.root,
                        environ={'PATH':os.path.join(self.root,'absent'),'HOME':self.root},max_candidates=64)
        paths=[row['path'] for row in catalog['candidates']]
        self.assertIn('/usr/bin/python3',paths)
        user=os.path.join(self.root,'.venvs','env00','bin','python3')
        self.assertIn(user,paths)
        self.assertLess(paths.index('/usr/bin/python3'),paths.index(user))
        self.assertTrue(catalog['limited'])

    def test_reserved_system_fallback_is_not_lost_when_path_already_hit_ceiling(self):
        directories=[]
        for index in range(8):
            candidate=self.executable('long path %d/python3'%index)
            directories.append(os.path.dirname(candidate))
        # /usr/bin is also encountered in PATH after the non-system cap.
        # A skipped entry must remain available for its reserved system slot.
        directories.append('/usr/bin')
        with self.system_fallback_catalog():
            catalog=python_runtime.discover_candidates(caller_dir=self.cwd,module_root=self.root,
                        environ={'PATH':os.pathsep.join(directories),'HOME':self.root},max_candidates=8)
        self.assertIn('/usr/bin/python3',[row['path'] for row in catalog['candidates']])
        self.assertTrue(catalog['limited'])

    def test_limit_remains_visible_when_every_discovered_candidate_is_rejected(self):
        discovery={'candidates':[{'path':'/owned/rejected','source':'fixture'}],
                   'limited':True,'limits':{'max_candidates':1}}
        rejected={'state':'rejected','stage':'tk_import','tk_init':'NOT TESTED','display_unavailable':False}
        with mock.patch.object(python_runtime,'discover_candidates',return_value=discovery),mock.patch.object(
                python_runtime,'probe_candidate',return_value=rejected):
            report=python_runtime.select_runtime(environ={'PATH':os.defpath},max_candidates=1)
        self.assertIsNone(report['selected'])
        self.assertTrue(report['discovery']['limited'])
        self.assertFalse(report['deadline_reached'])
        self.assertEqual(len(report['probes']),1)

    def test_nonexecutable_and_broken_symlink_reject_before_probe(self):
        path=self.executable("not executable python")
        os.chmod(path,0o600)
        link=os.path.join(self.root,"broken python")
        os.symlink(os.path.join(self.root,"absent"),link)
        for candidate in (path,link):
            result=python_runtime.probe_candidate(candidate,environ={"PATH":os.defpath},timeout=.1)
            self.assertEqual(result["state"],"rejected",result)
            self.assertEqual(result["stage"],"python_exec")

    def test_owned_timeout_retires_probe_and_its_child_group(self):
        record=os.path.join(self.root,"timeout-pids.json")
        script=("#!"+sys.executable+"\nimport json,os,subprocess,sys,time\n"
                "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'])\n"
                "json.dump({'parent':os.getpid(),'child':child.pid,'pgid':os.getpgrp()},open("+repr(record)+",'w'))\n"
                "time.sleep(60)\n")
        candidate=self.executable("timeout python",script)
        result=python_runtime.probe_candidate(candidate,environ={"PATH":os.defpath},timeout=.25)
        self.assertEqual(result["state"],"rejected",result)
        with open(record) as stream: pids=json.load(stream)
        self.assertNotEqual(pids['pgid'],os.getpgrp())
        def active(pid):
            try:
                with open('/proc/%d/stat'%pid) as stream: raw=stream.read()
                return raw[raw.rfind(')')+2:].split()[0] not in ('Z','X')
            except (IOError,OSError): return False
        try:
            deadline=time.monotonic()+2
            while active(pids['parent']) or active(pids['child']):
                if time.monotonic()>=deadline: break
                time.sleep(.02)
            self.assertFalse(active(pids['parent']))
            self.assertFalse(active(pids['child']))
        finally:
            if active(pids['parent']) or active(pids['child']):
                os.killpg(pids['pgid'],signal.SIGKILL)

    def test_owned_timeout_also_kills_child_ignoring_term_with_closed_pipes(self):
        record=os.path.join(self.root,'resistant-pids.json')
        ready=os.path.join(self.root,'child-ready')
        child_code=("import os,signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                    "open("+repr(ready)+",'w').write('ready'); time.sleep(60)")
        script=("#!"+sys.executable+"\nimport json,os,subprocess,sys,time\n"
                "child=subprocess.Popen([sys.executable,'-c',"+repr(child_code)+"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)\n"
                "while not os.path.exists("+repr(ready)+"): time.sleep(.005)\n"
                "json.dump({'parent':os.getpid(),'child':child.pid,'pgid':os.getpgrp()},open("+repr(record)+",'w'))\n"
                "time.sleep(60)\n")
        candidate=self.executable('resistant timeout python',script)
        result=python_runtime.probe_candidate(candidate,environ={'PATH':os.defpath},timeout=.4)
        self.assertTrue(result.get('timed_out'),result)
        with open(record) as stream: pids=json.load(stream)
        self.assertNotEqual(pids['pgid'],os.getpgrp())
        def active(pid):
            try:
                with open('/proc/%d/stat'%pid) as stream: raw=stream.read()
                return raw[raw.rfind(')')+2:].split()[0] not in ('Z','X')
            except (IOError,OSError): return False
        try:
            deadline=time.monotonic()+2
            while active(pids['child']) and time.monotonic()<deadline: time.sleep(.02)
            self.assertFalse(active(pids['child']))
        finally:
            if active(pids['parent']) or active(pids['child']):
                os.killpg(pids['pgid'],signal.SIGKILL)

    def test_failed_preferred_and_first_path_candidate_fall_through_to_second_path(self):
        preferred=os.path.join(self.root,"absent preferred")
        first=self.executable("reject first/python3")
        second=self.executable("valid second/python3")
        environ={"PATH":os.pathsep.join((os.path.dirname(first),os.path.dirname(second))),"HOME":self.root}
        def fake_probe(path,environ=None,timeout=3):
            return {"path":path,"state":"eligible" if path==second else "rejected","stage":"tk_import",
                    "tk_init":"NOT TESTED","display_unavailable":path==second}
        with mock.patch.object(python_runtime,"probe_candidate",side_effect=fake_probe):
            result=python_runtime.select_runtime(preferred=preferred,caller_dir=self.cwd,module_root=self.root,environ=environ)
        self.assertEqual(result["selected"],second)
        self.assertTrue(result["display_unavailable"])

    def test_total_deadline_caps_later_candidate_timeouts(self):
        clock=[0.0];timeouts=[]
        candidates=[{"path":"/owned/python%d"%i,"source":"owned fixture"} for i in range(10)]
        def fake_probe(path,environ=None,timeout=3):
            timeouts.append(timeout);clock[0]+=timeout
            return {"path":path,"state":"rejected","stage":"python_exec","tk_init":"NOT TESTED","display_unavailable":False}
        with mock.patch.object(python_runtime,"discover_candidates",return_value={"candidates":candidates,"limited":False,"limits":{}}), mock.patch.object(
                python_runtime,"probe_candidate",side_effect=fake_probe), mock.patch.object(
                python_runtime.time,"monotonic",side_effect=lambda:clock[0]):
            result=python_runtime.select_runtime(environ={"PATH":os.defpath},total_timeout=.5,probe_timeout=.2)
        self.assertIsNone(result["selected"])
        self.assertTrue(result["deadline_reached"])
        self.assertLessEqual(sum(timeouts),.501)
        self.assertLess(len(timeouts),len(candidates))


if __name__=='__main__': unittest.main()
