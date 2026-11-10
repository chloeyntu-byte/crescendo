"""Real owned cds.lib file graphs; parser checks do not certify native EDA."""
import json
import os
import shlex
import subprocess
import sys
import tempfile
import unittest

from crescendo_engine.cadence import write_readonly_cds_context
from crescendo_engine.pdk import parse_cds_lib
from crescendo_engine.utils import atomic_write_text, cds_lib_token
from crescendo_engine.workspace import build_workspace, tree_content_fingerprint


class CdsLibPathTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='crescendo cds token ')
        self.addCleanup(self.temp.cleanup)
        self.root=self.temp.name

    def write(self,path,text):
        os.makedirs(os.path.dirname(path),exist_ok=True)
        with open(path,'w',encoding='utf-8') as stream:stream.write(text)
        return path

    def fixture(self,label):
        case=os.path.join(self.root,label)
        design=os.path.join(case,'source design','D')
        device=os.path.join(case,'vendor reference')
        basic=os.path.join(case,'installation basic')
        for path in (design,device,basic):os.makedirs(path)
        setup_db=self.write(os.path.join(design,'TOP','maestro','maestro.sdb'),'owned fixture setup')
        self.write(os.path.join(design,'TOP','schematic','owned.txt'),'original design')
        # Independent, always-quoted fixtures avoid reusing the product serializer.
        source_cds=self.write(os.path.join(case,'source map','cds.lib'),
                    'DEFINE D '+json.dumps(design,ensure_ascii=False)+'\nDEFINE DEVICE '+json.dumps(device,ensure_ascii=False)+'\n')
        install_cds=self.write(os.path.join(case,'installation map','cds.lib'),
                    'DEFINE BASIC '+json.dumps(basic,ensure_ascii=False)+'\n')
        return design,device,basic,setup_db,source_cds,install_cds

    def directives(self,path):
        with open(path,encoding='utf-8') as stream:
            return [shlex.split(line,comments=True,posix=True) for line in stream if line.strip() and not line.lstrip().startswith('#')]

    def test_plain_tokens_are_preserved_and_significant_characters_round_trip(self):
        self.assertEqual(cds_lib_token('/safe/program/path'),'/safe/program/path')
        self.assertEqual(cds_lib_token('D'),'D')
        for value in ('with space',"with'apostrophe",'with"quote','with#hash',r'with\backslash'):
            encoded=cds_lib_token(value)
            self.assertEqual(shlex.split('DEFINE D '+encoded,comments=True,posix=True),['DEFINE','D',value])

    def test_tokens_reject_record_injection_and_nul(self):
        for value in ('bad\nDEFINE ATTACK /tmp','bad\rUNDEFINE D','bad\0path','',None):
            with self.subTest(value=repr(value)),self.assertRaises(ValueError):cds_lib_token(value)

    def test_readonly_context_keeps_both_includes_and_vendor_mapping(self):
        for label in ('space path',"apostrophe'path",'quote"path','hash#path',r'backslash\path'):
            with self.subTest(label=label):
                design,device,basic,setup_db,source_cds,install_cds=self.fixture(label)
                before=tree_content_fingerprint(design)
                work=os.path.join(self.root,'probe '+label)
                context=write_readonly_cds_context(work,'D',design,install_cds,source_cds_lib_override=source_cds)
                # The project parser joins surplus path tokens; token arity is
                # also checked to catch native truncation hidden by that parser.
                self.assertEqual(self.directives(context['local_cds_lib']),[
                    ['SOFTINCLUDE',os.path.realpath(install_cds)],['INCLUDE',source_cds],['DEFINE','D',design]])
                parsed=parse_cds_lib(context['local_cds_lib'])
                self.assertTrue(parsed['ok'],parsed)
                self.assertEqual(parsed['definitions'],{'D':design,'DEVICE':device,'BASIC':basic})
                self.assertEqual(tree_content_fingerprint(design),before)

    def test_private_workspace_mapping_retains_references_and_overrides_only_copy(self):
        design,device,basic,setup_db,source_cds,install_cds=self.fixture('space apostrophe\' quote" # backslash\\ path')
        before=tree_content_fingerprint(design)
        run_root=os.path.join(self.root,'run space \' " # \\')
        workspace_root=os.path.join(run_root,'candidates','owned','workspace')
        setup={'source':{'library_root':design,'maestro_sdb':setup_db},
               'tests':[{'name':'owned','library':'D','cell':'TOP','view':'schematic'}]}
        config={'runtime':{'run_root':run_root},'cadence':{'cds_lib':install_cds,'source_cds_lib_override':source_cds}}
        workspace=build_workspace(setup,{},config,workspace_root)
        copied=workspace['copied_libraries']['D']
        mapping=os.path.join(workspace['root'],'cds.lib')
        self.assertEqual(self.directives(mapping),[
            ['SOFTINCLUDE',install_cds],['INCLUDE',source_cds],['UNDEFINE','D'],['DEFINE','D',copied]])
        parsed=parse_cds_lib(mapping)
        self.assertTrue(parsed['ok'],parsed)
        self.assertEqual(parsed['definitions'],{'D':copied,'DEVICE':device,'BASIC':basic})
        self.assertNotEqual(copied,design)
        self.assertTrue(os.path.isfile(workspace['setup_sdb']))
        with open(os.path.join(workspace['root'],'.cdsinit'),encoding='utf-8') as stream:
            project_line=next(line for line in stream if line.startswith('envSetVal("asimenv.startup" "projectDir"'))
        encoded=project_line.split("'string ",1)[1].rsplit(')',1)[0].strip()
        self.assertEqual(json.loads(encoded),workspace['simulation_root'])
        self.assertEqual(tree_content_fingerprint(design),before)

    def test_physical_alias_with_distinct_maestro_and_schematic_cells_keeps_copy_identity(self):
        design=os.path.join(self.root,'physical source alias')
        setup_db=self.write(os.path.join(design,'TOP','maestro','maestro.sdb'),'<setupdb type="maestro"/>')
        self.write(os.path.join(design,'DUT','schematic','owned.txt'),'schematic source')
        source_cds=self.write(os.path.join(self.root,'cds.lib'),'DEFINE D '+json.dumps(design)+'\n')
        setup={'schema_version':'crescendo.ade_setup.v1','backend':'maestro',
               'source':{'library_root':design,'maestro_sdb':setup_db,'library':'D','library_name':'D','cell':'TOP','view':'maestro'},
               'tests':[{'name':'owned','library':'D','cell':'DUT','view':'schematic'}]}
        hierarchy={'designs':[{'design':{'library':'D','cell':'DUT','view':'schematic'},
                   'hierarchy':{'cells':[{'hierarchy_path':'/','library':'D','cell':'DUT','view':'schematic'}],
                                'instances':[],'parameters':[],'errors':[]}}]}
        run_root=os.path.join(self.root,'alias runs')
        config={'runtime':{'run_root':run_root},'cadence':{'source_cds_lib_override':source_cds}}
        before=tree_content_fingerprint(design)
        result=build_workspace(setup,hierarchy,config,os.path.join(run_root,'candidates','owned','workspace'))
        original=result['source_identity'];copied=result['copied_source_identity']
        self.assertEqual(original['libraries'][0]['library'],'D')
        self.assertEqual(original['libraries'][0]['paths'],['DUT','TOP'])
        self.assertEqual(copied,original)
        self.assertTrue(os.path.isfile(result['setup_sdb']))
        self.assertTrue(os.path.isfile(os.path.join(result['copied_libraries']['D'],'DUT','schematic','owned.txt')))
        self.assertEqual(tree_content_fingerprint(design),before)

    def test_atomic_writer_emits_utf8_tokens_even_in_ascii_locale(self):
        path=os.path.join(self.root,'utf8-token.lib')
        value='工艺库 引用'
        atomic_write_text(path,'DEFINE D '+cds_lib_token(value)+'\n')
        with open(path,'rb') as stream:payload=stream.read()
        self.assertIn(value.encode('utf-8'),payload)
        self.assertEqual(shlex.split(payload.decode('utf-8'),comments=True,posix=True),['DEFINE','D',value])

    def test_utf8_bom_mapping_is_read_without_changing_directive_identity(self):
        library=os.path.join(self.root,'library with BOM source')
        os.makedirs(library)
        path=os.path.join(self.root,'bom-cds.lib')
        with open(path,'wb') as stream:
            stream.write(b'\xef\xbb\xbf'+('DEFINE D '+json.dumps(library)+'\n').encode('utf-8'))
        parsed=parse_cds_lib(path)
        self.assertTrue(parsed['ok'],parsed)
        self.assertEqual(parsed['definitions'],{'D':library})

    def test_real_nonascii_library_path_in_fresh_utf8_runtime(self):
        # Python 3.6 initializes filesystem encoding at startup. A fresh child
        # with an existing UTF-8 locale exercises actual non-ASCII directories;
        # unsupported host locale is reported as skipped, never fake native PASS.
        program=r"""
import json,os,sys
if sys.getfilesystemencoding().lower() in ('ascii','us-ascii'):sys.exit(77)
from crescendo_engine.cadence import write_readonly_cds_context
from crescendo_engine.pdk import parse_cds_lib
from crescendo_engine.utils import atomic_write_text,cds_lib_token
root=sys.argv[1]
library=os.path.join(root,'真实工艺库 引号\" 和单引号\' #反斜杠\\')
os.makedirs(library)
source=os.path.join(root,'原生映射','cds.lib')
os.makedirs(os.path.dirname(source))
atomic_write_text(source,'DEFINE DEVICE '+cds_lib_token(library)+'\n')
work=os.path.join(root,'只读探针 目录')
context=write_readonly_cds_context(work,'D',library,source,source_cds_lib_override=source)
parsed=parse_cds_lib(context['local_cds_lib'])
assert parsed['ok'],parsed
assert parsed['definitions']=={'D':library,'DEVICE':library},parsed
print(json.dumps({'filesystem_encoding':sys.getfilesystemencoding(),'owner_filesystem_only':True,'native_eda':'NOT TESTED'}))
"""
        environment=dict(os.environ);environment['LC_ALL']='en_US.UTF-8';environment['PYTHONIOENCODING']='utf-8'
        bootstrap='import json;exec(json.loads('+repr(json.dumps(program,ensure_ascii=True))+'))'
        result=subprocess.run([sys.executable,'-B','-c',bootstrap,self.root],env=environment,
                              stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True,timeout=10)
        if result.returncode==77:self.skipTest('No working UTF-8 locale for this interpreter')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertTrue(json.loads(result.stdout)['owner_filesystem_only'])


if __name__=='__main__':unittest.main()
