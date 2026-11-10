import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from crescendo_engine.runtime_layout import design_storage_root, legacy_design_storage_root
from crescendo_engine.maestro_context import load_launch_context


class StableNamespaceTests(unittest.TestCase):
    def test_unrelated_map_and_tool_paths_do_not_change_user_state_namespace(self):
        source={'library':'Alias','cell':'VCO','view':'maestro','work_dir':'/project',
                'setup_path':'/project/Designs/VCO/maestro/maestro.sdb','cds_lib':'/project/cds.lib','ocean':'/install/a/ocean'}
        original=design_storage_root(source,{'Alias':'/project/Designs'})
        changed=dict(source,cds_lib='/other/cds.lib',ocean='/different/cadence/ocean')
        self.assertEqual(original,design_storage_root(changed,{'Alias':'/project/Designs','Other':'/other/lib'}))
        self.assertNotEqual(legacy_design_storage_root(source,{}),legacy_design_storage_root(changed,{}))
        migrated=dict(changed,work_dir='/new/machine/project',setup_path='/new/machine/project/Designs/VCO/maestro/maestro.sdb')
        self.assertEqual(os.path.basename(original),os.path.basename(design_storage_root(migrated,{})))
        alias=dict(source,library='OtherAlias')
        self.assertNotEqual(original,design_storage_root(alias,{}))
        relative=dict(source,setup_path='/project/Another/VCO/maestro/maestro.sdb')
        self.assertNotEqual(original,design_storage_root(relative,{}))

    def test_mapping_changes_publish_new_path_and_keep_active_old_file_immutable(self):
        from test_maestro_context import MaestroContextTests, record
        fixture=MaestroContextTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        path=fixture.root+'/context'
        with open(path,'w') as stream:stream.write('\n'.join(fixture.rows)+'\n')
        with mock.patch.dict(os.environ,{},clear=True):
            first=load_launch_context(path,consume=False)
            with open(first['effective_cds_lib']) as stream:original=stream.read()
            before=os.stat(first['effective_cds_lib'])
            newlib=fixture.root+'/newLib';os.makedirs(newlib)
            with open(path,'w') as stream:stream.write('\n'.join(fixture.rows+[record('L','ExtraLib',newlib)])+'\n')
            second=load_launch_context(path,consume=False)
        self.assertEqual(first['design_root'],second['design_root'])
        self.assertEqual(first['settings_root'],second['settings_root'])
        self.assertNotEqual(first['effective_cds_lib'],second['effective_cds_lib'])
        self.assertIn('/cache/mappings/',second['effective_cds_lib'])
        with open(first['effective_cds_lib']) as stream:self.assertEqual(stream.read(),original)
        after=os.stat(first['effective_cds_lib'])
        self.assertEqual((before.st_ino,before.st_mtime_ns),(after.st_ino,after.st_mtime_ns))
        with open(second['effective_cds_lib']) as stream:self.assertIn('ExtraLib',stream.read())
