import copy
import os
import unittest
from unittest import mock
from tests import test_gui_optional_settings as fixture
from tests import test_gui_eda_edit_retention as edit_fixture
from crescendo_engine.unified_configuration import build_configuration, match_configuration


@unittest.skipUnless(os.environ.get('DISPLAY'),'Real Tk requires DISPLAY')
class GuiNativeSettingsFlowTests(unittest.TestCase):
    setUp=fixture.GuiOptionalSettingsTkTests.setUp
    _close=fixture.GuiOptionalSettingsTkTests._close
    _wait=fixture.GuiOptionalSettingsTkTests._wait
    _prepared=fixture.GuiOptionalSettingsTkTests._prepared
    _edit_draft=edit_fixture.GuiEdaEditRetentionTkTests._edit_draft

    def test_partial_module_reload_automatically_applies_remaining_match(self):
        self._prepared();self._edit_draft()
        self.app.selected_cell_scope_ids={'cell-root','cell-B'}
        self.app.applied_cell_scope_ids={'cell-root','cell-B'}
        self.app.cell_scope_dirty=False
        self.service.scopes=mock.Mock(return_value=[self.service.scopes()[0]])
        self.service.apply_calls[:]=[]
        self.app.load_setup_button.invoke()
        self._wait(lambda:not self.app.setup_loading and self.app.eda_auto_revalidation is None)
        self.assertEqual(self.service.apply_calls,[('fixture-inspection',['cell-root'])])
        self.assertEqual(self.app.selected_cell_scope_ids,{'cell-root'})
        self.assertIsNotNone(self.app.prepared_id)
        self.assertFalse(self.app.eda_revalidation_required)
        self.assertTrue(any(row[2]=='WARN' for row in self.app.activity_journal.lines))
        self.service.start.assert_not_called()

    def test_execution_selection_configuration_roundtrip_and_current_native_conflicts(self):
        self._prepared();app=self.app
        app.prepared['setup']['tests']=[{'name':'VCO','enabled':True,'analyses':[
            {'name':'dc','enabled':True},{'name':'ac','enabled':False}]}]
        app.execution_selection_panel.set_setup(app.prepared['setup'])
        app.execution_selection_panel.restore({'tests':['VCO'],'analyses':{'VCO':['ac']}})
        raw,context=app._snapshot_ui_state()
        document=build_configuration(raw, context['parameter_by_id'], context['cell_scopes'], context['outputs'])
        self.assertEqual(document['execution_selection']['tests'],['VCO'])
        app.execution_selection_panel.restore({'tests':['VCO'],'analyses':{'VCO':['dc']}})
        result=match_configuration(document,context)
        app._apply_ui_settings(raw,result)
        self.assertEqual(app.execution_selection_panel.collect(),{'tests':['VCO'],'analyses':{'VCO':['ac']}})
        with self.assertRaisesRegex(ValueError, 'exactly the current Test'):
            app.execution_selection_panel.restore({'tests':['VCO','B'],'analyses':{}})
        changed=copy.deepcopy(context)
        changed['maestro_snapshot']['tests']['VCO']['enabled']=False
        result=match_configuration(document,changed)
        self.assertTrue(result['warnings'])

    def test_native_settings_summary_separates_source_and_owned_paths(self):
        self._prepared();app=self.app
        catalogue=copy.deepcopy(app.prepared)
        catalogue['setup']['save_options']={'effective':{'simulation_root':'/source/results','rdb_root':'/source/rdb'}}
        catalogue['setup_private_session']={'native_storage':{'save_res_dir':'/owned/results','save_dir':'/owned/rdb',
                                                               'configured_roots_verified':True}}
        app._render_native_settings(catalogue)
        text=app.private_settings_summary_var.get()
        for expected in ['/source/results','/source/rdb','/owned/results','/owned/rdb','native readback verified']:
            self.assertIn(expected,text)
        self.assertEqual(app.native_settings_detail['copy_policy'],'complete_saved_maestro_view')
        self.assertIn('External startup',text)

    def test_removed_objective_does_not_silently_become_default_measurement(self):
        self._prepared();app=self.app
        raw,context=app._snapshot_ui_state()
        raw['objectives'][0]['output_key']='removed_measurement'
        document=build_configuration(raw, context['parameter_by_id'], context['cell_scopes'], context['outputs'])
        result=match_configuration(document,context)
        app._apply_ui_settings(raw,result)
        self.assertEqual(app.objective_rows,[])
        self.assertTrue(result['warnings'])
        self.service.start.assert_not_called()

    def test_all_previous_corners_disabled_retains_empty_editable_metric(self):
        self._prepared();app=self.app
        app.prepared['setup']['test_corner_masks']={'known':True,'groups':{'tt':{'disabled_tests':[]}},
                                                     'nominal_enabled':False,'nominal_tests':{'VCO':False}}
        raw,context=app._snapshot_ui_state()
        document=build_configuration(raw, context['parameter_by_id'], context['cell_scopes'], context['outputs'])
        latest=copy.deepcopy(context)
        latest['maestro_snapshot']['corners']['tt']['enabled']=False
        result=match_configuration(document,latest)
        self.assertTrue(result['warnings'])
        app._apply_ui_settings(raw,result)
        self.assertEqual(len(app.objective_rows),1)
        self.assertEqual(app.objective_rows[0].corner_picker.selected(),[])
        self.assertEqual(app.objective_rows[0].output_var.get(),'frequency')
