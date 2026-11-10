"""Analysis controls for the single Test selected on Design Setup."""
import tkinter as tk
from tkinter import ttk


class ExecutionSelectionPanel(ttk.Frame):
    def __init__(self, parent, on_change=None):
        ttk.Frame.__init__(self, parent, style='Card.TFrame')
        self.on_change = on_change
        self.tests = {}
        self.selection = {}
        self.analysis_variables = []
        ttk.Label(self, text='Analyses', style='CardTitle.TLabel').pack(anchor='w')
        self.detail_label = ttk.Label(self, text='Select a Test on Design Setup.', style='Card.TLabel')
        self.detail_label.pack(anchor='w', pady=(3, 8))
        ttk.Label(self, text='Analysis settings are inherited from Maestro.', style='CardMuted.TLabel').pack(anchor='w')
        self.canvas = tk.Canvas(self, height=145, highlightthickness=0, background='#ffffff')
        scroll = ttk.Scrollbar(self, orient='vertical', command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=scroll.set)
        scroll.pack(side='right', fill='y'); self.canvas.pack(fill='both', expand=True)
        self.analysis_frame = ttk.Frame(self.canvas, style='Card.TFrame')
        self._window = self.canvas.create_window((0, 0), window=self.analysis_frame, anchor='nw')
        self.analysis_frame.bind('<Configure>', lambda event: self.canvas.configure(scrollregion=self.canvas.bbox('all')))
        self.canvas.bind('<Configure>', lambda event: self.canvas.itemconfigure(self._window, width=event.width))

    def set_setup(self, setup, preserve=False):
        previous = self.collect(False) if preserve else None
        rows = (setup or {}).get('tests') or []
        if len(rows) > 1:
            raise ValueError('Select exactly one Test before loading analyses.')
        self.tests = {row['name']: row for row in rows if row.get('name')}
        self.selection = {name: [a['name'] for a in row.get('analyses') or []
                                 if a.get('name') and a.get('enabled', True)]
                          for name, row in self.tests.items()}
        if previous:
            self.restore(previous, notify=False)
        self._render()

    def _render(self):
        for child in self.analysis_frame.winfo_children(): child.destroy()
        self.analysis_variables = []
        name = next(iter(self.tests), None)
        self.detail_label.configure(text='Analyses: ' + name if name else 'Select a Test on Design Setup.')
        if not name: return
        for analysis in self.tests[name].get('analyses') or []:
            key = analysis.get('name')
            if not key: continue
            variable = tk.BooleanVar(value=key in self.selection[name])
            self.analysis_variables.append((key, variable))
            ttk.Checkbutton(self.analysis_frame, text=key, variable=variable,
                            command=self._changed).pack(anchor='w', pady=(4, 0))

    def _changed(self):
        name = next(iter(self.tests), None)
        if name: self.selection[name] = [key for key, var in self.analysis_variables if var.get()]
        if self.on_change: self.on_change()

    def collect(self, require_selection=True):
        name = next(iter(self.tests), None)
        if not name:
            if require_selection: raise ValueError('Select one Test on Design Setup.')
            return None
        analyses = list(self.selection[name])
        if require_selection and self.tests[name].get('analyses') and not analyses:
            raise ValueError('Select at least one analysis for: ' + name)
        return {'tests': [name], 'analyses': {name: analyses} if self.tests[name].get('analyses') else {}}

    def restore(self, selection, notify=True):
        if not isinstance(selection, dict): return
        name = next(iter(self.tests), None)
        if not name: return
        selected_tests = selection.get('tests')
        if isinstance(selected_tests, list) and selected_tests != [name]:
            raise ValueError('Configuration must select exactly the current Test.')
        chosen = (selection.get('analyses') or {}).get(name)
        if isinstance(chosen, list):
            available = {item['name'] for item in self.tests[name].get('analyses') or [] if item.get('name')}
            self.selection[name] = [key for key in chosen if key in available]
        self._render()
        if notify and self.on_change: self.on_change()

    def destroy(self):
        self.on_change = None
        ttk.Frame.destroy(self)
        self.analysis_variables = []
