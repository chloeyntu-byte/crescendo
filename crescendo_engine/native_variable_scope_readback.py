"""Read native Global participation without altering saved ADE variables."""
from crescendo_engine.runtime import bind_text
_contract_text = bind_text()

import ast
import os
import re


def _names(setup):
    names = [row['name'] for row in setup.get('global_variables') or []]
    tests = [row['name'] for row in setup.get('tests') or [] if row.get('enabled', True)]
    for group in (names, tests):
        if len(set(group)) != len(group) or any(
                not isinstance(name, str) or not name or
                any(ord(char) < 32 or ord(char) == 127 for char in name) for name in group):
            raise ValueError('Native variable scope identities are invalid or duplicated')
    return names, tests


def _quote(value):
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'


def native_variable_scope_skill_lines(setup, path, main_sdb_expr='mainSDB'):
    """IC618 documented total/variable/per-Test flags; nil is a valid flag.

    errset success is checked separately from its returned value, so a failed
    native query can never be silently treated as a disabled Global.
    """
    names, tests = _names(setup)
    if (not os.path.isabs(path) or any(ord(char) < 32 for char in path) or
            not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', main_sdb_expr)):
        raise ValueError('Native variable scope path/database identity is invalid')
    db = main_sdb_expr
    return [
        _contract_text('let((crescScopeOut crescScopeResult crescScopeName crescScopeTest crescScopeHandle)'),
        "  unless(isCallable('axlGetAllVarsDisabled) && isCallable('axlGetVar) &&",
        "         isCallable('axlGetVarValue) && isCallable('axlGetEnabled) &&",
        "         isCallable('axlGetEnabledGlobalVarPerTest)",
        _contract_text('    error("Crescendo cannot read native Global variable participation\\n"))'),
        _contract_text('  crescScopeOut=outfile(%s)') % _quote(path),
        _contract_text('  unless(crescScopeOut error("Crescendo native variable scope file unavailable\\n"))'),
        '  unwindProtect(',
        '    progn(',
        _contract_text('      crescScopeResult=errset(axlGetAllVarsDisabled(%s) t)') % db,
        _contract_text('      unless(crescScopeResult error("Crescendo native Global total flag query failed\\n"))'),
        _contract_text('      unless(car(crescScopeResult)==t || null(car(crescScopeResult))'),
        _contract_text('        error("Crescendo native Global total flag is unknown\\n"))'),
        _contract_text('      fprintf(crescScopeOut "TOTAL\\t%L\\n" !car(crescScopeResult))'),
        _contract_text('      foreach(crescScopeName list(%s)') % ' '.join(_quote(name) for name in names),
        _contract_text('        crescScopeResult=errset(axlGetVar(%s crescScopeName) t)') % db,
        _contract_text('        unless(crescScopeResult && car(crescScopeResult) && car(crescScopeResult)!=0'),
        _contract_text('          error("Crescendo saved Global is absent from native setup: %s\\n" crescScopeName))'),
        _contract_text('        crescScopeHandle=car(crescScopeResult)'),
        _contract_text('        crescScopeResult=errset(axlGetVarValue(crescScopeHandle) t)'),
        _contract_text('        unless(crescScopeResult error("Crescendo native Global value query failed: %s\\n" crescScopeName))'),
        _contract_text('        fprintf(crescScopeOut "VALUE\\t%L\\t%L\\n" crescScopeName car(crescScopeResult))'),
        _contract_text('        crescScopeResult=errset(axlGetEnabled(crescScopeHandle) t)'),
        _contract_text('        unless(crescScopeResult error("Crescendo native Global enabled query failed: %s\\n" crescScopeName))'),
        _contract_text('        unless(car(crescScopeResult)==t || null(car(crescScopeResult))'),
        _contract_text('          error("Crescendo native Global enabled flag is unknown: %s\\n" crescScopeName))'),
        _contract_text('        fprintf(crescScopeOut "GLOBAL\\t%L\\t%L\\n" crescScopeName car(crescScopeResult))'),
        _contract_text('        foreach(crescScopeTest list(%s)') % ' '.join(_quote(name) for name in tests),
        _contract_text('          crescScopeResult=errset(axlGetEnabledGlobalVarPerTest(%s crescScopeName crescScopeTest) t)') % db,
        _contract_text('          unless(crescScopeResult error("Crescendo native per-Test Global query failed: %s/%s\\n" crescScopeName crescScopeTest))'),
        _contract_text('          unless(car(crescScopeResult)==t || null(car(crescScopeResult))'),
        _contract_text('            error("Crescendo native per-Test Global flag is unknown: %s/%s\\n" crescScopeName crescScopeTest))'),
        _contract_text('          fprintf(crescScopeOut "TEST\\t%L\\t%L\\t%L\\n" crescScopeName crescScopeTest car(crescScopeResult))'),
        '        )',
        '      )',
        _contract_text('      fprintf(crescScopeOut "DONE\\n")'),
        '      t',
        '    )',
        _contract_text('    close(crescScopeOut)'),
        '  )',
        ')',
    ]


def parse_native_variable_scope(path, setup):
    """Require complete native evidence for the saved expected identities."""
    names, tests = _names(setup)
    if os.path.getsize(path) > 32 * 1024 * 1024:
        raise ValueError('Native variable scope readback exceeds its byte bound')
    records, done = {}, False
    with open(path, encoding='utf-8') as stream:
        for line in stream:
            parts = line.rstrip('\n').split('\t')
            kind = parts[0]
            if done:
                raise ValueError('Unexpected native scope data after DONE')
            if parts == ['DONE']:
                done = True
                continue
            width = {'TOTAL': 2, 'VALUE': 3, 'GLOBAL': 3, 'TEST': 4}.get(kind)
            if width is None or len(parts) != width:
                raise ValueError('Malformed native variable scope record')
            values = []
            for index, value in enumerate(parts[1:]):
                if kind == 'VALUE' and index == 1 and value == 'nil':
                    values.append(None)
                else:
                    values.append(True if value == 't' else False if value == 'nil' else ast.literal_eval(value))
            key = (kind,) + tuple(values[:-1])
            if key in records:
                raise ValueError('Duplicate native variable scope record')
            records[key] = values[-1]
    expected = {('TOTAL',)}
    for name in names:
        expected.update({('VALUE', name), ('GLOBAL', name)})
        expected.update(('TEST', name, test) for test in tests)
    if not done or set(records) != expected:
        raise ValueError('Native variable scope readback is incomplete or has unknown identities')
    flags = [value for key, value in records.items() if key[0] != 'VALUE']
    if any(type(value) is not bool for value in flags):
        raise ValueError('Native variable scope has an unknown enabled flag')
    return {'read_ok': True, 'source': 'native_ADE_setup_readback', 'path': path,
            'global_variables_enabled': records[('TOTAL',)],
            'global_variables': [
                {'name': name, 'read_ok': True, 'enabled': records[('GLOBAL', name)],
                 'value': records[('VALUE', name)],
                 'tests': [{'test': test, 'read_ok': True,
                            'enabled': records[('TEST', name, test)]} for test in tests]}
                for name in names],
            'effective_point_values_proven': False}
