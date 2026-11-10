"""Bounded GUI-only missing-library discovery, using Flux v2.3.1 matching rules.

Only a demonstrated missing SONAME or Tcl/Tk script permits an overlay.
Pure stdlib; no installations, global changes, or dependency cache writes.
"""
import os
from pathlib import Path
import re
import struct

MAX_DIRECTORIES = 160
MAX_CHILDREN = 96
OVERLAY_KEYS = ('LD_LIBRARY_PATH', 'TCL_LIBRARY', 'TK_LIBRARY')
_MISSING = re.compile(r'(?P<name>[A-Za-z0-9_.+\-]+\.so(?:\.[A-Za-z0-9_.+\-]+)*)'
                      r': (?:cannot open shared object file|No such file or directory)')
_ABI = re.compile(r'lib(?P<kind>tcl|tk)(?P<version>[0-9]+\.[0-9]+)', re.I)

def _note(diagnostics, stage, message):
    diagnostics.append('Crescendo GUI dependency stage=%s: %s' % (stage, message))


def _canonical_directory(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        return None
    path = Path(value)
    if not path.is_absolute():
        return None
    try:
        resolved = path.resolve()
        return str(resolved) if resolved.is_dir() else None
    except (OSError, RuntimeError):
        return None


def _elf_info(filename):
    """Read only ELF headers/dynamic strings; avoid external tools or file scans."""
    try:
        with open(str(filename), 'rb') as stream:
            ident = stream.read(16)
            if len(ident) != 16 or ident[:4] != b'\x7fELF' or ident[4] not in (1, 2) or ident[5] not in (1, 2):
                return None
            bits = 32 if ident[4] == 1 else 64
            endian = '<' if ident[5] == 1 else '>'
            header_format = endian + ('HHIIIIIHHHHHH' if bits == 32 else 'HHIQQQIHHHHHH')
            header = struct.unpack(header_format, stream.read(struct.calcsize(header_format)))
            machine, phoff, phsize, phcount = header[1], header[4], header[8], header[9]
            if phcount > 256 or phsize > 256:
                return None
            loads, dynamic = [], None
            program_format = endian + ('IIIIIIII' if bits == 32 else 'IIQQQQQQ')
            program_size = struct.calcsize(program_format)
            if phsize < program_size:
                return None
            for index in range(phcount):
                stream.seek(phoff + index * phsize)
                entry = struct.unpack(program_format, stream.read(program_size))
                if bits == 32:
                    kind, offset, address, size = entry[0], entry[1], entry[2], entry[4]
                else:
                    kind, offset, address, size = entry[0], entry[2], entry[3], entry[5]
                if kind == 1:
                    loads.append((address, offset, size))
                elif kind == 2:
                    dynamic = (offset, size)
            result = dict(bits=bits, machine=machine, endian=endian,
                          needed=[], soname=None, rpaths=[])
            if dynamic is None:
                return result
            dynamic_format = endian + ('iI' if bits == 32 else 'qQ')
            dynamic_size = struct.calcsize(dynamic_format)
            if dynamic[1] > 262144:
                return None
            tags = []
            stream.seek(dynamic[0])
            for _ in range(dynamic[1] // dynamic_size):
                tag, value = struct.unpack(dynamic_format, stream.read(dynamic_size))
                if tag == 0:
                    break
                tags.append((tag, value))
            strings_address = next((value for tag, value in tags if tag == 5), None)
            strings_size = next((value for tag, value in tags if tag == 10), None)
            if strings_address is None or strings_size is None or strings_size > 2097152:
                return result
            strings_offset = next((offset + strings_address - address
                                   for address, offset, size in loads
                                   if address <= strings_address < address + size), None)
            if strings_offset is None:
                return None
            stream.seek(strings_offset)
            strings = stream.read(strings_size)
            def string_at(offset):
                if offset >= len(strings):
                    return ''
                return strings[offset:].split(b'\0', 1)[0].decode('utf-8', 'replace')
            result['needed'] = [string_at(value) for tag, value in tags if tag == 1]
            result['soname'] = next((string_at(value) for tag, value in tags if tag == 14), None)
            result['rpaths'] = [string_at(value) for tag, value in tags if tag in (15, 29)]
            return result
    except (OSError, ValueError, struct.error, OverflowError):
        return None


def _add_directory(directories, value):
    resolved = _canonical_directory(str(value))
    if resolved and resolved not in directories and len(directories) < MAX_DIRECTORIES:
        directories.append(resolved)


def _bounded_children(folder):
    """Inspect a fixed number of entries in one already selected directory."""
    names = []
    try:
        with os.scandir(folder) as entries:
            for entry in entries:
                names.append(entry.name)
                if len(names) >= MAX_CHILDREN:
                    break
    except OSError:
        return []
    return sorted(names)


def _rpath_directories(filename, info):
    result = []
    if not info:
        return result
    origin = str(Path(filename).resolve().parent)
    for value in info['rpaths']:
        for part in value.split(':'):
            part = part.replace('${ORIGIN}', origin).replace('$ORIGIN', origin)
            if '$' not in part:
                _add_directory(result, part)
    return result


def _candidate_directories(candidate, metadata, environment, diagnostics):
    directories, prefixes = [], []
    for executable in (Path(candidate), Path(candidate).resolve()):
        parent = executable.parent
        prefixes.append(parent.parent if parent.name in ('bin', 'sbin') else parent)
        _add_directory(directories, parent)
        for value in _rpath_directories(executable, _elf_info(executable)):
            _add_directory(directories, value)
    for key in ('prefix', 'base_prefix', 'exec_prefix'):
        value = metadata.get(key)
        if isinstance(value, str) and Path(value).is_absolute():
            prefixes.append(Path(value))
    for value in metadata.get('libdirs', []):
        if isinstance(value, str):
            _add_directory(directories, value)
    tkinter_file = metadata.get('tkinter_file')
    if isinstance(tkinter_file, str) and Path(tkinter_file).is_absolute():
        _add_directory(directories, Path(tkinter_file).parent)
        for value in _rpath_directories(tkinter_file, _elf_info(tkinter_file)):
            _add_directory(directories, value)
    for prefix in prefixes:
        _add_directory(directories, prefix)
        for suffix in ('lib', 'lib64', 'lib/tcl-tk', 'lib/tcl-tk/lib',
                       'lib64/tcl-tk', 'lib64/tcl-tk/lib', 'share/tcltk', 'share'):
            _add_directory(directories, prefix / suffix)
    # Old loader values are clues, never a loader environment to restore.
    original = environment.get('CRESCENDO_DEPENDENCY_LOADER_HINT', '')
    for part in original.split(':'):
        if part and Path(part).is_absolute():
            resolved = _canonical_directory(part)
            unrelated_qt = resolved and any(re.match(r'^qt(?:[0-9]+(?:\.[0-9]+)*)?$', value, re.I)
                                           for value in Path(resolved).parts)
            own_prefix = False
            if resolved:
                for prefix in prefixes:
                    try:
                        Path(resolved).relative_to(prefix.resolve())
                        own_prefix = True
                        break
                    except (ValueError, OSError):
                        pass
            # Do not restore an unrelated Qt directory from old Cadence LD
            # hints. Actual Python prefixes/sysconfig/RPATH remain authoritative,
            # including a legitimate installation whose name contains "Qt".
            if unrelated_qt and not own_prefix and resolved not in directories:
                _note(diagnostics, 'discovery', 'ignored unrelated Qt loader clue %r' % resolved)
                continue
            _add_directory(directories, part)
    # Enumerate only bounded immediate, named toolkit/Python children.
    parents = list(directories)
    for folder in parents:
        for child in _bounded_children(folder):
            if re.match(r'^(?:python[0-9]+\.[0-9]+[a-z]*|tcl[0-9]+\.[0-9]+|tk[0-9]+\.[0-9]+|[A-Za-z0-9_]+-linux-gnu|tcltk)$', child):
                path = Path(folder) / child
                _add_directory(directories, path)
                for suffix in ('lib', 'lib64', 'lib-dynload', 'config'):
                    _add_directory(directories, path / suffix)
    _note(diagnostics, 'discovery', 'bounded candidate directories=%r' % directories)
    return directories


def _reference_elf(candidate, metadata):
    for value in (metadata.get('tkinter_file'), candidate, str(Path(candidate).resolve())):
        if value:
            info = _elf_info(value)
            if info:
                return info
    return None


def _select_library(name, directories, reference, metadata, diagnostics):
    if reference is None:
        _note(diagnostics, 'library', 'cannot verify the selected executable ELF architecture')
        return None
    expected_abi = _ABI.search(name)
    for directory in directories:
        filename = Path(directory) / name
        if not filename.is_file():
            continue
        info = _elf_info(filename)
        if not info:
            _note(diagnostics, 'library', 'rejected non-ELF candidate %r' % str(filename))
            continue
        if any(info[key] != reference[key] for key in ('bits', 'machine', 'endian')):
            _note(diagnostics, 'library', 'rejected architecture mismatch %r' % str(filename))
            continue
        if info['soname'] not in (None, name):
            _note(diagnostics, 'library', 'rejected SONAME mismatch %r: %r' % (str(filename), info['soname']))
            continue
        actual_abi = _ABI.search(info['soname'] or str(filename.resolve()))
        if expected_abi and actual_abi and expected_abi.groupdict() != actual_abi.groupdict():
            _note(diagnostics, 'library', 'rejected Tcl/Tk SONAME ABI mismatch %r' % str(filename))
            continue
        if actual_abi:
            version = metadata.get(actual_abi.group('kind').lower() + '_version')
            if version and str(version) != actual_abi.group('version'):
                _note(diagnostics, 'library', 'rejected _tkinter Tcl/Tk ABI mismatch %r' % str(filename))
                continue
        _note(diagnostics, 'library', 'selected missing=%r file=%r SONAME=%r ELF=%s/%s' %
              (name, str(filename), info['soname'], info['bits'], info['machine']))
        return str(filename), info
    return None


def _data_candidates(kind, directories, metadata, tried, diagnostics):
    script = 'init.tcl' if kind == 'tcl' else 'tk.tcl'
    version = metadata.get(kind + '_version')
    candidates = []
    for folder in directories:
        _add_directory(candidates, folder)
        if version:
            _add_directory(candidates, Path(folder) / (kind + str(version)))
        for name in _bounded_children(folder):
            if re.match(r'^%s[0-9]+\.[0-9]+(?:\.[0-9]+)?$' % kind, name):
                _add_directory(candidates, Path(folder) / name)
    for directory in candidates:
        if directory in tried or not (Path(directory) / script).is_file():
            continue
        try:
            with open(str(Path(directory) / script), 'r', encoding='utf-8', errors='replace') as stream:
                text = stream.read(32768)
        except OSError:
            continue
        declared = re.search(r'package\s+(?:require(?:\s+-exact)?|provide)\s+%s\s+([0-9]+\.[0-9]+)' % ('Tcl' if kind == 'tcl' else 'Tk'), text)
        named = re.match(r'^%s([0-9]+\.[0-9]+)' % kind, Path(directory).name)
        actual = declared.group(1) if declared else (named.group(1) if named else None)
        if version and actual and actual != str(version):
            _note(diagnostics, 'data', 'rejected %s ABI mismatch %r' % (kind, directory))
            continue
        if version and not actual:
            _note(diagnostics, 'data', 'rejected %s data with unidentifiable ABI %r' % (kind, directory))
            continue
        _note(diagnostics, 'data', 'trying %s=%r for expected ABI=%r' % (script, directory, version))
        return directory
    return None


def _missing_data(stderr):
    lower = stderr.lower()
    if 'init.tcl' in lower and any(value in lower for value in ("can't find", 'cannot find', "couldn't find", "couldn't read", 'no such file', 'version conflict')):
        return 'tcl'
    if 'tk.tcl' in lower and any(value in lower for value in ("can't find", 'cannot find', "couldn't find", "couldn't read", 'no such file', 'version conflict')):
        return 'tk'
    return None


