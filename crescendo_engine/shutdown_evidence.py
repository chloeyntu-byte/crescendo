"""Bounded shutdown proof reader, including pre-split legacy documents."""
import io
import json
import os
import re
import stat

_MAX_FILE = 1024 * 1024 * 1024
_MAX_NODES = 2000000
_MAX_WORKERS = 4096
_SPECIAL = re.compile(r'["\\\x00-\x1f]')
_SPACE = re.compile(r'[ \t\r\n]*')
_NUMBER = re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z')


class _Reader(object):
    def __init__(self, stream):
        self.stream = stream
        self.buf = ''
        self.pos = 0
        self.nodes = 0

    def peek(self):
        if self.pos == len(self.buf):
            self.buf = self.stream.read(65536)
            self.pos = 0
        return self.buf[self.pos:self.pos + 1]

    def take(self):
        c = self.peek()
        self.pos += bool(c)
        return c

    def space(self):
        while self.peek():
            end = _SPACE.match(self.buf, self.pos).end()
            if end == self.pos:
                return
            self.pos = end

    def string(self, capture):
        if self.take() != '"':
            raise ValueError('expected string')
        pieces = []
        length = 0
        while True:
            if not self.peek():
                raise ValueError('unterminated string')
            match = _SPECIAL.search(self.buf, self.pos)
            end = match.start() if match else len(self.buf)
            part = self.buf[self.pos:end]
            self.pos = end
            if capture:
                pieces.append(part)
                length += len(part)
                if length > 65536:
                    raise ValueError('shutdown proof string too long')
            if not match:
                continue
            c = self.take()
            if c == '"':
                return json.loads('\"' + ''.join(pieces) + '\"') if capture else None
            if c != '\\':
                raise ValueError('invalid string control character')
            esc = self.take()
            if esc not in '"\\/bfnrtu' or not esc:
                raise ValueError('invalid string escape')
            raw = '\\' + esc
            if esc == 'u':
                digits = ''.join(self.take() for _ in range(4))
                if len(digits) != 4 or any(c not in '0123456789abcdefABCDEF' for c in digits):
                    raise ValueError('invalid unicode escape')
                raw += digits
            if capture:
                pieces.append(raw)
                length += len(raw)
                if length > 65536:
                    raise ValueError('shutdown proof string too long')

    def value(self, mode=None, depth=0):
        self.nodes += 1
        if self.nodes > _MAX_NODES or depth > 64:
            raise ValueError('shutdown JSON complexity limit exceeded')
        self.space()
        c = self.peek()
        if c == '{':
            self.take()
            out = {} if mode in ('root', 'worker', 'final', 'result') else None
            self.space()
            if self.peek() == '}':
                self.take()
                return out
            while True:
                self.space()
                key = self.string(True)
                self.space()
                if self.take() != ':':
                    raise ValueError('expected colon')
                child = None
                if mode == 'root':
                    child = {'schema_version': 'scalar', 'workers': 'workers', 'final_cleanup': 'final', 'finished_at': 'scalar'}.get(key)
                elif mode == 'worker':
                    child = {'worker_index': 'scalar', 'ok': 'scalar', 'error': 'scalar', 'details_path': 'scalar', 'result': 'result'}.get(key)
                elif mode == 'result':
                    child = dict((k, 'scalar') for k in ('cleanup_proven', 'termination_proven', 'process_still_alive', 'process_termination_proven', 'remote_stop_proven', 'artifact_cleanup_complete')).get(key)
                elif mode == 'final':
                    child = {'process_termination_proven': 'scalar', 'remote_stop_proven': 'scalar', 'artifact_cleanup_complete': 'scalar'}.get(key)
                value = self.value(child, depth + 1)
                if child:
                    if key in out:
                        raise ValueError('duplicate shutdown proof field')
                    out[key] = value
                self.space()
                sep = self.take()
                if sep == '}':
                    return out
                if sep != ',':
                    raise ValueError('expected object separator')
        if c == '[':
            self.take()
            out = [] if mode == 'workers' else None
            self.space()
            if self.peek() == ']':
                self.take()
                return out
            count = 0
            while True:
                value = self.value('worker' if mode == 'workers' else None, depth + 1)
                if out is not None:
                    out.append(value)
                    if len(out) > _MAX_WORKERS:
                        raise ValueError('too many shutdown workers')
                count += 1
                self.space()
                sep = self.take()
                if sep == ']':
                    return out
                if sep != ',':
                    raise ValueError('expected array separator')
        if c == '"':
            return self.string(mode == 'scalar')
        token = ''
        while self.peek() and self.peek() not in ' \t\r\n,]}':
            token += self.take()
            if len(token) > 128:
                raise ValueError('invalid JSON token')
        if token == 'true':
            return True if mode == 'scalar' else None
        if token == 'false':
            return False if mode == 'scalar' else None
        if token == 'null':
            return None
        if not _NUMBER.match(token):
            raise ValueError('invalid JSON token')
        return json.loads(token) if mode == 'scalar' else None


def read_shutdown(descriptor, description):
    before = os.fstat(descriptor)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise ValueError('%s must be a singly linked regular file' % description)
    if before.st_size > _MAX_FILE:
        raise ValueError('%s exceeds the shutdown evidence limit' % description)
    try:
        with io.open(os.dup(descriptor), 'r', encoding='utf-8', errors='strict') as stream:
            reader = _Reader(stream)
            result = reader.value('root')
            reader.space()
            if reader.peek():
                raise ValueError('trailing JSON data')
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ValueError('%s is not valid bounded JSON: %s' % (description, error))
    after = os.fstat(descriptor)
    identity = lambda s: (s.st_dev, s.st_ino, s.st_size, getattr(s, 'st_mtime_ns', s.st_mtime), getattr(s, 'st_ctime_ns', s.st_ctime), s.st_nlink)
    if identity(before) != identity(after):
        raise RuntimeError('%s changed while being read' % description)
    return result


def shutdown_proven(value):
    if not isinstance(value, dict) or value.get('schema_version') != 'crescendo.worker_shutdown.v1':
        return False
    workers = value.get('workers')
    if not isinstance(workers, list) or not workers:
        return False
    if any(not isinstance(row, dict) or row.get('ok') is not True for row in workers):
        return False
    for row in workers:
        result = row.get('result')
        if 'result' in row:
            if not isinstance(result, dict):
                return False
            for key in ('cleanup_proven', 'termination_proven', 'process_termination_proven',
                        'remote_stop_proven', 'artifact_cleanup_complete'):
                if key in result and result[key] is not True:
                    return False
            if 'process_still_alive' in result and result['process_still_alive'] is not False:
                return False
    if 'final_cleanup' not in value:
        return True
    final = value['final_cleanup']
    return isinstance(final, dict) and all(final.get(key) is True for key in
        ('process_termination_proven', 'remote_stop_proven', 'artifact_cleanup_complete'))
