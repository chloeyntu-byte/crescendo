"""Bounded JSON operations for large catalogues (Python 3.6, no C encoder loop).

The canonical byte protocol is identical to json.dumps(sort_keys=True,
ensure_ascii=True, separators=(',', ':')). Only scheduling and allocation change.
"""
import hashlib
import json
import os
import re
import tempfile
import time

_SPACE = re.compile(r'[ \t\r\n]*')
_END = re.compile(r'[ \t\r\n,\]}]')
_isolated_process = False


def _small(value):
    """Conservative, bounded fast path; never traverse an entire large graph."""
    pending, nodes, text_size = [value], 0, 0
    while pending:
        item = pending.pop()
        nodes += 1
        if nodes > 512:
            return False
        if isinstance(item, str):
            text_size += len(item)
            if text_size > 32768:
                return False
        elif isinstance(item, dict):
            if len(item) > 128:
                return False
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, (list, tuple)):
            if len(item) > 128:
                return False
            pending.extend(item)
    return True


def chunks(value):
    if _isolated_process or _small(value):
        yield json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(',', ':'))
        return
    # iterencode without _one_shot uses the Python iterator, which releases the
    # GIL between bytecodes even when called from a background thread.
    encoder = json.JSONEncoder(sort_keys=True, ensure_ascii=True, separators=(',', ':'))
    deadline = time.monotonic() + .01
    for index, part in enumerate(encoder.iterencode(value)):
        if index % 256 == 0 and time.monotonic() >= deadline:
            time.sleep(.001)  # bound worker starvation even with a long switch interval
            deadline = time.monotonic() + .01
        yield part


def digest(value):
    result = hashlib.sha256()
    for part in chunks(value):
        result.update(part.encode('utf-8'))
    return result.hexdigest()


def write(path, value):
    from .utils import ensure_dir, _fsync_directory, _replace_file
    from .storage_errors import annotate_storage_error
    directory = ensure_dir(os.path.dirname(os.path.abspath(path)))
    temporary = None
    operation = 'create temporary file'
    try:
        fd, temporary = tempfile.mkstemp(prefix='.catalog-', suffix='.tmp', dir=directory)
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            operation = 'write and flush file'
            stream.writelines(chunks(value))
            stream.write('\n')
            stream.flush()
            operation = 'sync file'
            os.fsync(stream.fileno())
        operation = 'replace file'
        _replace_file(temporary, path)
        temporary = None
        operation = 'sync directory'
        _fsync_directory(directory)
    except Exception as error:
        annotate_storage_error(error, path, operation)
        raise
    finally:
        if temporary is not None:
            os.unlink(temporary)


class _Reader(object):
    """Stream containers; C decodes only complete subtrees in the 64 KiB buffer."""
    def __init__(self, stream, initial=''):
        self.stream, self.buffer, self.pos, self.eof = stream, initial, 0, False
        self.count, self.deadline = 0, time.monotonic() + .01
        self.decoder = json.JSONDecoder()

    def more(self):
        part = self.stream.read(65536)
        self.buffer = self.buffer[self.pos:] + part
        self.pos = 0
        self.eof = not part
        return bool(part)

    def peek(self):
        while True:
            match = _SPACE.match(self.buffer, self.pos)
            self.pos = match.end()
            if self.pos < len(self.buffer):
                return self.buffer[self.pos]
            if self.eof or not self.more():
                return ''

    def take(self, expected):
        if self.peek() != expected:
            raise ValueError('Invalid catalogue JSON: expected ' + expected)
        self.pos += 1

    def value(self):
        self.count += 1
        if self.count % 16 == 0 and time.monotonic() >= self.deadline:
            time.sleep(.001)
            self.deadline = time.monotonic() + .01
        token = self.peek()
        if token in ('{', '['):
            try:
                result, end = self.decoder.raw_decode(self.buffer, self.pos)
            except ValueError:
                pass  # container crosses the buffer; stream its children
            else:
                self.pos = end
                return result
        if token == '{':
            self.pos += 1
            result = {}
            if self.peek() != '}':
                while True:
                    if self.peek() != '"':
                        raise ValueError('Invalid catalogue JSON object key')
                    key = self.value()
                    self.take(':')
                    result[key] = self.value()
                    if self.peek() != ',':
                        break
                    self.pos += 1
            self.take('}')
            return result
        if token == '[':
            self.pos += 1
            result = []
            if self.peek() != ']':
                while True:
                    result.append(self.value())
                    if self.peek() != ',':
                        break
                    self.pos += 1
            self.take(']')
            return result
        if token == '"':
            while True:
                try:
                    result, end = json.decoder.scanstring(self.buffer, self.pos + 1, True)
                    self.pos = end
                    return result
                except ValueError:
                    if self.eof or not self.more():
                        raise ValueError('Invalid catalogue JSON string')
        if not token:
            raise ValueError('Unexpected end of catalogue JSON')
        while True:
            match = _END.search(self.buffer, self.pos)
            if match is not None or self.eof:
                end = match.start() if match else len(self.buffer)
                raw = self.buffer[self.pos:end]
                self.pos = end
                return json.loads(raw)
            self.more()


def load(stream):
    if _isolated_process:
        return json.load(stream)
    initial = stream.read(65537)
    if len(initial) <= 65536:
        return json.loads(initial)
    reader = _Reader(stream, initial)
    result = reader.value()
    if reader.peek():
        raise ValueError('Trailing data in catalogue JSON')
    return result
