"""Small in-memory UI activity journal; no file reads or execution authority."""
from collections import OrderedDict, deque
import time


class ActivityJournal(object):
    def __init__(self, max_lines=600, max_characters=131072, max_message=2048):
        self.max_lines = max_lines
        self.max_characters = max_characters
        self.max_message = max_message
        self.lines = deque()
        self.characters = 0
        self.revision = 0
        self.signatures = OrderedDict()
        self.active = OrderedDict()

    def append(self, message, level='INFO', key=None):
        message = ' '.join(str(message or '').replace('\x00', '').splitlines()).strip()
        if not message:
            return None
        message = message[:self.max_message]
        signature = (level, message)
        if key is not None:
            key = str(key)
            if self.signatures.get(key) == signature:
                return None
            self.signatures[key] = signature
            self.signatures.move_to_end(key)
            while len(self.signatures) > self.max_lines:
                self.signatures.popitem(last=False)
        self.revision += 1
        text = '{} [{}] {}\n'.format(time.strftime('%H:%M:%S'), level, message)
        row = (self.revision, text, level)
        self.lines.append(row)
        self.characters += len(text)
        while len(self.lines) > self.max_lines or self.characters > self.max_characters:
            self.characters -= len(self.lines.popleft()[1])
        return row

    def begin(self, token, label, total=None, completed=0, priority=20):
        self.active[token] = {'label': str(label)[:160], 'total': total,
                              'completed': completed, 'priority': priority}
        self.active.move_to_end(token)

    def update(self, token, label=None, total=None, completed=None):
        row = self.active.get(token)
        if row is None:
            return
        if label is not None:
            row['label'] = str(label)[:160]
        if total is not None:
            row['total'] = total
        if completed is not None:
            row['completed'] = completed

    def end(self, token):
        self.active.pop(token, None)

    def current(self):
        if not self.active:
            return None
        # Later equal-priority operations are the most specific current stage.
        return max(enumerate(self.active.values()), key=lambda item: (item[1]['priority'], item[0]))[1]
