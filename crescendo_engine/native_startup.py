"""Observe the owned Ocean launch until the Native Run bootstrap starts."""
import math
import os
import re
import time

from .runtime import bind_text
_contract_text = bind_text()

DEFAULTS = {
    "native_startup_crash_quiet_seconds": 15,
    "native_startup_crash_retries": 1,
    "native_cleanup_proof_timeout_seconds": 15,
}
STAGE = _contract_text("CRESCENDO_NATIVE_RUN_STAGE Startup BEGIN").encode("ascii")
CRASH = re.compile(rb"^sh: cdsCrashReport: command not found\r?$", re.MULTILINE)


def settings(values):
    result = {}
    for key, default in DEFAULTS.items():
        value = values.get(key, default)
        retries = key == "native_startup_crash_retries"
        requirement = "a nonnegative integer" if retries else "finite positive seconds"
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            number = float("nan")
        if (isinstance(value, bool) or not math.isfinite(number) or
                (number < 0 or number != int(number) if retries else number <= 0)):
            raise ValueError("%s must be %s" % (key, requirement))
        result[key] = int(number) if retries else number
    return result


class _LogCursor(object):
    def __init__(self, path):
        self.path = path
        self.offset = 0
        self.extent = 0
        self.overlap = b""

    def read(self):
        try:
            with open(self.path, "rb") as stream:
                size = os.fstat(stream.fileno()).st_size
                if size < self.offset:
                    self.offset = 0
                    self.overlap = b""
                stream.seek(self.offset)
                data = stream.read(65536)
        except FileNotFoundError:
            return b"", False, False
        progressed = size != self.extent or bool(data)
        self.extent = size
        self.offset += len(data)
        text = self.overlap + data
        self.overlap = text[-256:]
        return text, progressed, self.offset < size


class StartupObservation(object):
    def __init__(self, control, quiet_seconds):
        self.stdout = _LogCursor(os.path.join(control, "ocean.stdout"))
        self.stderr = _LogCursor(os.path.join(control, "ocean.stderr"))
        self.quiet_seconds = quiet_seconds
        self.last_progress = time.monotonic()
        self.stage_seen = False
        self.signature = None

    def observe(self, protocol_rows):
        if protocol_rows or self.stage_seen:
            self.stage_seen = True
            return None
        stdout, progressed, backlog = self.stdout.read()
        if STAGE in stdout:
            self.stage_seen = True
            return None
        now = time.monotonic()
        if progressed:
            self.last_progress = now
        if self.signature is None:
            stderr, _, _ = self.stderr.read()
            match = CRASH.search(stderr)
            if match:
                self.signature = match.group().decode("ascii").rstrip()
        if self.signature and not backlog and now - self.last_progress >= self.quiet_seconds:
            # Recheck the producer extent immediately before declaring quiet.
            try:
                if os.stat(self.stdout.path).st_size != self.stdout.extent:
                    return None
            except FileNotFoundError:
                if self.stdout.extent:
                    return None
            return self.signature
        return None
