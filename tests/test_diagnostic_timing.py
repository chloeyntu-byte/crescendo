import unittest

from crescendo_engine.diagnostic_timing import DiagnosticTiming


class FakeClock(object):
    def __init__(self):
        self.value = 0.0
        self.failed = False

    def __call__(self):
        if self.failed:
            raise RuntimeError('clock unavailable')
        return self.value


class DiagnosticTimingTests(unittest.TestCase):
    def test_lifecycle_intervals_and_live_snapshot(self):
        clock = FakeClock()
        timing = DiagnosticTiming(clock)
        self.assertEqual(timing.snapshot()['status'], 'not_started')
        self.assertIsNone(timing.snapshot()['total_seconds'])
        timing.start('registration')
        clock.value = 2
        timing.transition('preparation')
        clock.value = 5
        live = timing.snapshot()
        self.assertEqual(live['total_seconds'], 5)
        self.assertEqual(live['phases_seconds'], {'registration': 2, 'preparation': 3})
        clock.value = 8
        timing.transition('execution_measurement')
        clock.value = 12
        timing.transition('cleanup')
        clock.value = 13
        timing.transition('collection')
        clock.value = 15
        timing.finish('completed')
        clock.value = 100
        result = timing.snapshot()
        self.assertEqual(result['total_seconds'], 15)
        self.assertEqual(sum(result['phases_seconds'].values()), 15)
        self.assertEqual(result['status'], 'completed')
        self.assertIsNone(result['active_phase'])

    def test_cancel_and_failure_preserve_elapsed_and_finish_is_idempotent(self):
        for status in ('cancelled', 'failed'):
            clock = FakeClock()
            timing = DiagnosticTiming(clock)
            timing.start('execution_measurement')
            clock.value = 3
            timing.finish(status)
            clock.value = 9
            timing.finish('completed')
            self.assertEqual(timing.snapshot()['status'], status)
            self.assertEqual(timing.snapshot()['total_seconds'], 3)

    def test_clock_failure_never_reports_partial_timings(self):
        clock = FakeClock()
        timing = DiagnosticTiming(clock)
        timing.start('registration')
        clock.failed = True
        timing.transition('execution_measurement')
        timing.finish('failed')
        result = timing.snapshot()
        self.assertFalse(result['available'])
        self.assertIsNone(result['total_seconds'])
        self.assertIsNone(result['phases_seconds'])
        self.assertEqual(result['status'], 'failed')

    def test_snapshot_during_transition_uses_one_published_state(self):
        clock = FakeClock()
        timing = DiagnosticTiming(clock)
        timing.start('registration')
        clock.value = 4
        called = []
        def transition_during_read():
            if not called:
                called.append(True)
                timing.transition('execution')
            return clock.value
        timing._clock = transition_during_read
        result = timing.snapshot()
        self.assertEqual(result['active_phase'], 'registration')
        self.assertEqual(result['phases_seconds'], {'registration': 4})
        self.assertEqual(result['total_seconds'], 4)
        self.assertEqual(timing.snapshot()['active_phase'], 'execution')

    def test_snapshot_during_finish_and_clock_failure(self):
        clock = FakeClock()
        timing = DiagnosticTiming(clock)
        timing.start('execution')
        clock.value = 3
        called = []
        def finish_during_read():
            if not called:
                called.append(True)
                timing.finish('cancelled')
            return clock.value
        timing._clock = finish_during_read
        result = timing.snapshot()
        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['phases_seconds'], {'execution': 3})
        self.assertEqual(timing.snapshot()['status'], 'cancelled')
        timing._available = False
        result = timing.snapshot()
        self.assertFalse(result['available'])
        self.assertIsNone(result['total_seconds'])
        self.assertIsNone(result['phases_seconds'])

    def test_backwards_clock_is_clamped(self):
        clock = FakeClock()
        timing = DiagnosticTiming(clock)
        timing.start('one')
        clock.value = 4
        timing.transition('two')
        clock.value = 2
        timing.finish()
        result = timing.snapshot()
        self.assertEqual(result['total_seconds'], 4)
        self.assertEqual(result['phases_seconds'], {'one': 4, 'two': 0})


if __name__ == '__main__':
    unittest.main()
