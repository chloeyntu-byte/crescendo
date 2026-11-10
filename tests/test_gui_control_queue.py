from __future__ import print_function

import threading
import time
import unittest

from crescendo_engine.gui_state import BackgroundTaskQueue


class FakeRoot(object):
    def __init__(self):
        self.callbacks = []
        self.lock = threading.Lock()

    def after(self, _milliseconds, callback):
        with self.lock:
            self.callbacks.append(callback)

    def step(self):
        with self.lock:
            callbacks = list(self.callbacks)
            self.callbacks = []
        for callback in callbacks:
            callback()


class GuiControlQueueTests(unittest.TestCase):
    def test_control_task_bypasses_blocked_normal_worker_and_callback_uses_drain(self):
        root = FakeRoot()
        task_queue = BackgroundTaskQueue(root, interval_ms=1)
        normal_started = threading.Event()
        normal_release = threading.Event()
        control_executed = threading.Event()
        control_threads = []
        received = []

        def blocked_normal_task():
            normal_started.set()
            normal_release.wait(2)
            return "normal"

        def control_task():
            control_threads.append(threading.current_thread().name)
            control_executed.set()
            return "control"

        try:
            task_queue.submit(blocked_normal_task, received.append)
            self.assertTrue(normal_started.wait(1))
            task_queue.submit_control(control_task, received.append)
            self.assertTrue(control_executed.wait(1))
            self.assertFalse(normal_release.is_set())
            self.assertEqual(received, [])

            deadline = time.time() + 1
            while not received and time.time() < deadline:
                root.step()
                time.sleep(0.005)
            self.assertEqual(received, ["control"])
            self.assertEqual(control_threads, ["crescendo-gui-control"])
        finally:
            normal_release.set()
            task_queue.close()
            task_queue.worker.join(2)
            task_queue.control_worker.join(2)

    def test_close_is_idempotent_and_rejects_new_tasks_on_both_queues(self):
        root = FakeRoot()
        task_queue = BackgroundTaskQueue(root, interval_ms=1)
        task_queue.close()
        task_queue.close()

        with self.assertRaisesRegex(RuntimeError, "queue is closed"):
            task_queue.submit(lambda: None)
        with self.assertRaisesRegex(RuntimeError, "queue is closed"):
            task_queue.submit_control(lambda: None)

        task_queue.worker.join(2)
        task_queue.control_worker.join(2)
        self.assertFalse(task_queue.worker.is_alive())
        self.assertFalse(task_queue.control_worker.is_alive())

    def test_callback_error_does_not_disable_future_result_drains(self):
        root = FakeRoot()
        task_queue = BackgroundTaskQueue(root, interval_ms=1)

        def fail(_value):
            raise ValueError("callback failed")

        try:
            with root.lock:
                root.callbacks = []
            task_queue.results.put((True, "first", fail))
            with self.assertRaisesRegex(ValueError, "callback failed"):
                task_queue._drain()
            with root.lock:
                self.assertTrue(root.callbacks)
        finally:
            task_queue.close()
            task_queue.worker.join(2)
            task_queue.control_worker.join(2)

    def test_closing_callback_skips_remaining_results(self):
        root = FakeRoot()
        task_queue = BackgroundTaskQueue(root, interval_ms=1)
        received = []

        def close_from_callback(value):
            received.append(value)
            task_queue.close()

        task_queue.results.put((True, "exit", close_from_callback))
        task_queue.results.put((True, "stale", received.append))
        task_queue._drain()

        task_queue.worker.join(2)
        task_queue.control_worker.join(2)
        self.assertEqual(received, ["exit"])
        self.assertFalse(task_queue.worker.is_alive())
        self.assertFalse(task_queue.control_worker.is_alive())


if __name__ == "__main__":
    unittest.main()
