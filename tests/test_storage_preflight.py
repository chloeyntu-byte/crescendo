import os
import tempfile
import unittest
from collections import namedtuple
from unittest import mock

from crescendo_engine.storage_preflight import (
    MIB, StoragePreflightError, estimate_workspace_copy_bytes,
    startup_storage_preflight,
)


DiskUsage = namedtuple("DiskUsage", "total used free")


def _distinct_device_stat(paths):
    real_stat = os.stat
    devices = dict(
        (os.path.abspath(path), index + 1000)
        for index, path in enumerate(paths)
    )

    def stat_with_devices(path, *args, **kwargs):
        result = real_stat(path, *args, **kwargs)
        device = devices.get(os.path.abspath(path))
        if device is None:
            return result
        values = list(result)
        values[2] = device
        return os.stat_result(values)

    return stat_with_devices


class StoragePreflightTests(unittest.TestCase):
    def test_workspace_estimate_matches_materialized_copy_and_ignores_results(self):
        with tempfile.TemporaryDirectory() as root:
            library = os.path.join(root, "HH")
            maestro = os.path.join(library, "TB", "maestro")
            os.makedirs(os.path.join(maestro, "results"))
            with open(os.path.join(library, "payload.bin"), "wb") as stream:
                stream.write(b"a" * 17)
            with open(os.path.join(maestro, "results", "raw.bin"), "wb") as stream:
                stream.write(b"b" * 100)
            with open(os.path.join(maestro, "maestro.sdb"), "wb") as stream:
                stream.write(b"c" * 23)
            setup = {"source": {
                "library_root": library,
                "maestro_sdb": os.path.join(maestro, "maestro.sdb"),
            }}
            self.assertEqual(estimate_workspace_copy_bytes(setup, {}, config={}), 40)

    def test_same_filesystem_reserves_minimum_once_and_three_checkpoint_writes(self):
        with tempfile.TemporaryDirectory() as root:
            report = startup_storage_preflight(
                os.path.join(root, "runs", "run-a"), root, 1,
                checkpoint={"large": "x" * 100}, workspace_required=False,
                disk_usage=lambda _path: DiskUsage(100 * MIB, 0, 100 * MIB),
            )
            self.assertEqual(len(report["filesystems"]), 1)
            self.assertEqual(report["filesystems"][0]["minimum_free_bytes"], 1 * MIB)
            self.assertEqual(report["filesystems"][0]["headroom_bytes"], 3 * 16 * MIB)
            self.assertEqual(
                report["filesystems"][0]["required_bytes"],
                1 * MIB + 3 * 16 * MIB,
            )

    def test_distinct_filesystems_each_reserve_minimum_and_local_headroom(self):
        with tempfile.TemporaryDirectory() as root:
            run_parent = os.path.join(root, "run-fs")
            anchor_root = os.path.join(root, "anchor-fs")
            os.makedirs(run_parent)
            os.makedirs(anchor_root)
            with mock.patch(
                    "crescendo_engine.storage_preflight.os.stat",
                    side_effect=_distinct_device_stat([run_parent, anchor_root])):
                report = startup_storage_preflight(
                    os.path.join(run_parent, "run-a"), anchor_root, 2,
                    checkpoint={"large": "x" * 100}, workspace_required=False,
                    disk_usage=lambda _path: DiskUsage(100 * MIB, 0, 100 * MIB),
                )
            self.assertEqual(len(report["filesystems"]), 2)
            rows = dict((row["kinds"][0], row) for row in report["filesystems"])
            self.assertEqual(rows["run"]["minimum_free_bytes"], 2 * MIB)
            self.assertEqual(rows["run"]["headroom_bytes"], 16 * MIB)
            self.assertEqual(rows["run"]["required_bytes"], 2 * MIB + 16 * MIB)
            self.assertEqual(rows["checkpoint_anchor"]["minimum_free_bytes"], 2 * MIB)
            self.assertEqual(rows["checkpoint_anchor"]["headroom_bytes"], 2 * 16 * MIB)
            self.assertEqual(
                rows["checkpoint_anchor"]["required_bytes"],
                2 * MIB + 2 * 16 * MIB,
            )

    def test_distinct_anchor_filesystem_low_space_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            run_parent = os.path.join(root, "run-fs")
            anchor_root = os.path.join(root, "anchor-fs")
            os.makedirs(run_parent)
            os.makedirs(anchor_root)

            def disk_usage(path):
                free = 100 * MIB if os.path.abspath(path) == run_parent else 32 * MIB
                return DiskUsage(100 * MIB, 0, free)

            with mock.patch(
                    "crescendo_engine.storage_preflight.os.stat",
                    side_effect=_distinct_device_stat([run_parent, anchor_root])):
                with self.assertRaisesRegex(
                        StoragePreflightError, "checkpoint_anchor"):
                    startup_storage_preflight(
                        os.path.join(run_parent, "run-a"), anchor_root, 1,
                        checkpoint={"large": "x" * 100}, workspace_required=False,
                        disk_usage=disk_usage,
                    )

    def test_low_space_fails_before_any_target_directory_is_created(self):
        with tempfile.TemporaryDirectory() as root:
            run_root = os.path.join(root, "not-created", "run-a")
            with self.assertRaisesRegex(StoragePreflightError, "before checkpoint/workspace"):
                startup_storage_preflight(
                    run_root, root, 100, workspace_required=False,
                    disk_usage=lambda _path: DiskUsage(10 * MIB, 0, 10 * MIB),
                )
            self.assertFalse(os.path.exists(os.path.join(root, "not-created")))

    def test_invalid_threshold_fails_closed(self):
        with tempfile.TemporaryDirectory() as root:
            for value in (0, -1, float("nan"), "invalid"):
                with self.assertRaisesRegex(StoragePreflightError, "finite positive"):
                    startup_storage_preflight(
                        os.path.join(root, "run"), root, value,
                        workspace_required=False,
                    )


if __name__ == "__main__":
    unittest.main()
