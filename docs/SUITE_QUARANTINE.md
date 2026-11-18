# Working-copy isolation and evidence recycling

Service startup and each working-copy list refresh check isolated or unsafe suites.
Checks read existing records and `/proc`; they never launch tools, send Stop or
signals, change view permissions, or remove/move OA locks. The suite lease is
opened without creation and checked with a nonblocking exclusive advisory lock.

A successful check requires:

- Native guardian and session evidence proves shutdown, with recorded PID/start
  ticks and boot identity checked against the current process. Reused or unreadable
  identities remain isolated, including when an older terminal receipt exists.
- Capture roots and existing phase/session directories must be real directories
  with canonical paths. Symlinks, dangling links and non-directories keep native
  shutdown unproven and retain every live view during recycling.
- Every registered view has the exact ownership marker and expected library path.
- No `.cdslck` variant or `.oa-lock` exists in those view trees.
- No active suite lease or pending creation/refresh/restore transaction remains.

Success writes `suite_verification.json` (versioned schema, timestamp, owner,
check evidence and original quarantine record), publishes a released metadata
transaction, then renames the original `suite_quarantine.json` to a unique
`.verified-*` archive. Repeated checks leave the receipt unchanged. Existing v1
quarantine records require no migration. A partial ordinary deletion keeps its
existing `delete_incomplete` retry protocol.

`blocked_reasons` is an array of `{code, message, ...evidence}` with English
messages. The shared suite manager displays it in both GUI presentations.
Principal codes are `native_process_alive`, `native_identity_unproven`,
`native_session_unproven`, `oa_lock_held`, `lease_active`, `lease_unproven`,
`marker_mismatch`, and `recovery_pending`. Damaged ownership/registry data cannot
license deletion and is reported as unavailable with an explicit reason.

An isolated suite allows evidence-preserving Delete. A separately serialized
administrative transaction creates
`<storage-root>/quarantine_recycle/<timestamp>_<suite>_<nonce>/`. Its `records/`
contains validated metadata snapshots and `receipt.json` contains all checks,
per-view actions/destinations/errors, and moved/retained lists. Exact owned,
unlocked views are renamed into this directory, preserving their bytes and modes.
Cross-device paths remain in place; there is no copy-and-delete fallback. Locked
views remain at their original path. An active lease or unproven native writer
retains all views. The GUI returns the recycle receipt even when live views remain;
`cleanup_proven` stays false in that case.

The original registry root remains as a tombstone (`suite_recycle.json`), rather
than being relocated under an active controller or invalidating capture references.
It retains the complete original snapshot/history evidence; the archived metadata
is sufficient to inspect the recycled membership. Recycled suites leave the active
list, appear among deleted records, and cannot be reopened. Native/lease holders
are never evicted. The receipt journals device/inode identity before each rename;
service startup, list refresh and repeated Delete resolve an interrupted rename without touching another view and explicitly
retains members not yet visited. Completed receipts are left byte-for-byte unchanged;
repeated Delete returns the existing receipt without granting load authority. Neither recycling nor verification purges evidence.

The force-exit 0.01-second per-suite release budget stays unchanged. Extending it
would accumulate across suites and still cannot guarantee native shutdown. The
subsequent service/list check supplies the recovery path without adding exit work.
Unknown native or remote terminal evidence continues to require its original owner.
