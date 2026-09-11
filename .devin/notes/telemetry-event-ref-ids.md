# Stable TelemetryRef/EventRef ids in hermes-yamcs

Status as of 2026-09-11.

## Bug

`crates/hermes-yamcs/src/convert.rs`:
- `TelemetryRef.id` was hardcoded to `0` in `yamcs_param_to_hermes`. Downstream SQL recorders
  (`pkg/timescaledb`, `cmd/sqlrecord`) key `telemetryDefs` by this id, so every channel collapsed
  into one row — discovered because Grafana/TimescaleDB only ever showed one channel
  (`CCSDS_Packet_ID`) no matter how many distinct F Prime channels were actually streaming.
- `EventRef.id` used `yamcs_event.seq_number` (a per-occurrence counter) instead of a stable
  per-definition id — same class of bug, opposite symptom (churns a new `eventDefs` row per
  event instead of collapsing).

## Fix

Added `stable_id()`: FNV-1a hash of the ref's qualified name (`param_name` for telemetry,
`source/event_type` for events), masked to 31 bits. Deterministic across restarts (std's
`DefaultHasher` is not — its keys are randomized per process). Used for both `TelemetryRef.id`
and `EventRef.id`.

Branch: `fix/stable-telemetry-event-ref-ids`, stacked on top of
`feature/binary-element-type-numeric-kind` (not yet split into its own PR — same as that
branch's status, see `binary-blob-telemetry.md`). 1 new unit test (`stable_id_is_deterministic_and_distinguishes_distinct_keys`),
29/29 pass.

## Second bug found while verifying this live: `cmd/sqlrecord` FK ordering

Once defs actually varied per channel, inserts started failing:
`Key (telemetrydefid)=(...) is not present in table "telemetrydefs"`. Cause:
`SQLTx.Commit()` (`cmd/sqlrecord/sqlhelper.go`) inserted from `tx.inserts`, a Go map — iteration
order is randomized, so `telemetry`/`events` sometimes got inserted before their
`telemetryDefs`/`eventDefs` row in the same transaction, violating the FK. Invisible before
because the old `id=0` bug meant the def row usually already existed from a prior commit.

Fix: `insertOrder` in `Commit()` inserts `telemetryDefs`/`eventDefs` before
`telemetry`/`events`, any other tables after. Verified live: truncated all 4 tables, restarted
`sqlrecord`, 43 distinct channels now land correctly in `telemetryDefs` with no FK errors.

## Follow-ups

- Two separate bugs, two separate repos-worth of concern (hermes-yamcs Rust vs. sqlrecord Go) —
  split into two PRs when organizing the stack. Both independent of the `!binary` work.
- Event-side fix (`EventRef.id`) verified only by inspection + unit test, not a live EVR.
