# File Transfer / Data Product tracking (exploration, not wired to the Grafana pipeline)

Status as of 2026-09-15. Branch: `hermes-yamcs/file-transfer-exploration`, stacked on top of
`hermes-yamcs/numeric-id-telemetry-resolution`.

## What this branch is, and what it isn't

This is **not** part of the `!binary` telemetry work that ended up feeding Grafana. It explores
a completely different delivery mechanism: **file downlink / CFDP data products** — the "Option
1" from the original conversation with Andrei (as opposed to "Option 2", chunked/binary
telemetry, which is what actually got built out and demoed).

Concretely, this branch gives Hermes visibility into F Prime's file downlink system (the
"Downlink" pane): which files have finished downlinking, which are still in progress, and
whether a data product (`.fdp`) file's container header can be parsed for metadata. None of it
is required for telemetry (including `!binary` blobs) to reach Grafana — that path only needs
`hermes-yamcs/numeric-id-telemetry-resolution`.

It's kept here, not discarded, because the work is real, tested against live data, and directly
relevant if the file-downlink approach to large/binary data ever gets picked back up.

## Why it's a separate branch from the numeric-id fix

`main`'s `hermes-yamcs` (already merged, PR #43) can't actually hold a real YAMCS telemetry
subscription open past the first message — see `numeric-id-telemetry-resolution`'s own note for
why. That fix is genuinely load-bearing for *any* telemetry, so it's isolated on its own branch.
Everything in *this* branch is downstream of that fix only by chronological accident (it was
written after, in the same working session) — it doesn't depend on it functionally. It's
sequenced after it here just because that's the order it was originally written in, so cherry-picking
was clean; there's no reason it couldn't be rebased to sit directly on `main` instead if the
numeric-id fix lands separately.

## Commits, in order

1. **Add bucket access to yamcs-http client** — `YamcsClient::list_buckets`, `list_objects`,
   `get_object`. Bucket object names can contain `/` and a literal leading `./` (confirmed
   against real F Prime-produced names, e.g. `./DpCat/Dp_268521472_1788986684_00693201.fdp`), so
   they're percent-encoded as a single path segment rather than interpolated raw. **Verified
   live**: fetched a real object through this exact encoding against a running YAMCS instance
   and got back the correct byte-for-byte content.

2. **File transfer state RPCs, bucket→FileDownlink mapping, interval tracking, packet
   header parsing (pure logic)** — three independent pieces in `file_transfer.rs`:
   - `bucket_object_to_file_downlink`: a YAMCS bucket object only carries name/size/created-time,
     so this maps what's available into a `FileDownlink`. Per F8 (the original design doc), YAMCS
     only ever writes a bucket object once its CFDP checksum passes, so an object's mere
     *presence* means the transfer completed — there's no way to see a failed transfer here.
   - `IntervalTracker`: tracks which byte ranges of an in-flight transfer have arrived, without
     keeping the payload itself (deliberately — reconstructing file content isn't a goal here).
     Coalesces adjacent ranges, tracks duplicate bytes, computes gaps against a known total size.
   - `parse_file_packet`: parses the raw CCSDS/F Prime file-packet header (START/DATA/END/CANCEL).
     **Verified against real captured traffic**: every field this parses matches, byte-for-byte,
     what an independent scratch script (`catch.py`, written earlier against real downlink
     traffic) does with the same bytes.

3. **Raw packet subscription in yamcs-http** — `YamcsClient::subscribe_packets`, the client-side
   plumbing `file_transfer.rs` needs to actually receive file packets live, rather than just
   parse bytes handed to it in a test.

4. **Wire packet-based interval tracking into the bridge** — `TransferTracker`: the stateful
   layer that actually calls `IntervalTracker`/`parse_file_packet` as real packets arrive,
   keyed by `(instance, destination_path)`. Reports in-progress transfers (no END seen yet) and,
   after a grace period past END with no matching bucket object, `DOWNLINK_PARTIAL` with the
   exact gaps observed — which is a capability YAMCS itself doesn't have (per F8, YAMCS silently
   discards a failed transfer's partial data; this is the only place that failure becomes
   observable at all).

   **Found and fixed here** (not present in the original commit, added during review): a DATA/END
   packet was matched against "the first in-flight transfer with no END yet" across *all*
   instances, not scoped to the instance the packet actually came from. Invisible with one YAMCS
   instance (today's only real setup); with two instances each mid-transfer at once, a packet
   from instance A could silently update instance B's progress. Fixed with a regression test
   (`transfer_tracker_data_is_scoped_to_its_own_instance`).

5. **Data product container parsing** — `dp_container.rs`: parses the `.fdp` container header
   (F Prime data products' on-disk format — container id, priority, timestamp, transmission
   state, record size) and surfaces it as `FileDownlink.metadata` (`dp.containerId`, `dp.time`,
   etc.) for any bucket object ending in `.fdp`. **Independently verified**: downloaded a real
   `.fdp` object from a live YAMCS bucket and parsed it by hand in Python, from scratch, with no
   reference to this Rust code at all — every field matched exactly (container id
   `0x10015000`/`268521472`, timestamp, 1212-byte record area, 1277-byte total length).

   **Found and fixed here**: the original code fetched and fully downloaded every `.fdp` object's
   *entire* content on every 2-second poll, forever, just to read a 61-byte header. Harmless at
   today's demo scale (a couple of small files) but would become real, unbounded bandwidth/CPU
   waste with more or larger accumulated data products in a long-running deployment. Fixed by
   caching parsed metadata by `(instance, bucket, object name, size)` — a completed object's
   bytes never change, so it's now fetched at most once per object, ever, instead of once per
   poll forever.

6. **Split hermes-yamcs into lib+bin so unit tests run under CI's actual gate** — infrastructure
   fix, no behavior change. CI's `build-rust-crates.yml` only runs `cargo test --workspace
   --all-features --lib`, and `--lib` never exercises a bin-only crate's `#[cfg(test)]` modules.
   Every test added across this whole branch (and the numeric-id branch below it) was silently
   invisible to CI until this commit added `src/lib.rs` and made `main.rs` a thin binary over it.
   **Verified**: ran the exact CI command locally and confirmed `hermes_yamcs`'s own test binary
   now appears and all tests run under it. Sequenced last because it's the first point at which
   all the files it touches (`dp_container.rs` included) actually exist.

7. **Fix Bucket/BucketObject 'size' JSON deserialization** — YAMCS renders 64-bit integers as
   JSON strings, but `size`/`maxSize` expected bare numbers, so `list_objects` silently failed
   and looked exactly like "bucket not configured" (a swallowed debug log). Also fixes the real
   field being `maxObjects`, not `maxNumObjects`. Found by pointing the bridge at a live YAMCS
   5.12.0 server; tests use response bodies captured verbatim from it.

8. **Fix two transfer-reporting bugs found by watching a real lossy downlink** — found by
   actually sending a 25 MB file over a lossy link, not reachable from unit tests alone:
   - A transfer that lost its END packet (just another packet, can be dropped like any other)
     never left the in-progress list and hung in the UI forever. Fixed with a stall timeout:
     no packets for 10s finalizes the transfer as `DOWNLINK_PARTIAL` using whatever gaps were
     observed.
   - Partial transfers were reported once and then vanished, since `FileTransferState` is a
     full snapshot the client replaces wholesale every poll. Now retained and included in every
     later snapshot, clearable via `ClearDownlinkTransferState` (previously a no-op).

9. **Fix nonsensical downlink timestamps in the UI** — the client renders a missing timestamp
   as "now", so leaving `time_start` empty made bucket-derived files show an ever-increasing
   start time against a fixed end time (duration going more negative every refresh), and made
   partial transfers show a duration of exactly 0 (both timestamps empty, so they're equal).
   Packet-tracked transfers now report the real observed start/last-packet times; bucket-derived
   ones report `start == end` (an honest "unknown duration" instead of a misleading one).

## Test coverage

22 unit tests in `hermes-yamcs` (file transfer, interval tracking, packet parsing, DP container
parsing), 13 in `yamcs-http` (bucket types, packet subscription types). All independently
verified against real captured/live data where it mattered, not just internal consistency — see
above for specifics.

## If this gets picked back up later

- The cross-instance and full-file-redownload bugs above are fixed, but this branch has not been
  reviewed as carefully as the numeric-id branch for *design* (only correctness) — e.g. the
  "attribute a DATA packet to the only in-flight transfer on this instance" simplification
  (documented in `TransferTracker`'s own comments) would need real work to support genuinely
  concurrent transfers on the same instance.
- `sub_file_downlink`/`sub_file_transfer`/`get_file_transfer_state` each independently re-poll
  YAMCS (instance list + bucket listing) every 2 seconds with no sharing across concurrent
  subscribers — fine for a handful of Hermes clients, would need work to scale further.
- None of this has been tested with the VS Code extension's actual Downlink pane rendering it —
  only verified at the gRPC/data-model level.
