# Binary blob telemetry (YAMCS opaque decode + typed reconstruction in Hermes)

Status as of 2026-09-11. This spans three repos; this note is the map between them.

## Goal

Send large binary blobs (chunked telemetry, data-product-style buffers) from F Prime through
YAMCS without YAMCS decoding every element individually, and have Hermes reconstruct a typed
value (not just raw opaque bytes) on the other end.

## Repos and branches

| Repo | Local path | Branch | Status |
|---|---|---|---|
| `fprime-community/fprime-xtce` | `~/Documents/code/fprime-xtce` | `feature/binary-annotation` | Ready to push/PR. Squashed, single commit on top of upstream `main`. |
| `fprime-community/fprime-xtce` | `~/Documents/code/fprime-xtce` | `feature/binary-element-type` | Ready to push/PR. Stacked on top of `feature/binary-annotation`; squashed, single commit. |
| `hermes` (this repo) | `~/Documents/code/hermes` | `feature/binary-element-type-numeric-kind` | Functional, tested, **not yet split into a reviewable PR stack** - see Follow-ups. |
| `fprime/big-data` (demo/test project) | `~/Documents/code/fprime/big-data` | working tree, uncommitted | `BigData/Types/BigDataTypes.fpp`: `MapChunk.data` tagged `@< !binary`. This is the project used to test the whole pipeline live; not a real deliverable, just the harness. |

`fprime-community/fprime-xtce` is the **real, public, actively-developed** upstream (confirmed
via `git clone` + GitHub API). An earlier assumption that it lived at
`open-source-space-foundation/fprime-to-xtce` was wrong - that org/repo doesn't exist
(404 even via the API). `fprime-yamcs` (the separate PyPI package used by the `big-data` demo
project) needs **no changes** - it only shells out to the `fprime-to-xtce` CLI as a subprocess.

## What's implemented

### 1. `fprime-xtce` / `feature/binary-annotation`

A `!binary` first-line marker in an F Prime doc-comment annotation, on either a top-level named
`array` type or an inline array struct member (e.g. `data: [256] U8 @< !binary`), makes
`convert_array_definition` emit an XTCE `BinaryParameterType` instead of `ArrayParameterType` -
so YAMCS (and the wire) get one opaque blob instead of N decoded elements.

Also fixes a real, independent bug found along the way: inline array struct members with a
`"size"` key were silently collapsed to a scalar by `convert_struct_definition` before this
work (confirmed also present in F Prime's own bundled `Ref` deployment). That specific bug was
*separately* already fixed upstream, unreleased, in commit `a5bbd3b` (co-authored by a prior
Devin session) - discovered partway through this work. `feature/binary-annotation` is based on
top of that fix, not a re-implementation of it.

18 unit tests (upstream's existing 12 + our 5, some since extended by the next branch), plus
manual end-to-end verification: live YAMCS instance, real command trigger, MDB REST API
(`engType: "binary"`), and `hermes-yamcs` (unmodified, pre-existing `Value::Binary` handling)
correctly decoding it as a `BytesValue`.

### 2. `fprime-xtce` / `feature/binary-element-type` (stacked on `feature/binary-annotation`)

Widens `!binary` from U8/I8-only to any numeric primitive (any integer or float size - matches
Hermes's `NumberKind` exactly: U8/I8/U16/I16/U32/I32/U64/I64/F32/F64). Tags the resulting
`BinaryParameterType` with an XTCE `AliasSet` / `Alias` (`nameSpace="fprime:elementType"`,
`alias="<TypeName>"`) naming the real element type.

**Why `AliasSet`, not `AncillaryDataSet`:** `AncillaryDataType` has *mixed content* (an
attribute + a text body), which `fprime_xtce/xtce.py`'s generic dict-to-XML serializer
(`recurse_xml_dictionary`) does not support today - confirmed by hand-testing it, not assumed.
Extending that shared serializer for one feature felt disproportionate. `AliasType` is
attribute-only (`nameSpace`, `alias`, no text), identical in shape to things already emitted
(e.g. `EnumerationList`'s `Enumeration` entries), so it needs **zero serializer changes** - and
it's a legitimate, spec-intended use of the construct ("alternate name/ID for this item, in a
namespace"), not a repurposing. Verified against a live YAMCS instance that `AliasSet` (like
`AncillaryDataSet`, also checked) round-trips through the REST MDB API
(`GET /api/mdb/{instance}/parameters/{name}` returns `type.member[].alias`).

18 unit tests, schema-validated (`xmlschema` against the real XTCE XSD - same 2 pre-existing,
unrelated `TypeAlias` errors as baseline, no new ones), and manually verified end-to-end against
the live `big-data` project + YAMCS instance.

### 3. `hermes` / `feature/binary-element-type-numeric-kind`

`hermes-yamcs`'s `convert.rs` previously hardcoded every YAMCS `Value::Binary` to
`NumberKind::NumberU8`. Now:

- `BinaryTypeHints` (in `convert.rs`) captures a resolved `NumberKind` for a value itself and,
  one level deep, for members of an aggregate.
- `binary_type_hints_for_parameter` builds one from a `yamcs_http::types::mdb::Parameter`,
  reading the `fprime:elementType` alias off the parameter's type and (for an aggregate) each
  member's type - all fields already existed in `yamcs-http`'s MDB types, no changes needed
  there.
- `yamcs_value_to_hermes` now takes `Option<&BinaryTypeHints>` and uses it for `Binary` values
  and to look up per-member hints when recursing into an `Aggregate`.
- `YamcsApiService` gained a lazily-populated cache (`binary_type_hints`, keyed by
  `"<instance><qualified name>"`) so each parameter's MDB definition is fetched via the existing
  `get_parameter` client method at most once, not on every telemetry update. Wired into the
  `sub_telemetry` subscription loop.
- Switched `BytesValue.big_endian` from `false` to `true`, matching F Prime's
  `mostSignificantByteFirst` encoding (`IntegerDataEncoding`/`FloatDataEncoding` in
  `fprime-xtce`). Harmless for the previously-only-supported U8 case; required for anything
  multi-byte.

7 new unit tests in `convert.rs` (`cargo test -p hermes-yamcs --lib`: 28/28 pass). Verified live:
rebuilt `hermes-yamcs`, pointed it at a `big-data` YAMCS instance running the
`feature/binary-element-type` dictionary, confirmed via `grpcurl` that the live `MapStream.data`
value now carries `bigEndian: true` (proving the new code path is live) with the kind matching
the `U8` alias tag independently confirmed via the MDB REST API. The F32/aggregate-member cases
are covered by the unit tests directly (didn't add a live F32 channel to `big-data` just to
re-demonstrate what the unit tests already prove).

### 4. `hermes` / `cmd/sqlrecord` (not yet on its own branch — currently on
`fix/stable-telemetry-event-ref-ids`, needs to move)

`sqlrecord` was dumping every `BytesValue` as an opaque `bytes` blob, discarding the `Kind`
`hermes-yamcs` had already resolved (see above) — so a `!binary` numeric array (e.g.
`FloatSamplesTlm`, F32) reached TimescaleDB as one unplottable hex blob per point, not a chart.

Fix in `insertValue` (`cmd/sqlrecord/sqlhelper.go`): `Value_R` with kind U8/I8 (the opaque-blob
fallback) still stores as raw bytes; any other kind decodes via `pb.ValueToAny` and expands into
one row per element (`value[0]`, `value[1]`, ...), same pattern as `Value_A`. Verified live:
`SEND_FLOAT_SAMPLES(seed=42)` now lands as `value[0]=42, value[1]=43, ...` (real `float` rows),
not hex.

## Follow-ups / open items

- **hermes-yamcs PR stack**: not yet split up for review. Probably: (1) the `yamcs-http`
  MDB-fetch-and-cache plumbing / `BinaryTypeHints` data model, (2) the `NumberKind` resolution
  + `big_endian` fix, as a reasonable two-way split - but revisit once actually organizing this.
- **Nested aggregates**: `BinaryTypeHints` only resolves one level (a parameter or its direct
  members). A member that's itself an aggregate with its own `!binary` members won't get hints.
  Not needed for the current use case (`MapChunk.data` is a direct member); would need
  `binary_type_hints_for_parameter` to recurse if that changes.
- **Cache staleness**: `YamcsApiService::binary_type_hints` is fetched once per parameter, ever,
  for the lifetime of the service. Fine for how F Prime dictionaries actually change (a
  redeploy restarts everything anyway), but worth a comment/ticket if `hermes-yamcs` ever needs
  to survive a live MDB reload.
- **`fprime-xtce` upstream PRs**: `feature/binary-annotation` and `feature/binary-element-type`
  are ready to push and open as (stacked) PRs against `fprime-community/fprime-xtce`. Not yet
  pushed - repo access/PR etiquette (who reviews, Andrei's involvement as an existing
  contributor) hasn't been confirmed.
- **Chunking**: deliberately out of scope throughout this work (current test data is a single
  `!binary` blob under ~1KB). The wider chunked-telemetry design discussion (this feature vs.
  the DP/file-downlink path vs. a custom APID) is tracked separately, not in this note.
- **`big-data`'s `MapChunk.data` tag**: currently uncommitted in the `big-data` working tree
  (it's a demo/test project, not something we're shipping) - just flagging so it isn't lost if
  that tree gets reset.

## Key files

- `fprime-xtce`: `src/fprime_xtce/type_converter.py` (`convert_array_definition`,
  `convert_struct_definition`), `src/fprime_xtce/utilities.py` (`extract_binary_marker`,
  `BINARY_ANNOTATION_MARKER`, `BINARY_ELEMENT_TYPE_ALIAS_NAMESPACE`), `tests/test.py`
  (`TestBinaryAnnotation`).
- `hermes`: `crates/hermes-yamcs/src/convert.rs` (`BinaryTypeHints`,
  `binary_type_hints_for_parameter`, `numeric_kind_from_fprime_name`, `yamcs_value_to_hermes`),
  `crates/hermes-yamcs/src/service.rs` (`binary_type_hints` cache, `resolve_binary_type_hints`).
