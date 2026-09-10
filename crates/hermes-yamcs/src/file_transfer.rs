//! File downlink support: bucket listing (Phase 2) and packet-based interval tracking (Phase 3).
//!
//! Per F8, `FprimeFilePacketService` already reassembles APID 3 file packets into a YAMCS
//! bucket (default name `fprimeFilesIn`) and validates them byte-perfect. This module maps
//! bucket objects into Hermes `FileDownlink` records, and separately tracks the byte ranges
//! observed in raw file packets without holding onto their payloads (see `IntervalTracker`).

use hermes_pb::{FileDownlink, FileDownlinkChunk, FileDownlinkCompletionStatus};
use prost_types::Timestamp;

/// Default bucket name used by `FprimeFilePacketService` for downlinked files, per F8.
pub const DEFAULT_DOWNLINK_BUCKET: &str = "fprimeFilesIn";

/// Parse a YAMCS ISO8601 timestamp string into a protobuf `Timestamp`.
///
/// Returns `None` (rather than erroring) on a missing/unparseable time, since bucket objects
/// don't always carry every field YAMCS could in principle report.
pub fn parse_yamcs_timestamp(time_str: &str) -> Option<Timestamp> {
    let dt = chrono::DateTime::parse_from_rfc3339(time_str).ok()?;
    let utc: chrono::DateTime<chrono::Utc> = dt.into();
    Some(Timestamp {
        seconds: utc.timestamp(),
        nanos: utc.timestamp_subsec_nanos() as i32,
    })
}

/// Map a bucket object to a `FileDownlink`.
///
/// A bucket object only carries name, size and created-time — so most `FileDownlink` fields
/// are left at their default (empty/zero). Per F8, YAMCS only ever writes the object once the
/// CFDP checksum has passed, so an object's mere presence means the transfer completed
/// successfully; there is no way to observe a bucket object for a failed transfer.
pub fn bucket_object_to_file_downlink(
    object: &yamcs_http::types::buckets::BucketObject,
    source: &str,
) -> FileDownlink {
    FileDownlink {
        uid: object.name.clone(),
        time_start: None,
        time_end: object
            .created
            .as_deref()
            .and_then(parse_yamcs_timestamp),
        status: FileDownlinkCompletionStatus::DownlinkCompleted as i32,
        source: source.to_string(),
        source_path: String::new(),
        destination_path: object.name.clone(),
        file_path: object.name.clone(),
        missing_chunks: vec![],
        duplicate_chunks: vec![],
        size: object.size.unwrap_or(0),
        metadata: Default::default(),
    }
}

/// Tracks which byte ranges of an in-flight file transfer have arrived, without retaining the
/// payload itself (see plan's "Deferred: partial reconstruction" — this is a deliberate
/// non-goal).
///
/// Intervals are stored sorted and coalesced, so a 200 MB transfer costs an interval list, not
/// a buffer.
#[derive(Debug, Clone, Default)]
pub struct IntervalTracker {
    /// Sorted, non-overlapping, non-adjacent (i.e. coalesced) `[start, end)` intervals.
    intervals: Vec<(u64, u64)>,
    /// Total bytes observed as duplicates (i.e. bytes that fell within an already-received
    /// interval when inserted).
    duplicate_bytes: u64,
}

impl IntervalTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that bytes `[offset, offset+length)` have arrived. Overlap with any
    /// already-received range is counted as duplicate, not double-counted as received.
    pub fn record(&mut self, offset: u64, length: u64) {
        if length == 0 {
            return;
        }
        let new_start = offset;
        let new_end = offset + length;

        // Find overlap with existing intervals to compute duplicate bytes, then merge.
        let mut merged_start = new_start;
        let mut merged_end = new_end;
        let mut duplicate = 0u64;
        let mut result = Vec::with_capacity(self.intervals.len() + 1);

        for &(s, e) in &self.intervals {
            if e < merged_start || s > merged_end {
                // No overlap/adjacency with the new range: keep as-is.
                result.push((s, e));
            } else {
                // Overlapping or adjacent: compute the duplicate overlap against the *new*
                // range before merging.
                let overlap_start = s.max(new_start);
                let overlap_end = e.min(new_end);
                if overlap_start < overlap_end {
                    duplicate += overlap_end - overlap_start;
                }
                merged_start = merged_start.min(s);
                merged_end = merged_end.max(e);
            }
        }
        result.push((merged_start, merged_end));
        result.sort_unstable_by_key(|&(s, _)| s);
        self.intervals = result;
        self.duplicate_bytes += duplicate;
    }

    /// Total number of bytes received (deduplicated).
    pub fn received_bytes(&self) -> u64 {
        self.intervals.iter().map(|&(s, e)| e - s).sum()
    }

    /// Total number of bytes that arrived more than once.
    pub fn duplicate_bytes(&self) -> u64 {
        self.duplicate_bytes
    }

    /// Gaps in `[0, total_size)` not covered by any received interval, as
    /// `FileDownlinkChunk { offset, size }`. Empty if the transfer is complete.
    pub fn gaps(&self, total_size: u64) -> Vec<FileDownlinkChunk> {
        let mut gaps = Vec::new();
        let mut cursor = 0u64;
        for &(s, e) in &self.intervals {
            if s > cursor {
                gaps.push(FileDownlinkChunk {
                    offset: cursor,
                    size: s - cursor,
                });
            }
            cursor = cursor.max(e);
        }
        if cursor < total_size {
            gaps.push(FileDownlinkChunk {
                offset: cursor,
                size: total_size - cursor,
            });
        }
        gaps
    }
}

/// Parsed header of an F Prime file downlink packet (APID 3, descriptor 3).
///
/// Layout from Appendix A / `catch.py`:
/// ```text
/// [0:2]   CCSDS packet id      APID in the low 11 bits (3 = FILE)
/// [6:8]   F Prime descriptor   U16, 3 = FILE
/// [8]     FilePacketType       0=START 1=DATA 2=END 3=CANCEL
/// [9:13]  SequenceIndex        U32
/// [13:]   body
/// ```
/// All integers are big-endian.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilePacket {
    Start {
        sequence_index: u32,
        total_size: u32,
        source_path: String,
        destination_path: String,
    },
    Data {
        sequence_index: u32,
        offset: u32,
        length: u16,
    },
    End {
        sequence_index: u32,
        checksum: u32,
    },
    Cancel {
        sequence_index: u32,
    },
}

/// F Prime file downlink APID, per `ComCfg.fpp`.
pub const FILE_APID: u16 = 3;
/// F Prime file descriptor value carried in bytes `[6:8]` of a file packet.
pub const FILE_DESCRIPTOR: u16 = 3;

/// Parse a raw CCSDS/F Prime file packet.
///
/// Returns `None` if the packet is not a file packet (wrong APID or descriptor) or too short
/// to parse, rather than erroring — non-file packets are common on the same stream and are not
/// a failure.
pub fn parse_file_packet(data: &[u8]) -> Option<FilePacket> {
    if data.len() < 13 {
        return None;
    }
    let apid = u16::from_be_bytes([data[0], data[1]]) & 0x07FF;
    if apid != FILE_APID {
        return None;
    }
    let descriptor = u16::from_be_bytes([data[6], data[7]]);
    if descriptor != FILE_DESCRIPTOR {
        return None;
    }

    let packet_type = data[8];
    let sequence_index = u32::from_be_bytes([data[9], data[10], data[11], data[12]]);
    let body = &data[13..];

    match packet_type {
        0 => {
            // START: size U32, then length-prefixed source and destination strings.
            if body.len() < 5 {
                return None;
            }
            let total_size = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
            let src_len = body[4] as usize;
            if body.len() < 5 + src_len + 1 {
                return None;
            }
            let source_path = String::from_utf8_lossy(&body[5..5 + src_len]).into_owned();
            let dst_len_offset = 5 + src_len;
            let dst_len = body[dst_len_offset] as usize;
            let dst_start = dst_len_offset + 1;
            if body.len() < dst_start + dst_len {
                return None;
            }
            let destination_path =
                String::from_utf8_lossy(&body[dst_start..dst_start + dst_len]).into_owned();
            Some(FilePacket::Start {
                sequence_index,
                total_size,
                source_path,
                destination_path,
            })
        }
        1 => {
            // DATA: offset U32, length U16, then the bytes (which we deliberately never read).
            if body.len() < 6 {
                return None;
            }
            let offset = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
            let length = u16::from_be_bytes([body[4], body[5]]);
            Some(FilePacket::Data {
                sequence_index,
                offset,
                length,
            })
        }
        2 => {
            // END: U32 CFDP modular checksum.
            if body.len() < 4 {
                return None;
            }
            let checksum = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
            Some(FilePacket::End {
                sequence_index,
                checksum,
            })
        }
        3 => Some(FilePacket::Cancel { sequence_index }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(name: &str, size: u64, created: Option<&str>) -> yamcs_http::types::buckets::BucketObject {
        yamcs_http::types::buckets::BucketObject {
            name: name.to_string(),
            size: Some(size),
            created: created.map(|s| s.to_string()),
            content_type: None,
            metadata: Default::default(),
        }
    }

    #[test]
    fn bucket_object_maps_to_completed_downlink() {
        let obj = object(
            "./DpCat/Dp_268521472_1788986684_00693201.fdp",
            1277,
            Some("2026-01-01T00:00:00.000Z"),
        );
        let fd = bucket_object_to_file_downlink(&obj, "myinstance");
        assert_eq!(fd.uid, "./DpCat/Dp_268521472_1788986684_00693201.fdp");
        assert_eq!(fd.size, 1277);
        assert_eq!(fd.source, "myinstance");
        assert_eq!(fd.status, FileDownlinkCompletionStatus::DownlinkCompleted as i32);
        assert!(fd.missing_chunks.is_empty());
        assert!(fd.duplicate_chunks.is_empty());
        assert!(fd.time_end.is_some());
    }

    #[test]
    fn bucket_object_without_created_time_has_no_time_end() {
        let obj = object("pic.jpg", 42, None);
        let fd = bucket_object_to_file_downlink(&obj, "myinstance");
        assert!(fd.time_end.is_none());
    }

    #[test]
    fn interval_tracker_out_of_order_chunks_sum_correctly() {
        let mut tracker = IntervalTracker::new();
        tracker.record(1186, 1186); // second chunk arrives first
        tracker.record(0, 1186); // first chunk arrives second
        assert_eq!(tracker.received_bytes(), 2372);
        assert_eq!(tracker.duplicate_bytes(), 0);
        assert!(tracker.gaps(2372).is_empty());
    }

    #[test]
    fn interval_tracker_duplicate_offset_counts_as_duplicate() {
        let mut tracker = IntervalTracker::new();
        tracker.record(0, 100);
        tracker.record(0, 100); // exact duplicate
        assert_eq!(tracker.received_bytes(), 100);
        assert_eq!(tracker.duplicate_bytes(), 100);
    }

    #[test]
    fn interval_tracker_partial_overlap_counts_only_overlapping_bytes_as_duplicate() {
        let mut tracker = IntervalTracker::new();
        tracker.record(0, 100);
        tracker.record(50, 100); // 50 bytes overlap [50,100), 50 new bytes [100,150)
        assert_eq!(tracker.received_bytes(), 150);
        assert_eq!(tracker.duplicate_bytes(), 50);
    }

    #[test]
    fn interval_tracker_adjacent_intervals_coalesce() {
        let mut tracker = IntervalTracker::new();
        tracker.record(0, 100);
        tracker.record(100, 100);
        // Coalesced into a single [0, 200) interval.
        assert_eq!(tracker.intervals.len(), 1);
        assert_eq!(tracker.intervals[0], (0, 200));
    }

    #[test]
    fn interval_tracker_gaps_are_the_complement_against_total_size() {
        let mut tracker = IntervalTracker::new();
        tracker.record(0, 100);
        tracker.record(200, 100);
        let gaps = tracker.gaps(300);
        assert_eq!(
            gaps,
            vec![FileDownlinkChunk {
                offset: 100,
                size: 100
            }]
        );
    }

    #[test]
    fn interval_tracker_gaps_empty_for_complete_transfer() {
        let mut tracker = IntervalTracker::new();
        tracker.record(0, 300);
        assert!(tracker.gaps(300).is_empty());
    }

    #[test]
    fn parse_file_packet_header_matches_appendix_a_fixture() {
        // Fixture from Appendix A: "0003 c004 0025 0003 01 00000001 000000..."
        // APID 3, seq count 4, CCSDS length 37, descriptor 3, type 1 (DATA), SequenceIndex 1,
        // offset 0, then 25 bytes of data (only the first few of which are given).
        let mut data = vec![
            0x00, 0x03, // CCSDS packet id: APID 3
            0xc0, 0x04, // sequence flags + count
            0x00, 0x25, // CCSDS length 37
            0x00, 0x03, // F Prime descriptor 3
            0x01, // FilePacketType DATA
            0x00, 0x00, 0x00, 0x01, // SequenceIndex 1
            0x00, 0x00, 0x00, 0x00, // offset 0
            0x00, 0x19, // length 25
        ];
        data.extend(std::iter::repeat(0u8).take(25)); // 25 bytes of payload

        let packet = parse_file_packet(&data).expect("should parse as a file packet");
        assert_eq!(
            packet,
            FilePacket::Data {
                sequence_index: 1,
                offset: 0,
                length: 25,
            }
        );
    }

    #[test]
    fn parse_file_packet_rejects_non_file_apid() {
        let mut data = vec![0u8; 20];
        // APID 1 (telemetry), not 3 (file)
        data[0] = 0x00;
        data[1] = 0x01;
        assert_eq!(parse_file_packet(&data), None);
    }

    #[test]
    fn parse_file_packet_start_extracts_size_and_paths() {
        let mut body = Vec::new();
        body.extend_from_slice(&100u32.to_be_bytes()); // total size
        let src = b"/src/path";
        body.push(src.len() as u8);
        body.extend_from_slice(src);
        let dst = b"./dst/path";
        body.push(dst.len() as u8);
        body.extend_from_slice(dst);

        let mut data = vec![
            0x00, 0x03, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        data.extend(body);

        let packet = parse_file_packet(&data).expect("should parse");
        assert_eq!(
            packet,
            FilePacket::Start {
                sequence_index: 0,
                total_size: 100,
                source_path: "/src/path".to_string(),
                destination_path: "./dst/path".to_string(),
            }
        );
    }
}
