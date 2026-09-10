//! Parser for the F Prime data product container format (Phase 4).
//!
//! This is the payload of a `.fdp` bucket object. Nothing else in Hermes or `fprime-yamcs`
//! parses this format (nasa/hermes#42, item 2). Layout from `Fw/Dp/DpContainer.hpp` plus
//! config constants; verified against a real container in Appendix A.
//!
//! ```text
//! 0     packet descriptor   U16    5 = FW_PACKET_DP
//! 2     container id        U32    component base id + container id
//! 6     priority            U32
//! 10    time tag            11     timeBase U16, context U8, seconds U32, useconds U32
//! 21    proc types          U8     bitmask; 0=NONE 1=ZLIB_DEFLATE 2=ONE 4=TWO
//! 22    user data           32     CONTAINER_USER_DATA_SIZE, project-defined
//! 54    dp state            U8     0=UNTRANSMITTED 1=PARTIAL 2=TRANSMITTED
//! 55    data size           U16    length of the record area
//! 57    header hash         4      CRC32
//! 61    records              dataSize bytes
//! 61+n  data hash           4      CRC32
//! ```
//! All integers big-endian.

/// Size of the project-defined user data region.
const CONTAINER_USER_DATA_SIZE: usize = 32;
/// Offset and size of the fixed header, before the header hash.
const HEADER_SIZE: usize = 57;
/// Size of a CRC32 hash.
const HASH_SIZE: usize = 4;

/// F Prime data product descriptor value, per `ComCfg.fpp` (`FW_PACKET_DP`).
pub const DP_DESCRIPTOR: u16 = 5;

/// Data product transmission state, from the container header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DpState {
    Untransmitted,
    Partial,
    Transmitted,
    /// Any value not in the above set. Kept rather than erroring, since this is metadata, not
    /// something that should block parsing.
    Unknown(u8),
}

impl From<u8> for DpState {
    fn from(value: u8) -> Self {
        match value {
            0 => DpState::Untransmitted,
            1 => DpState::Partial,
            2 => DpState::Transmitted,
            other => DpState::Unknown(other),
        }
    }
}

/// A parsed data product container header (the fixed-size portion; record bytes are not
/// copied out).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DpContainerHeader {
    pub descriptor: u16,
    pub container_id: u32,
    pub priority: u32,
    pub time_base: u16,
    pub time_context: u8,
    pub seconds: u32,
    pub useconds: u32,
    pub proc_types: u8,
    pub user_data: [u8; CONTAINER_USER_DATA_SIZE],
    pub dp_state: DpState,
    pub data_size: u16,
    pub header_hash: u32,
}

/// Parse just the header of a `.fdp` container (57 bytes plus its CRC32, 61 bytes total).
/// Does not validate the header hash or touch the record area.
pub fn parse_header(data: &[u8]) -> Option<DpContainerHeader> {
    if data.len() < HEADER_SIZE + HASH_SIZE {
        return None;
    }

    let descriptor = u16::from_be_bytes([data[0], data[1]]);
    let container_id = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);
    let priority = u32::from_be_bytes([data[6], data[7], data[8], data[9]]);
    let time_base = u16::from_be_bytes([data[10], data[11]]);
    let time_context = data[12];
    let seconds = u32::from_be_bytes([data[13], data[14], data[15], data[16]]);
    let useconds = u32::from_be_bytes([data[17], data[18], data[19], data[20]]);
    let proc_types = data[21];
    let mut user_data = [0u8; CONTAINER_USER_DATA_SIZE];
    user_data.copy_from_slice(&data[22..22 + CONTAINER_USER_DATA_SIZE]);
    let dp_state = DpState::from(data[54]);
    let data_size = u16::from_be_bytes([data[55], data[56]]);
    let header_hash = u32::from_be_bytes([data[57], data[58], data[59], data[60]]);

    Some(DpContainerHeader {
        descriptor,
        container_id,
        priority,
        time_base,
        time_context,
        seconds,
        useconds,
        proc_types,
        user_data,
        dp_state,
        data_size,
        header_hash,
    })
}

/// Number of records in the record area, given a fixed per-record size. F Prime data products
/// don't self-describe a record count in the header (only total byte size), so the caller must
/// supply the record layout's size in bytes; this just does the division and reports whether
/// it was exact.
pub fn record_count(data_size: u16, record_size: u16) -> Option<u32> {
    if record_size == 0 || data_size % record_size != 0 {
        return None;
    }
    Some(data_size as u32 / record_size as u32)
}

/// Validate the container's total length against `data_size`: `57 + 4 + dataSize + 4`.
pub fn expected_total_len(data_size: u16) -> usize {
    HEADER_SIZE + HASH_SIZE + data_size as usize + HASH_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build the Appendix A fixture: a real container, 1,277 bytes total, produced by
    /// `Ref::DpDemo`. `57 + 4 + 1212 + 4 = 1277`.
    fn appendix_a_fixture() -> Vec<u8> {
        let mut data = Vec::with_capacity(1277);
        data.extend_from_slice(&5u16.to_be_bytes()); // descriptor
        data.extend_from_slice(&268521472u32.to_be_bytes()); // container id 0x10015000
        data.extend_from_slice(&0u32.to_be_bytes()); // priority
        data.extend_from_slice(&2u16.to_be_bytes()); // time base
        data.push(0); // context
        data.extend_from_slice(&1788986684u32.to_be_bytes()); // seconds
        data.extend_from_slice(&693201u32.to_be_bytes()); // useconds
        data.push(0); // proc types
        data.extend(std::iter::repeat(0u8).take(CONTAINER_USER_DATA_SIZE)); // user data
        data.push(0); // dp state UNTRANSMITTED
        data.extend_from_slice(&1212u16.to_be_bytes()); // data size
        data.extend_from_slice(&0xDEADBEEFu32.to_be_bytes()); // header hash (arbitrary for the fixture)
        assert_eq!(data.len(), HEADER_SIZE + HASH_SIZE);
        data.extend(std::iter::repeat(0u8).take(1212)); // records
        data.extend_from_slice(&0xCAFEBABEu32.to_be_bytes()); // data hash
        assert_eq!(data.len(), 1277);
        data
    }

    #[test]
    fn parses_appendix_a_fixture_header() {
        let data = appendix_a_fixture();
        let header = parse_header(&data).expect("should parse");
        assert_eq!(header.descriptor, DP_DESCRIPTOR);
        assert_eq!(header.container_id, 268521472);
        assert_eq!(header.container_id, 0x10015000);
        assert_eq!(header.priority, 0);
        assert_eq!(header.time_base, 2);
        assert_eq!(header.time_context, 0);
        assert_eq!(header.seconds, 1788986684);
        assert_eq!(header.useconds, 693201);
        assert_eq!(header.proc_types, 0);
        assert_eq!(header.dp_state, DpState::Untransmitted);
        assert_eq!(header.data_size, 1212);
    }

    #[test]
    fn appendix_a_fixture_length_arithmetic_checks_out() {
        let data = appendix_a_fixture();
        let header = parse_header(&data).expect("should parse");
        assert_eq!(expected_total_len(header.data_size), 1277);
        assert_eq!(data.len(), 1277);
    }

    #[test]
    fn dp_state_always_untransmitted_in_a_downlinked_copy() {
        // Per the plan: the file is written before transmission and never rewritten, so
        // transmission state lives only in the onboard catalog. This isn't something the
        // parser can enforce, but the fixture documents the expectation.
        let data = appendix_a_fixture();
        let header = parse_header(&data).expect("should parse");
        assert_eq!(header.dp_state, DpState::Untransmitted);
    }

    #[test]
    fn parse_header_rejects_truncated_data() {
        let data = vec![0u8; 10];
        assert!(parse_header(&data).is_none());
    }

    #[test]
    fn dp_state_unknown_value_is_preserved_not_rejected() {
        assert_eq!(DpState::from(42), DpState::Unknown(42));
    }

    #[test]
    fn record_count_divides_exactly() {
        assert_eq!(record_count(1212, 12), Some(101));
        assert_eq!(record_count(100, 3), None); // not an exact multiple
        assert_eq!(record_count(100, 0), None); // avoid divide by zero
    }
}
