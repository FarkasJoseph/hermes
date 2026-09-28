//! The same recorded responses as `offline.rs`, decoded as protobuf instead of json.
//!
//! YAMCS serves either encoding of the same message, so each `.pb` file is what the server sent
//! for the identical request its `.json` twin came from. Re-record them with the commands in
//! `offline.rs`, adding `-H 'Accept: application/protobuf'`.
//!
//! The two `ws_subscribe_*.pb` files are the inner payload of the websocket Any envelope, taken
//! from a connection that offered the `protobuf` subprotocol at handshake. Re-record a `.json`
//! and its `.pb` together, or they will drift apart.

use prost::Message;
use yamcs_http::pb::yamcs::protobuf::{
    events::ListEventsResponse,
    mdb::ListParametersResponse,
    processing::{ListProcessorsResponse, SubscribeParametersData},
    pvalue::ParameterValue,
    server::GetServerInfoResponse,
    value::Type,
};

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("Failed to read {path}: {e}"))
}

fn fixture_str(name: &str) -> String {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("Failed to read {path}: {e}"))
}

#[test]
fn test_parameters_page_decodes() {
    let page = ListParametersResponse::decode(&fixture("mdb_parameters_page.pb")[..])
        .expect("Failed to decode parameters page");

    assert_eq!(page.parameters.len(), 3);
    assert!(
        page.continuation_token.is_some(),
        "a partial page carries a token"
    );
    assert!(
        page.total_size.unwrap_or(0) > 3,
        "total_size spans every page"
    );
}

#[test]
fn test_systems_page_has_no_parameters() {
    let page = ListParametersResponse::decode(&fixture("mdb_parameters_systems.pb")[..])
        .expect("Failed to decode systems page");

    assert_eq!(page.parameters.len(), 0);
    assert_eq!(page.systems.len(), 3);
}

/// The json path cannot read this message. The hand-written `ParameterValue` requires `numericId`,
/// which only the websocket sends, so the whole message fails even though the 64-bit value inside
/// it parses on its own.
#[test]
fn test_sint64_value_needs_no_string_parsing() {
    let pv = ParameterValue::decode(&fixture("parameter_value_64bit.pb")[..])
        .expect("Failed to decode parameter value");

    let eng = pv.eng_value.expect("Failed to find eng value");
    assert_eq!(Type::try_from(eng.r#type), Ok(Type::Sint64));
    assert!(eng.sint64_value.unwrap_or(0) > 0);
    assert!(
        pv.numeric_id.is_none(),
        "http responses carry no numeric id"
    );
}

#[test]
fn test_first_frame_carries_name_mapping() {
    let frame = SubscribeParametersData::decode(&fixture("ws_subscribe_first.pb")[..])
        .expect("Failed to decode first frame");

    assert_eq!(
        frame.mapping.len(),
        3,
        "names arrive only on the first frame"
    );
    assert!(frame.values[0].numeric_id.is_some());
}

/// Every reading in an update frame is lost on the json path, because `id` is required there
/// and YAMCS stops sending it once the mapping is known.
#[test]
fn test_update_frame_keyed_by_numeric_id() {
    let frame = SubscribeParametersData::decode(&fixture("ws_subscribe_update.pb")[..])
        .expect("Failed to decode update frame");

    let value = &frame.values[0];
    assert!(value.numeric_id.is_some(), "value is keyed by numeric id");
    assert!(value.id.is_none(), "yamcs omits the name once it can");
    assert!(
        frame.mapping.is_empty(),
        "names arrive only on the first frame"
    );
}

/// `event` is deprecated in the proto, but YAMCS still sends both fields.
#[test]
#[allow(deprecated)]
fn test_event_response_carries_event_and_events() {
    let list = ListEventsResponse::decode(&fixture("archive_events.pb")[..])
        .expect("Failed to decode event list");

    assert!(!list.event.is_empty(), "deprecated field still populated");
    assert_eq!(list.event.len(), list.events.len());
}

#[test]
fn test_processor_list_decodes() {
    let page = ListProcessorsResponse::decode(&fixture("processors.pb")[..])
        .expect("Failed to decode processor list");

    assert!(!page.processors.is_empty());
}

#[test]
fn test_server_info_decodes() {
    let info = GetServerInfoResponse::decode(&fixture("server_info.pb")[..])
        .expect("Failed to decode server info");

    assert!(!info.yamcs_version.unwrap_or_default().is_empty());
}

/// Enforces what the two websocket tests above rely on. The hand-written `ParameterValue` marks
/// ten fields as required, and YAMCS sends a different subset per message: a websocket frame
/// omits `id`, `monitoringResult`, `alarmRange` and `expireMillis`, while an http response omits
/// `numericId`. Asserting on the error text keeps this honest, since a bare `is_err()` would still
/// pass once one of the four is fixed.
#[test]
fn test_json_path_rejects_websocket_frames() {
    for name in ["ws_subscribe_first.json", "ws_subscribe_update.json"] {
        let frame: serde_json::Value =
            serde_json::from_str(&fixture_str(name)).expect("Failed to parse fixture as json");
        let value = frame["values"][0].clone();
        let error = serde_json::from_value::<yamcs_http::types::monitoring::ParameterValue>(value)
            .expect_err("the hand-written ParameterValue should fail to parse this");
        assert!(
            error.to_string().contains("missing field"),
            "{name}: expected a missing field, got {error}"
        );
    }
}
