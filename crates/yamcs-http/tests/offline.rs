//! Tests that parse recorded YAMCS responses. No server needed, so these run in CI.
//!
//! Every fixture came from a stock `yamcs/example-simulation` container reporting version
//! 5.13.5, which `server_info.json` records. To re-record them:
//!
//!     docker run -d -p 8091:8090 yamcs/example-simulation:latest
//!     Y=localhost:8091
//!     S=$(curl -s $Y/api | python -c 'import json,sys; print(json.load(sys.stdin)["serverId"])')
//!     P=$Y/api/processors/simulator/realtime/parameters
//!
//!     curl "$Y/api/mdb/simulator/parameters?limit=3"  > mdb_parameters_page.json
//!     curl "$Y/api/mdb/simulator/parameters?system=/" > mdb_parameters_systems.json
//!     curl "$Y/api/processors"                        > processors.json
//!     curl "$Y/api"                                   > server_info.json
//!     curl "$P/yamcs/$S/jvm/totalMemory"              > parameter_value_64bit.json
//!
//! The events fixture needs events to exist first, so publish two before reading them back:
//!
//!     E="$Y/api/archive/simulator/events"
//!     H='Content-Type: application/json'
//!     B='{"type":"FIXTURE","severity":"INFO","source":"User","message":"fixture capture 1"}'
//!     curl -X POST "$E" -H "$H" -d "$B"
//!     curl -X POST "$E" -H "$H" -d "${B/capture 1/capture 2}"
//!     curl "$E?limit=2"                               > archive_events.json
//!
//! Re-record rather than hand-edit them, so they keep matching what YAMCS sends.

use yamcs_http::types::{events, mdb, system};

fn fixture(name: &str) -> String {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("Failed to read {path}: {e}"))
}

#[test]
fn test_parameters_page_parses() {
    let page: mdb::ParametersPage = serde_json::from_str(&fixture("mdb_parameters_page.json"))
        .expect("Failed to parse parameters page");

    assert_eq!(page.parameters.expect("Failed to find parameters").len(), 3);
    assert!(
        page.continuation_token.is_some(),
        "a partial page carries a token"
    );
    assert!(page.total_size > 3, "total_size spans every page");
}

/// A `system` filter returns subsystems in a list that no other response carries.
#[test]
fn test_systems_page_parses() {
    let page: mdb::ParametersPage = serde_json::from_str(&fixture("mdb_parameters_systems.json"))
        .expect("Failed to parse systems page");

    assert_eq!(page.systems.expect("Failed to find systems").len(), 3);
}

/// YAMCS serializes 64-bit integers as json strings, not numbers.
#[test]
fn test_sint64_value_sent_as_a_string() {
    let outer: serde_json::Value = serde_json::from_str(&fixture("parameter_value_64bit.json"))
        .expect("Failed to parse fixture as json");

    let eng: yamcs_http::Value =
        serde_json::from_value(outer["engValue"].clone()).expect("Failed to parse value");
    // The number is live jvm memory, so assert the shape rather than the value.
    assert!(matches!(eng, yamcs_http::Value::Sint64 { sint64_value } if sint64_value > 0));
}

/// YAMCS sends both the deprecated `event` field and the current `events` field.
#[test]
fn test_event_response_carries_event_and_events() {
    let raw = fixture("archive_events.json");

    #[derive(serde::Deserialize)]
    struct Both {
        event: Vec<events::Event>,
        events: Vec<events::Event>,
    }
    let parsed: Both = serde_json::from_str(&raw).expect("Failed to parse event list");
    assert_eq!(parsed.event.len(), parsed.events.len());
}

#[test]
fn test_processor_list_parses() {
    #[derive(serde::Deserialize)]
    struct Page {
        processors: Vec<system::Processor>,
    }
    let page: Page =
        serde_json::from_str(&fixture("processors.json")).expect("Failed to parse processor list");

    assert!(!page.processors.is_empty());
}

#[test]
fn test_server_info_parses() {
    let info: system::GeneralInfo =
        serde_json::from_str(&fixture("server_info.json")).expect("Failed to parse server info");

    assert!(!info.yamcs_version.is_empty());
}
