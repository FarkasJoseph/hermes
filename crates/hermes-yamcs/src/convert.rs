use chrono::{DateTime, Utc};
use hermes_pb::*;
use prost_types::Timestamp;
use std::collections::HashMap;
use tonic::Status;
use tracing::debug;

/// Convert Hermes CommandValue to YAMCS IssueCommandOptions
pub fn command_value_to_yamcs(
    cmd_value: &CommandValue,
    cmd_def: &CommandDef,
) -> Result<yamcs_http::types::monitoring::IssueCommandOptions, Status> {
    let mut args = HashMap::new();

    // Convert each argument
    for (i, arg_value) in cmd_value.args.iter().enumerate() {
        if i >= cmd_def.arguments.len() {
            return Err(Status::invalid_argument(format!(
                "Too many arguments: expected {}, got {}",
                cmd_def.arguments.len(),
                cmd_value.args.len()
            )));
        }

        let arg_def = &cmd_def.arguments[i];
        let json_value = hermes_value_to_json(arg_value)?;
        args.insert(arg_def.name.clone(), json_value);
    }

    let mut options = yamcs_http::types::monitoring::IssueCommandOptions {
        args: Some(args),
        ..Default::default()
    };

    // Copy metadata fields
    if let Some(origin) = cmd_value.metadata.get("origin") {
        options.origin = Some(origin.clone());
    }
    if let Some(comment) = cmd_value.metadata.get("comment") {
        options.comment = Some(comment.clone());
    }

    Ok(options)
}

/// Convert Hermes Value to JSON value
fn hermes_value_to_json(value: &Value) -> Result<serde_json::Value, Status> {
    if let Some(ref v) = value.value {
        match v {
            value::Value::I(i) => Ok(serde_json::Value::Number((*i).into())),
            value::Value::U(u) => Ok(serde_json::Value::Number((*u).into())),
            value::Value::F(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .ok_or_else(|| Status::invalid_argument("Invalid float value")),
            value::Value::B(b) => Ok(serde_json::Value::Bool(*b)),
            value::Value::S(s) => Ok(serde_json::Value::String(s.clone())),
            value::Value::E(e) => {
                // For enums, use the raw value
                Ok(serde_json::Value::Number(e.raw.into()))
            }
            value::Value::O(obj) => {
                let mut map = serde_json::Map::new();
                for (key, val) in &obj.o {
                    map.insert(key.clone(), hermes_value_to_json(val)?);
                }
                Ok(serde_json::Value::Object(map))
            }
            value::Value::A(arr) => {
                let values: Result<Vec<_>, _> =
                    arr.value.iter().map(hermes_value_to_json).collect();
                Ok(serde_json::Value::Array(values?))
            }
            value::Value::R(_bytes) => {
                // For bytes, convert to base64 string
                Err(Status::unimplemented(
                    "Bytes conversion not yet implemented",
                ))
            }
        }
    } else {
        Ok(serde_json::Value::Null)
    }
}

/// FNV-1a hash of a ref's qualified name, used as a stable `TelemetryRef`/`EventRef` id
/// (unlike std's `DefaultHasher`, whose keys are randomized per-process).
fn stable_id(key: &str) -> i32 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash & 0x7fff_ffff) as i32 // mask to 31 bits to fit non-negative in proto's int32
}

/// Convert YAMCS Event to Hermes SourcedEvent
pub fn yamcs_event_to_hermes(
    yamcs_event: &yamcs_http::types::events::Event,
    filter: &BusFilter,
) -> Result<Option<SourcedEvent>, Status> {
    // Apply source filter
    if !filter.source.is_empty() && filter.source != yamcs_event.source {
        return Ok(None);
    }

    // Apply name filter (match against event type)
    if !filter.names.is_empty() && !filter.names.contains(&yamcs_event.event_type) {
        return Ok(None);
    }

    // Parse generation time
    let time = parse_yamcs_time(&yamcs_event.generation_time)?;

    // Map YAMCS severity to Hermes severity
    let severity = match yamcs_event.severity {
        yamcs_http::types::events::EventSeverity::Info => EvrSeverity::EvrActivityLow,
        yamcs_http::types::events::EventSeverity::Watch => EvrSeverity::EvrActivityHigh,
        yamcs_http::types::events::EventSeverity::Warning => EvrSeverity::EvrWarningLow,
        yamcs_http::types::events::EventSeverity::Distress => EvrSeverity::EvrWarningHigh,
        yamcs_http::types::events::EventSeverity::Critical => EvrSeverity::EvrWarningHigh,
        yamcs_http::types::events::EventSeverity::Severe => EvrSeverity::EvrFatal,
        #[allow(deprecated)]
        yamcs_http::types::events::EventSeverity::Error => EvrSeverity::EvrWarningLow,
    };

    // id identifies the event definition (source+type), not the occurrence — seq_number was wrong here.
    let event_ref = EventRef {
        id: stable_id(&format!("{}/{}", yamcs_event.source, yamcs_event.event_type)),
        name: yamcs_event.event_type.clone(),
        component: yamcs_event.source.clone(),
        severity: severity as i32,
        arguments: vec![],
        dictionary: String::new(),
    };

    // Convert extra fields to tags
    let mut tags = HashMap::new();
    if let Some(ref extra) = yamcs_event.extra {
        for (key, val) in extra {
            tags.insert(
                key.clone(),
                Value {
                    value: Some(value::Value::S(val.clone())),
                },
            );
        }
    }

    let event = Event {
        r#ref: Some(event_ref),
        time: Some(time),
        message: yamcs_event.message.clone(),
        args: vec![],
        tags,
    };

    let sourced_event = SourcedEvent {
        event: Some(event),
        source: yamcs_event.source.clone(),
        context: SourceContext::Realtime as i32,
    };

    Ok(Some(sourced_event))
}

/// Which F Prime numeric type (if any) a "!binary" value's raw bytes represent, so it can be
/// surfaced as a typed `BytesValue` instead of raw `U8` bytes. Built from a parameter's MDB
/// definition (see [`binary_type_hints_for_parameter`]) and threaded through
/// [`yamcs_value_to_hermes`], since the hint lives on the *type*, not the streamed value.
#[derive(Debug, Default, Clone)]
pub struct BinaryTypeHints {
    /// Numeric kind for this value itself, if it is (or resolves to) a "!binary" blob.
    own_kind: Option<NumberKind>,
    /// Hints for this value's members, if it's an aggregate. Only one level deep today: a
    /// member that's itself a nested aggregate with its own "!binary" members isn't resolved.
    members: HashMap<String, BinaryTypeHints>,
}

/// Map an F Prime primitive type name (as named by fprime-xtce's "fprime:elementType" Alias)
/// to the matching Hermes `NumberKind`.
fn numeric_kind_from_fprime_name(name: &str) -> Option<NumberKind> {
    Some(match name {
        "U8" => NumberKind::NumberU8,
        "I8" => NumberKind::NumberI8,
        "U16" => NumberKind::NumberU16,
        "I16" => NumberKind::NumberI16,
        "U32" => NumberKind::NumberU32,
        "I32" => NumberKind::NumberI32,
        "U64" => NumberKind::NumberU64,
        "I64" => NumberKind::NumberI64,
        "F32" => NumberKind::NumberF32,
        "F64" => NumberKind::NumberF64,
        _ => return None,
    })
}

/// The `fprime:elementType` Alias fprime-xtce tags a "!binary" BinaryParameterType with (see
/// fprime-xtce's `BINARY_ELEMENT_TYPE_ALIAS_NAMESPACE`).
const FPRIME_ELEMENT_TYPE_ALIAS_NAMESPACE: &str = "fprime:elementType";

fn numeric_kind_from_aliases(
    aliases: &[yamcs_http::types::common::NamedObjectId],
) -> Option<NumberKind> {
    aliases
        .iter()
        .find(|a| a.namespace.as_deref() == Some(FPRIME_ELEMENT_TYPE_ALIAS_NAMESPACE))
        .and_then(|a| numeric_kind_from_fprime_name(&a.name))
}

/// Build [`BinaryTypeHints`] for a parameter from its MDB definition, so "!binary" members (or
/// a bare "!binary" parameter) can be tagged with their real numeric element type.
pub fn binary_type_hints_for_parameter(
    parameter: &yamcs_http::types::mdb::Parameter,
) -> BinaryTypeHints {
    let Some(param_type) = &parameter.parameter_type else {
        return BinaryTypeHints::default();
    };
    let members = param_type
        .member
        .as_ref()
        .map(|members| {
            members
                .iter()
                .filter_map(|m| {
                    numeric_kind_from_aliases(&m.alias).map(|kind| {
                        (
                            m.name.clone(),
                            BinaryTypeHints {
                                own_kind: Some(kind),
                                members: HashMap::new(),
                            },
                        )
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    BinaryTypeHints {
        own_kind: numeric_kind_from_aliases(&param_type.alias),
        members,
    }
}

/// Convert YAMCS ParameterValue to Hermes SourcedTelemetry
pub fn yamcs_param_to_hermes(
    param: &yamcs_http::types::monitoring::ParameterValue,
    filter: &BusFilter,
    binary_hints: Option<&BinaryTypeHints>,
) -> Result<Option<SourcedTelemetry>, Status> {
    // Resolve parameter name: prefer id.name if present, otherwise skip (numeric_id will be resolved by caller)
    let param_name = match &param.id {
        Some(id) => id.name.clone(),
        None => {
            // Numeric ID without a name mapping — skip this value
            debug!(
                numeric_id = param.numeric_id,
                "Skipping parameter value with unresolved numeric_id"
            );
            return Ok(None);
        }
    };

    // Apply name filter
    if !filter.names.is_empty()
        && !filter.names.contains(&param_name)
        && !filter.names.contains(&"*".to_string())
    {
        return Ok(None);
    }

    // Parse generation time
    let time = parse_yamcs_time(&param.generation_time)?;

    // Convert YAMCS value to Hermes value (prefer eng_value, fall back to raw_value)
    let value = if let Some(eng_val) = &param.eng_value {
        yamcs_value_to_hermes(eng_val, binary_hints)?
    } else if let Some(raw_val) = &param.raw_value {
        yamcs_value_to_hermes(raw_val, binary_hints)?
    } else {
        // No value available; skip this parameter
        debug!(
            numeric_id = param.numeric_id,
            "Skipping parameter with no value"
        );
        return Ok(None);
    };

    // Extract component and name from qualified name
    // YAMCS qualified names are typically "/component/name" or "/parent/component/name"
    // Split on "/" and use the last part as name, parent as component
    let (component, name) = split_qualified_name(&param_name);

    // id was hardcoded to 0, collapsing every channel into one telemetryDefs row downstream.
    let telem_ref = TelemetryRef {
        id: stable_id(&param_name),
        name,
        component,
        dictionary: "".to_string(),
    };

    let telemetry = Telemetry {
        r#ref: Some(telem_ref),
        time: Some(time),
        value: Some(value),
        labels: HashMap::default(),
    };

    let sourced_telemetry = SourcedTelemetry {
        telemetry: Some(telemetry),
        source: filter.source.clone(),
        context: SourceContext::Realtime as i32,
    };

    Ok(Some(sourced_telemetry))
}

/// Split a YAMCS qualified name into component and name
/// E.g., "/BigData/bigDataComponent/Counter" -> ("BigData/bigDataComponent", "Counter")
fn split_qualified_name(qualified_name: &str) -> (String, String) {
    let trimmed = qualified_name.trim_start_matches('/');
    if let Some(last_slash) = trimmed.rfind('/') {
        let component = trimmed[..last_slash].to_string();
        let name = trimmed[last_slash + 1..].to_string();
        (component, name)
    } else {
        // No slash found, use the whole thing as name and empty component
        ("".to_string(), trimmed.to_string())
    }
}

/// Convert YAMCS Value to Hermes Value
fn yamcs_value_to_hermes(
    yamcs_value: &yamcs_http::Value,
    hints: Option<&BinaryTypeHints>,
) -> Result<Value, Status> {
    match yamcs_value {
        yamcs_http::Value::Float { float_value } => Ok(Value {
            value: Some(value::Value::F(*float_value as f64)),
        }),
        yamcs_http::Value::Double { double_value } => Ok(Value {
            value: Some(value::Value::F(*double_value)),
        }),
        yamcs_http::Value::Uint32 { uint32_value } => Ok(Value {
            value: Some(value::Value::U(*uint32_value as u64)),
        }),
        yamcs_http::Value::Sint32 { sint32_value } => Ok(Value {
            value: Some(value::Value::I(*sint32_value as i64)),
        }),
        yamcs_http::Value::Uint64 { uint64_value } => Ok(Value {
            value: Some(value::Value::U(*uint64_value)),
        }),
        yamcs_http::Value::Sint64 { sint64_value } => Ok(Value {
            value: Some(value::Value::I(*sint64_value)),
        }),
        yamcs_http::Value::Boolean { boolean_value } => Ok(Value {
            value: Some(value::Value::B(*boolean_value)),
        }),
        yamcs_http::Value::String { string_value } => Ok(Value {
            value: Some(value::Value::S(string_value.clone())),
        }),
        yamcs_http::Value::Binary { binary_value } => {
            // Binary is base64 encoded, decode it
            let decoded =
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, binary_value)
                    .map_err(|e| Status::invalid_argument(format!("Invalid base64: {}", e)))?;
            // A "!binary" array/member is tagged (via fprime-xtce's AliasSet) with the F Prime
            // numeric type its bytes actually represent; `hints` carries that back from the MDB.
            // Absent a hint (untagged binary, or an older dictionary), fall back to raw U8 bytes.
            let numeric_kind = hints
                .and_then(|h| h.own_kind)
                .unwrap_or(NumberKind::NumberU8);
            Ok(Value {
                value: Some(value::Value::R(BytesValue {
                    kind: numeric_kind as i32,
                    big_endian: true,
                    value: decoded,
                })),
            })
        }
        yamcs_http::Value::Timestamp { timestamp_value } => {
            // Convert timestamp to string
            Ok(Value {
                value: Some(value::Value::S(format!("{}", timestamp_value))),
            })
        }
        yamcs_http::Value::Aggregate { aggregate_value } => {
            // Convert aggregate to object
            let mut obj = HashMap::new();
            for (i, name) in aggregate_value.name.iter().enumerate() {
                if let Some(val) = aggregate_value.value.get(i) {
                    let member_hints = hints.and_then(|h| h.members.get(name.as_str()));
                    obj.insert(name.clone(), yamcs_value_to_hermes(val, member_hints)?);
                }
            }
            Ok(Value {
                value: Some(value::Value::O(ObjectValue { o: obj })),
            })
        }
        yamcs_http::Value::Array { array_value } => {
            let values: Result<Vec<_>, _> = array_value
                .iter()
                .map(|v| yamcs_value_to_hermes(v, None))
                .collect();
            Ok(Value {
                value: Some(value::Value::A(ArrayValue { value: values? })),
            })
        }
        yamcs_http::Value::Enumerated { string_value } => {
            // Enumerated values are represented as strings
            Ok(Value {
                value: Some(value::Value::S(string_value.clone())),
            })
        }
        yamcs_http::Value::None => {
            // No value
            Ok(Value { value: None })
        }
    }
}

/// Parse YAMCS timestamp string to Hermes Time
fn parse_yamcs_time(time_str: &str) -> Result<Time, Status> {
    // Parse ISO8601 timestamp
    let dt = DateTime::parse_from_rfc3339(time_str)
        .map_err(|e| Status::invalid_argument(format!("Invalid timestamp: {}", e)))?;

    let utc: DateTime<Utc> = dt.into();

    Ok(Time {
        unix: Some(Timestamp {
            seconds: utc.timestamp(),
            nanos: utc.timestamp_subsec_nanos() as i32,
        }),
        sclk: 0.0,
    })
}

/// Convert YAMCS Instance to Hermes Fsw
pub fn yamcs_instance_to_fsw(instance: &yamcs_http::types::system::Instance) -> Fsw {
    // YAMCS instances support commanding
    let capabilities = vec![FswCapability::Command as i32];

    Fsw {
        id: instance.name.clone(),
        r#type: "yamcs".to_string(),
        profile_id: "yamcs".to_string(),
        forwards: vec![],
        capabilities,
        dictionary: instance.name.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yamcs_http::types::common::NamedObjectId;
    use yamcs_http::types::mdb::{Parameter, ParameterMember, ParameterType};

    #[test]
    fn stable_id_is_deterministic_and_distinguishes_distinct_keys() {
        assert_eq!(
            stable_id("/BigData/bigDataComponent/Counter"),
            stable_id("/BigData/bigDataComponent/Counter")
        );
        assert_ne!(
            stable_id("/BigData/bigDataComponent/Counter"),
            stable_id("/BigData/bigDataComponent/MapStream")
        );
        assert!(stable_id("/BigData/bigDataComponent/Counter") >= 0);
    }

    fn binary_value(base64: &str) -> yamcs_http::Value {
        yamcs_http::Value::Binary {
            binary_value: base64.to_string(),
        }
    }

    fn fprime_element_type_alias(name: &str) -> NamedObjectId {
        NamedObjectId {
            namespace: Some(FPRIME_ELEMENT_TYPE_ALIAS_NAMESPACE.to_string()),
            name: name.to_string(),
        }
    }

    fn parameter_type(
        alias: Vec<NamedObjectId>,
        member: Option<Vec<ParameterMember>>,
    ) -> ParameterType {
        ParameterType {
            name: "T".to_string(),
            qualified_name: "/T".to_string(),
            short_description: None,
            long_description: None,
            alias,
            eng_type: "aggregate".to_string(),
            array_info: None,
            data_encoding: None,
            unit_set: None,
            default_alarm: None,
            context_alarm: vec![],
            enum_values: vec![],
            enum_ranges: vec![],
            absolute_time_info: None,
            member,
            signed: None,
            size_in_bits: None,
            one_string_value: None,
            zero_string_value: None,
            used_by: None,
            initial_value: None,
            raw_valid_range: None,
            eng_valid_range: None,
        }
    }

    #[test]
    fn numeric_kind_from_fprime_name_covers_every_hermes_number_kind() {
        for (name, kind) in [
            ("U8", NumberKind::NumberU8),
            ("I8", NumberKind::NumberI8),
            ("U16", NumberKind::NumberU16),
            ("I16", NumberKind::NumberI16),
            ("U32", NumberKind::NumberU32),
            ("I32", NumberKind::NumberI32),
            ("U64", NumberKind::NumberU64),
            ("I64", NumberKind::NumberI64),
            ("F32", NumberKind::NumberF32),
            ("F64", NumberKind::NumberF64),
        ] {
            assert_eq!(numeric_kind_from_fprime_name(name), Some(kind));
        }
        assert_eq!(numeric_kind_from_fprime_name("bool"), None);
    }

    #[test]
    fn binary_type_hints_for_bare_binary_parameter() {
        let parameter = Parameter {
            name: "CostMap".to_string(),
            qualified_name: "/CostMap".to_string(),
            alias: None,
            short_description: None,
            long_description: None,
            data_source: None,
            parameter_type: Some(Box::new(parameter_type(
                vec![fprime_element_type_alias("F32")],
                None,
            ))),
            used_by: None,
            path: None,
        };

        let hints = binary_type_hints_for_parameter(&parameter);
        assert_eq!(hints.own_kind, Some(NumberKind::NumberF32));
        assert!(hints.members.is_empty());
    }

    #[test]
    fn binary_type_hints_for_aggregate_member() {
        let data_member = ParameterMember {
            name: "data".to_string(),
            member_type: Box::new(parameter_type(vec![], None)),
            initial_value: None,
            short_description: None,
            long_description: None,
            alias: vec![fprime_element_type_alias("U8")],
        };
        let parameter = Parameter {
            name: "MapStream".to_string(),
            qualified_name: "/MapStream".to_string(),
            alias: None,
            short_description: None,
            long_description: None,
            data_source: None,
            parameter_type: Some(Box::new(parameter_type(vec![], Some(vec![data_member])))),
            used_by: None,
            path: None,
        };

        let hints = binary_type_hints_for_parameter(&parameter);
        assert_eq!(hints.own_kind, None);
        let data_hints = hints.members.get("data").expect("data member hints");
        assert_eq!(data_hints.own_kind, Some(NumberKind::NumberU8));
    }

    #[test]
    fn binary_type_hints_absent_when_untagged() {
        let parameter = Parameter {
            name: "Plain".to_string(),
            qualified_name: "/Plain".to_string(),
            alias: None,
            short_description: None,
            long_description: None,
            data_source: None,
            parameter_type: Some(Box::new(parameter_type(vec![], None))),
            used_by: None,
            path: None,
        };
        assert_eq!(binary_type_hints_for_parameter(&parameter).own_kind, None);
    }

    #[test]
    fn binary_value_uses_hinted_numeric_kind() {
        let hints = BinaryTypeHints {
            own_kind: Some(NumberKind::NumberF32),
            members: HashMap::new(),
        };
        let value = yamcs_value_to_hermes(&binary_value("AAAAAA=="), Some(&hints)).unwrap();
        let Some(value::Value::R(bytes)) = value.value else {
            panic!("expected a bytes value");
        };
        assert_eq!(bytes.kind, NumberKind::NumberF32 as i32);
    }

    #[test]
    fn binary_value_without_hints_defaults_to_u8() {
        let value = yamcs_value_to_hermes(&binary_value("AAAAAA=="), None).unwrap();
        let Some(value::Value::R(bytes)) = value.value else {
            panic!("expected a bytes value");
        };
        assert_eq!(bytes.kind, NumberKind::NumberU8 as i32);
    }

    #[test]
    fn aggregate_member_binary_value_uses_its_own_hint() {
        let mut member_hints = HashMap::new();
        member_hints.insert(
            "data".to_string(),
            BinaryTypeHints {
                own_kind: Some(NumberKind::NumberI16),
                members: HashMap::new(),
            },
        );
        let hints = BinaryTypeHints {
            own_kind: None,
            members: member_hints,
        };

        let aggregate = yamcs_http::Value::Aggregate {
            aggregate_value: yamcs_http::types::common::AggregateValue {
                name: vec!["data".to_string()],
                value: vec![binary_value("AAAAAA==")],
            },
        };

        let converted = yamcs_value_to_hermes(&aggregate, Some(&hints)).unwrap();
        let Some(value::Value::O(obj)) = converted.value else {
            panic!("expected an object value");
        };
        let Some(value::Value::R(bytes)) = obj.o.get("data").and_then(|v| v.value.clone()) else {
            panic!("expected member 'data' to be a bytes value");
        };
        assert_eq!(bytes.kind, NumberKind::NumberI16 as i32);
    }
}
