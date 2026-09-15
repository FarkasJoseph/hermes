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

    // Build event reference
    let event_ref = EventRef {
        id: yamcs_event.seq_number as i32,
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

/// F Prime numeric types attached to binary values and aggregate members.
#[derive(Debug, Default, Clone)]
pub struct BinaryTypeHints {
    /// Type of this binary value, when known.
    own_kind: Option<NumberKind>,
    /// Types of binary aggregate members.
    members: HashMap<String, BinaryTypeHints>,
}

/// Map an F Prime element type alias to the matching Hermes `NumberKind`.
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

const FPRIME_ELEMENT_TYPE_ALIAS_NAMESPACE: &str = "fprime:elementType";

fn numeric_kind_from_aliases(
    aliases: &[yamcs_http::types::common::NamedObjectId],
) -> Option<NumberKind> {
    aliases
        .iter()
        .find(|a| a.namespace.as_deref() == Some(FPRIME_ELEMENT_TYPE_ALIAS_NAMESPACE))
        .and_then(|a| numeric_kind_from_fprime_name(&a.name))
}

/// Build binary type hints from a parameter's MDB definition.
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
    // YAMCS only sends a channel's name the first time it reports that channel; after that it
    // sends just a numeric id to save bandwidth, and service.rs looks the name back up before
    // calling us. If we still get no name, the mapping for this id hasn't arrived yet (e.g. a
    // subscription that just started) - skip for now, it resolves itself on the next update.
    let param_name = match &param.id {
        Some(id) => id.name.clone(),
        None => {
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

    let Some(eng_val) = &param.eng_value else {
        // No value available; skip this parameter
        debug!(
            numeric_id = param.numeric_id,
            "Skipping parameter with no value"
        );
        return Ok(None);
    };
    let value = yamcs_value_to_hermes(eng_val, binary_hints)?;

    // Hermes keeps a channel's component and name as separate fields (so the UI can group by
    // component), but YAMCS gives us both as one path, e.g. "/BigData/bigDataComponent/Counter".
    let (component, name) = split_qualified_name(&param_name);

    let telem_ref = TelemetryRef {
        id: 0,
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
            // Preserve raw U8 bytes when the MDB has no F Prime element type alias.
            let numeric_kind = hints.and_then(|h| h.own_kind);
            Ok(Value {
                value: Some(value::Value::R(BytesValue {
                    kind: numeric_kind.unwrap_or(NumberKind::NumberU8) as i32,
                    big_endian: numeric_kind.is_some(),
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

    fn parameter_type(
        alias: Vec<NamedObjectId>,
        member: Option<Vec<ParameterMember>>,
    ) -> ParameterType {
        ParameterType {
            name: "TestType".to_string(),
            qualified_name: "/TestType".to_string(),
            short_description: None,
            long_description: None,
            alias,
            eng_type: "binary".to_string(),
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

    fn parameter(parameter_type: ParameterType) -> Parameter {
        Parameter {
            name: "TestParameter".to_string(),
            qualified_name: "/TestParameter".to_string(),
            alias: None,
            short_description: None,
            long_description: None,
            data_source: None,
            parameter_type: Some(Box::new(parameter_type)),
            used_by: None,
            path: None,
        }
    }

    #[test]
    fn binary_type_hints_read_parameter_and_member_aliases() {
        let hints = binary_type_hints_for_parameter(&parameter(parameter_type(
            vec![NamedObjectId::with_namespace("fprime:elementType", "F32")],
            Some(vec![ParameterMember {
                name: "samples".to_string(),
                member_type: Box::new(parameter_type(vec![], None)),
                initial_value: None,
                short_description: None,
                long_description: None,
                alias: vec![NamedObjectId::with_namespace("fprime:elementType", "F64")],
            }]),
        )));

        assert_eq!(hints.own_kind, Some(NumberKind::NumberF32));
        assert_eq!(
            hints.members["samples"].own_kind,
            Some(NumberKind::NumberF64)
        );
    }

    #[test]
    fn binary_values_fall_back_to_raw_u8_without_a_type_hint() {
        let value = yamcs_value_to_hermes(
            &yamcs_http::Value::Binary {
                binary_value: "AQI=".to_string(),
            },
            None,
        )
        .expect("binary value should decode");

        let Some(value::Value::R(bytes)) = value.value else {
            panic!("expected a bytes value");
        };
        assert_eq!(bytes.kind, NumberKind::NumberU8 as i32);
        assert!(!bytes.big_endian);
    }

    #[test]
    fn split_qualified_name_splits_on_last_slash() {
        let (component, name) = split_qualified_name("/BigData/bigDataComponent/Counter");
        assert_eq!(component, "BigData/bigDataComponent");
        assert_eq!(name, "Counter");
    }

    #[test]
    fn split_qualified_name_with_no_slash_has_empty_component() {
        let (component, name) = split_qualified_name("Counter");
        assert_eq!(component, "");
        assert_eq!(name, "Counter");
    }
}
