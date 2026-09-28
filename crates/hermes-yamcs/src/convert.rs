use hermes_pb::*;
use std::collections::HashMap;
use tonic::Status;
use yamcs_http::pb::yamcs::protobuf::Value as YamcsValue;
use yamcs_http::pb::yamcs::protobuf::events::Event as YamcsEvent;
use yamcs_http::pb::yamcs::protobuf::events::event::EventSeverity;
use yamcs_http::pb::yamcs::protobuf::pvalue::ParameterValue as YamcsParameterValue;
use yamcs_http::pb::yamcs::protobuf::value::Type as ValueType;

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
    yamcs_event: &YamcsEvent,
    filter: &BusFilter,
) -> Result<Option<SourcedEvent>, Status> {
    let source = yamcs_event.source.clone().unwrap_or_default();
    let event_type = yamcs_event.r#type.clone().unwrap_or_default();

    // Apply source filter
    if !filter.source.is_empty() && filter.source != source {
        return Ok(None);
    }

    // Apply name filter (match against event type)
    if !filter.names.is_empty() && !filter.names.contains(&event_type) {
        return Ok(None);
    }

    // Yamcs sends a real timestamp here, so there is nothing to parse.
    let time = Time {
        unix: yamcs_event.generation_time,
        sclk: 0.0,
    };

    // Map YAMCS severity to Hermes severity
    let severity = match yamcs_event
        .severity
        .and_then(|s| EventSeverity::try_from(s).ok())
    {
        Some(EventSeverity::Watch) => EvrSeverity::EvrActivityHigh,
        Some(EventSeverity::Warning) | Some(EventSeverity::WarningNew) => {
            EvrSeverity::EvrWarningLow
        }
        Some(EventSeverity::Distress) | Some(EventSeverity::Critical) => {
            EvrSeverity::EvrWarningHigh
        }
        Some(EventSeverity::Severe) => EvrSeverity::EvrFatal,
        #[allow(deprecated)]
        Some(EventSeverity::Error) => EvrSeverity::EvrWarningLow,
        Some(EventSeverity::Info) | None => EvrSeverity::EvrActivityLow,
    };

    // Build event reference
    let event_ref = EventRef {
        id: yamcs_event.seq_number.unwrap_or_default(),
        name: event_type,
        component: source.clone(),
        severity: severity as i32,
        arguments: vec![],
        dictionary: String::new(),
    };

    // Convert extra fields to tags
    let mut tags = HashMap::new();
    for (key, val) in &yamcs_event.extra {
        tags.insert(
            key.clone(),
            Value {
                value: Some(value::Value::S(val.clone())),
            },
        );
    }

    let event = Event {
        r#ref: Some(event_ref),
        time: Some(time),
        message: yamcs_event.message.clone().unwrap_or_default(),
        args: vec![],
        tags,
    };

    let sourced_event = SourcedEvent {
        event: Some(event),
        source,
        context: SourceContext::Realtime as i32,
    };

    Ok(Some(sourced_event))
}

/// Qualified name of a YAMCS ParameterValue, empty when Yamcs sent only a numeric id
pub fn parameter_name(param: &YamcsParameterValue) -> String {
    param
        .id
        .as_ref()
        .map(|id| id.name.clone())
        .unwrap_or_default()
}

/// Convert YAMCS ParameterValue to Hermes SourcedTelemetry
pub fn yamcs_param_to_hermes(
    param: &YamcsParameterValue,
    filter: &BusFilter,
) -> Result<Option<SourcedTelemetry>, Status> {
    // Build full parameter name
    let param_name = parameter_name(param);

    // Apply name filter
    if !filter.names.is_empty()
        && !filter.names.contains(&param_name)
        && !filter.names.contains(&"*".to_string())
    {
        return Ok(None);
    }

    // Yamcs sends a real timestamp here, so there is nothing to parse.
    let time = Time {
        unix: param.generation_time,
        sclk: 0.0,
    };

    // Convert YAMCS value to Hermes value
    let value = match &param.eng_value {
        Some(eng_value) => yamcs_value_to_hermes(eng_value)?,
        None => Value { value: None },
    };

    // Build telemetry reference
    // let telem_ref = TelemetryRef {
    //     instance_id: String::new(), // TODO: populate from YAMCS instance
    //     qualified_name: param_name.clone(),
    // };

    let telem_ref = TelemetryRef {
        id: 0,
        name: "".to_string(),
        component: "".to_string(),
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

/// Convert YAMCS Value to Hermes Value
///
/// The type tag selects which of the value fields Yamcs populated; a tag whose field is absent
/// converts to an empty Hermes value.
fn yamcs_value_to_hermes(yamcs_value: &YamcsValue) -> Result<Value, Status> {
    let value_type = ValueType::try_from(yamcs_value.r#type).map_err(|_| {
        Status::invalid_argument(format!("Unknown value type: {}", yamcs_value.r#type))
    })?;

    let value = match value_type {
        ValueType::Float => yamcs_value.float_value.map(|v| value::Value::F(v as f64)),
        ValueType::Double => yamcs_value.double_value.map(value::Value::F),
        ValueType::Uint32 => yamcs_value.uint32_value.map(|v| value::Value::U(v as u64)),
        ValueType::Sint32 => yamcs_value.sint32_value.map(|v| value::Value::I(v as i64)),
        ValueType::Uint64 => yamcs_value.uint64_value.map(value::Value::U),
        ValueType::Sint64 => yamcs_value.sint64_value.map(value::Value::I),
        ValueType::Boolean => yamcs_value.boolean_value.map(value::Value::B),
        ValueType::String => yamcs_value.string_value.clone().map(value::Value::S),
        ValueType::Binary => yamcs_value.binary_value.clone().map(|bytes| {
            value::Value::R(BytesValue {
                kind: NumberKind::NumberU8 as i32,
                big_endian: false,
                value: bytes,
            })
        }),
        // Yamcs sends microseconds since the epoch; Hermes has no integer timestamp value.
        ValueType::Timestamp => yamcs_value
            .timestamp_value
            .map(|v| value::Value::S(format!("{}", v))),
        ValueType::Aggregate => {
            // Convert aggregate to object
            let mut obj = HashMap::new();
            if let Some(aggregate_value) = &yamcs_value.aggregate_value {
                for (i, name) in aggregate_value.name.iter().enumerate() {
                    if let Some(val) = aggregate_value.value.get(i) {
                        obj.insert(name.clone(), yamcs_value_to_hermes(val)?);
                    }
                }
            }
            Some(value::Value::O(ObjectValue { o: obj }))
        }
        ValueType::Array => {
            let values: Result<Vec<_>, _> = yamcs_value
                .array_value
                .iter()
                .map(yamcs_value_to_hermes)
                .collect();
            Some(value::Value::A(ArrayValue { value: values? }))
        }
        ValueType::Enumerated => {
            // Enumerated values are represented as strings
            yamcs_value.string_value.clone().map(value::Value::S)
        }
        ValueType::None => None,
    };

    Ok(Value { value })
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
