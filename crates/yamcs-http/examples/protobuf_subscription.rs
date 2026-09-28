//! Subscribe to parameters over the websocket using protobuf instead of json.
//!
//! Yamcs picks the websocket encoding from the subprotocol offered at handshake, so this asks
//! for `protobuf` and then speaks binary frames in both directions. Sending a json frame on a
//! protobuf connection makes Yamcs close the connection without an error message.
//!
//! Run with: cargo run -p yamcs-http --example protobuf_subscription --features websocket

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use yamcs_http::pb::yamcs::api::{ClientMessage, ServerMessage};
use yamcs_http::pb::yamcs::protobuf::NamedObjectId;
use yamcs_http::pb::yamcs::protobuf::processing::{
    SubscribeParametersData, SubscribeParametersRequest,
};

const URL: &str = "ws://localhost:8090/api/websocket";
const INSTANCE: &str = "bigdata";
const PARAMETER: &str = "/BigData_YamcsDeployment/BigData/bigDataComponent/FloatSamplesTlm";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut request = URL.into_client_request()?;
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "protobuf".parse()?);

    let (mut socket, response) = tokio_tungstenite::connect_async(request).await?;
    println!(
        "negotiated subprotocol: {:?}",
        response.headers().get("sec-websocket-protocol")
    );

    let subscribe = SubscribeParametersRequest {
        instance: Some(INSTANCE.to_string()),
        processor: Some("realtime".to_string()),
        id: vec![NamedObjectId {
            name: PARAMETER.to_string(),
            namespace: None,
        }],
        ..Default::default()
    };
    let client_message = ClientMessage {
        r#type: "parameters".to_string(),
        id: 1,
        options: Some(prost_types::Any::from_msg(&subscribe)?),
        ..Default::default()
    };
    socket
        .send(WsMessage::Binary(client_message.encode_to_vec()))
        .await?;

    let mut frames = 0;
    while let Some(frame) = socket.next().await {
        let WsMessage::Binary(bytes) = frame? else {
            continue;
        };
        let server_message = ServerMessage::decode(&bytes[..])?;
        let Some(data) = server_message.data else {
            continue;
        };
        println!("type={} type_url={}", server_message.r#type, data.type_url);

        if server_message.r#type == "parameters" {
            let payload: SubscribeParametersData = data.to_msg()?;
            for value in &payload.values {
                let bytes = value
                    .eng_value
                    .as_ref()
                    .and_then(|v| v.binary_value.as_ref())
                    .map(|b| b.len());
                println!("  numeric_id={:?} binary_len={:?}", value.numeric_id, bytes);
            }
            frames += 1;
            if frames == 2 {
                break;
            }
        }
    }
    Ok(())
}
