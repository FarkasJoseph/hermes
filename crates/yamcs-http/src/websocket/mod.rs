//! WebSocket client for YAMCS real-time subscriptions
//!
//! This module provides WebSocket support for subscribing to real-time data streams
//! from YAMCS, including parameter values, events, alarms, and command updates.
//!
//! **Note:** This module is only available when the `websocket` feature is enabled.
//!
//! # Architecture
//!
//! Subscriptions use tokio channels instead of callbacks. When you subscribe, you receive
//! an `UnboundedReceiver` that yields deserialized subscription data. The subscription is
//! automatically cancelled when the receiver is dropped.
//!
//! # Example
//!
//! ```no_run
//! use yamcs_http::websocket::WebSocketClient;
//! use yamcs_http::pb::yamcs::protobuf::NamedObjectId;
//! use yamcs_http::pb::yamcs::protobuf::processing::{
//!     SubscribeParametersData, SubscribeParametersRequest,
//! };
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let ws_client = WebSocketClient::new("http://localhost:8090");
//!
//!     // Connect to WebSocket
//!     ws_client.connect().await?;
//!
//!     // Subscribe to parameters - returns a channel receiver
//!     let request = SubscribeParametersRequest {
//!         instance: Some("myproject".to_string()),
//!         processor: Some("realtime".to_string()),
//!         id: vec![NamedObjectId {
//!             name: "/MySystem/MyParameter".to_string(),
//!             namespace: None,
//!         }],
//!         ..Default::default()
//!     };
//!     let mut rx = ws_client
//!         .subscribe::<_, SubscribeParametersData>("parameters", request)
//!         .await?;
//!
//!     // Receive updates from the channel
//!     while let Some(data) = rx.recv().await {
//!         println!("Received parameter update: {:?}", data);
//!     }
//!
//!     // Subscription is automatically cancelled when rx is dropped
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod subscription;

pub use crate::pb::yamcs::api::{ClientMessage, ServerMessage};
pub use client::{ConnectionState, WebSocketClient};
