use crate::error::{Result, YamcsError};
use crate::pb::yamcs::api::{CancelOptions, ClientMessage, Reply, ServerMessage};
use crate::websocket::subscription::Subscription;
use futures_util::{SinkExt, StreamExt};
use prost::{Message as ProstMessage, Name};
use prost_types::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, trace};
use url::Url;

/// Internal structure to track pending requests awaiting replies
struct PendingRequest {
    tx: tokio::sync::oneshot::Sender<Result<(u32, Any)>>,
}

/// WebSocket connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}

/// Frame loss callback type
type FrameLossCallback = Arc<Mutex<Option<Box<dyn Fn() + Send + Sync>>>>;

/// WebSocket client for YAMCS real-time subscriptions
pub struct WebSocketClient {
    base_url: String,
    state: Arc<RwLock<ConnectionState>>,
    subscriptions: Arc<Mutex<HashMap<u32, Subscription>>>,
    pending_requests: Arc<Mutex<HashMap<u32, PendingRequest>>>,
    request_sequence: Arc<Mutex<u32>>,
    tx: Arc<Mutex<Option<mpsc::UnboundedSender<ClientMessage>>>>,
    frame_loss_callback: FrameLossCallback,
}

impl WebSocketClient {
    /// Create a new WebSocket client
    ///
    /// # Arguments
    /// * `base_url` - Base URL for the YAMCS server (e.g., "http://localhost:8090")
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            request_sequence: Arc::new(Mutex::new(0)),
            tx: Arc::new(Mutex::new(None)),
            frame_loss_callback: Arc::new(Mutex::new(None)),
        }
    }

    /// Set a callback to be called when frame loss is detected
    pub async fn set_frame_loss_callback<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut cb = self.frame_loss_callback.lock().await;
        *cb = Some(Box::new(callback));
    }

    /// Get current connection state
    pub async fn state(&self) -> ConnectionState {
        *self.state.read().await
    }

    /// Connect to the WebSocket endpoint
    pub async fn connect(&self) -> Result<()> {
        // Check if already connected
        {
            let state = self.state.read().await;
            if *state == ConnectionState::Connected {
                return Ok(());
            }
        }

        // Update state to connecting
        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Connecting;
        }

        // Build WebSocket URL
        let ws_url = self.build_websocket_url()?;

        // Connect to WebSocket
        let mut request = ws_url
            .into_client_request()
            .map_err(|e| YamcsError::WebSocket(format!("Invalid websocket url: {}", e)))?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            "protobuf"
                .parse()
                .map_err(|e| YamcsError::WebSocket(format!("Invalid subprotocol: {}", e)))?,
        );

        let (ws_stream, _) = connect_async(request)
            .await
            .map_err(|e| YamcsError::WebSocket(format!("Connection failed: {}", e)))?;

        // Split the WebSocket stream
        let (write, read) = ws_stream.split();

        // Create message channel
        let (tx, mut rx) = mpsc::unbounded_channel::<ClientMessage>();

        // Store the sender
        {
            let mut sender = self.tx.lock().await;
            *sender = Some(tx);
        }

        // Update state to connected
        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Connected;
        }

        // Spawn write task
        let write_handle = tokio::spawn(async move {
            let mut write = write;
            while let Some(msg) = rx.recv().await {
                if let Err(e) = write.send(Message::Binary(msg.encode_to_vec())).await {
                    tracing::error!("WebSocket send error: {}", e);
                    break;
                }
            }
        });

        // Spawn read task
        let subscriptions = Arc::clone(&self.subscriptions);
        let pending_requests = Arc::clone(&self.pending_requests);
        let state = Arc::clone(&self.state);
        let frame_loss_cb = Arc::clone(&self.frame_loss_callback);

        let read_handle = tokio::spawn(async move {
            let mut read = read;
            while let Some(result) = read.next().await {
                match result {
                    Ok(Message::Binary(bytes)) => {
                        match ServerMessage::decode(&bytes[..]) {
                            Ok(msg) => {
                                trace!(msg = ?msg, "server message");

                                // Check if this is a reply message
                                if msg.r#type == "reply" {
                                    let reply = msg.data.as_ref().and_then(|d| d.to_msg::<Reply>().ok());
                                    if let Some(reply) = reply {
                                        let mut pending = pending_requests.lock().await;
                                        if let Some(request) =
                                            pending.remove(&(reply.reply_to as u32))
                                        {
                                            let result = if let Some(exception) = reply.exception {
                                                Err(YamcsError::WebSocket(format!(
                                                    "Request failed: {:?}",
                                                    exception
                                                )))
                                            } else {
                                                Ok((msg.call as u32, msg.data.unwrap_or_default()))
                                            };

                                            let _ = request.tx.send(result);
                                        }
                                    }
                                } else {
                                    let call_id = msg.call as u32;
                                    // Route message to appropriate subscription
                                    let subs = subscriptions.lock().await;
                                    if let Some(sub) = subs.get(&call_id) {
                                        let frame_loss = sub.consume(msg).await;
                                        if frame_loss {
                                            let cb = frame_loss_cb.lock().await;
                                            if let Some(callback) = cb.as_ref() {
                                                callback();
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Failed to parse server message: {}", e);
                            }
                        }
                    }
                    Ok(Message::Close(_)) => {
                        tracing::info!("WebSocket closed by server");
                        break;
                    }
                    Err(e) => {
                        tracing::error!("WebSocket read error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }

            // Update state to disconnected
            let mut s = state.write().await;
            *s = ConnectionState::Disconnected;
        });

        // Store handles (we could use these for cleanup later)
        drop(write_handle);
        drop(read_handle);

        Ok(())
    }

    /// Create a subscription
    ///
    /// Subscriptions are managed via tokio channels. The returned `UnboundedReceiver` yields
    /// deserialized subscription data. The subscription is automatically cancelled when the
    /// receiver is dropped.
    ///
    /// # Arguments
    /// * `subscription_type` - Type of subscription (e.g., "parameters", "events")
    /// * `options` - Subscription options
    ///
    /// # Returns
    /// An `UnboundedReceiver` that yields deserialized subscription data
    ///
    /// # Cancellation
    /// The subscription is automatically cancelled when the receiver is dropped. A cancel
    /// message is sent to the server and the subscription is removed from the internal
    /// subscription map.
    pub async fn subscribe<O, D>(
        &self,
        subscription_type: impl Into<String>,
        options: O,
    ) -> Result<mpsc::UnboundedReceiver<D>>
    where
        O: ProstMessage + Name,
        D: ProstMessage + Default + Name + Send + 'static,
    {
        self.create_subscription(subscription_type, options).await
    }

    async fn create_subscription<O, D>(
        &self,
        subscription_type: impl Into<String>,
        options: O,
    ) -> Result<mpsc::UnboundedReceiver<D>>
    where
        O: ProstMessage + Name,
        D: ProstMessage + Default + Name + Send + 'static,
    {
        // Ensure we're connected
        if self.state().await != ConnectionState::Connected {
            return Err(YamcsError::WebSocket(
                "Not connected to WebSocket".to_string(),
            ));
        }

        let subscription_type = subscription_type.into();

        // Create channel for passing between contexts
        let (tx, rx) = mpsc::unbounded_channel::<D>();

        // Create subscription with deserialization/send closure
        let sender_tx = tx.clone();
        let subscription =
            Subscription::new(subscription_type.clone(), move |data| match data.to_msg::<D>() {
                Ok(data) => {
                    let _ = sender_tx.send(data);
                }
                Err(e) => {
                    tracing::error!("Failed to decode {}: {}", D::full_name(), e);
                }
            });

        let (call_id, _) = self.request(subscription_type, options).await?;

        // Store subscription with call ID
        {
            let mut subs = self.subscriptions.lock().await;
            subs.insert(call_id, subscription);
        }

        // Spawn a task to monitor for cancellation (when handle is dropped)
        let subscriptions = Arc::clone(&self.subscriptions);
        let ws_tx = self.tx.clone();
        tokio::spawn(async move {
            // Wait for the rx pipe to be dropped
            let _ = tx.closed().await;

            // Remove from subscriptions
            {
                let mut subs = subscriptions.lock().await;
                subs.remove(&call_id);
            }

            // Send cancel message to server
            let options = CancelOptions {
                call: call_id as i32,
            };
            let msg = ClientMessage {
                r#type: "cancel".to_string(),
                options: Any::from_msg(&options).ok(),
                ..Default::default()
            };

            let tx_guard = ws_tx.lock().await;
            if let Some(sender) = tx_guard.as_ref() {
                let _ = sender.send(msg);
            }
        });

        Ok(rx)
    }

    /// Send a request and wait for a reply
    ///
    /// This is a general-purpose method for sending WebSocket requests that expect a reply.
    /// The request will be assigned a unique ID, and the method will wait for the server's
    /// reply message before returning.
    ///
    /// # Arguments
    /// * `request_type` - The type of request (e.g., "management", "stream")
    /// * `options` - Request options/data to send
    ///
    /// # Returns
    /// A tuple of `(call_id, data)` where:
    /// - `call_id` is the server-assigned call ID for this request
    /// - `data` is the reply data from the server
    ///
    /// # Errors
    /// Returns an error if:
    /// - Not connected to WebSocket
    /// - Failed to send the request
    /// - Server returns an exception
    /// - Reply timeout (not implemented yet)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use yamcs_http::websocket::WebSocketClient;
    /// # use serde_json::json;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = WebSocketClient::new("http://localhost:8090");
    /// client.connect().await?;
    ///
    /// let (call_id, reply) = client.request("management", json!({
    ///     "action": "getInfo"
    /// })).await?;
    ///
    /// println!("Call ID: {}, Reply: {:?}", call_id, reply);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn request<O>(
        &self,
        request_type: impl Into<String>,
        options: O,
    ) -> Result<(u32, Any)>
    where
        O: ProstMessage + Name,
    {
        // Ensure we're connected
        if self.state().await != ConnectionState::Connected {
            return Err(YamcsError::WebSocket(
                "Not connected to WebSocket".to_string(),
            ));
        }

        // Get next request ID
        let request_id = {
            let mut seq = self.request_sequence.lock().await;
            *seq += 1;
            *seq
        };

        // Create oneshot channel for reply
        let (tx, rx) = tokio::sync::oneshot::channel();

        // Register pending request
        {
            let mut pending = self.pending_requests.lock().await;
            pending.insert(request_id, PendingRequest { tx });
        }

        // Send request message
        let msg = ClientMessage {
            r#type: request_type.into(),
            id: request_id as i32,
            options: Some(
                Any::from_msg(&options)
                    .map_err(|e| YamcsError::WebSocket(format!("Failed to encode options: {e}")))?,
            ),
            ..Default::default()
        };

        debug!(
            id = %request_id,
            request_type = %msg.r#type,
            "sending websocket request"
        );
        self.send_message(msg).await?;

        // Wait for reply
        match rx.await {
            Ok(Ok((call_id, data))) => {
                debug!(
                    id = %request_id,
                    call_id = %call_id,
                    data = ?data,
                    "websocket request succeeded"
                );
                Ok((call_id, data))
            }
            Ok(Err(err)) => Err(YamcsError::WebSocket(format!("Request failed: {}", err))),
            Err(err) => Err(YamcsError::WebSocket(format!(
                "Failed to receive request reply: {}",
                err
            ))),
        }
    }

    /// Send a typed request and wait for a typed reply
    ///
    /// This is a convenience wrapper around `request` that deserializes the reply
    /// into the specified type.
    ///
    /// # Type Parameters
    /// * `O` - Request options type (must be serializable)
    /// * `R` - Reply type (must be deserializable)
    ///
    /// # Arguments
    /// * `request_type` - The type of request
    /// * `options` - Request options/data to send
    ///
    /// # Returns
    /// A tuple of `(call_id, data)` where:
    /// - `call_id` is the server-assigned call ID for this request
    /// - `data` is the deserialized reply data
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use yamcs_http::websocket::WebSocketClient;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Serialize)]
    /// # struct MyRequest { action: String }
    /// # #[derive(Deserialize)]
    /// # struct MyReply { result: String }
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = WebSocketClient::new("http://localhost:8090");
    /// client.connect().await?;
    ///
    /// let (call_id, reply): (u32, MyReply) = client.request_typed(
    ///     "management",
    ///     MyRequest { action: "getInfo".to_string() }
    /// ).await?;
    ///
    /// println!("Call ID: {}, Result: {}", call_id, reply.result);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn request_typed<O, R>(
        &self,
        request_type: impl Into<String>,
        options: O,
    ) -> Result<(u32, R)>
    where
        O: ProstMessage + Name,
        R: ProstMessage + Default + Name,
    {
        let (call_id, any) = self.request(request_type, options).await?;
        let data = any
            .to_msg::<R>()
            .map_err(|e| YamcsError::WebSocket(format!("Failed to decode reply: {e}")))?;
        Ok((call_id, data))
    }

    async fn send_message(&self, msg: ClientMessage) -> Result<()> {
        let tx = self.tx.lock().await;
        if let Some(sender) = tx.as_ref() {
            sender
                .send(msg)
                .map_err(|e| YamcsError::WebSocket(format!("Failed to send message: {}", e)))?;
            Ok(())
        } else {
            Err(YamcsError::WebSocket("Not connected".to_string()))
        }
    }

    fn build_websocket_url(&self) -> Result<String> {
        let base = Url::parse(&self.base_url)?;
        let scheme = if base.scheme() == "https" || base.scheme() == "wss" {
            "wss"
        } else {
            "ws"
        };

        let host = base
            .host_str()
            .ok_or_else(|| YamcsError::InvalidUrl(url::ParseError::EmptyHost))?;

        let port = base
            .port()
            .unwrap_or_else(|| if scheme == "wss" { 443 } else { 8090 });

        Ok(format!("{}://{}:{}/api/websocket", scheme, host, port))
    }

    /// Close the WebSocket connection
    pub async fn close(&self) {
        // Clear all subscriptions
        {
            let mut subs = self.subscriptions.lock().await;
            subs.clear();
        }

        // Close the sender (which will cause the write task to exit)
        {
            let mut tx = self.tx.lock().await;
            *tx = None;
        }

        // Update state
        {
            let mut state = self.state.write().await;
            *state = ConnectionState::Disconnected;
        }
    }
}

fn get_type_name<T>(_: PhantomData<T>) -> &'static str {
    std::any::type_name::<T>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_websocket_url_construction() {
        let client = WebSocketClient::new("http://localhost:8090");
        let url = client.build_websocket_url().unwrap();
        assert_eq!(url, "ws://localhost:8090/api/websocket");

        let client = WebSocketClient::new("https://example.com");
        let url = client.build_websocket_url().unwrap();
        assert_eq!(url, "wss://example.com:443/api/websocket");
    }
}
