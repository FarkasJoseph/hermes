use crate::pb::yamcs::api::ServerMessage;
use prost_types::Any;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) struct Subscription {
    subscription_type: String,
    seq: Mutex<u32>,
    frame_loss: Mutex<bool>,
    callback: Arc<dyn Fn(Any) + Send + Sync>,
}

impl Subscription {
    pub(crate) fn new<F>(subscription_type: String, callback: F) -> Self
    where
        F: Fn(Any) + Send + Sync + 'static,
    {
        Self {
            subscription_type,
            seq: Mutex::new(0),
            frame_loss: Mutex::new(false),
            callback: Arc::new(callback),
        }
    }

    /// Process a server message
    ///
    /// Returns true if frame loss was detected
    pub(crate) async fn consume(&self, msg: ServerMessage) -> bool {
        if msg.r#type != self.subscription_type {
            tracing::warn!(
                expected_type = %self.subscription_type,
                received_type = %msg.r#type,
                "got unexpected message type on websocket subscription"
            );
            return false;
        }

        // Check for frame loss by comparing sequence numbers
        let seq_val = msg.seq as u32;
        let mut frame_loss_detected = false;
        {
            let mut seq = self.seq.lock().await;
            let expected_seq = *seq + 1;
            if *seq > 0 && seq_val != expected_seq {
                let mut frame_loss = self.frame_loss.lock().await;
                if !*frame_loss {
                    *frame_loss = true;
                    frame_loss_detected = true;
                    tracing::warn!(
                        topic = self.subscription_type,
                        expected = expected_seq,
                        actual = seq_val,
                        "Frame loss detected"
                    );
                }
            }
            *seq = seq_val;
        }

        if let Some(data) = msg.data {
            (self.callback)(data);
        }

        frame_loss_detected
    }

    /// Check if frame loss has been detected
    #[allow(dead_code)]
    pub async fn has_frame_loss(&self) -> bool {
        *self.frame_loss.lock().await
    }
}
