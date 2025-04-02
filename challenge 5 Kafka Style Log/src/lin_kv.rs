use crate::message::{Body, Message, Payload};

pub struct LinKv {
    node_id: String,
}
impl LinKv {
    pub fn new(node_id: String) -> Self {
        LinKv { node_id }
    }
    pub fn get_read_message(&self, key: String) -> Message {
        Message::new(
            self.node_id.clone(),
            "lin-kv".to_string(),
            Body {
                msg_id: None,
                in_reply_to: None,
                payload: Payload::Read { key },
            },
        )
    }
    pub fn get_cas_message(&self, key: String, from: usize, to: usize) -> Message {
        Message::new(
            self.node_id.clone(),
            "lin-kv".to_string(),
            Body {
                msg_id: None,
                in_reply_to: None,
                payload: Payload::Cas { key, from, to },
            },
        )
    }
    pub fn write_message(&self, key: String, value: usize) -> Message {
        Message::new(
            self.node_id.clone(),
            "lin-kv".to_string(),
            Body {
                msg_id: None,
                in_reply_to: None,
                payload: Payload::Write { key, value },
            },
        )
    }
}
