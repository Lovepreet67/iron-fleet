use std::sync::mpsc;

pub enum LinkKVResponse {
    KeyNotFound,
    Value(usize),
    CompareFailed,
    Success,
    Failed,
}

use crate::{
    message::{Body, Message, Payload},
    message_queue::MessageQueue,
};
// assuming that there will be only one request to link-kv (no parrallel calls) and our main thread
// will be blocked to get this value we can use
#[derive(Debug)]
pub struct LinkKv {
    node_name: String,
    link_kv_receiver: mpsc::Receiver<Message>,
    message_queue: MessageQueue,
}

impl LinkKv {
    pub fn new(link_kv_receiver: mpsc::Receiver<Message>, message_queue: MessageQueue) -> Self {
        LinkKv {
            link_kv_receiver,
            message_queue,
            node_name: "default".to_string(),
        }
    }
    pub fn set_node_name(&mut self, node_name: String) {
        self.node_name = node_name;
    }
    pub fn get(&self, key: String) -> LinkKVResponse {
        // we will create a message
        let body = Body::new(None, None, Payload::Read { key });
        let message = Message::new(self.node_name.clone(), "lin-kv".to_string(), body);
        self.message_queue.add(message);
        // after sending we will wait for reply
        match self
            .link_kv_receiver
            .recv()
            .expect("error while receiving reply for get in lin-kv")
            .get_payload()
        {
            Payload::ReadOk { value } => LinkKVResponse::Value(*value),
            Payload::Error { code, text: _ } => {
                if code == &20 {
                    return LinkKVResponse::KeyNotFound;
                }
                LinkKVResponse::Failed
            }
            _ => LinkKVResponse::Failed,
        }
    }
    pub fn set(&self, key: String, value: usize) {
        let body = Body::new(None, None, Payload::Write { key, value });
        let message = Message::new(self.node_name.clone(), "lin-kv".to_string(), body);
        self.message_queue.add(message);
        eprintln!("wating write ok");
        let _ = self
            .link_kv_receiver
            .recv()
            .expect("error while waiting for the write_ok");
    }
    pub fn compare_and_set(&self, key: String, value: usize, new_value: usize) -> LinkKVResponse {
        let body = Body::new(
            None,
            None,
            Payload::Cas {
                key,
                from: value,
                to: new_value,
            },
        );
        let message = Message::new(self.node_name.clone(), "lin-kv".to_string(), body);
        self.message_queue.add(message);
        match self
            .link_kv_receiver
            .recv()
            .expect("error while receiving reply for get in lin-kv")
            .get_payload()
        {
            Payload::CasOk => LinkKVResponse::Success,
            Payload::Error { code, text: _ } => {
                if code == &20 {
                    LinkKVResponse::KeyNotFound
                } else if code == &21 {
                    LinkKVResponse::CompareFailed
                } else {
                    LinkKVResponse::Failed
                }
            }

            _ => LinkKVResponse::Failed,
        }
    }
}
