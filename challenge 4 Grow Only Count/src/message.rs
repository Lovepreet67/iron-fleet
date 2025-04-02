use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::node_state::NodeState;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    src: String,
    dest: String,
    body: Body,
}

impl Message {
    pub fn new(src: String, dst: String, body: Body) -> Self {
        Message {
            src,
            dest: dst,
            body,
        }
    }
    pub fn reply(self, payload: Payload) -> Self {
        Message {
            src: self.dest,
            dest: self.src,
            body: Body {
                msg_id: None,
                in_reply_to: self.body.msg_id,
                payload,
            },
        }
    }
    pub fn set_msg_id(&mut self, msg_id: usize) {
        self.body.msg_id = Some(msg_id);
    }
    pub fn get_payload(&self) -> &Payload {
        &self.body.payload
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Body {
    pub msg_id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Generate,
    GenerateOk {
        id: String,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Gossip {
        state: NodeState,
    },
    GossipOk,
    Add {
        delta: usize,
    },
    AddOk,
    Read,
    ReadOk {
        value: usize,
    },
}
