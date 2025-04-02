use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
    pub fn get_src(&self) -> String {
        self.src.clone()
    }
    pub fn set_msg_id(&mut self, msg_id: usize) {
        self.body.msg_id = Some(msg_id);
    }
    pub fn set_dest(&mut self, dest: String) {
        self.dest = dest;
    }
    pub fn get_reply_to_msg_id(&self) -> Option<usize> {
        self.body.in_reply_to
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
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    //for internal
    ForwardSend {
        key: String,
        msg: usize,
        offset: usize,
    },
    ForwardSendOk,
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<Vec<usize>>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ForwardCommitOffsets {
        sender: String,
        offsets: HashMap<String, usize>,
    },
    ForwardCommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },

    // for link-kv
    Read {
        key: String,
    },
    ReadOk {
        value: usize,
    },
    Write {
        key: String,
        value: usize,
    },
    WriteOk,
    Cas {
        key: String,
        from: usize,
        to: usize,
    },
    CasOk,
    Error {
        code: usize,
        text: String,
    },
}
