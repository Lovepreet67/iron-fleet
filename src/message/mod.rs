use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::node_state::NodeState;

#[derive(Debug, Serialize, Deserialize,Clone)] 
#[serde(rename_all = "snake_case")]
pub enum TxnType{
    R,
    W, 
}


#[derive(Debug,Deserialize,Serialize,Clone)] 
pub struct TxnEntry(pub TxnType,pub i32,pub Option<i32>);    

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}
impl Message {
    pub fn new(src: String, dst: String, body: Body) -> Self {
        Message { src, dst, body }
    }
    pub fn get_payload(&self) -> &Payload {
        self.body.get_payload()
    }
    pub fn get_src(&self) -> &str {
        &self.src
    }
    pub fn get_dst(&self) -> &str {
        &self.dst
    }
    pub fn get_message_id(&self) -> Option<usize> {
        self.body.get_message_id()
    }
    pub fn get_in_reply_to(&self) -> Option<usize> {
        self.body.get_in_reply_to()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Body {
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

impl Body {
    pub fn new(id: Option<usize>, in_reply_to: Option<usize>, payload: Payload) -> Self {
        Body {
            id,
            in_reply_to,
            payload,
        }
    }
    pub fn get_message_id(&self) -> Option<usize> {
        self.id
    }
    pub fn get_in_reply_to(&self) -> Option<usize> {
        self.in_reply_to
    }
    pub fn get_payload(&self) -> &Payload {
        &self.payload
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum Payload {
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
    // gossip to share the state to other nodes
    Gossip {
        received_state: NodeState,
    },
    GossipOk,
    // for transactions
    Txn {
        txn:Vec<TxnEntry> 
    },
    TxnOk{
        txn:Vec<TxnEntry>
    }
} 
