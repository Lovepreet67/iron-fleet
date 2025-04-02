use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
    thread::{self, sleep},
    time::Duration,
};

use crate::{
    maelstrom::Maelstrom,
    message::{self, Message, Transaction, TransactionType},
    node_state::NodeState,
};
enum Reply {
    None,
    Message(Message),
}

pub struct Node {
    id: String,
    neighbors: Vec<String>,
    maelstrom: Maelstrom,
    unique_id: usize,
    state: Arc<Mutex<NodeState>>,
    message_buffer: VecDeque<Message>,
    received_reply_for: Arc<Mutex<HashSet<usize>>>,
}

// for internal functions
impl Node {
    fn is_read_only(txn: &Transaction) -> bool {
        for command in txn {
            if command.0 == TransactionType::Wrire {
                return false;
            }
        }
        true
    }
    fn send_sure(&mut self, mut message: Message) {
        let received_reply_for = Arc::clone(&self.received_reply_for);
        let sender = self.maelstrom.get_sender();
        let msg_id = self.maelstrom.send(message.clone());
        thread::spawn(move || {
            message.set_msg_id(msg_id);
            for i in 5..20 {
                sleep(Duration::from_millis(1000 * i));
                let received_reply_for = received_reply_for
                    .lock()
                    .expect("error while taking lock on received status");
                if received_reply_for.contains(&msg_id) {
                    return;
                }
                sender(message.clone());
            }
        });
    }
    fn send_to_all(&mut self, mut message: Message) {
        for neighbour in self.neighbors.clone() {
            message.set_dest(neighbour.to_string());
            self.send_sure(message.clone());
        }
    }
    fn handle_message(&mut self, message: Message) {
        let process_result = self.process(message);
        match process_result {
            Reply::Message(message) => {
                self.maelstrom.send(message);
            }
            Reply::None => {}
        }
    }
}
impl Node {
    pub fn new(id: String, neighbors: Vec<String>, maelstrom: Maelstrom) -> Self {
        Node {
            id,
            neighbors,
            maelstrom,
            unique_id: 0,
            state: Arc::default(),
            message_buffer: VecDeque::default(),
            received_reply_for: Arc::default(),
        }
    }
    pub fn start(&mut self) {
        loop {
            if let Some(message) = self.message_buffer.pop_front() {
                self.handle_message(message);
                continue;
            }
            let message = self.maelstrom.receive();
            self.handle_message(message);
        }
    }
}

impl Node {
    /// will return reply
    fn process(&mut self, message: Message) -> Reply {
        match message.clone().get_payload() {
            message::Payload::Echo { echo } => {
                Reply::Message(message.reply(message::Payload::EchoOk {
                    echo: echo.to_string(),
                }))
            }
            message::Payload::Topology { topology } => {
                self.neighbors = topology
                    .keys()
                    .filter(|node| *node != &self.id)
                    .cloned()
                    .collect();
                Reply::Message(message.reply(message::Payload::TopologyOk))
            }
            message::Payload::Txn { txn } => {
                let mut updated_txn = txn.clone();
                if !Self::is_read_only(txn) {
                    self.unique_id += 1;
                    let forward_message = Message::new(
                        self.id.to_string(),
                        self.id.to_string(),
                        message::Body {
                            msg_id: None,
                            in_reply_to: None,
                            payload: message::Payload::ForwardTxn {
                                txn_id: format!("{}_{}", self.id, self.unique_id),
                                txn: txn.clone(),
                            },
                        },
                    );
                    self.send_to_all(forward_message);
                }
                let mut state = self
                    .state
                    .lock()
                    .expect("error while getting lock on the mutex");
                state.process_transaction(&mut updated_txn);
                drop(state);

                Reply::Message(message.reply(message::Payload::TxnOk {
                    txn: updated_txn.clone(),
                }))
            }
            message::Payload::ForwardTxn { txn_id, txn } => {
                let mut state = self
                    .state
                    .lock()
                    .expect("error while getting lock on the mutex");
                if !state.is_processed(txn_id.to_string()) {
                    eprintln!("processing transaciton forward");
                    state.process_transaction(&mut txn.clone());
                }
                drop(state);
                Reply::Message(message.reply(message::Payload::ForwardTxnOk))
            }
            message::Payload::ForwardTxnOk => {
                let mut received_reply_for = self
                    .received_reply_for
                    .lock()
                    .expect("error while taking lock on received_reply_for");
                if let Some(reply_to_msg_id) = message.get_reply_to_msg_id() {
                    received_reply_for.insert(reply_to_msg_id);
                }
                Reply::None
            }
            _ => Reply::None,
        }
    }
}
