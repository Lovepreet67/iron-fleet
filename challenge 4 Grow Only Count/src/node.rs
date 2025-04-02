use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use rand::random_bool;

use crate::{
    maelstrom::Maelstrom,
    message::{self, Message},
    node_state::NodeState,
};

pub struct Node {
    id: String,
    neighbors: Vec<String>,
    maelstrom: Maelstrom,
    unique_id: usize,
    topology: HashMap<String, Vec<String>>,
    state: Arc<Mutex<NodeState>>,
}
impl Node {
    pub fn new(id: String, neighbors: Vec<String>, maelstrom: Maelstrom) -> Self {
        Node {
            id,
            neighbors,
            maelstrom,
            unique_id: 0,
            topology: HashMap::new(),
            state: Arc::default(),
        }
    }
    pub fn start(&mut self) {
        let state_clone = Arc::clone(&self.state);
        let send = self.maelstrom.get_sender();
        let neighbors = self.neighbors.clone();
        let name = self.id.clone();
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_millis(500));
                let state = state_clone
                    .lock()
                    .expect("error while aquiring lock on the state in gossip thread");
                for node in &neighbors {
                    if random_bool(0.70) || node == &name {
                        continue;
                    }
                    let gossip_message = Message::new(
                        name.clone(),
                        node.to_string(),
                        message::Body {
                            msg_id: None,
                            in_reply_to: None,
                            payload: message::Payload::Gossip {
                                state: state.clone(),
                            },
                        },
                    );
                    send(gossip_message);
                }
                drop(state);
            }
        });
        loop {
            let message = self.maelstrom.receive();
            let process_result = self.process(message);
            if let Some(reply) = process_result {
                self.maelstrom.send(reply);
            }
        }
    }
}

impl Node {
    /// will return reply
    fn process(&mut self, message: Message) -> Option<Message> {
        match message.clone().get_payload() {
            message::Payload::Echo { echo } => Some(message.reply(message::Payload::EchoOk {
                echo: echo.to_string(),
            })),
            message::Payload::Generate => {
                self.unique_id += 1;
                Some(message.reply(message::Payload::GenerateOk {
                    id: format!("{}_{}", self.id, self.unique_id),
                }))
            }
            message::Payload::Topology { topology } => {
                self.topology = topology.clone();
                Some(message.reply(message::Payload::TopologyOk))
            }
            message::Payload::Add { delta } => {
                self.unique_id += 1;
                let increment = format!("{}_{}_{}", self.id, self.unique_id, delta);
                let mut state = self
                    .state
                    .lock()
                    .expect("error while getting lock on the state");
                state.add_value(increment);
                Some(message.reply(message::Payload::AddOk))
            }
            message::Payload::Read => {
                let state = self
                    .state
                    .lock()
                    .expect("error while aquiring lock on the state");
                Some(message.reply(message::Payload::ReadOk {
                    value: state.counter,
                }))
            }
            message::Payload::Gossip {
                state: received_state,
            } => {
                let mut state = self.state.lock().expect("error while aquiring state");
                state.merge(received_state.clone());
                Some(message.reply(message::Payload::GossipOk))
            }
            _ => None,
        }
    }
}
