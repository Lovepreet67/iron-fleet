use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    thread::sleep,
    time::Duration,
};

use rand::random;

use crate::{
    lin_kv::LinKv,
    maelstrom::Maelstrom,
    message::{self, Message},
    node_state::NodeState,
};
enum Reply {
    None,
    Message(Message),
    Messages(Vec<Message>),
}

pub struct Node {
    id: String,
    neighbors: Vec<String>,
    maelstrom: Maelstrom,
    state: Arc<Mutex<NodeState>>,
    message_buffer: VecDeque<Message>,
    link_kv: LinKv,
}

// for internal functions
impl Node {
    fn send_sync(&mut self, message: Message) -> Message {
        let msg_id = self.maelstrom.send(message);
        loop {
            let received_message = self.maelstrom.receive();
            if msg_id == received_message.get_reply_to_msg_id().unwrap_or(0) {
                return received_message;
            }
            match received_message.clone().get_payload() {
                message::Payload::ForwardSend { .. }
                | message::Payload::ForwardCommitOffsets { .. } => {
                    self.handle_message(received_message);
                }
                _ => {
                    self.message_buffer.push_back(received_message);
                }
            }
        }
    }
    fn handle_message(&mut self, message: Message) {
        let process_result = self.process(message);
        match process_result {
            Reply::Message(message) => {
                self.maelstrom.send(message);
            }
            Reply::Messages(messages) => {
                for message in messages {
                    self.maelstrom.send(message);
                }
            }
            Reply::None => {}
        }
    }
    fn send_to_all(&mut self, mut message: Message) {
        for node in self.neighbors.clone() {
            message.set_dest(node);
            self.send_sync(message.clone());
        }
    }
    fn get_valid_offset(&mut self, key: String) -> usize {
        loop {
            //first we will try to aquire the
            if let message::Payload::Error { code, text: _ } = self
                .send_sync(self.link_kv.get_cas_message(format!("{}_lock", key), 0, 1))
                .get_payload()
            {
                if *code == 20 {
                    // key doesn't exist so we will create a key
                    self.maelstrom
                        .send(self.link_kv.write_message(format!("{}_lock", key), 0));
                    continue;
                } else {
                    sleep(Duration::from_millis((random::<u64>() % 4) + 6));
                    continue;
                }
            }
            // get existing Value
            let curr_offset = match self
                .send_sync(self.link_kv.get_read_message(key.clone()))
                .get_payload()
            {
                message::Payload::ReadOk { value } => *value,
                _ => {
                    let random_offset = rand::random::<u8>() as usize;
                    self.maelstrom
                        .send(self.link_kv.write_message(key.clone(), random_offset));
                    random_offset
                }
            };
            let cas_message =
                self.link_kv
                    .get_cas_message(key.clone(), curr_offset, curr_offset + 1);
            if let message::Payload::CasOk = self.send_sync(cas_message).get_payload() {
                return curr_offset + 1;
            }
        }
    }
}
impl Node {
    pub fn new(id: String, neighbors: Vec<String>, maelstrom: Maelstrom) -> Self {
        Node {
            link_kv: LinKv::new(id.clone()),
            id,
            neighbors,
            maelstrom,
            state: Arc::default(),
            message_buffer: VecDeque::default(),
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
            message::Payload::CommitOffsets { offsets } => {
                let send_forward = Message::new(
                    self.id.clone(),
                    self.id.clone(), /* since it will be change*/
                    message::Body {
                        msg_id: None,
                        in_reply_to: None,
                        payload: message::Payload::ForwardCommitOffsets {
                            sender: message.get_src().clone(),
                            offsets: offsets.clone(),
                        },
                    },
                );
                self.send_to_all(send_forward);

                let mut state = self
                    .state
                    .lock()
                    .expect("error while getting lock on state");
                state.commit_offsets(&message.get_src(), offsets);
                Reply::Message(message.reply(message::Payload::CommitOffsetsOk))
            }
            message::Payload::Send { key, msg } => {
                let offset = self.get_valid_offset(key.clone());
                // forward message to all
                let send_forward = Message::new(
                    self.id.clone(),
                    self.id.clone(), /* since it will be change*/
                    message::Body {
                        msg_id: None,
                        in_reply_to: None,
                        payload: message::Payload::ForwardSend {
                            key: key.clone(),
                            msg: *msg,
                            offset,
                        },
                    },
                );
                self.send_to_all(send_forward);
                let mut state = self
                    .state
                    .lock()
                    .expect("error while getting lock on state");
                state.add_message(key, *msg, offset);
                drop(state);
                Reply::Messages(vec![
                    message.reply(message::Payload::SendOk { offset }),
                    self.link_kv.write_message(format!("{}_lock", key), 0),
                ])
            }
            message::Payload::Poll { offsets } => {
                let state = self
                    .state
                    .lock()
                    .expect("error while getting lock on state");
                Reply::Message(message.reply(message::Payload::PollOk {
                    msgs: state.poll(offsets),
                }))
            }
            message::Payload::ListCommittedOffsets { keys } => {
                let state = self
                    .state
                    .lock()
                    .expect("error while getting lock on state");
                let src = message.get_src();
                Reply::Message(message.reply(message::Payload::ListCommittedOffsetsOk {
                    offsets: state.get_offsets(&src, keys),
                }))
            }
            message::Payload::ForwardSend { key, msg, offset } => {
                let mut state = self
                    .state
                    .lock()
                    .expect("error while getting lock on state");
                state.add_message(key, *msg, *offset);
                Reply::Message(message.reply(message::Payload::ForwardSendOk))
            }
            message::Payload::ForwardCommitOffsets { offsets, sender } => {
                let mut state = self
                    .state
                    .lock()
                    .expect("error while getting lock on state");
                state.commit_offsets(sender, offsets);
                Reply::Message(message.reply(message::Payload::ForwardCommitOffsetsOk))
            }
            _ => Reply::None,
        }
    }
}
