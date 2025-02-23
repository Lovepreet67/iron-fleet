use crate::{
    message::{Body, Message, Payload},
    message_queue,
    node_state::NodeState,
};
use message_queue::MessageQueue;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{stderr, Error, Stderr, Write},
};

#[derive(Debug)]
pub struct Node {
    name: String,
    connected_to: Vec<String>,
    generate_counter: i32,
    msg_id: usize,
    topology: HashMap<String, Vec<String>>,
    message_queue: MessageQueue,
    state: NodeState,
    add_msg_id:i32 
}

impl Node {
    pub fn new(message_queue: MessageQueue) -> Self {
        Node {
            name: "no_name".to_string(),
            connected_to: vec![],
            msg_id: 0,
            generate_counter: 0,
            topology: HashMap::new(),
            message_queue,
            state: NodeState::default(),
            add_msg_id:0 
        }
    }
    pub fn step(&mut self, input: Message) -> Result<(), Error> {
        match self.reply_generator(&input) {
            Some(reply) => {
                self.message_queue.add(reply);
                Ok(())
            }
            None => Ok(()),
        }
    }
}

impl Node {
    fn nodes_not_reachable_from_parrent(&self, parrent: &str) -> Vec<String> {
        let mut candidates = HashSet::<String>::from_iter(self.connected_to.clone());
        candidates.remove(&self.name);
        // doing the bfs from parrent
        let mut to_visit = VecDeque::from([parrent]);
        let mut visited = HashSet::<String>::new();
        visited.insert(self.name.clone());

        while let Some(curr_node) = to_visit.pop_front() {
            if let Some(nodes) = self.topology.get(curr_node) {
                for node in nodes {
                    if visited.contains(node) {
                        continue;
                    }
                    visited.insert(node.to_string());
                    to_visit.push_back(node);
                }
            }
        }
        for node in visited.into_iter() {
            candidates.remove(&node);
        }
        candidates.into_iter().collect()
    }
    fn gossip(&mut self, from: &str) {
        let nodes = self.nodes_not_reachable_from_parrent(from);
        for node in nodes {
            let body = Body::new(
                Some(self.msg_id),
                None,
                Payload::Gossip {
                    received_state: self.state.clone(),
                },
            );
            let message = Message::new(self.name.to_string(), node.to_string(), body);
            self.message_queue.add(message);
            self.msg_id += 1;
        }
    } 
}

impl Node {
    fn reply_generator(&mut self, input: &Message) -> Option<Message> {
        if let Some(reply_id) = input.get_in_reply_to() {
            self.message_queue.recieved_response(reply_id);
        }
        match input.get_payload() {
            Payload::Init { node_id, node_ids } => {
                self.name = node_id.to_string();
                self.connected_to.clone_from(node_ids);
                let body = Body::new(Some(self.msg_id), input.get_message_id(), Payload::InitOk);
                let reply = Message::new(
                    input.get_dst().to_string(),
                    input.get_src().to_string(),
                    body,
                );
                self.msg_id += 1;
                Some(reply)
            }
            Payload::InitOk => None,
            Payload::Echo { echo } => {
                let body = Body::new(
                    Some(self.msg_id),
                    input.get_message_id(),
                    Payload::EchoOk {
                        echo: echo.to_string(),
                    },
                );

                let reply = Message::new(self.name.clone(), input.get_src().to_string(), body);
                self.msg_id += 1;
                Some(reply)
            }
            Payload::EchoOk { .. } => None,
            Payload::Generate => {
                let body = Body::new(
                    Some(self.msg_id),
                    input.get_message_id(),
                    Payload::GenerateOk {
                        id: format!("{}_{}", self.name, self.generate_counter),
                    },
                );
                let reply = Message::new(self.name.clone(), input.get_src().to_string(), body);
                self.generate_counter += 1;
                self.msg_id += 1;
                Some(reply)
            }
            Payload::GenerateOk { .. } => None,
            Payload::Topology { topology } => {
                for enteries in topology.iter() {
                    self.topology
                        .insert(enteries.0.to_string(), enteries.1.to_vec());
                }
                let body = Body::new(
                    Some(self.msg_id),
                    input.get_message_id(),
                    Payload::TopologyOk,
                );
                let reply = Message::new(self.name.clone(), input.get_src().to_string(), body);
                self.msg_id += 1;
                Some(reply)
            }
            Payload::Broadcast { message:_ } => {
                None  
                //if !self.state.received_messages.contains(message) {
                //    self.state.received_messages.insert(message);
                //}
                //let body = Body::new(
                //    Some(self.msg_id),
                //    input.get_message_id(),
                //    Payload::BroadcastOk,
                //);
                //let reply = Message::new(self.name.clone(), input.get_src().to_string(), body);
                //self.msg_id += 1;
                //self.gossip(input.get_src());
                //Some(reply)
            }
            Payload::BroadcastOk => None,
            Payload::Read => {
                    let body = Body::new(
                    Some(self.msg_id),
                    input.get_message_id(),
                    Payload::ReadOk {
                        value:self.state.current_counter 
                    },
                );
                let reply = Message::new(self.name.clone(), input.get_src().to_string(), body);
                self.msg_id += 1;
                Some(reply)

            }
            Payload::Add { delta }=>{
                //eprintln!("recieved value to add {}",delta);  
                //we will add this message to the nodes state
                if delta > &0  {
                    let to_store = format!("{}_{}_{}",self.name,self.add_msg_id,delta);
                    self.state.received_messages.insert(to_store);
                    self.state.current_counter+=delta; 
                self.add_msg_id+=1;  
                self.gossip(input.get_src()); 
                }
                let body = Body::new(Some(self.msg_id), input.get_message_id(), Payload::AddOk);
                let reply = Message::new(self.name.to_string(),input.get_src().to_string(), body);
                self.msg_id+=1; 
                Some(reply)  
            }
            Payload::Gossip { received_state } => {
                self.state.sync(received_state);
                let body = Body::new(
                    Some(self.msg_id),
                    input.get_message_id(),
                    Payload::TopologyOk,
                );
                let reply = Message::new(self.name.clone(), input.get_src().to_string(), body);
                self.msg_id += 1;
                Some(reply)
            }
            Payload::GossipOk => None,
            _ => None,
        }
    }
}
