use std::collections::HashSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeState {
    pub received_messages: HashSet<usize>,
}
impl NodeState {
    pub fn merge(&mut self, received_state: Self) {
        for entry in received_state.received_messages {
            self.received_messages.insert(entry);
        }
    }
    pub fn add_message(&mut self, message: usize) {
        self.received_messages.insert(message);
    }
    pub fn get_messages(&self)->HashSet<usize>{ 
        self.received_messages.clone()
    }
}
