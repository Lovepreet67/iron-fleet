use std::collections::HashSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct NodeState {
    pub received_messages: HashSet<i32>,
}
impl NodeState {
    pub fn sync(&mut self, received_state: &NodeState) {
        // syncing the recieved message
        for items in &received_state.received_messages {
            self.received_messages.insert(*items);
        }
    }
}
