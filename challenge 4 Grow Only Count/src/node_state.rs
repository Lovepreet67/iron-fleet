use std::collections::HashSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeState {
    pub received_increments: HashSet<String>,
    pub counter: usize,
}
impl NodeState {
    fn get_increment_value(increment: &str) -> usize {
        increment
            .rsplit("_")
            .next()
            .expect("error while converting the increment to usize")
            .parse::<usize>()
            .expect("error while converting to usize")
    }
    pub fn merge(&mut self, received_state: Self) {
        for entry in received_state.received_increments {
            if !self.received_increments.contains(&entry) {
                self.counter += Self::get_increment_value(&entry);
                self.received_increments.insert(entry);
            }
        }
    }
    pub fn add_value(&mut self, increment: String) {
        if !self.received_increments.contains(&increment) {
            self.counter += Self::get_increment_value(&increment);
            self.received_increments.insert(increment);
        }
    }
}
