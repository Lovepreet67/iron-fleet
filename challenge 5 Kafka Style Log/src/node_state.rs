use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Topic {
    pub committed_offset: HashMap<String, usize>,
    pub messages: BTreeMap<usize, usize>,
}
impl Topic {
    pub fn new(committed_offset: HashMap<String, usize>) -> Self {
        Topic {
            messages: BTreeMap::new(),
            committed_offset,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeState {
    topics: HashMap<String, Topic>,
}
impl NodeState {
    pub fn commit_offsets(&mut self, sender: &str, offsets: &HashMap<String, usize>) {
        for topic in offsets {
            if let Some(curr_topic) = self.topics.get_mut(topic.0) {
                curr_topic
                    .committed_offset
                    .insert(sender.to_string(), *topic.1);
            }
        }
    }
    pub fn add_message(&mut self, key: &str, value: usize, offset: usize) {
        if let Some(curr_topic) = self.topics.get_mut(key) {
            curr_topic.messages.insert(offset, value);
        } else {
            // topic is not present in the state so we create new
            let mut topic = Topic::new(HashMap::new());
            topic.messages.insert(offset, value);
            self.topics.insert(key.to_string(), topic);
        }
    }
    pub fn poll(&self, offsets: &HashMap<String, usize>) -> HashMap<String, Vec<Vec<usize>>> {
        let mut result = HashMap::<String, Vec<Vec<usize>>>::new();
        let mut max_enteries = 3;
        for topic in offsets {
            let mut to_send = Vec::<Vec<usize>>::new();
            if let Some(curr_topic) = self.topics.get(topic.0) {
                for message in &curr_topic.messages {
                    if *message.0 >= *topic.1 {
                        to_send.push(vec![*message.0, *message.1]);
                        result.insert(topic.0.to_string(), vec![]);
                        max_enteries -= 1;
                        if max_enteries == 0 {
                            max_enteries = 3;
                            break;
                        }
                    }
                }
            }
            result.insert(topic.0.to_string(), to_send);
        }
        result
    }
    pub fn get_offsets(&self, sender: &str, keys: &Vec<String>) -> HashMap<String, usize> {
        let mut result = HashMap::<String, usize>::new();
        for key in keys {
            if let Some(curr_topic) = self.topics.get(key) {
                if let Some(committed_offset) = curr_topic.committed_offset.get(sender) {
                    result.insert(key.to_string(), *committed_offset);
                }
            }
        }
        result
    }
}
