use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::link_kv::{LinkKVResponse, LinkKv};


// for custom serielization and deserialization
 

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Topic {
    pub committed_offset: HashMap<String, usize>,
    pub messages: BTreeMap<usize, i32>, 
} 
impl Topic {
    pub fn new(committed_offset: HashMap<String, usize>) -> Self {
        Topic { 
            messages: BTreeMap::new(),
            committed_offset,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct NodeState {
    topics: HashMap<String, Topic>,
}

fn get_valid_offset(key: &str, key_value_store: &LinkKv) -> usize {  
    loop {
        // first we will try to get the offset
        match key_value_store.get(key.to_string()) {
            LinkKVResponse::Value(curr_offset) => {
                match key_value_store.compare_and_set(key.to_string(), curr_offset, curr_offset + 1)
                {
                    LinkKVResponse::Success => {
                        // if this is success ww have secured the index and now we can use it
                        return curr_offset + 1; 
                    }
                    _ => {
                        continue;
                    }
                }
            }
            LinkKVResponse::KeyNotFound => {
                key_value_store.set(key.to_string(), 0);
                return 0;  
            }
            _ => {
                continue;
            }
        }
    }
}

impl NodeState {
    pub fn commit_offsets(&mut self, sender: &str, offsets: &HashMap<String, usize>) {
        eprintln!("commiting offset recieved value : {:?}", offsets);
        eprintln!("before commiting {:?}", self);
        for topic in offsets {
            if let Some(curr_topic) = self.topics.get_mut(topic.0) {
                //curr_topic.messages.retain(|&key,_| key>*topic.1);
                curr_topic
                    .committed_offset
                    .insert(sender.to_string(), *topic.1);
                //curr_topic.committed_offset = Some(*topic.1);
            }
        }
        eprintln!("after commiting : {:?}", self);
    }
    pub fn add_message(&mut self, key: &str, value: i32, key_value_store: &LinkKv) -> usize {
        eprintln!("request to add message in topic  {} : {}", key, value);
        let valid_offset = get_valid_offset(key, key_value_store);
        // first we will get the offset from the for the value
        if let Some(curr_topic) = self.topics.get_mut(key) {
            curr_topic.messages.insert(valid_offset, value);
            eprintln!("after adding message : {:?}", curr_topic);
        } else {
            // topic is not present in the state so we create new
            let mut topic = Topic::new(HashMap::new());
            topic.messages.insert(valid_offset, value); 
            self.topics.insert(key.to_string(), topic);
            eprintln!("after adding the message : {:?}", self);
        }
        valid_offset 
    }
    pub fn poll(&self, offsets: &HashMap<String, usize>) -> HashMap<String, Vec<Vec<i32>>> {
        let mut result = HashMap::<String, Vec<Vec<i32>>>::new();
        let mut max_enteries = 3;
        for topic in offsets {
            let mut to_send = Vec::<Vec<i32>>::new();
            if let Some(curr_topic) = self.topics.get(topic.0) {
                for message in &curr_topic.messages {
                    if *message.0 >= *topic.1 {
                        to_send.push(vec![*message.0 as i32, *message.1]);
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
        eprintln!("poll with param : {:?}", offsets);
        eprintln!("state : {:?}", self);
        eprintln!("polled value {:?}", result);
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
        eprintln!("get_offsets with param : {:?}", keys);
        eprintln!("state : {:?}", self);
        eprintln!("ofsets value {:?}", result);

        result
    }
}

impl NodeState {
    pub fn sync(&mut self, received_state: &NodeState) {
        // for each topic we are going to set its base and update the messages
        for topic in &received_state.topics {
            // of the topic is already present we wil update the value of topic
            if let Some(curr_topic) = self.topics.get_mut(topic.0) {
                // we will sync the messages
                for message in &topic.1.messages {
                    // if we have message we don't need to add message
                    if curr_topic.messages.contains_key(message.0) {
                        continue;
                    }
                    curr_topic.messages.insert(*message.0, *message.1);
                }
                // syncing the commited offset
                for commited_offset in &topic.1.committed_offset {
                    if let Some(curr_commited_offset) =
                        curr_topic.committed_offset.get(commited_offset.0)
                    {
                        if curr_commited_offset < commited_offset.1 {
                            curr_topic
                                .committed_offset
                                .insert(commited_offset.0.to_string(), *commited_offset.1);
                        }
                    }
                }
            }
            // if this topic is new to the node we should just insert the topic and continue
            else {
                self.topics.insert(topic.0.to_string(), topic.1.clone());
                continue;
            }
        }
    }
}
