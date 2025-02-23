use std::collections::HashSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct NodeState {
    pub received_messages : HashSet<String>, 
    pub current_counter : i32,
}
impl NodeState {
    pub fn sync(&mut self, received_state: &NodeState) {
        //eprintln!("recieved a sync request for the messages : {:?}",received_state.received_messages);
        //eprintln!("\town messages : {:?}",self.received_messages); 
        //eprintln!("\tcurrent_counter is {}",self.current_counter); 
        // syncing the recieved message
        for item in &received_state.received_messages {
            if self.received_messages.contains(item) {
                continue;
            }
            if let Some(num_str) =  item.split("_").last(){
                self.current_counter+= num_str.parse::<i32>().unwrap_or(0);  
            }
            self.received_messages.insert(item.to_string());  
        }
        //eprintln!("\tcurrent_counter after sync is {}",self.current_counter); 
    }
}
