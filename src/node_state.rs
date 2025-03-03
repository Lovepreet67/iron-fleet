use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::message::TxnEntry;
// for custom serielization and deserialization

//#[serde_as]
//#[derive(Deserialize, Serialize, Debug, Clone)]
//pub struct Topic {
//    pub committed_offset: HashMap<String, usize>,
//    #[serde_as(as = "BTreeMap<DisplayFromStr,_>")]
//    pub messages: BTreeMap<usize, i32>,
//}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct NodeState { 
    pub key_value_store: HashMap<i32, i32>, 
}
impl NodeState {
        pub fn process_transaction(&mut self,txn:Vec<TxnEntry>)->Vec<TxnEntry>{  
            let before_snapshot = self.key_value_store.clone();
            let mut result = Vec::<TxnEntry>::new(); 
            for txn_entry in txn {
                match txn_entry.0 { 
                    crate::message::TxnType::R=>{
                        if let Some(value) = before_snapshot.get(&txn_entry.1) {
                            result.push(TxnEntry(crate::message::TxnType::R,txn_entry.1,Some(*value))); 
                        }  
                        else {
                            result.push(TxnEntry(crate::message::TxnType::R,txn_entry.1,None));  
                        }
                    },
                    crate::message::TxnType::W =>{
                        result.push(txn_entry.clone()); 
                        self.key_value_store.insert(txn_entry.1,txn_entry.2.unwrap_or(0));    
                    }
                }
            };
            result 

    }
}
impl NodeState {
    pub fn sync(&mut self, received_state: &NodeState) {
        // for each topic we are going to set its base and update the messages
        self.key_value_store = received_state.key_value_store.clone();   
    }
}
