use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::message::{Transaction, TransactionType};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeState {
    processed: HashSet<String>,
    store: HashMap<usize, usize>,
}
impl NodeState {
    pub fn process_transaction(&mut self, txn: &mut Transaction) {
        let mut updated_store = self.store.clone();
        for command in txn {
            if command.0 == TransactionType::Read {
                command.2 = self.store.get(&command.1).copied();
            } else {
                updated_store.insert(command.1, command.2.unwrap_or(0));
            }
        }
        self.store = updated_store;
    }
    pub fn is_processed(&mut self, transaction_id: String) -> bool {
        if self.processed.contains(&transaction_id) {
            return true;
        }
        self.processed.insert(transaction_id);
        false
    }
}
