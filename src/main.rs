use std::io::{self, stdin, stdout};
pub mod message;
pub mod message_queue;
mod node;
pub mod node_state;
use message::Message;
use message_queue::MessageQueue;
use node::Node;

fn main() -> Result<(), io::Error> {
    let stdin = stdin().lock();
    let stdout = stdout().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
    let message_queue = MessageQueue::new(stdout); //default();
    let mut state = Node::new(message_queue);
    for input in inputs {
        let input = input?;
        state.step(input)?;
    }
    Ok(())
}
