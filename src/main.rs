use std::{
     io::{self, stdin}, sync::mpsc, thread
};
pub mod message;
pub mod message_queue;
mod node;
pub mod node_state;
use message::Message;
use message_queue::MessageQueue;
use node::Node;

fn main() -> Result<(), io::Error> {
    let (main_sender, main_reciever) = mpsc::channel::<Message>();
    let message_queue = MessageQueue::default(); //default();
    // creating a main thread
    thread::spawn(move || {
        let stdin = stdin().lock();
        let messages = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
        for message in messages {
            let message = message.expect("error while trying to parse message");
            main_sender 
                        .send(message)
                        .expect("error while sending message to main thread");
        }
    });
    let mut state = Node::new(message_queue);
    loop {
        let message = main_reciever
            .recv()
            .expect("error while recieving the message to main thread");
        state.step(message)?;
    } 
}
