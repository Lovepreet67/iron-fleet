use std::{
    io::{self, stdin},
    sync::mpsc,
    thread,
};
pub mod input_queue;
pub mod link_kv;
pub mod message;
pub mod message_queue;
mod node;
pub mod node_state;
use link_kv::LinkKv;
use message::Message;
use message_queue::MessageQueue;
use node::Node;

fn main() -> Result<(), io::Error> {
    let (link_kv_sender, link_kv_reciever) = mpsc::channel::<Message>();
    let (main_sender, main_reciever) = mpsc::channel::<Message>();
    // creating a main thread
    thread::spawn(move || {
        let stdin = stdin().lock();
        let messages = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
        for message in messages {
            // here we will seprat the messages for main and lin-kv
            let message = message.expect("error while trying to parse message");
            match message.get_src() {
                "lin-kv" => {
                    // this message will go to the lin_kv thread
                    link_kv_sender
                        .send(message)
                        .expect("error while sending message to lin-kv");
                }
                _ => {
                    // otherwise it goes to the main thread
                    main_sender
                        .send(message)
                        .expect("error while sending message to main thread");
                }
            }
        }
    });

    // creating a link kv store

    let message_queue = MessageQueue::default(); //default();
    let key_value_store = LinkKv::new(link_kv_reciever, message_queue.clone());
    let mut state = Node::new(message_queue, key_value_store);
    loop {
        let message = main_reciever
            .recv()
            .expect("error while recieving the message to main thread");
        state.step(message)?;
    }
}
