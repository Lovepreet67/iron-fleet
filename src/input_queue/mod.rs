use crate::Message;
use std::{io::stdin, thread};

pub struct InputQueue {}

impl InputQueue {
    pub fn new() -> Self {
        thread::spawn(move || {
            let stdin = stdin().lock();
            let messages = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
            for message in messages {
                // here we will seprat the messages for main and lin-kv
                let message = message.expect("error while trying to parse message");
                match message.get_src() {
                    "lin-kv" => {
                        // this message will go to the lin_kv thread
                    }
                    _ => {
                        // otherwise it goes to the main thread
                    }
                }
            }
        });
        InputQueue {}
    }
}
