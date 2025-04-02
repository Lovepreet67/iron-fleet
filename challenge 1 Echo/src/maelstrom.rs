use std::{
    io::{Write, stdin, stdout},
    sync::mpsc,
    thread,
};

use crate::message::Message;

pub struct Maelstrom {
    msg_id: usize,
    sender: mpsc::Sender<Message>,
    receiver: mpsc::Receiver<Message>,
}
impl Maelstrom {
    pub fn new() -> Self {
        let (sender, sender_receiver) = mpsc::channel::<Message>();
        let (receiver_sender, receiver) = mpsc::channel::<Message>();
        thread::spawn(move || {
            let stdin = stdin().lock();
            let messages = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
            for message in messages {
                let message = message.expect("error while converting received message");
                receiver_sender
                    .send(message)
                    .expect("error while sending the message");
            }
        });
        thread::spawn(move || {
            let mut stdout = stdout().lock();
            loop {
                let message = sender_receiver
                    .recv()
                    .expect("error while fetching the message to send");
                serde_json::to_writer(&mut stdout, &message)
                    .expect("error while writing to buffer");
                stdout
                    .write_all(b"\n")
                    .expect("error while writing to buffer");
            }
        });
        Maelstrom {
            sender,
            receiver,
            msg_id: 0,
        }
    }
}
impl Maelstrom {
    pub fn receive(&self) -> Message {
        self.receiver
            .recv()
            .expect("error while receiving message from maelstrom")
    }
    pub fn send(&mut self, mut message: Message) -> usize {
        self.msg_id += 1;
        message.set_msg_id(self.msg_id);
        self.sender
            .send(message)
            .expect("error while sending message to sender channel");
        self.msg_id
    }
}
