use crate::{
    maelstrom::Maelstrom,
    message::{self, Message},
};

pub struct Node {
    maelstrom: Maelstrom,
}
impl Node {
    pub fn new(maelstrom: Maelstrom) -> Self {
        Node { maelstrom }
    }
    pub fn start(&mut self) {
        loop {
            let message = self.maelstrom.receive();
            let process_result = self.process(message);
            if let Some(reply) = process_result {
                self.maelstrom.send(reply);
            }
        }
    }
}

impl Node {
    /// will return reply
    fn process(&mut self, message: Message) -> Option<Message> {
        match message.clone().get_payload() {
            message::Payload::Echo { echo } => Some(message.reply(message::Payload::EchoOk {
                echo: echo.to_string(),
            })),
            _ => None,
        }
    }
}
