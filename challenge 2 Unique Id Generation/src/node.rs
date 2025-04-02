use crate::{
    maelstrom::Maelstrom,
    message::{self, Message},
};

pub struct Node {
    id: String,
    maelstrom: Maelstrom,
    unique_id: usize,
}
impl Node {
    pub fn new(id: String, maelstrom: Maelstrom) -> Self {
        Node {
            id,
            maelstrom,
            unique_id: 0,
        }
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
            message::Payload::Generate => {
                self.unique_id += 1;
                Some(message.reply(message::Payload::GenerateOk {
                    id: format!("{}_{}", self.id, self.unique_id),
                }))
            }
            _ => None,
        }
    }
}
