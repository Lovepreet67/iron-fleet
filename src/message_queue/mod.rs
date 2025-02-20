use crate::message::Message;
use std::collections::{HashSet, VecDeque};
use std::io::{StdoutLock, Write};
use std::usize;

#[derive(Debug)]
pub struct MessageQueue<'a> {
    queue: VecDeque<(Message, bool)>,
    reply_ids: HashSet<usize>,
    writer: StdoutLock<'a>,
}

impl<'a> MessageQueue<'a> {
    pub fn new(writer: StdoutLock<'a>) -> Self {
        MessageQueue {
            queue: VecDeque::new(),
            reply_ids: HashSet::new(),
            writer,
        }
    }
    pub fn add(&mut self, message: Message, wait_for_reply: bool) {
        self.queue.push_back((message, wait_for_reply));
    }
    pub fn recieved_response(&mut self, msg_id: usize) {
        self.reply_ids.insert(msg_id);
    }
    pub fn run(&mut self) {
        let mut size = self.queue.len();
        while size > 0 {
            size -= 1;
            if let Some(entry) = self.queue.pop_front() {
                if let Some(msg_id) = entry.0.get_message_id() {
                    if self.reply_ids.contains(&msg_id) {
                        self.reply_ids.remove(&msg_id);
                        continue;
                    }
                }
                serde_json::to_writer(&mut self.writer, &entry.0)
                    .expect("error while writing the message to stdout");
                self.writer
                    .write_all(b"\n")
                    .expect("error while writing new line");
                if entry.1 {
                    self.queue.push_back(entry);
                }
            }
        }
    }
}
