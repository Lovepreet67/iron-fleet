use crate::message::{Message, Payload};
use std::collections::{HashSet, VecDeque};
use std::io::{stdout, Write};
use std::thread;
use std::time::{Duration, Instant};

enum Event {
    Send { message: Message },
    SendSure { message: Message },
    Received { msg_id: usize },
    Flush,
    None,
}

#[derive(Debug)]
pub struct MessageQueue {
    sender: std::sync::mpsc::Sender<Event>,
}
impl Clone for MessageQueue {
    fn clone(&self) -> Self {
        MessageQueue {
            sender: self.sender.clone(),
        }
    }
}

impl Default for MessageQueue {
    fn default() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let mut queue = VecDeque::<Message>::new();
            let mut gossip_queue = VecDeque::<Message>::new();
            let mut writer = stdout().lock();
            let mut to_check = HashSet::<usize>::new();
            let mut reply_ids = HashSet::<usize>::new();
            let mut last_gossiped = Instant::now();
            loop {
                let event = match rx.try_recv() {
                    Ok(event) => event,
                    Err(_) => Event::None,
                };
                match event {
                    Event::Send { message } => {
                        if let Payload::Gossip { received_state: _ } = message.get_payload() {
                            gossip_queue.push_back(message);
                        } else {
                            queue.push_back(message);
                        }
                    }
                    Event::SendSure { message } => {
                        if let Some(msg_id) = message.get_message_id() {
                            to_check.insert(msg_id);
                        }
                        queue.push_back(message);
                    }
                    Event::Received { msg_id } => {
                        reply_ids.insert(msg_id);
                    }
                    Event::Flush => {
                        // send all the message in queue by default we just have to make the gossip
                        // messages to flush, we can do that by seting the last_gossiped time in
                        // past
                        last_gossiped = Instant::now() - Duration::from_secs(10);
                    }
                    Event::None => {
                        // do nothing
                    }
                }

                // send all the messages which are not gossip
                let mut size = queue.len();
                while size > 0 {
                    size -= 1;
                    if let Some(message) = queue.pop_front() {
                        if let Some(msg_id) = message.get_message_id() {
                            if reply_ids.contains(&msg_id) {
                                reply_ids.remove(&msg_id);
                                to_check.remove(&msg_id);
                                continue;
                            }
                        }
                        serde_json::to_writer(&mut writer, &message)
                            .expect("error while writing the message to stdout");
                        writer
                            .write_all(b"\n")
                            .expect("error while writing new line");
                        if let Some(msg_id) = message.get_message_id() {
                            if to_check.contains(&msg_id) {
                                queue.push_back(message);
                            }
                        }
                    }
                }
                // this will gossip ony after 50 miliseconds
                // now we will check of we have gossiped small time before
                if last_gossiped.elapsed() > Duration::from_millis(50) {
                    let mut already_sent_latest_to = HashSet::<String>::new();
                    while let Some(message) = gossip_queue.pop_back() {
                        if already_sent_latest_to.contains(message.get_dst()) {
                            continue;
                        }
                        already_sent_latest_to.insert(message.get_dst().to_string());
                        serde_json::to_writer(&mut writer, &message)
                            .expect("error while writing the message to stdout");
                        writer
                            .write_all(b"\n")
                            .expect("error while writing new line");
                    }
                    last_gossiped = Instant::now();
                }
            }
        });

        MessageQueue { sender: tx }
    }
}

impl MessageQueue {
    pub fn add(&self, message: Message) {
        self.sender
            .send(Event::Send { message })
            .expect("error while adding new message to queue");
    }
    pub fn add_and_check(&self, message: Message) {
        self.sender
            .send(Event::SendSure { message })
            .expect("error while sending msgsure");
    }
    pub fn recieved_response(&self, msg_id: usize) {
        self.sender
            .send(Event::Received { msg_id })
            .expect("error while sending addin gthe received response");
    }
}
