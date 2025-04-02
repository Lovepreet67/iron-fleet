use core::panic;

use maelstrom::Maelstrom;
use message::Message;
use node::Node;

mod maelstrom;
mod message;
mod node;
mod node_state;
fn node_from_init(msg: Message, mut maelstrom: Maelstrom) -> Node {
    eprint!("{:?}", msg);
    match msg.clone().get_payload() {
        message::Payload::Init { node_id, node_ids } => {
            maelstrom.send(msg.reply(message::Payload::InitOk));
            Node::new(
                node_id.to_string(),
                node_ids
                    .iter()
                    .filter(|node| *node != node_id)
                    .cloned()
                    .collect(),
                maelstrom,
            )
        }
        _ => {
            panic!("invaild init message");
        }
    }
}
fn main() {
    let maelstrom = Maelstrom::new();
    let init_message = maelstrom.receive();
    let mut node = node_from_init(init_message, maelstrom);
    node.start();
}
