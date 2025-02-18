use std::io::{self, stdin, stdout };
mod node;  
pub mod message;
use message::Message;
use node::Node; 
     
fn main()->Result<(),io::Error> { 
    let stdin = stdin().lock();
    let mut stdout = stdout().lock();  
     
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
  
    let mut state = Node::new(0);  

    for input in inputs {
        let input = input?;
        state.step(input,&mut stdout)?;   
    } 
    Ok(()) 
} 
