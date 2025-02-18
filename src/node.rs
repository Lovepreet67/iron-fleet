use std::io::{Error, StdoutLock, Write};   
use crate::message::{Body, Message, Payload};  
pub struct Node {
    id : usize,
    name : String,
    other_node :Vec<String>,
    generate_counter: i32 
}   
 
impl Node { 
    pub fn new(id:usize)->Self{ 
        Node {id,name:"no_name".to_string(),other_node:vec![],generate_counter:0}   
    } 
    pub fn step(&mut self,input:Message,writer : &mut StdoutLock)->Result<(),Error>{
        match self.reply_generator(input){
            Some(reply)=> {
              self.send_message(reply,writer)?; 
              Ok(()) 
            }
            None=>{
                Ok(())
            }
        } 
    } 
} 

impl Node {  
    fn send_message(&mut self,message:Message,writer: &mut StdoutLock)->Result<(),Error> {
        serde_json::to_writer(&mut *writer,&message)?; 
        writer.write_all(b"\n")?;   
        Ok(())   
    }  
    fn reply_generator(&mut self,input:Message)->Option<Message> {   
      match input.get_payload()  {    
          Payload::Init { node_id, node_ids }  => {
              self.name = node_id.to_string(); 
              self.other_node = node_ids.to_vec();   
              let body = Body::new(Some(self.id), input.get_message_id(), Payload::InitOk); 
              let reply = Message::new(input.get_dst().to_string(), input.get_src().to_string(), body); 
              self.id+=1;
              Some(reply) 
          } 
          Payload::InitOk =>{
              None
          } 
          Payload::Echo {echo}=> { 
              let body =  Body::new(Some(self.id), input.get_message_id(),Payload::EchoOk {  echo:echo.to_string() });   
              let reply = Message::new(input.get_dst().to_string(),input.get_src().to_string(), body);                                  self.id+=1;
              Some(reply)  
          }
          Payload::EchoOk {..} => {
              None
          } 
          Payload::Generate => {
              let body =  Body::new(Some(self.id), input.get_message_id(),Payload::GenerateOk { id: format!("{}_{}",self.name,self.generate_counter) }); 
              let reply = Message::new(self.name.clone(), input.get_src().to_string(), body);
              self.generate_counter+=1;  
              Some(reply) 
          }
          Payload::GenerateOk { .. } => {
              None
          }
      }  
  }
   

}
