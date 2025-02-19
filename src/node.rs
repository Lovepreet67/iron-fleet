use std::{collections::{HashMap, HashSet, VecDeque}, io::{Error, StdoutLock, Write}, sync::{Arc,Mutex}, usize};   
use crate::message::{Body, Message, Payload}; 

#[derive(Debug)] 
pub struct BroadcastHandler {
    remaining_broadcasted_message: Vec<Message>,
    broadcasted_messages: HashSet<usize> 
}
impl BroadcastHandler {
    pub fn new()->Self{  
        BroadcastHandler {remaining_broadcasted_message:Vec::new(),broadcasted_messages:HashSet::new()} 
    }
    pub fn broadcast_message(&mut self,message:Message){
        self.remaining_broadcasted_message.push(message);   
    }
    pub fn broadcasted_message(&mut self,message_id:usize){
        self.broadcasted_messages.insert(message_id); 
    }
} 

#[derive(Debug)] 
pub struct Node {
    id : usize,
    name : String,
    connected_to :Vec<String>, 
    generate_counter: i32, 
    msg_id:usize, 
    topology:HashMap<String,Vec<String>>,
    received_broadcast_message: HashSet<i32>,
    broadcast_handler:Arc<Mutex<BroadcastHandler>> 
}   
 
impl Node { 
    pub fn new(id:usize)->Self{  
       Node {
           id,
            name:"no_name".to_string(),
            connected_to:vec![],
            msg_id:0,
            generate_counter:0,
            topology: HashMap::new(),
            received_broadcast_message:HashSet::new(),
            broadcast_handler : Arc::new(Mutex::new(BroadcastHandler::new())) 
       }      
    }   
    pub fn step(&mut self,input:Message,writer : &mut StdoutLock)->Result<(),Error>{
        match self.reply_generator(input,writer){ 
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
    fn nodes_not_reachable_from_parrent(&self,parrent:&str)->Vec<String> {       
        let mut candidates = HashSet::<String>::from_iter(self.connected_to.clone()); 
        candidates.remove(&self.name);  
        // doing the bfs from parrent  
        let mut to_visit = VecDeque::from([parrent]);  
        let mut visited = HashSet::<String>::new();   
        visited.insert(self.name.clone()); 
         
        while let Some(curr_node) = to_visit.pop_front(){ 
                    if let Some(nodes) = self.topology.get(curr_node) {   
                        for node in nodes {
                            if visited.contains(node) {
                                continue;
                            } 
                            visited.insert(node.to_string());  
                            to_visit.push_back(node);  
                        } 
                    } 
        }
        for node in visited.into_iter(){
            candidates.remove(&node); 
        } 
        candidates.into_iter().collect() 
    } 
    fn handle_broadcast(&mut self,message:i32,received_from:&str,writer:&mut StdoutLock)->Result<(),std::io::Error>{ 
        if self.received_broadcast_message.contains(&message)
        { 
            return Ok(());
         } 
        self.received_broadcast_message.insert(message); 
        let to_send = self.nodes_not_reachable_from_parrent(received_from); 
        for node in to_send {
            //create a broadcast method
            let body = Body::new(Some(self.msg_id), None, Payload::Broadcast { message});
            let reply = Message::new(self.name.clone() , node, body);
            self.send_message(reply, writer)?; 
        }
        Ok(())  
        // now we send the message to the broadcast message to the node which are not reachable
        // from parrent  
    }

    fn reply_generator(&mut self,input:Message,writer:&mut StdoutLock)->Option<Message> {    
      match input.get_payload()  {    
          Payload::Init { node_id, node_ids}   => {
              self.name = node_id.to_string();
              self.connected_to.clone_from(node_ids);  
              let body = Body::new(Some(self.msg_id), input.get_message_id(), Payload::InitOk); 
              let reply = Message::new(input.get_dst().to_string(), input.get_src().to_string(), body); 
              self.id+=1;
              self.msg_id+=1; 
              Some(reply) 
          } 
          Payload::InitOk =>{
              None
          } 
          Payload::Echo {echo}=> { 
              let body =  Body::new(Some(self.msg_id), input.get_message_id(),Payload::EchoOk {  echo:echo.to_string() });   
              let reply = Message::new(input.get_dst().to_string(),input.get_src().to_string(), body);                                  self.id+=1;
              self.msg_id+=1;  
              Some(reply)  
          }
          Payload::EchoOk {..} => {
              None
          } 
          Payload::Generate => {
              let body =  Body::new(Some(self.msg_id), input.get_message_id(),Payload::GenerateOk { id: format!("{}_{}",self.name,self.generate_counter) }); 
              let reply = Message::new(self.name.clone(), input.get_src().to_string(), body);
              self.generate_counter+=1;  
              self.msg_id+=1; 
              Some(reply) 
          }
          Payload::GenerateOk { .. } => {
              None
          }
          Payload::Topology { topology } =>{
              for enteries in topology.iter() { 
                  self.topology.insert(enteries.0.to_string(), enteries.1.to_vec());   
              }  
              let body = Body::new(Some(self.msg_id),input.get_message_id(),Payload::TopologyOk);
              let reply = Message::new(self.name.clone(),input.get_src().to_string(), body);  
              self.msg_id+=1;
              Some(reply)   
          }
          Payload::Broadcast { message } => { 
                            let body  = Body::new(Some(self.msg_id), input.get_message_id(),Payload::BroadcastOk); 
              let reply = Message::new(self.name.clone(),input.get_src().to_string(), body); 
              self.msg_id+=1;  
              self.handle_broadcast(*message, input.get_src(),writer).unwrap();     
              Some(reply)  
          }
          Payload::BroadcastOk => {
              None
          }
          Payload::Read => {
              let body  = Body::new(Some(self.msg_id), input.get_message_id(),Payload::ReadOk { messages: self.received_broadcast_message.iter().cloned().collect()});    
              let reply = Message::new(self.name.clone(),input.get_src().to_string(), body); 
              self.msg_id+=1;  
              Some(reply)  

          }
          _ =>{
              None 
          }
      }  
  }
   

}
