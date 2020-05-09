use paho_mqtt as mqtt;

pub trait MessageGenerator {
    fn next_message(&mut self) -> Option<mqtt::Message>;
}

pub struct VerifiableMessageGenerator {
    id: String,
    seq_no: usize,
    total_count: usize,
}

impl VerifiableMessageGenerator {
    pub fn new(id: String, total_count: usize) -> Self {
        Self {
            id: id,
            seq_no: 0,
            total_count: total_count,
        }
    }

    pub fn topic_name(&self) -> String {
        "testo".to_owned()
    }
}

impl MessageGenerator for VerifiableMessageGenerator {
    fn next_message(&mut self) -> Option<mqtt::Message> {
        if self.seq_no >= self.total_count {
            None
        } else {
            self.seq_no += 1;
            let topic = self.topic_name();
            let message = format!("{}:{}/{}", self.id, self.seq_no, self.total_count);
            Some(mqtt::Message::new(topic, message, 0))
        }
    }
}

#[test]
fn verifiable_message_generator() {
    let mut generator = VerifiableMessageGenerator::new("id".to_owned(), 2);
    assert_eq!("id:1/2", generator.next_message().unwrap().payload_str());
    assert_eq!("id:2/2", generator.next_message().unwrap().payload_str());
    assert!(generator.next_message().is_none());
}
