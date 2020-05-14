use futures::{future, stream::Stream};
use futures_timer::Interval;
use paho_mqtt as mqtt;
use std::cell::Cell;
use std::time::Duration;

pub trait Source {
    fn messages(self) -> crate::MessageStream;
}

pub struct VerifiableSource {
    id: String,
    seq_no: Cell<usize>,
    total_count: usize,
    frequency: f32,
}

impl VerifiableSource {
    pub fn new(id: String, total_count: usize, frequency: f32) -> Self {
        Self {
            id,
            seq_no: Cell::new(0),
            total_count,
            frequency,
        }
    }

    pub fn next_message(&self) -> Option<mqtt::Message> {
        if self.seq_no.get() >= self.total_count {
            None
        } else {
            self.seq_no.set(self.seq_no.get() + 1);
            let topic = "testo".to_owned();
            let message = format!("{}:{}/{}", self.id, self.seq_no.get(), self.total_count);
            Some(mqtt::Message::new(topic, message, 0))
        }
    }
}

impl Source for VerifiableSource {
    fn messages(self) -> crate::MessageStream {
        Box::new(
            Interval::new(Duration::from_micros(
                (1_000_000f32 / self.frequency) as u64,
            ))
            .map_err(|err| crate::MqttVerifyError::SourceTimerError { source: err })
            .map(move |_| self.next_message())
            .take_while(|message| future::ok(message.is_some()))
            .map(|message| message.unwrap()),
        )
    }
}

#[test]
fn verifiable_source_iteration() {
    let source = VerifiableSource::new("id".to_owned(), 2, 1.0);
    assert_eq!("id:1/2", source.next_message().unwrap().payload_str());
    assert_eq!("id:2/2", source.next_message().unwrap().payload_str());
    assert!(source.next_message().is_none());
}
