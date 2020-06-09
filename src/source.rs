use crate::context::ContextualValue;
use crate::errors;
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
    pub topic: ContextualValue,
    seq_no: Cell<usize>,
    total_count: usize,
    frequency: f32,
}

impl VerifiableSource {
    pub fn new(id: String, topic: ContextualValue, total_count: usize, frequency: f32) -> Self {
        Self {
            id,
            topic,
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
            let message = format!("{}:{}/{}", self.id, self.seq_no.get(), self.total_count);
            Some(mqtt::Message::new(self.topic.value(), message, 0))
        }
    }
}

impl Source for VerifiableSource {
    fn messages(self) -> crate::MessageStream {
        Box::new(
            Interval::new(Duration::from_micros(
                (1_000_000f32 / self.frequency) as u64,
            ))
            .map_err(|err| errors::MqttVerifyError::SourceTimerError { source: err })
            .map(move |_| self.next_message())
            .take_while(|message| future::ok(message.is_some()))
            .map(|message| message.unwrap()),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::context::{ContextualValue, OverlayContext};
    use evalexpr::build_operator_tree;

    #[test]
    fn verifiable_source_topic() {
        let topic = ContextualValue::new(
            build_operator_tree("\"ze-topic\"").unwrap(),
            OverlayContext::root(),
        );
        let source = super::VerifiableSource::new("id".to_owned(), topic, 1, 1.0);
        assert_eq!("ze-topic", source.next_message().unwrap().topic());
    }

    #[test]
    fn verifiable_source_iteration() {
        let topic = ContextualValue::new(
            build_operator_tree("\"ze-topic\"").unwrap(),
            OverlayContext::root(),
        );
        let source = super::VerifiableSource::new("id".to_owned(), topic, 2, 1.0);
        assert_eq!("id:1/2", source.next_message().unwrap().payload_str());
        assert_eq!("id:2/2", source.next_message().unwrap().payload_str());
        assert!(source.next_message().is_none());
    }
}
