use crate::errors;
use paho_mqtt as mqtt;
use std::cmp::Ordering;

#[derive(Debug, PartialEq)]
pub enum State {
    Continue,
    Done,
}

pub trait Analyzer {
    fn analyze(&mut self, message: mqtt::Message) -> Result<State, errors::MqttVerifyError>;
}

pub struct SessionIdFilter {
    id: String,
    child: Box<dyn Analyzer>,
}

impl SessionIdFilter {
    pub fn new(mut id: String, child: Box<dyn Analyzer>) -> Self {
        id.push(':');
        Self { id, child }
    }
}

impl Analyzer for SessionIdFilter {
    fn analyze(&mut self, message: mqtt::Message) -> Result<State, errors::MqttVerifyError> {
        if message.payload_str().starts_with(&self.id) {
            self.child.analyze(message)
        } else {
            Ok(State::Continue)
        }
    }
}

pub struct CountingAnalyzer {
    count: usize,
    expected_total: usize,
}

impl CountingAnalyzer {
    pub fn new(total_count: usize) -> Self {
        Self {
            count: 0,
            expected_total: total_count,
        }
    }
}

impl Analyzer for CountingAnalyzer {
    fn analyze(&mut self, _message: mqtt::Message) -> Result<State, errors::MqttVerifyError> {
        self.count += 1;
        match self.count.cmp(&self.expected_total) {
            Ordering::Greater => Err(errors::MqttVerifyError::VerificationFailure {
                reason: format!("Expected only {} messages", self.expected_total),
            }),
            Ordering::Equal => Ok(State::Done),
            Ordering::Less => Ok(State::Continue),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Analyzer, State};
    use crate::errors;
    use paho_mqtt as mqtt;

    struct DoneAnalyzer;
    impl super::Analyzer for DoneAnalyzer {
        fn analyze(&mut self, _message: mqtt::Message) -> Result<State, errors::MqttVerifyError> {
            Ok(State::Done)
        }
    }

    #[test]
    fn session_id_filter() {
        let child = Box::new(DoneAnalyzer {});
        let mut filter = super::SessionIdFilter::new("foo".to_owned(), child);
        let processed_message = mqtt::Message::new("ze-topic", "foo:...", 0);
        let ignored_message = mqtt::Message::new("ze-topic", "bar:...", 0);
        assert_eq!(State::Done, filter.analyze(processed_message).unwrap());
        assert_eq!(State::Continue, filter.analyze(ignored_message).unwrap());
    }

    #[test]
    fn session_id_filter_prefix() {
        let child = Box::new(DoneAnalyzer {});
        let mut filter = super::SessionIdFilter::new("foo".to_owned(), child);
        let ignored_message = mqtt::Message::new("ze-topic", "foobar:...", 0);
        assert_eq!(State::Continue, filter.analyze(ignored_message).unwrap());
    }

    #[test]
    fn counting_analyzer() {
        let mut analyzer = super::CountingAnalyzer::new(3);
        let message = mqtt::Message::new("ze-topic", "message", 0);
        assert_eq!(State::Continue, analyzer.analyze(message.clone()).unwrap());
        assert_eq!(State::Continue, analyzer.analyze(message.clone()).unwrap());
        assert_eq!(State::Done, analyzer.analyze(message.clone()).unwrap());
        match analyzer.analyze(mqtt::Message::new("ze-topic", "1", 0)) {
            Err(errors::MqttVerifyError::VerificationFailure { reason: _ }) => (),
            _ => panic!("Expected a verification failure"),
        };
    }
}
