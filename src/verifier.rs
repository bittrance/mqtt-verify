use crate::errors;
use paho_mqtt as mqtt;

#[derive(Debug, PartialEq)]
pub enum State {
    Healthy,
    Done,
}

pub trait Verifier {
    fn verify(&mut self, message: mqtt::Message) -> Result<State, errors::MqttVerifyError>;
}

pub struct SessionIdFilter {
    id: String,
    child: Box<dyn Verifier>,
}

impl SessionIdFilter {
    pub fn new(mut id: String, child: Box<dyn Verifier>) -> Self {
        id.push(':');
        Self { id, child }
    }
}

impl Verifier for SessionIdFilter {
    fn verify(&mut self, message: mqtt::Message) -> Result<State, errors::MqttVerifyError> {
        if message.payload_str().starts_with(&self.id) {
            self.child.verify(message)
        } else {
            Ok(State::Healthy)
        }
    }
}

pub struct CountingVerifier {
    count: usize,
    expected_total: usize,
}

impl CountingVerifier {
    pub fn new(total_count: usize) -> Self {
        Self {
            count: 0,
            expected_total: total_count,
        }
    }
}

impl Verifier for CountingVerifier {
    fn verify(&mut self, _message: mqtt::Message) -> Result<State, errors::MqttVerifyError> {
        self.count += 1;
        if self.count > self.expected_total {
            Err(errors::MqttVerifyError::VerificationFailure {
                reason: format!("Expected only {} messages", self.expected_total),
            })
        } else if self.count == self.expected_total {
            Ok(State::Done)
        } else {
            Ok(State::Healthy)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{State, Verifier};
    use crate::errors;
    use paho_mqtt as mqtt;

    struct DoneVerifier;
    impl super::Verifier for DoneVerifier {
        fn verify(&mut self, _message: mqtt::Message) -> Result<State, errors::MqttVerifyError> {
            Ok(State::Done)
        }
    }

    #[test]
    fn session_id_filter() {
        let child = Box::new(DoneVerifier {});
        let mut filter = super::SessionIdFilter::new("foo".to_owned(), child);
        let processed_message = mqtt::Message::new("ze-topic", "foo:...", 0);
        let ignored_message = mqtt::Message::new("ze-topic", "bar:...", 0);
        assert_eq!(State::Done, filter.verify(processed_message).unwrap());
        assert_eq!(State::Healthy, filter.verify(ignored_message).unwrap());
    }

    #[test]
    fn session_id_filter_prefix() {
        let child = Box::new(DoneVerifier {});
        let mut filter = super::SessionIdFilter::new("foo".to_owned(), child);
        let ignored_message = mqtt::Message::new("ze-topic", "foobar:...", 0);
        assert_eq!(State::Healthy, filter.verify(ignored_message).unwrap());
    }

    #[test]
    fn counting_verifier() {
        let mut verifier = super::CountingVerifier::new(3);
        let message = mqtt::Message::new("ze-topic", "message", 0);
        assert_eq!(State::Healthy, verifier.verify(message.clone()).unwrap());
        assert_eq!(State::Healthy, verifier.verify(message.clone()).unwrap());
        assert_eq!(State::Done, verifier.verify(message.clone()).unwrap());
        match verifier.verify(mqtt::Message::new("ze-topic", "1", 0)) {
            Err(errors::MqttVerifyError::VerificationFailure { reason: _ }) => (),
            _ => panic!("Expected a verification failure"),
        };
    }
}
