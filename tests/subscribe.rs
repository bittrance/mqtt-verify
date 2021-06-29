use futures::{
    future::{join, select, Either},
    FutureExt,
};
use futures_timer::Delay;
use mqtt_verify::analyzers;
use mqtt_verify::errors;
use mqtt_verify::scenario;
use paho_mqtt as mqtt;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};
use support::mosquitto::*;
use support::mqtt::*;
use support::with_timeout;

pub mod support;

pub struct TestingSink {
    pub received: Rc<RefCell<Vec<mqtt::Message>>>,
    pub expected_count: u16,
}

impl analyzers::Analyzer for TestingSink {
    fn analyze(
        &mut self,
        message: mqtt::Message,
    ) -> Result<analyzers::State, errors::MqttVerifyError> {
        self.received.borrow_mut().push(message);
        self.expected_count -= 1;
        if self.expected_count == 0 {
            Ok(analyzers::State::Done)
        } else {
            Ok(analyzers::State::Continue)
        }
    }
}

fn make_subscriber(
    port: u16,
    topic_name: String,
    expected_count: u16,
) -> (scenario::Subscriber, Rc<RefCell<Vec<mqtt::Message>>>) {
    let received = Rc::new(RefCell::new(Vec::new()));
    let sink = TestingSink {
        received: received.clone(),
        expected_count,
    };
    let subscriber = scenario::Subscriber {
        client: client(port),
        connect_options: scenario::ConnectOptions {
            connect_timeout: Duration::from_secs(1),
            reconnect_interval: Some(Duration::from_secs(1)),
        },
        topics: vec![topic_name],
        sinks: vec![Box::new(sink)],
    };
    (subscriber, received)
}

#[tokio::test]
async fn terminate_when_analyzer_done() {
    let port = ensure_mosquitto().await;
    let topic_name = random_topic("terminate_when_analyzer_done");
    let (subscriber, received) = make_subscriber(port, topic_name.clone(), 2);
    let subscriber = mqtt_verify::run_subscriber(subscriber);
    let scenario = Delay::new(Duration::from_millis(1200)) // Weird synchronization here; delays below 1050 fails occasionally with subscribe delayed 2 ms after publish
        .then(|_| publish_message(port, mqtt::Message::new(topic_name.clone(), "payload", 0)))
        .then(|_| publish_message(port, mqtt::Message::new(topic_name.clone(), "payload", 0)));
    let (s_err, p_err) = join(
        with_timeout(Box::pin(subscriber), Duration::from_secs(5)),
        scenario,
    )
    .await;
    s_err.unwrap();
    p_err.unwrap();
    assert_eq!(2, received.borrow().len());
}

#[tokio::test]
async fn connection_timeout() {
    let (subscriber, _) = make_subscriber(9, "ignored".to_owned(), 0);
    let start_time = Instant::now();
    match select(
        Box::pin(mqtt_verify::run_subscriber(subscriber)),
        Box::pin(Delay::new(Duration::from_secs(5))),
    )
    .await
    {
        Either::Left(_) if Instant::now() - start_time >= Duration::from_millis(100) => (),
        Either::Left(_) => panic!("Subscriber gave up too quickly"),
        Either::Right(_) => panic!("Timout waiting for timeout"),
    }
}

#[tokio::test]
async fn subscriber_reconnects() {
    let port = ensure_mosquitto().await;
    let topic_name = random_topic("subscriber_reconnects");
    let (subscriber, received) = make_subscriber(port, topic_name.clone(), 2);
    let subscriber = mqtt_verify::run_subscriber(subscriber);
    let scenario = Delay::new(Duration::from_millis(1500))
        .then(|_| publish_message(port, mqtt::Message::new(topic_name.clone(), "before", 0)))
        .then(|_| stop_mosquitto())
        .then(|_| Delay::new(Duration::from_secs(2)))
        .then(|_| restart_mosquitto())
        .then(|_| Delay::new(Duration::from_secs(4)))
        .then(|_| publish_message(port, mqtt::Message::new(topic_name.clone(), "after", 0)));
    let (s_err, p_err) = join(
        with_timeout(Box::pin(subscriber), Duration::from_secs(20)),
        scenario,
    )
    .await;
    s_err.unwrap();
    p_err.unwrap();
    assert_eq!(2, received.borrow().len());
}
