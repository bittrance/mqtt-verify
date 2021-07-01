use futures::future::{join, select, Either, Future, FutureExt};
use futures::stream::StreamExt;
use futures_ticker::Ticker;
use futures_timer::Delay;
use mqtt_verify::scenario;
use mqtt_verify::source;
use mqtt_verify::MessageStream;
use paho_mqtt as mqtt;
use std::any::Any;
use std::time::Duration;
use support::mosquitto::*;
use support::mqtt::client;
use support::with_timeout;

pub mod support;

struct TestingSource {}

impl source::Source for TestingSource {
    fn messages(self: Box<Self>) -> MessageStream {
        let message = mqtt::Message::new(random_topic("publisher"), "payload", 0);
        Box::pin(Ticker::new(Duration::from_millis(100)).map(move |_| Ok(message.clone())))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn make_publisher(port: u16, reconnecting: bool) -> scenario::Publisher {
    let interval = Some(Duration::from_secs(1)).filter(|_| reconnecting);
    scenario::Publisher {
        client: client(port),
        connect_options: scenario::ConnectOptions {
            connect_timeout: Duration::from_secs(1),
            reconnect_interval: interval,
        },
        sources: vec![Box::new(TestingSource {})],
    }
}

fn mosquitto_restarting() -> impl Future<Output = ()> {
    Delay::new(Duration::from_millis(1500))
        .then(|_| stop_mosquitto())
        .then(|_| Delay::new(Duration::from_secs(1)))
        .then(|_| restart_mosquitto())
        .then(|_| Delay::new(Duration::from_secs(1)))
}

#[tokio::test]
async fn publisher_stops_on_broker_restart() {
    let port = ensure_mosquitto().await;
    let publisher = make_publisher(port, false);
    let publisher = mqtt_verify::run_publisher(publisher);
    match join(
        Box::pin(with_timeout(Box::pin(publisher), Duration::from_secs(10))),
        Box::pin(Delay::new(Duration::from_millis(500)).then(|_| stop_mosquitto())),
    )
    .await
    {
        (Err(_), ()) => (),
        (Ok(_), ()) => panic!("Expected publisher to error"),
    }
}

#[tokio::test]
async fn reconnecting_publisher_survives_broker_restart() {
    let port = ensure_mosquitto().await;
    let publisher = make_publisher(port, true);
    let publisher = mqtt_verify::run_publisher(publisher);
    match select(
        Box::pin(with_timeout(Box::pin(publisher), Duration::from_secs(10))),
        Box::pin(mosquitto_restarting()),
    )
    .await
    {
        Either::Left((res, _)) => panic!("Publisher died unexpectedly with {:?}", res),
        Either::Right(_) => (),
    }
}
