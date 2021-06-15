use crate::source::Source;
use async_std::task;
use futures::{
    channel::mpsc, future, future::TryFutureExt, stream, stream::StreamExt, stream::TryStreamExt,
};
use paho_mqtt as mqtt;
use std::cmp;
use std::iter::FromIterator;
use std::pin::Pin;
use std::time::{Duration, Instant};

pub mod analyzers;
pub mod context;
pub mod errors;
pub mod scenario;
pub mod source;

trait EventStream {
    fn eventstream(&mut self) -> Pin<Box<dyn stream::Stream<Item = mqtt::AsyncClient> + Send>>;
}

impl EventStream for mqtt::AsyncClient {
    fn eventstream(&mut self) -> Pin<Box<dyn stream::Stream<Item = mqtt::AsyncClient> + Send>> {
        let (tx, rx) = mpsc::unbounded();
        self.set_connected_callback(move |client| {
            tx.unbounded_send(client.clone()).unwrap();
        });
        Box::pin(rx)
    }
}

pub fn client(uri: &str) -> mqtt::AsyncClient {
    let mqtt_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(uri)
        .persistence(mqtt::create_options::PersistenceType::None)
        .finalize();
    mqtt::AsyncClient::new(mqtt_opts).unwrap()
}

pub type MessageStream =
    Pin<Box<dyn stream::Stream<Item = Result<mqtt::Message, errors::MqttVerifyError>>>>;

fn publisher_messages(publisher: scenario::Publisher) -> MessageStream {
    Box::pin(stream::select_all(
        publisher
            .sources
            .into_iter()
            .map(|generator| generator.messages()),
    ))
}

async fn connect(
    client: &mqtt::AsyncClient,
    timeout: &Duration,
) -> Result<(), errors::MqttVerifyError> {
    let ref max_interval = Duration::from_secs(1);
    let interval = cmp::min(timeout, max_interval);
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .clean_session(true)
        .connect_timeout(*interval)
        .automatic_reconnect(*interval, *interval)
        .finalize();

    let deadline = Instant::now() + *timeout;
    loop {
        match client.connect(conn_opts.clone()).await {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() < deadline => continue,
            Err(err) => return Err(errors::MqttVerifyError::MqttConnectError { source: err }),
        }
    }
}

pub async fn run_publisher(publisher: scenario::Publisher) -> Result<(), errors::MqttVerifyError> {
    let client = publisher.client.clone();
    connect(&client, &publisher.initial_timeout).await?;
    publisher_messages(publisher)
        .try_for_each_concurrent(None, |message| {
            let client2 = client.clone();
            async move {
                client2
                    .publish(message)
                    .await
                    .map_err(|err| errors::MqttVerifyError::MqttPublishError { source: err })
            }
        })
        .await?;
    client
        .disconnect_after(Duration::from_secs(3))
        .await
        .map(|_| ()) // TODO: What is this ServerResponse thing anyway?
        .map_err(|err| errors::MqttVerifyError::MqttDisconnectError { source: err })
}

pub async fn run_subscriber(
    mut subscriber: scenario::Subscriber,
) -> Result<(), errors::MqttVerifyError> {
    let mut analyzer = subscriber.sinks.remove(0);
    let mut client = subscriber.client.clone();
    let topics = subscriber.topics.clone();
    task::spawn(client.eventstream().map(Ok).try_for_each(move |client| {
        client
            .subscribe_many(&topics, &vec![0; topics.len()])
            .map_ok(|_| ())
            .map_err(|err| errors::MqttVerifyError::MqttSubscribeError { source: err })
    }));
    connect(&client, &subscriber.initial_timeout).await?;
    let mut messages = client.get_stream(100);
    while let Some(message) = messages.next().await {
        if let Some(message) = message {
            match analyzer.analyze(message)? {
                analyzers::State::Continue => (),
                analyzers::State::Done => break,
            }
        }
    }
    client
        .disconnect_after(Duration::from_secs(3))
        .await
        .map(|_| ()) // TODO: What is this ServerResponse thing anyway?
        .map_err(|err| errors::MqttVerifyError::MqttDisconnectError { source: err })
}

pub fn run_scenario(
    mut scenario: scenario::Scenario,
) -> Pin<Box<dyn stream::Stream<Item = Result<(), errors::MqttVerifyError>>>> {
    type FutureResult = Pin<Box<dyn future::Future<Output = Result<(), errors::MqttVerifyError>>>>;
    let results = scenario
        .publishers
        .drain(..)
        .map(|publisher| Box::pin(Box::pin(run_publisher(publisher))) as FutureResult)
        .chain(
            scenario
                .subscribers
                .drain(..)
                .map(|subscriber| Box::pin(run_subscriber(subscriber)) as FutureResult),
        );

    Box::pin(stream::FuturesUnordered::from_iter(results).fuse())
}
