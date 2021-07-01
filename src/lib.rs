use async_std::task;
use futures::{
    channel::mpsc, future, future::TryFutureExt, stream, stream::StreamExt, stream::TryStreamExt,
};
use paho_mqtt as mqtt;
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
            .map(|source| source.messages()),
    ))
}

async fn connect(
    client: &mqtt::AsyncClient,
    options: &dyn scenario::AsConnectOptions,
) -> Result<(), errors::MqttVerifyError> {
    let conn_opts = options.as_connect_options();
    let deadline = Instant::now() + options.initial_timeout();
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
    let should_reconnect = publisher.connect_options.reconnect_interval.is_some();
    connect(&client, &publisher).await?;
    publisher_messages(publisher)
        .try_for_each_concurrent(None, |message| {
            let client2 = client.clone();
            async move {
                let result = client2.publish(message).await;
                match result {
                    Ok(()) => Ok(()),
                    Err(_) if should_reconnect => Ok(()),
                    Err(err) => Err(errors::MqttVerifyError::MqttPublishError { source: err }),
                }
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
    connect(&client, &subscriber).await?;
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

    Box::pin(results.collect::<stream::FuturesUnordered<_>>().fuse())
}
