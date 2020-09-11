use crate::source::Source;
use futures::{future, future::FutureExt, stream, stream::StreamExt};
use paho_mqtt as mqtt;
use std::pin::Pin;
use std::time::Duration;

pub mod analyzers;
pub mod context;
pub mod errors;
pub mod scenario;
pub mod source;

pub fn client(uri: &str) -> mqtt::AsyncClient {
    let mqtt_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(uri)
        .persistence(mqtt::create_options::PersistenceType::None)
        .finalize();
    mqtt::AsyncClient::new(mqtt_opts).unwrap()
}

pub type MessageStream = Pin<Box<dyn stream::Stream<Item = mqtt::Message>>>;

fn publisher_messages(publisher: scenario::Publisher) -> MessageStream {
    Box::pin(stream::select_all(
        publisher
            .sources
            .into_iter()
            .map(|generator| generator.messages()),
    ))
}

pub async fn run_publisher(publisher: scenario::Publisher) -> Result<(), errors::MqttVerifyError> {
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .clean_session(true)
        .finalize();
    let client1 = publisher.client.clone();
    let client2 = publisher.client.clone();
    client1
        .connect(conn_opts)
        .await
        .map_err(|err| errors::MqttVerifyError::MqttConnectError { source: err })?;
    publisher_messages(publisher)
        .for_each_concurrent(None, move |message| {
            // TODO: Yikes! Don't eat the error!
            client2.publish(message).map(|_| ())
        })
        .await;
    client1
        .disconnect_after(Duration::from_secs(3))
        .await
        .map(|_| ()) // TODO: What is this ServerResponse thing anyway?
        .map_err(|err| errors::MqttVerifyError::MqttDisconnectError { source: err })
}

pub async fn run_subscriber(
    mut subscriber: scenario::Subscriber,
) -> Result<(), errors::MqttVerifyError> {
    let mut analyzer = subscriber.sinks.remove(0);
    let mut client1 = subscriber.client.clone();
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .clean_session(true)
        .finalize();
    client1
        .connect(conn_opts)
        .await
        .map_err(|err| errors::MqttVerifyError::MqttConnectError { source: err })?;
    client1
        .subscribe_many(&subscriber.topics, &vec![0; subscriber.topics.len()])
        .await
        .map_err(|err| errors::MqttVerifyError::MqttSubscribeError { source: err })?;
    client1
        .get_stream(100)
        .take_while(|message| future::ready(message.is_some()))
        .map(move |message| analyzer.analyze(message.unwrap()))
        .take_while(|analysis| match analysis {
            Ok(analyzers::State::Continue) => future::ready(true),
            Ok(analyzers::State::Done) => future::ready(false),
            Err(err) => future::ready(false),
        })
        .fold(None::<u8>, |_, _| future::ready(None))
        .await;
    client1
        .disconnect_after(Duration::from_secs(3))
        .await
        .map(|_| ()) // TODO: What is this ServerResponse thing anyway?
        .map_err(|err| errors::MqttVerifyError::MqttDisconnectError { source: err })
}

pub async fn run_scenario(mut scenario: scenario::Scenario) -> () {
    let publishers = future::join_all(
        scenario
            .publishers
            .drain(..)
            .map(|publisher| run_publisher(publisher)),
    );
    let subscribers = future::join_all(
        scenario
            .subscribers
            .drain(..)
            .map(|subscriber| run_subscriber(subscriber)),
    );
    // TODO: Probably a FuturesUnordered?
    future::join(publishers, subscribers).map(|_| ()).await
}
