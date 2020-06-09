use futures::{future, stream, stream::Stream, Future};
use paho_mqtt as mqtt;
use std::time::Duration;
use structopt::StructOpt;

mod analyzers;
mod context;
mod errors;
mod scenario;
mod source;

use crate::source::Source;

fn split_on_equal(input: &str) -> Result<(String, String), errors::MqttVerifyError> {
    let pair: Vec<&str> = input.splitn(2, '=').collect();
    if pair.len() == 2 {
        Ok((pair[0].to_owned(), pair[1].to_owned()))
    } else {
        Err(errors::MqttVerifyError::MalformedParameter {
            input: input.to_owned(),
        })
    }
}

#[derive(StructOpt, Debug)]
#[structopt()]
pub struct Opt {
    /// URI to publish messages to
    #[structopt(long = "publish-uri", env = "PUBLISH_URI")]
    publish_uri: String,
    /// Number of parallel publishers
    #[structopt(long = "publishers", env = "PUBLISHERS", default_value = "1")]
    publishers: u64,
    /// Frequency (Hz) messages messages per session
    #[structopt(long = "frequency", env = "FREQUENCY", default_value = "1.0")]
    frequency: f32,
    /// Session length in seconds
    #[structopt(long = "length", env = "LENGTH", default_value = "10.0")]
    length: f32,
    /// Topic to publish to
    #[structopt(long = "topic", env = "TOPIC", default_value = "1")]
    topic: String,
    /// URI to verify messages from
    #[structopt(long = "subscribe-uri", env = "PUBLISH_URI")]
    subscribe_uri: String,
    /// Parameter for expansion
    #[structopt(long = "parameter", parse(try_from_str = split_on_equal))]
    parameters: Vec<(String, String)>,
}

pub fn client(uri: &str) -> mqtt::AsyncClient {
    let mqtt_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(uri)
        .persistence(mqtt::create_options::PersistenceType::None)
        .finalize();
    mqtt::AsyncClient::new(mqtt_opts).unwrap()
}

pub type MessageStream =
    Box<dyn stream::Stream<Item = mqtt::Message, Error = errors::MqttVerifyError>>;

fn publisher_messages(mut publisher: scenario::Publisher) -> MessageStream {
    // FIXME: Merging a large number of streams like this is probably quite inefficient since it
    // creates a chain of Select objects, but until paho_mqtt supports futures 0.3 with proper
    // multi-select, we'll have to live with it.
    publisher
        .sources
        .drain(..)
        .map(|generator| generator.messages())
        .fold(Box::new(stream::empty()), |acc, stream| {
            Box::new(stream.select(acc))
        })
}

fn run_scenario(
    mut scenario: scenario::Scenario,
) -> Box<dyn Future<Item = (), Error = errors::MqttVerifyError>> {
    let mut actors = Vec::new();
    for publisher in scenario.publishers.drain(..) {
        actors.push(run_publisher(publisher));
    }
    for subscriber in scenario.subscribers.drain(..) {
        actors.push(run_subscriber(subscriber));
    }
    Box::new(future::join_all(actors).map(|_| ()))
}

fn run_publisher(
    publisher: scenario::Publisher,
) -> Box<dyn Future<Item = (), Error = errors::MqttVerifyError>> {
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .clean_session(true)
        .finalize();
    let client1 = publisher.client.clone();
    let client2 = publisher.client.clone();
    let client3 = publisher.client.clone();
    let session = client1
        .connect(conn_opts)
        .map_err(|err| errors::MqttVerifyError::MqttConnectError { source: err })
        .and_then(move |_| {
            publisher_messages(publisher).for_each(move |message| {
                client2
                    .publish(message)
                    .map_err(|err| errors::MqttVerifyError::MqttPublishError { source: err })
            })
        })
        .and_then(move |_| {
            client3
                .disconnect_after(Duration::from_secs(3))
                .map_err(|err| errors::MqttVerifyError::MqttDisconnectError { source: err })
        });
    Box::new(session.map(|_| ()))
}

fn run_subscriber(
    mut subscriber: scenario::Subscriber,
) -> Box<dyn Future<Item = (), Error = errors::MqttVerifyError>> {
    let mut analyzer = subscriber.sinks.remove(0);
    let mut client1 = subscriber.client.clone();
    let client2 = subscriber.client.clone();
    let client3 = subscriber.client.clone();
    let client4 = subscriber.client.clone();
    let stream = client1
        .get_stream(100)
        .map_err(|_| errors::MqttVerifyError::VerificationFailure {
            reason: "stream broke".to_owned(),
        })
        .take_while(|message| future::ok(message.is_some()))
        .map(move |message| analyzer.analyze(message.unwrap()))
        .and_then(|analysis| match analysis {
            Ok(state) => future::ok(state),
            Err(err) => future::err(err),
        })
        .take_while(|analysis| match analysis {
            analyzers::State::Continue => future::ok(true),
            analyzers::State::Done => future::ok(false),
        })
        .for_each(|_| future::ok(()))
        .and_then(move |_| {
            client2
                .disconnect_after(Duration::from_secs(3))
                .map_err(|err| errors::MqttVerifyError::MqttDisconnectError { source: err })
        });
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .clean_session(true)
        .finalize();
    let session = client3
        .connect(conn_opts)
        .map_err(|err| errors::MqttVerifyError::MqttConnectError { source: err })
        .and_then(move |_| {
            client4
                .subscribe_many(&subscriber.topics, &vec![0; subscriber.topics.len()])
                .map_err(|err| errors::MqttVerifyError::MqttSubscribeError { source: err })
        })
        .and_then(|_| stream);
    Box::new(session.map(|_| ()))
}

fn main() -> Result<(), errors::MqttVerifyError> {
    let opt = Opt::from_args();
    let scenario = scenario::make_cli_scenario(&opt)?;

    run_scenario(scenario)
        .wait()
        .map(|_| ())
        .unwrap_or_else(|err| {
            println!("Error: {}", err);
        });
    Ok(())
}
