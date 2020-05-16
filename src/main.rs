use evalexpr::{Context, HashMapContext, Value};
use futures::{future, stream, stream::Stream, Future};
use paho_mqtt as mqtt;
use std::time::Duration;
use structopt::StructOpt;

mod analyzers;
mod errors;
mod eval;
mod source;

use crate::source::Source;

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
}

fn client(uri: &str) -> mqtt::AsyncClient {
    let mqtt_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(uri.clone())
        .persistence(mqtt::create_options::PersistenceType::None)
        .finalize();
    mqtt::AsyncClient::new(mqtt_opts).unwrap()
}

pub type MessageStream =
    Box<dyn stream::Stream<Item = mqtt::Message, Error = errors::MqttVerifyError>>;

pub struct Scenario {
    publishers: Vec<Publisher>,
}

pub struct Publisher {
    client: mqtt::AsyncClient,
    sources: Vec<source::VerifiableSource>,
}

fn make_cli_scenario(opt: &Opt) -> Result<Scenario, errors::MqttVerifyError> {
    let mut sources = Vec::new();
    for i in 1..=opt.publishers {
        let mut context = HashMapContext::new();
        context
            .set_value("publisher".to_owned(), Value::String(format!("p-{}", i)))
            .unwrap();
        let topic = eval::ContextualValue::new(eval::precompile(&opt.topic)?, context);
        sources.push(source::VerifiableSource::new(
            format!("{}", i),
            topic,
            (opt.frequency * opt.length) as usize,
            opt.frequency,
        ));
    }
    Ok(Scenario {
        publishers: vec![Publisher {
            client: client(&opt.publish_uri),
            sources: sources,
        }],
    })
}

fn publisher_messages(mut publisher: Publisher) -> MessageStream {
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
    mut scenario: Scenario,
) -> Box<dyn Future<Item = (), Error = errors::MqttVerifyError>> {
    let mut publishers = Vec::new();
    for publisher in scenario.publishers.drain(..) {
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .clean_session(true)
            .finalize();
        let client1 = publisher.client.clone();
        let client2 = publisher.client.clone();
        let client3 = publisher.client.clone();
        let s = client1
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
        publishers.push(s);
    }
    Box::new(future::join_all(publishers).map(|_| ()))
}

fn verify(
    opt: &Opt,
    mut analyzer: Box<dyn analyzers::Analyzer>,
) -> Box<dyn Future<Item = mqtt::server_response::ServerResponse, Error = errors::MqttVerifyError>>
{
    let mut cli = client(&opt.subscribe_uri);
    let topic = opt.topic.clone();
    let cli1 = cli.clone();
    let cli2 = cli.clone();
    let result = cli
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
            cli.clone()
                .disconnect_after(Duration::from_secs(3))
                .map_err(|err| errors::MqttVerifyError::MqttDisconnectError { source: err })
        });
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .clean_session(true)
        .finalize();
    let session = cli1
        .connect(conn_opts)
        .map_err(|err| errors::MqttVerifyError::MqttConnectError { source: err })
        .and_then(move |_| {
            cli2.subscribe(topic, 0)
                .map_err(|err| errors::MqttVerifyError::MqttSubscribeError { source: err })
        });
    Box::new(session.and_then(|_| result))
}

fn main() -> Result<(), errors::MqttVerifyError> {
    let opt = Opt::from_args();
    let scenario = make_cli_scenario(&opt)?;

    future::join_all((1..=opt.publishers).map(|i| {
        let analyzer = Box::new(analyzers::SessionIdFilter::new(
            format!("{}", i),
            Box::new(analyzers::CountingAnalyzer::new(
                (opt.frequency * opt.length) as usize,
            )),
        ));
        verify(&opt, analyzer)
    }))
    .join(run_scenario(scenario))
    .wait()
    .map(|_| ())
    .unwrap_or_else(|err| {
        println!("Error: {}", err);
    });
    Ok(())
}
