use futures::{future, stream::Stream, Future};
use futures_timer::Interval;
use paho_mqtt as mqtt;
use snafu::Snafu;
use std::time::Duration;
use structopt::StructOpt;

mod source;

use crate::source::MessageGenerator;

#[derive(StructOpt, Debug)]
#[structopt()]
pub struct Opt {
    /// URI to publish messages to
    #[structopt(long = "publish-uri", env = "PUBLISH_URI")]
    publish_uri: String,
    /// Number of parallel sessions
    #[structopt(long = "sessions", env = "SESSIONS", default_value = "1")]
    sessions: u64,
    /// Frequency (Hz) messages messages per session
    #[structopt(long = "frequency", env = "FREQUENCY", default_value = "1.0")]
    frequency: f32,
    /// Session length in seconds
    #[structopt(long = "length", env = "LENGTH", default_value = "10.0")]
    length: f32,
}

#[derive(Debug, Snafu)]
pub enum MqttVerifyError {
    #[snafu(display("Timer borked: {}", source))]
    SourceTimerError { source: std::io::Error },
    #[snafu(display("Connect borked: {}", source))]
    MqttConnectError {
        source: paho_mqtt::errors::MqttError,
    },
    #[snafu(display("Disconnect borked: {}", source))]
    MqttDisconnectError {
        source: paho_mqtt::errors::MqttError,
    },
    #[snafu(display("Publish borked: {}", source))]
    MqttPublishError {
        source: paho_mqtt::errors::MqttError,
    },
}

fn client(uri: &str) -> mqtt::AsyncClient {
    let mqtt_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(uri.clone())
        .persistence(mqtt::create_options::PersistenceType::None)
        .finalize();
    mqtt::AsyncClient::new(mqtt_opts).unwrap()
}

fn session(
    opt: &Opt,
    mut generator: source::VerifiableMessageGenerator,
) -> Box<dyn Future<Item = (), Error = MqttVerifyError>> {
    let frequency = opt.frequency;
    let cli = client(&opt.publish_uri);
    let cli1 = cli.clone();
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .clean_session(true)
        .finalize();
    let session = cli
        .connect(conn_opts)
        .map_err(|err| MqttVerifyError::MqttConnectError { source: err })
        .and_then(move |_| {
            Interval::new(Duration::from_micros((1_000_000f32 / frequency) as u64))
                .map_err(|err| MqttVerifyError::SourceTimerError { source: err })
                .map(move |_| generator.next_message())
                .take_while(|message| future::ok(message.is_some()))
                .and_then(move |message| {
                    cli.publish(message.unwrap())
                        .map_err(|err| MqttVerifyError::MqttPublishError { source: err })
                })
                .for_each(|_| future::ok(()))
        })
        .and_then(move |_| {
            cli1.disconnect_after(Duration::from_secs(3))
                .map_err(|err| MqttVerifyError::MqttDisconnectError { source: err })
        })
        .map(|_| ());
    Box::new(session)
}

fn main() {
    let opt = Opt::from_args();
    future::join_all((1..=opt.sessions).map(|i| {
        let generator = source::VerifiableMessageGenerator::new(
            format!("{}", i),
            (opt.frequency * opt.length) as usize,
        );
        session(&opt, generator)
    }))
    .wait()
    .map(|_| ())
    .unwrap_or_else(|err| {
        println!("Error: {}", err);
    });
}
