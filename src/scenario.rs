use crate::analyzers;
use crate::source;
use paho_mqtt as mqtt;
use std::time::Duration;

pub trait AsConnectOptions {
    fn as_connect_options(&self) -> mqtt::ConnectOptions;
    fn initial_timeout(&self) -> Duration;
}

pub struct Scenario {
    pub publishers: Vec<Publisher>,
    pub subscribers: Vec<Subscriber>,
}

pub struct Publisher {
    pub client: mqtt::AsyncClient,
    pub connect_options: ConnectOptions,
    pub sources: Vec<source::VerifiableSource>,
}

pub struct Subscriber {
    pub client: mqtt::AsyncClient,
    pub connect_options: ConnectOptions,
    pub topics: Vec<String>,
    pub sinks: Vec<Box<dyn analyzers::Analyzer>>,
}

pub struct ConnectOptions {
    pub connect_timeout: Duration,
    pub reconnect_interval: Option<Duration>,
}

fn as_connect_options(connect_options: &ConnectOptions) -> mqtt::ConnectOptions {
    let mut builder = mqtt::ConnectOptionsBuilder::new();
    builder.clean_session(true);
    builder.connect_timeout(connect_options.connect_timeout);
    if let Some(reconnect_interval) = connect_options.reconnect_interval {
        builder.automatic_reconnect(reconnect_interval, reconnect_interval);
    }
    builder.finalize()
}

impl AsConnectOptions for Publisher {
    fn as_connect_options(&self) -> mqtt::ConnectOptions {
        as_connect_options(&self.connect_options)
    }

    fn initial_timeout(&self) -> Duration {
        self.connect_options.connect_timeout
    }
}

impl AsConnectOptions for Subscriber {
    fn as_connect_options(&self) -> mqtt::ConnectOptions {
        as_connect_options(&self.connect_options)
    }

    fn initial_timeout(&self) -> Duration {
        self.connect_options.connect_timeout
    }
}
