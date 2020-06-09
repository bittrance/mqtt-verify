use crate::analyzers;
use crate::source;
use paho_mqtt as mqtt;

pub struct Scenario {
    pub publishers: Vec<Publisher>,
    pub subscribers: Vec<Subscriber>,
}

pub struct Publisher {
    pub client: mqtt::AsyncClient,
    pub sources: Vec<source::VerifiableSource>,
}

pub struct Subscriber {
    pub client: mqtt::AsyncClient,
    pub topics: Vec<String>,
    pub sinks: Vec<Box<dyn analyzers::Analyzer>>,
}
