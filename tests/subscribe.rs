use bollard::container::{
    APIContainers, Config, CreateContainerOptions, HostConfig, ListContainersOptions,
    StartContainerOptions,
};
use bollard::Docker;
use futures::{future::join, FutureExt};
use futures_timer::Delay;
use mqtt_verify::analyzers;
use mqtt_verify::errors;
use mqtt_verify::scenario;
use paho_mqtt as mqtt;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::cell::RefCell;
use std::collections::HashMap;
use std::default::Default;
use std::rc::Rc;
use std::time::Duration;

const MOSQUITTO_NAME: &str = "mqtt-verify-mosquitto";

fn random_topic(prefix: &str) -> String {
    let rand_string: String = thread_rng().sample_iter(&Alphanumeric).take(30).collect();
    format!("{}/{}", prefix, rand_string)
}

async fn find_mosquitto(docker: &Docker) -> Option<APIContainers> {
    let mut filters = HashMap::new();
    filters.insert("name", vec![MOSQUITTO_NAME]);
    filters.insert("status", vec!["running"]);
    let options = Some(ListContainersOptions {
        all: true,
        filters: filters,
        ..Default::default()
    });
    docker
        .list_containers(options)
        .await
        .unwrap()
        .drain(..)
        .next()
}

async fn ensure_mosquitto() -> u16 {
    let docker = Docker::connect_with_local_defaults().unwrap();
    let mut mosquitto = find_mosquitto(&docker).await;
    if mosquitto.is_none() {
        let options = Some(CreateContainerOptions {
            name: MOSQUITTO_NAME,
        });
        let config = Config {
            image: Some("eclipse-mosquitto:latest"),
            host_config: Some(HostConfig {
                publish_all_ports: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };
        docker.create_container(options, config).await.unwrap();
        docker
            .start_container(MOSQUITTO_NAME, None::<StartContainerOptions<String>>)
            .await
            .unwrap();
        mosquitto = find_mosquitto(&docker).await
    }
    mosquitto
        .unwrap()
        .ports
        .iter()
        .find(|p| p.private_port == 1883)
        .unwrap()
        .public_port
        .unwrap() as u16
}

pub fn client(port: u16) -> mqtt::AsyncClient {
    let mqtt_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(format!("tcp://localhost:{}", port))
        .persistence(mqtt::create_options::PersistenceType::None)
        .finalize();
    mqtt::AsyncClient::new(mqtt_opts).unwrap()
}

async fn publish_message(port: u16, message: mqtt::Message) -> Result<(), mqtt::errors::Error> {
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .clean_session(true)
        .finalize();
    let client = client(port);
    client.connect(conn_opts).await?;
    client.publish(message).await?;
    client.disconnect_after(Duration::from_secs(3)).await?;
    Ok(())
}

pub struct TestingSink {
    pub received: Rc<RefCell<Vec<mqtt::Message>>>,
}

impl analyzers::Analyzer for TestingSink {
    fn analyze(
        &mut self,
        message: mqtt::Message,
    ) -> Result<analyzers::State, errors::MqttVerifyError> {
        self.received.borrow_mut().push(message);
        Ok(analyzers::State::Done)
    }
}

fn make_subscriber(
    port: u16,
    topic_name: String,
) -> (scenario::Subscriber, Rc<RefCell<Vec<mqtt::Message>>>) {
    let received = Rc::new(RefCell::new(Vec::new()));
    let sink = TestingSink {
        received: received.clone(),
    };
    let subscriber = scenario::Subscriber {
        client: client(port),
        topics: vec![topic_name],
        sinks: vec![Box::new(sink)],
    };
    (subscriber, received)
}

#[tokio::test]
async fn terminate_when_analyzer_done() {
    let port = ensure_mosquitto().await;
    let topic_name = random_topic("terminate_when_analyzer_done");
    let (subscriber, received) = make_subscriber(port, topic_name.clone());
    let subscriber = mqtt_verify::run_subscriber(subscriber);
    let publish = Delay::new(Duration::from_millis(100))
        .then(|_| publish_message(port, mqtt::Message::new(topic_name, "payload", 0)));
    let (s_err, p_err) = join(subscriber, publish).await;
    s_err.unwrap();
    p_err.unwrap();
    assert_eq!(1, received.borrow().len());
}
