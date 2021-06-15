use bollard::container::{
    APIContainers, Config, CreateContainerOptions, HostConfig, ListContainersOptions,
    StartContainerOptions, StopContainerOptions,
};
use bollard::{container::PortBinding, Docker};
use futures::{
    future::{join, select, Either, Future},
    FutureExt,
};
use futures_timer::Delay;
use mqtt_verify::analyzers;
use mqtt_verify::errors;
use mqtt_verify::scenario;
use paho_mqtt as mqtt;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::cell::RefCell;
use std::collections::HashMap;
use std::default::Default;
use std::net::{SocketAddrV4, TcpListener};
use std::pin::Pin;
use std::rc::Rc;
use std::time::{Duration, Instant};

const MOSQUITTO_NAME: &str = "mqtt-verify-mosquitto";

fn random_port() -> u16 {
    let socket = SocketAddrV4::new("127.0.0.1".parse().unwrap(), 0);
    let listener = TcpListener::bind(socket).unwrap();
    listener.local_addr().unwrap().port()
}

fn random_topic(prefix: &str) -> String {
    let rand_string: String = thread_rng().sample_iter(&Alphanumeric).take(30).collect();
    format!("{}/{}", prefix, rand_string)
}

async fn find_mosquitto(docker: &Docker) -> Option<APIContainers> {
    let mut filters = HashMap::new();
    filters.insert("name", vec![MOSQUITTO_NAME]);
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
    match mosquitto {
        None => {
            create_mosquitto().await;
            mosquitto = find_mosquitto(&docker).await;
        }
        Some(ref m) if m.state == "exited" => {
            restart_mosquitto().await;
        }
        Some(ref m) if m.state != "running" => panic!("Can't handle state {}", m.state),
        Some(_) => (),
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

async fn create_mosquitto() -> () {
    let docker = Docker::connect_with_local_defaults().unwrap();
    let options = Some(CreateContainerOptions {
        name: MOSQUITTO_NAME,
    });
    let mut port_bindings = HashMap::new();
    let port = random_port().to_string();
    port_bindings.insert(
        "1883/tcp",
        vec![PortBinding {
            host_ip: "127.0.0.1",
            host_port: &port,
        }],
    );
    let config = Config {
        image: Some("eclipse-mosquitto:1.6.15"),
        cmd: Some(vec![
            "/usr/sbin/mosquitto",
            "-c",
            "/mosquitto/config/mosquitto.conf",
            "-v",
        ]),
        host_config: Some(HostConfig {
            port_bindings: Some(port_bindings),
            ..Default::default()
        }),
        ..Default::default()
    };
    docker.create_container(options, config).await.unwrap();
    docker
        .start_container(MOSQUITTO_NAME, None::<StartContainerOptions<String>>)
        .await
        .unwrap();
}

async fn stop_mosquitto() -> () {
    let docker = Docker::connect_with_local_defaults().unwrap();
    if let Some(mosquitto) = find_mosquitto(&docker).await {
        docker
            .stop_container(&mosquitto.id, None::<StopContainerOptions>)
            .await
            .unwrap();
    }
}

async fn restart_mosquitto() -> () {
    let docker = Docker::connect_with_local_defaults().unwrap();
    if let Some(mosquitto) = find_mosquitto(&docker).await {
        docker
            .start_container(&mosquitto.id, None::<StartContainerOptions<String>>)
            .await
            .unwrap();
    }
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
    pub expected_count: u16,
}

impl analyzers::Analyzer for TestingSink {
    fn analyze(
        &mut self,
        message: mqtt::Message,
    ) -> Result<analyzers::State, errors::MqttVerifyError> {
        self.received.borrow_mut().push(message);
        self.expected_count -= 1;
        if self.expected_count == 0 {
            Ok(analyzers::State::Done)
        } else {
            Ok(analyzers::State::Continue)
        }
    }
}

fn make_subscriber(
    port: u16,
    topic_name: String,
    expected_count: u16,
) -> (scenario::Subscriber, Rc<RefCell<Vec<mqtt::Message>>>) {
    let received = Rc::new(RefCell::new(Vec::new()));
    let sink = TestingSink {
        received: received.clone(),
        expected_count,
    };
    let subscriber = scenario::Subscriber {
        client: client(port),
        initial_timeout: Duration::from_secs(1),
        topics: vec![topic_name],
        sinks: vec![Box::new(sink)],
    };
    (subscriber, received)
}

fn with_timeout(
    subscriber: Pin<Box<dyn Future<Output = Result<(), errors::MqttVerifyError>>>>,
    delay: Duration,
) -> Pin<Box<dyn Future<Output = Result<(), errors::MqttVerifyError>>>> {
    Box::pin(
        select(subscriber, Box::pin(Delay::new(delay))).map(|v| match v {
            Either::Left((inner, _)) => inner,
            Either::Right(_) => panic!("Timout waiting for timeout"),
        }),
    )
}

#[tokio::test]
async fn terminate_when_analyzer_done() {
    let port = ensure_mosquitto().await;
    let topic_name = random_topic("terminate_when_analyzer_done");
    let (subscriber, received) = make_subscriber(port, topic_name.clone(), 2);
    let subscriber = mqtt_verify::run_subscriber(subscriber);
    let scenario = Delay::new(Duration::from_millis(1200)) // Weird synchronization here; delays below 1050 fails occasionally with subscribe delayed 2 ms after publish
        .then(|_| publish_message(port, mqtt::Message::new(topic_name.clone(), "payload", 0)))
        .then(|_| publish_message(port, mqtt::Message::new(topic_name.clone(), "payload", 0)));
    let (s_err, p_err) = join(
        with_timeout(Box::pin(subscriber), Duration::from_secs(5)),
        scenario,
    )
    .await;
    s_err.unwrap();
    p_err.unwrap();
    assert_eq!(2, received.borrow().len());
}

#[tokio::test]
async fn connection_timeout() {
    let (subscriber, _) = make_subscriber(9, "ignored".to_owned(), 0);
    let start_time = Instant::now();
    match select(
        Box::pin(mqtt_verify::run_subscriber(subscriber)),
        Box::pin(Delay::new(Duration::from_secs(5))),
    )
    .await
    {
        Either::Left(_) if Instant::now() - start_time >= Duration::from_millis(100) => (),
        Either::Left(_) => panic!("Subscriber gave up too quickly"),
        Either::Right(_) => panic!("Timout waiting for timeout"),
    }
}

#[tokio::test]
async fn subscriber_reconnects() {
    let port = ensure_mosquitto().await;
    let topic_name = random_topic("subscriber_reconnects");
    let (subscriber, received) = make_subscriber(port, topic_name.clone(), 2);
    let subscriber = mqtt_verify::run_subscriber(subscriber);
    let scenario = Delay::new(Duration::from_millis(1500))
        .then(|_| publish_message(port, mqtt::Message::new(topic_name.clone(), "before", 0)))
        .then(|_| stop_mosquitto())
        .then(|_| Delay::new(Duration::from_secs(2)))
        .then(|_| restart_mosquitto())
        .then(|_| Delay::new(Duration::from_secs(4)))
        .then(|_| publish_message(port, mqtt::Message::new(topic_name.clone(), "after", 0)));
    let (s_err, p_err) = join(
        with_timeout(Box::pin(subscriber), Duration::from_secs(20)),
        scenario,
    )
    .await;
    s_err.unwrap();
    p_err.unwrap();
    assert_eq!(2, received.borrow().len());
}
