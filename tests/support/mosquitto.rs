use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, StartContainerOptions,
    StopContainerOptions,
};
use bollard::models::{ContainerSummaryInner, HostConfig, PortBinding};
use bollard::Docker;
use futures::stream::StreamExt;
use futures_ticker::Ticker;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::collections::HashMap;
use std::default::Default;
use std::net::{SocketAddrV4, TcpListener};
use std::time::Duration;

pub const MOSQUITTO_NAME: &str = "mqtt-verify-mosquitto";

pub fn random_port() -> u16 {
    let socket = SocketAddrV4::new("127.0.0.1".parse().unwrap(), 0);
    let listener = TcpListener::bind(socket).unwrap();
    listener.local_addr().unwrap().port()
}

pub fn random_topic(prefix: &str) -> String {
    let rand_string: String = thread_rng()
        .sample_iter(Alphanumeric)
        .map(char::from)
        .take(30)
        .collect();
    format!("{}/{}", prefix, rand_string)
}

async fn find_port() -> Option<u16> {
    let docker = Docker::connect_with_local_defaults().unwrap();
    let mut strobe = Ticker::new(Duration::from_millis(2000)).take(5);
    while let Some(_) = strobe.next().await {
        let mosquitto = find_mosquitto(&docker).await.unwrap();
        if let Some(port) = mosquitto
            .ports
            .as_ref()
            .and_then(|ports| ports.iter().find(|p| p.private_port == 1883))
        {
            let port_number = port.public_port.unwrap() as u16;
            return Some(port_number);
        }
    }
    None
}

pub async fn find_mosquitto(docker: &Docker) -> Option<ContainerSummaryInner> {
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

pub async fn ensure_mosquitto() -> u16 {
    let docker = Docker::connect_with_local_defaults().unwrap();
    let mosquitto = find_mosquitto(&docker).await;
    match mosquitto {
        None => {
            create_mosquitto().await;
        }
        Some(ref m) if m.state == Some("exited".to_owned()) => {
            restart_mosquitto().await;
        }
        Some(ref m) if m.state != Some("running".to_owned()) => {
            panic!("Can't handle state {:?}", m.state)
        }
        Some(_) => (),
    }
    find_port().await.unwrap() as u16
}

pub async fn create_mosquitto() -> () {
    let docker = Docker::connect_with_local_defaults().unwrap();
    let options = Some(CreateContainerOptions {
        name: MOSQUITTO_NAME,
    });
    let mut port_bindings = HashMap::new();
    let port = random_port().to_string();
    port_bindings.insert(
        "1883/tcp".to_owned(),
        Some(vec![PortBinding {
            host_ip: Some("127.0.0.1".to_owned()),
            host_port: Some(port),
        }]),
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

pub async fn stop_mosquitto() -> () {
    let docker = Docker::connect_with_local_defaults().unwrap();
    let mosquitto = find_mosquitto(&docker).await.unwrap();
    docker
        .stop_container(&mosquitto.id.unwrap(), None::<StopContainerOptions>)
        .await
        .unwrap();
}

pub async fn restart_mosquitto() -> () {
    let docker = Docker::connect_with_local_defaults().unwrap();
    let mosquitto = find_mosquitto(&docker).await.unwrap();
    docker
        .start_container(
            &mosquitto.id.unwrap(),
            None::<StartContainerOptions<String>>,
        )
        .await
        .unwrap();
}
