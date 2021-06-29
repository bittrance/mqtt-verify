use paho_mqtt as mqtt;
use std::time::Duration;

pub fn client(port: u16) -> mqtt::AsyncClient {
    let mqtt_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(format!("tcp://localhost:{}", port))
        .persistence(mqtt::create_options::PersistenceType::None)
        .finalize();
    mqtt::AsyncClient::new(mqtt_opts).unwrap()
}

pub async fn publish_message(port: u16, message: mqtt::Message) -> Result<(), mqtt::errors::Error> {
    let conn_opts = mqtt::ConnectOptionsBuilder::new()
        .clean_session(true)
        .finalize();
    let client = client(port);
    client.connect(conn_opts).await?;
    client.publish(message).await?;
    client.disconnect_after(Duration::from_secs(3)).await?;
    Ok(())
}
