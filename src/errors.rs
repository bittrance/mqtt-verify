use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum MqttVerifyError {
    #[snafu(display("Expected {} to be on format X=Y", input))]
    MalformedParameter { input: String },
    #[snafu(display("Timer borked: {}", source))]
    SourceTimerError { source: std::io::Error },
    #[snafu(display("Connect borked: {}", source))]
    MqttConnectError { source: paho_mqtt::errors::Error },
    #[snafu(display("Disconnect borked: {}", source))]
    MqttDisconnectError { source: paho_mqtt::errors::Error },
    #[snafu(display("Publish borked: {}", source))]
    MqttPublishError { source: paho_mqtt::errors::Error },
    #[snafu(display("Subscribe borked: {}", source))]
    MqttSubscribeError { source: paho_mqtt::errors::Error },
    #[snafu(display("Malformed value {}", value))]
    MalformedValue { value: String },
    #[snafu(display("Malformed expression in value {}: {}", value, source))]
    MalformedExpression {
        value: String,
        source: evalexpr::EvalexprError,
    },
    #[snafu(display("Verification failed: {}", reason))]
    VerificationFailure { reason: String },
}
