use futures::future::Future;
use futures::{
    future::{select, Either},
    FutureExt,
};
use futures_timer::Delay;
use mqtt_verify::errors;
use std::pin::Pin;
use std::time::Duration;

pub mod mosquitto;
pub mod mqtt;

pub fn with_timeout(
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
