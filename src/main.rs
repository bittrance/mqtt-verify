use async_std::task;
use evalexpr::Value;
use futures::stream::StreamExt;
use mqtt_verify::{analyzers, context, errors, scenario, source};
use std::rc::Rc;
use std::str::FromStr;
use std::time::Duration;
use structopt::StructOpt;

fn split_on_equal(input: &str) -> Result<(String, String), errors::MqttVerifyError> {
    let pair: Vec<&str> = input.splitn(2, '=').collect();
    if pair.len() == 2 {
        Ok((pair[0].to_owned(), pair[1].to_owned()))
    } else {
        Err(errors::MqttVerifyError::MalformedParameter {
            input: input.to_owned(),
        })
    }
}

fn duration_from_str(input: &str) -> Result<Duration, errors::MqttVerifyError> {
    let secs = f32::from_str(input).map_err(|_| errors::MqttVerifyError::MalformedValue {
        value: input.to_owned(),
    })?;
    Ok(Duration::from_secs_f32(secs))
}

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
    /// Timeout waiting to connect to broker, both when publishing and subscribing
    #[structopt(long = "initial-timeout", env = "INITIAL_TIMEOUT", default_value = "1.0", parse(try_from_str = duration_from_str))]
    initial_timeout: Duration,
    /// Delay before trying to reconnect when broker connection lost
    #[structopt(long = "reconnect-interval", env = "RECONNECT_INTERVAL", parse(try_from_str = duration_from_str))]
    reconnect_interval: Option<Duration>,
    /// Parameter for expansion
    #[structopt(long = "parameter", parse(try_from_str = split_on_equal))]
    parameters: Vec<(String, String)>,
}

pub fn make_cli_scenario(opt: &Opt) -> Result<scenario::Scenario, errors::MqttVerifyError> {
    let mut root = context::OverlayContext::root();
    for (k, v) in &opt.parameters {
        Rc::get_mut(&mut root)
            .unwrap()
            .insert(k.clone(), Value::String(v.clone()));
    }
    let mut sources: Vec<Box<dyn source::Source>> = Vec::new();
    let mut sinks: Vec<Box<dyn analyzers::Analyzer>> = Vec::new();
    for i in 1..=opt.publishers {
        let mut context = context::OverlayContext::subcontext(root.clone());
        Rc::get_mut(&mut context)
            .unwrap()
            .insert("publisher".to_owned(), Value::String(format!("p-{}", i)));
        sources.push(Box::new(source::VerifiableSource::new(
            format!("{}", i),
            context::OverlayContext::value_for(context.clone(), &opt.topic)?,
            (opt.frequency * opt.length) as usize,
            opt.frequency,
        )));
        sinks.push(Box::new(analyzers::SessionIdFilter::new(
            format!("{}", i),
            Box::new(analyzers::CountingAnalyzer::new(
                (opt.frequency * opt.length) as usize,
            )),
        )));
    }
    Ok(scenario::Scenario {
        publishers: vec![scenario::Publisher {
            client: mqtt_verify::client(&opt.publish_uri),
            connect_options: scenario::ConnectOptions {
                connect_timeout: opt.initial_timeout,
                reconnect_interval: opt.reconnect_interval,
            },
            sources,
        }],
        subscribers: vec![scenario::Subscriber {
            client: mqtt_verify::client(&opt.subscribe_uri),
            connect_options: scenario::ConnectOptions {
                connect_timeout: opt.initial_timeout,
                reconnect_interval: opt.reconnect_interval,
            },
            topics: vec![opt.topic.clone()],
            sinks,
        }],
    })
}

fn main() -> Result<(), errors::MqttVerifyError> {
    let opt = Opt::from_args();
    let scenario = make_cli_scenario(&opt)?;

    task::block_on(async {
        let mut results = mqtt_verify::run_scenario(scenario);
        while let Some(result) = results.next().await {
            match result {
                Ok(_) => (),
                Err(err) => return Err(err),
            }
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::Opt;
    use mqtt_verify::{errors, source};
    use std::any::Any;
    use structopt::StructOpt;

    fn basic_options(extra: Vec<&str>) -> Opt {
        let mut args = vec![
            "./mqtt-verify",
            "--publish-uri",
            "tcp://localhost:1883",
            "--subscribe-uri",
            "tcp://localhost:1883",
        ];
        args.extend(extra);
        Opt::from_iter(args)
    }

    #[test]
    fn make_cli_scenario_creates_soruces_with_expansion() -> Result<(), errors::MqttVerifyError> {
        let opt = basic_options(vec!["--topic", "{{publisher}}"]);
        let scenario = super::make_cli_scenario(&opt)?;
        assert_eq!(1, scenario.publishers.len());
        let publisher = scenario.publishers.get(0).unwrap();
        assert_eq!(1, publisher.sources.len());
        let source: &dyn Any = publisher.sources.get(0).unwrap().as_any();
        assert_eq!(
            "p-1".to_owned(),
            source
                .downcast_ref::<source::VerifiableSource>()
                .unwrap()
                .topic
                .value()
        );
        Ok(())
    }

    #[test]
    fn make_cli_scenario_expands_from_parameter() -> Result<(), errors::MqttVerifyError> {
        let opt = basic_options(vec!["--topic", "{{foo}}", "--parameter", "foo=bar"]);
        let scenario = super::make_cli_scenario(&opt)?;
        let publisher = scenario.publishers.get(0).unwrap();
        let source: &dyn Any = publisher.sources.get(0).unwrap().as_any();
        assert_eq!(
            "bar".to_owned(),
            source
                .downcast_ref::<source::VerifiableSource>()
                .unwrap()
                .topic
                .value()
        );
        Ok(())
    }
}
