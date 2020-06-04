use crate::context::OverlayContext;
use crate::errors;
use crate::source;
use crate::Opt;
use evalexpr::Value;
use paho_mqtt as mqtt;
use std::rc::Rc;

pub struct Scenario {
    pub publishers: Vec<Publisher>,
}

pub struct Publisher {
    pub client: mqtt::AsyncClient,
    pub sources: Vec<source::VerifiableSource>,
}

pub fn make_cli_scenario(opt: &Opt) -> Result<Scenario, errors::MqttVerifyError> {
    let mut root = OverlayContext::root();
    for (k, v) in &opt.parameters {
        Rc::get_mut(&mut root)
            .unwrap()
            .insert(k.clone(), Value::String(v.clone()));
    }
    let mut sources = Vec::new();
    for i in 1..=opt.publishers {
        let mut context = OverlayContext::subcontext(root.clone());
        Rc::get_mut(&mut context)
            .unwrap()
            .insert("publisher".to_owned(), Value::String(format!("p-{}", i)));
        sources.push(source::VerifiableSource::new(
            format!("{}", i),
            OverlayContext::value_for(context.clone(), &opt.topic)?,
            (opt.frequency * opt.length) as usize,
            opt.frequency,
        ));
    }
    Ok(Scenario {
        publishers: vec![Publisher {
            client: crate::client(&opt.publish_uri),
            sources: sources,
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::Opt;
    use crate::errors;
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
        let source = publisher.sources.get(0).unwrap();
        assert_eq!("p-1".to_owned(), source.topic.value());
        Ok(())
    }

    #[test]
    fn make_cli_scenario_expands_from_parameter() -> Result<(), errors::MqttVerifyError> {
        let opt = basic_options(vec!["--topic", "{{foo}}", "--parameter", "foo=bar"]);
        let scenario = super::make_cli_scenario(&opt)?;
        let publisher = scenario.publishers.get(0).unwrap();
        let source = publisher.sources.get(0).unwrap();
        assert_eq!("bar".to_owned(), source.topic.value());
        Ok(())
    }
}
