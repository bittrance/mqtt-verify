use evalexpr::{build_operator_tree, HashMapContext, Node};

pub struct ContextualValue {
    context: HashMapContext,
    value: Node,
}

impl ContextualValue {
    pub fn new(value: Node, context: HashMapContext) -> Self {
        Self { value, context }
    }

    pub fn value(&self) -> String {
        self.value.eval_string_with_context(&self.context).unwrap()
    }
}

fn maybe_push_str(parts: &mut Vec<String>, part: &str) {
    if !part.is_empty() {
        parts.push(format!("\"{}\"", part));
    }
}

pub fn precompile(value: &str) -> Result<Node, crate::MqttVerifyError> {
    let mut stop = 0;
    let mut parts: Vec<String> = Vec::new();
    for (start, _) in value.match_indices("{{") {
        maybe_push_str(&mut parts, &value[stop..start]);
        stop = start
            + value[start..]
                .find("}}")
                .ok_or_else(|| crate::MqttVerifyError::MalformedValue {
                    value: value.to_owned(),
                })?;
        parts.push(value[start + 2..stop].to_owned());
        stop += 2;
    }
    maybe_push_str(&mut parts, &value[stop..]);
    build_operator_tree(&parts.join("+")).map_err(|err| {
        crate::MqttVerifyError::MalformedExpression {
            value: value.to_owned(),
            source: err,
        }
    })
}

#[cfg(test)]
mod tests {
    use evalexpr::{Context, HashMapContext, Value};

    #[test]
    fn precompile_text() {
        let node = super::precompile("foobar").unwrap();
        assert_eq!(
            "foobar".to_owned(),
            node.eval_string_with_context(&HashMapContext::new())
                .unwrap()
        );
    }

    #[test]
    fn precompile_expression() {
        let mut context = HashMapContext::new();
        context
            .set_value("some".to_owned(), Value::String("value".to_owned()))
            .unwrap();
        context
            .set_value("other".to_owned(), Value::String("stuff".to_owned()))
            .unwrap();
        let node = super::precompile("foo{{ some + other }}bar").unwrap();
        assert_eq!(
            "foovaluestuffbar".to_owned(),
            node.eval_string_with_context(&context).unwrap()
        );
    }
}
