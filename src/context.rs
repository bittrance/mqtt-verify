use crate::errors::MqttVerifyError;
use evalexpr::{build_operator_tree, Context, Function, Node, Value};
use std::collections::HashMap;
use std::rc::Rc;

fn maybe_push_str(parts: &mut Vec<String>, part: &str) {
    if !part.is_empty() {
        parts.push(format!("\"{}\"", part));
    }
}

pub fn precompile(value: &str) -> Result<Node, MqttVerifyError> {
    let mut stop = 0;
    let mut parts: Vec<String> = Vec::new();
    for (start, _) in value.match_indices("{{") {
        maybe_push_str(&mut parts, &value[stop..start]);
        stop = start
            + value[start..]
                .find("}}")
                .ok_or_else(|| MqttVerifyError::MalformedValue {
                    value: value.to_owned(),
                })?;
        parts.push(value[start + 2..stop].to_owned());
        stop += 2;
    }
    maybe_push_str(&mut parts, &value[stop..]);
    build_operator_tree(&parts.join("+")).map_err(|err| MqttVerifyError::MalformedExpression {
        value: value.to_owned(),
        source: err,
    })
}

pub struct ContextualValue {
    context: Rc<OverlayContext>,
    value: Node,
}

impl ContextualValue {
    pub fn new(value: Node, context: Rc<OverlayContext>) -> Self {
        Self { value, context }
    }

    pub fn value(&self) -> String {
        self.value
            .eval_string_with_context(self.context.as_ref())
            .unwrap()
    }
}

pub struct OverlayContext {
    parent: Option<Rc<OverlayContext>>,
    map: HashMap<String, Value>,
}

impl OverlayContext {
    pub fn root() -> Rc<Self> {
        Rc::new(Self {
            parent: None,
            map: HashMap::new(),
        })
    }

    pub fn subcontext(parent: Rc<OverlayContext>) -> Rc<OverlayContext> {
        Rc::new(Self {
            parent: Some(parent),
            map: HashMap::new(),
        })
    }

    pub fn insert(&mut self, key: String, val: Value) {
        self.map.insert(key, val);
    }

    pub fn value_for(
        context: Rc<OverlayContext>,
        val: &str,
    ) -> Result<ContextualValue, MqttVerifyError> {
        Ok(ContextualValue::new(precompile(val)?, context))
    }
}

impl Context for OverlayContext {
    fn get_value(&self, key: &str) -> Option<&Value> {
        if let Some(val) = self.map.get(key) {
            Some(val)
        } else if let Some(ref parent) = self.parent {
            parent.get_value(key)
        } else {
            None
        }
    }

    fn get_function(&self, _: &str) -> Option<&Function> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::OverlayContext;
    use evalexpr::{Context, Value};
    use std::rc::Rc;

    #[test]
    fn precompile_text() {
        let node = super::precompile("foobar").unwrap();
        assert_eq!(
            "foobar".to_owned(),
            node.eval_string_with_context(OverlayContext::root().as_ref())
                .unwrap()
        );
    }

    #[test]
    fn precompile_expression() {
        let mut context = OverlayContext::root();
        Rc::get_mut(&mut context)
            .unwrap()
            .insert("some".to_owned(), Value::String("value".to_owned()));
        Rc::get_mut(&mut context)
            .unwrap()
            .insert("other".to_owned(), Value::String("stuff".to_owned()));
        let node = super::precompile("foo{{ some + other }}bar").unwrap();
        assert_eq!(
            "foovaluestuffbar".to_owned(),
            node.eval_string_with_context(context.as_ref()).unwrap()
        );
    }

    #[test]
    fn overlay_context() {
        let mut root = OverlayContext::root();
        Rc::get_mut(&mut root)
            .unwrap()
            .insert("foo".to_owned(), Value::String("gazonk".to_owned()));
        Rc::get_mut(&mut root)
            .unwrap()
            .insert("quux".to_owned(), Value::String("bass".to_owned()));
        let mut child1 = OverlayContext::subcontext(root.clone());
        let mut child2 = OverlayContext::subcontext(root.clone());
        Rc::get_mut(&mut child1)
            .unwrap()
            .insert("foo".to_owned(), Value::String("bar".to_owned()));
        Rc::get_mut(&mut child2)
            .unwrap()
            .insert("bar".to_owned(), Value::String("baz".to_owned()));
        assert_eq!(
            Some(&Value::String("bar".to_owned())),
            child1.get_value("foo")
        );
        assert_eq!(
            Some(&Value::String("gazonk".to_owned())),
            child2.get_value("foo")
        );
    }
}
