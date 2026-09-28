use roxmltree::Node;

use super::{Action, BTreeMap, Fault, SOAP, Service, action_arguments, ncname};

pub(crate) fn parse_action(
    body: &[u8],
    soap_action: &str,
    service: Service,
) -> Result<Action, Fault> {
    let invalid = Fault::InvalidArgs;
    let source = std::str::from_utf8(body).map_err(|_| invalid)?;
    let header = soap_action.trim();

    let header = if header.starts_with('"') {
        header
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .ok_or(invalid)?
    } else {
        header
    };

    let (namespace, requested) = header.split_once('#').ok_or(invalid)?;

    if namespace != service.namespace() || !ncname(requested) {
        return Err(invalid);
    }

    let document = roxmltree::Document::parse_with_options(
        source,
        roxmltree::ParsingOptions {
            // Every nested element consumes a node before recursive descent.
            // Together with the 64 KiB HTTP limit, this bounds depth and allocation.
            nodes_limit: 64,
            ..Default::default()
        },
    )
    .map_err(|_| invalid)?;

    let roots = children(document.root())?;
    let [envelope] = roots.as_slice() else {
        return Err(invalid);
    };

    if !envelope.has_tag_name((SOAP, "Envelope")) {
        return Err(invalid);
    }

    let content = children(*envelope)?;

    let body = match content.as_slice() {
        [body] => *body,

        [header, body]
            if header.has_tag_name((SOAP, "Header")) && children(*header)?.is_empty() =>
        {
            *body
        }

        _ => return Err(invalid),
    };

    if !body.has_tag_name((SOAP, "Body")) {
        return Err(invalid);
    }

    let actions = children(body)?;
    let [action] = actions.as_slice() else {
        return Err(invalid);
    };

    if !action.has_tag_name((service.namespace(), requested)) {
        return Err(invalid);
    }

    let mut arguments = BTreeMap::new();

    for argument in children(*action)? {
        if argument
            .tag_name()
            .namespace()
            .is_some_and(|ns| !ns.is_empty())
            || argument.attributes().len() != 0
        {
            return Err(invalid);
        }

        let mut value = String::new();

        for child in argument.children() {
            if child.is_text() {
                value.push_str(child.text().unwrap());
            } else if !child.is_comment() && !child.is_pi() {
                return Err(invalid);
            }
        }

        if arguments
            .insert(argument.tag_name().name(), value)
            .is_some()
        {
            return Err(invalid);
        }
    }

    action_arguments(service, requested, arguments)
}

// Structural SOAP nodes permit encodingStyle, comments, PIs, and whitespace.
// Argument text is collected separately, without trimming or dropping fragments.
fn children<'a, 'input>(node: Node<'a, 'input>) -> Result<Vec<Node<'a, 'input>>, Fault> {
    for attribute in node.attributes() {
        if attribute.namespace() != Some(SOAP)
            || attribute.name() != "encodingStyle"
            || attribute.value() != "http://schemas.xmlsoap.org/soap/encoding/"
        {
            return Err(Fault::InvalidArgs);
        }
    }

    let mut elements = Vec::new();

    for child in node.children() {
        if child.is_element() {
            elements.push(child);
        } else if !child.is_comment()
            && !child.is_pi()
            && !(child.is_text()
                && child
                    .text()
                    .unwrap()
                    .chars()
                    .all(|c| matches!(c, ' ' | '\t' | '\n' | '\r')))
        {
            return Err(Fault::InvalidArgs);
        }
    }

    Ok(elements)
}
