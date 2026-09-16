use std::collections::{BTreeMap, BTreeSet};

use quick_xml::{
    Reader,
    events::{BytesStart, Event},
    name::{Namespace, NamespaceResolver, PrefixDeclaration, ResolveResult},
};

pub const HEADER_BYTES: usize = 16 * 1024;
pub(crate) const SOAP_BODY_BYTES: usize = 64 * 1024;
pub(crate) const SOAP_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const XML_DEPTH: usize = 32;

pub const CONTENT_DIRECTORY: &str = "urn:schemas-upnp-org:service:ContentDirectory:1";
pub const CONNECTION_MANAGER: &str = "urn:schemas-upnp-org:service:ConnectionManager:1";
const SOAP: &str = "http://schemas.xmlsoap.org/soap/envelope/";
const ENVELOPE_START: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\"><s:Body>";
const ENVELOPE_END: &str = "</s:Body></s:Envelope>";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Service {
    ContentDirectory,
    ConnectionManager,
}

impl Service {
    fn namespace(self) -> &'static str {
        match self {
            Self::ContentDirectory => CONTENT_DIRECTORY,
            Self::ConnectionManager => CONNECTION_MANAGER,
        }
    }

    fn inputs(self, action: &str) -> Result<&'static [&'static str], Fault> {
        match (self, action) {
            (Self::ContentDirectory, "Browse") => Ok(&[
                "ObjectID",
                "BrowseFlag",
                "Filter",
                "StartingIndex",
                "RequestedCount",
                "SortCriteria",
            ]),

            (Self::ConnectionManager, "GetCurrentConnectionInfo") => Ok(&["ConnectionID"]),

            (
                Self::ContentDirectory,
                "GetSearchCapabilities" | "GetSortCapabilities" | "GetSystemUpdateID",
            )
            | (Self::ConnectionManager, "GetProtocolInfo" | "GetCurrentConnectionIDs") => Ok(&[]),

            _ => Err(Fault { code: 401 }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fault {
    pub code: u16,
}

pub fn fault_xml(fault: Fault) -> String {
    let description = match fault.code {
        401 => "Invalid Action",
        402 => "Invalid Args",
        501 => "Action Failed",
        701 => "No Such Object",
        706 => "Invalid Connection Reference",
        709 => "Unsupported or Invalid Sort Criteria",
        710 => "No Such Container",
        _ => "Action Failed",
    };

    format!(
        "{ENVELOPE_START}<s:Fault><faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring><detail><UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>{}</errorCode><errorDescription>{description}</errorDescription></UPnPError></detail></s:Fault>{ENVELOPE_END}",
        fault.code
    )
}

pub fn device_description(friendly_name: &str, uuid: uuid::Uuid) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root xmlns=\"urn:schemas-upnp-org:device-1-0\"><specVersion><major>1</major><minor>0</minor></specVersion><device><deviceType>urn:schemas-upnp-org:device:MediaServer:1</deviceType><friendlyName>{}</friendlyName><manufacturer>immich-dlna-proxy</manufacturer><modelName>immich-dlna-proxy</modelName><modelNumber>{}</modelNumber><UDN>uuid:{uuid}</UDN><serviceList><service><serviceType>{CONTENT_DIRECTORY}</serviceType><serviceId>urn:upnp-org:serviceId:ContentDirectory</serviceId><SCPDURL>/upnp/content-directory/scpd.xml</SCPDURL><controlURL>/upnp/content-directory/control</controlURL><eventSubURL>/upnp/content-directory/events</eventSubURL></service><service><serviceType>{CONNECTION_MANAGER}</serviceType><serviceId>urn:upnp-org:serviceId:ConnectionManager</serviceId><SCPDURL>/upnp/connection-manager/scpd.xml</SCPDURL><controlURL>/upnp/connection-manager/control</controlURL><eventSubURL>/upnp/connection-manager/events</eventSubURL></service></serviceList></device></root>",
        escape_text(friendly_name),
        env!("CARGO_PKG_VERSION")
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Action {
    Browse(BrowseArguments),
    GetSearchCapabilities,
    GetSortCapabilities,
    GetSystemUpdateId,
    GetProtocolInfo,
    GetCurrentConnectionIds,
    GetCurrentConnectionInfo(i32),
}

impl Action {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Browse(_) => "Browse",
            Self::GetSearchCapabilities => "GetSearchCapabilities",
            Self::GetSortCapabilities => "GetSortCapabilities",
            Self::GetSystemUpdateId => "GetSystemUpdateID",
            Self::GetProtocolInfo => "GetProtocolInfo",
            Self::GetCurrentConnectionIds => "GetCurrentConnectionIDs",
            Self::GetCurrentConnectionInfo(_) => "GetCurrentConnectionInfo",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrowseArguments {
    pub object_id: String,
    pub metadata: bool,
    pub starting_index: u32,
    pub requested_count: u32,
    /// None uses catalog order; Some(true) sorts dates descending.
    pub sort: Option<bool>,
    pub filter: Filter,
}

fn action_arguments(
    service: Service,
    name: &str,
    mut arguments: BTreeMap<String, String>,
) -> Result<Action, Fault> {
    let invalid = Fault { code: 402 };
    let inputs = service.inputs(name)?;

    // Check the complete shape before value validation, including sort faults.
    if arguments.len() != inputs.len() || inputs.iter().any(|name| !arguments.contains_key(*name)) {
        return Err(invalid);
    }

    match name {
        "GetSearchCapabilities" => return Ok(Action::GetSearchCapabilities),
        "GetSortCapabilities" => return Ok(Action::GetSortCapabilities),
        "GetSystemUpdateID" => return Ok(Action::GetSystemUpdateId),
        "GetProtocolInfo" => return Ok(Action::GetProtocolInfo),
        "GetCurrentConnectionIDs" => return Ok(Action::GetCurrentConnectionIds),

        "GetCurrentConnectionInfo" => {
            let id = arguments["ConnectionID"]
                .parse::<i32>()
                .map_err(|_| invalid)?;

            return Ok(Action::GetCurrentConnectionInfo(id));
        }

        "Browse" => {}

        _ => return Err(Fault { code: 401 }),
    }

    let number = |name| {
        let value = &arguments[name];

        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid);
        }

        value.parse::<u32>().map_err(|_| invalid)
    };

    let starting_index = number("StartingIndex")?;
    let requested_count = number("RequestedCount")?;

    let metadata = match arguments["BrowseFlag"].as_str() {
        "BrowseMetadata" => true,
        "BrowseDirectChildren" => false,
        _ => return Err(invalid),
    };

    if metadata && starting_index != 0 {
        return Err(invalid);
    }

    let sort = match arguments["SortCriteria"].as_str() {
        "" => None,
        "+dc:date" => Some(false),
        "-dc:date" => Some(true),
        _ => return Err(Fault { code: 709 }),
    };

    let filter = Filter::parse(&arguments["Filter"])?;

    Ok(Action::Browse(BrowseArguments {
        object_id: arguments.remove("ObjectID").ok_or(invalid)?,
        metadata,
        starting_index,
        requested_count,
        sort,
        filter,
    }))
}

fn xml_char(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}')
}

fn name_start(c: char) -> bool {
    matches!(c, 'A'..='Z' | '_' | 'a'..='z' | '\u{c0}'..='\u{d6}' | '\u{d8}'..='\u{f6}' | '\u{f8}'..='\u{2ff}' | '\u{370}'..='\u{37d}' | '\u{37f}'..='\u{1fff}' | '\u{200c}'..='\u{200d}' | '\u{2070}'..='\u{218f}' | '\u{2c00}'..='\u{2fef}' | '\u{3001}'..='\u{d7ff}' | '\u{f900}'..='\u{fdcf}' | '\u{fdf0}'..='\u{fffd}' | '\u{10000}'..='\u{effff}')
}

fn ncname(name: &str) -> bool {
    let mut chars = name.chars();

    chars.next().is_some_and(name_start)
        && chars.all(|c| {
            name_start(c)
                || matches!(c, '-' | '.' | '0'..='9' | '\u{b7}' | '\u{300}'..='\u{36f}' | '\u{203f}'..='\u{2040}')
        })
}

fn qname(name: &str) -> bool {
    match name.split_once(':') {
        Some((prefix, local)) => ncname(prefix) && ncname(local),
        None => ncname(name),
    }
}

fn resolved_namespace(namespace: ResolveResult<'_>) -> Result<&str, Fault> {
    match namespace {
        ResolveResult::Bound(namespace) => {
            std::str::from_utf8(namespace.into_inner()).map_err(|_| Fault { code: 402 })
        }

        ResolveResult::Unbound => Ok(""),

        ResolveResult::Unknown(_) => Err(Fault { code: 402 }),
    }
}

pub fn parse_action(body: &[u8], soap_action: &str, service: Service) -> Result<Action, Fault> {
    let invalid = Fault { code: 402 };

    if body.len() > SOAP_BODY_BYTES || soap_action.len() > HEADER_BYTES {
        return Err(Fault { code: 501 });
    }

    let source = std::str::from_utf8(body).map_err(|_| invalid)?;

    if !source.chars().all(xml_char) {
        return Err(invalid);
    }

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

    let mut reader = Reader::from_str(source);
    reader.config_mut().expand_empty_elements = true;
    reader.config_mut().check_comments = true;
    let mut namespaces = NamespaceResolver::default();
    let mut stack = Vec::new();
    let mut envelope_seen = false;
    let mut header_seen = false;
    let mut body_seen = false;
    let mut action_seen = false;
    let mut declaration_seen = false;

    let mut arguments = BTreeMap::new();

    loop {
        match reader.read_event().map_err(|_| invalid)? {
            Event::Start(element) => {
                if stack.len() >= XML_DEPTH {
                    return Err(Fault { code: 501 });
                }

                let qualified = element.name();
                let spelling = std::str::from_utf8(qualified.as_ref()).map_err(|_| invalid)?;

                if !qname(spelling) {
                    return Err(invalid);
                }

                // Open a scope without raw bindings; validate and add decoded declarations below.
                namespaces
                    .push(&BytesStart::new("scope"))
                    .map_err(|_| invalid)?;

                let mut attributes = Vec::new();

                for attribute in element.attributes() {
                    let attribute = attribute.map_err(|_| invalid)?;

                    let key = std::str::from_utf8(attribute.key.as_ref()).map_err(|_| invalid)?;

                    let value = attribute
                        .decode_and_unescape_value(reader.decoder())
                        .map_err(|_| invalid)?;

                    if !qname(key)
                        || !value.chars().all(xml_char)
                        || attribute.value.contains(&b'<')
                    {
                        return Err(invalid);
                    }

                    if let Some(prefix) = attribute.key.as_namespace_binding() {
                        // NamespaceResolver checks reserved prefixes, but not default bindings.
                        if prefix == PrefixDeclaration::Default
                            && matches!(
                                value.as_ref(),
                                "http://www.w3.org/XML/1998/namespace"
                                    | "http://www.w3.org/2000/xmlns/"
                            )
                        {
                            return Err(invalid);
                        }

                        namespaces
                            .add(prefix, Namespace(value.as_bytes()))
                            .map_err(|_| invalid)?;
                    } else {
                        attributes.push((attribute.key, value));
                    }
                }

                let (resolved, local) = namespaces.resolve_element(qualified);
                let namespace = resolved_namespace(resolved)?;
                let local = std::str::from_utf8(local.as_ref()).map_err(|_| invalid)?;
                let mut names = BTreeSet::new();

                for (key, value) in attributes {
                    let (resolved, local) = namespaces.resolve_attribute(key);
                    let ns = resolved_namespace(resolved)?;

                    if !names.insert((ns, local.as_ref().to_vec()))
                        || ns != SOAP
                        || local.as_ref() != b"encodingStyle"
                        || value != "http://schemas.xmlsoap.org/soap/encoding/"
                        || stack.len() > 2
                    {
                        return Err(invalid);
                    }
                }

                match stack.len() {
                    0 if !envelope_seen && namespace == SOAP && local == "Envelope" => {
                        envelope_seen = true;
                    }

                    1 if !header_seen && !body_seen && namespace == SOAP && local == "Header" => {
                        header_seen = true;
                    }

                    1 if !body_seen && namespace == SOAP && local == "Body" => {
                        body_seen = true;
                    }

                    2 if stack[1] == "Body"
                        && !action_seen
                        && namespace == service.namespace()
                        && local == requested =>
                    {
                        action_seen = true;
                    }

                    3 if namespace.is_empty() => {
                        if arguments.insert(local.to_owned(), String::new()).is_some() {
                            return Err(invalid);
                        }
                    }

                    _ => return Err(invalid),
                }

                stack.push(local.to_owned());
            }

            Event::End(_) => {
                stack.pop().ok_or(invalid)?;
                namespaces.pop();
            }

            Event::Text(text) => {
                let text = text.xml10_content().map_err(|_| invalid)?;

                if text.contains("]]>") {
                    return Err(invalid);
                }

                if stack.len() == 4 {
                    arguments.get_mut(&stack[3]).ok_or(invalid)?.push_str(&text);
                } else if !text.chars().all(|c| matches!(c, ' ' | '\t' | '\r' | '\n')) {
                    return Err(invalid);
                }
            }

            Event::CData(text) => {
                if stack.len() != 4 {
                    return Err(invalid);
                }

                let text = text.xml10_content().map_err(|_| invalid)?;

                arguments.get_mut(&stack[3]).ok_or(invalid)?.push_str(&text);
            }

            Event::GeneralRef(reference) => {
                if stack.len() != 4 {
                    return Err(invalid);
                }

                let c = if let Some(c) = reference.resolve_char_ref().map_err(|_| invalid)? {
                    c
                } else {
                    match reference.decode().map_err(|_| invalid)?.as_ref() {
                        "amp" => '&',
                        "lt" => '<',
                        "gt" => '>',
                        "quot" => '"',
                        "apos" => '\'',
                        _ => return Err(invalid),
                    }
                };

                if !xml_char(c) {
                    return Err(invalid);
                }

                arguments.get_mut(&stack[3]).ok_or(invalid)?.push(c);
            }

            Event::Decl(declaration) if !declaration_seen && !envelope_seen => {
                declaration_seen = true;

                if declaration.version().map_err(|_| invalid)?.as_ref() != b"1.0"
                    || reader.buffer_position() != (declaration.len() + 4) as u64
                {
                    return Err(invalid);
                }

                let content = std::str::from_utf8(declaration.as_ref()).map_err(|_| invalid)?;
                let declaration = BytesStart::from_content(content, 3);
                let mut standalone_seen = false;

                for attribute in declaration.attributes() {
                    let attribute = attribute.map_err(|_| invalid)?;

                    match (attribute.key.as_ref(), attribute.value.as_ref()) {
                        (b"version", b"1.0") => {}

                        (b"encoding", value)
                            if !standalone_seen && value.eq_ignore_ascii_case(b"utf-8") => {}

                        (b"standalone", b"yes" | b"no") => {
                            standalone_seen = true;
                        }

                        _ => return Err(invalid),
                    }
                }
            }

            Event::Comment(_) => {}

            Event::Eof => break,

            _ => return Err(invalid),
        }
    }

    if !stack.is_empty() || !body_seen || !action_seen {
        return Err(invalid);
    }

    action_arguments(service, requested, arguments)
}

fn escaped(c: char, buffer: &mut [u8; 4]) -> &str {
    match c {
        '&' => "&amp;",
        '<' => "&lt;",
        '>' => "&gt;",
        '"' => "&quot;",
        '\'' => "&apos;",
        '\r' => "&#13;",
        c if xml_char(c) => c.encode_utf8(buffer),
        _ => "\u{fffd}",
    }
}

pub fn escape_text(text: &str) -> String {
    let mut result = String::new();

    for c in text.chars() {
        result.push_str(escaped(c, &mut [0; 4]));
    }

    result
}

// DIDL accounts for its subsequent SOAP text escaping before growing its buffer.
struct Xml {
    value: String,
    used: usize,
    limit: usize,
    outer_escape: bool,
}

impl Xml {
    fn new(limit: usize, outer_escape: bool) -> Self {
        Self {
            value: String::new(),
            used: 0,
            limit,
            outer_escape,
        }
    }

    fn raw(&mut self, text: &str) -> Result<(), Fault> {
        let cost = if self.outer_escape {
            text.chars().try_fold(0usize, |sum, c| {
                sum.checked_add(escaped(c, &mut [0; 4]).len())
            })
        } else {
            Some(text.len())
        };

        let used = cost.and_then(|cost| self.used.checked_add(cost));

        if used.is_none_or(|used| used > self.limit) {
            return Err(Fault { code: 501 });
        }

        self.used = used.unwrap();
        self.value.push_str(text);

        Ok(())
    }

    fn text(&mut self, text: &str) -> Result<(), Fault> {
        for c in text.chars() {
            self.raw(escaped(c, &mut [0; 4]))?;
        }

        Ok(())
    }

    fn element(&mut self, name: &str, text: &str) -> Result<(), Fault> {
        self.raw("<")?;
        self.raw(name)?;
        self.raw(">")?;
        self.text(text)?;
        self.raw("</")?;
        self.raw(name)?;

        self.raw(">")
    }
}

pub fn action_response(
    service: Service,
    action: &str,
    args: &[(&str, &str)],
) -> Result<String, Fault> {
    action_response_bounded(service, action, args, SOAP_RESPONSE_BYTES)
}

fn action_response_bounded(
    service: Service,
    action: &str,
    args: &[(&str, &str)],
    limit: usize,
) -> Result<String, Fault> {
    service.inputs(action)?;
    let mut names = BTreeSet::new();
    let mut xml = Xml::new(limit, false);
    xml.raw(ENVELOPE_START)?;
    xml.raw("<u:")?;
    xml.raw(action)?;
    xml.raw("Response xmlns:u=\"")?;
    xml.raw(service.namespace())?;
    xml.raw("\">")?;

    for &(name, value) in args {
        if !ncname(name) || !names.insert(name) {
            return Err(Fault { code: 402 });
        }

        xml.element(name, value)?;
    }

    xml.raw("</u:")?;
    xml.raw(action)?;
    xml.raw("Response>")?;
    xml.raw(ENVELOPE_END)?;

    Ok(xml.value)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Filter {
    date: bool,
    art: bool,
    res: bool,
    duration: bool,
    child_count: bool,
}

impl Filter {
    pub fn parse(value: &str) -> Result<Self, Fault> {
        let mut filter = Self::default();

        if value.trim().is_empty() {
            return Ok(filter);
        }

        for selector in value.split(',').map(str::trim) {
            if selector == "*" {
                filter = Self {
                    date: true,
                    art: true,
                    res: true,
                    duration: true,
                    child_count: true,
                };

                continue;
            }

            let (element, attribute) = match selector.split_once('@') {
                Some((element, attribute)) => (element, Some(attribute)),
                None => (selector, None),
            };

            if (!element.is_empty() && !qname(element))
                || (element.is_empty() && attribute.is_none())
                || attribute.is_some_and(|attribute| !qname(attribute))
            {
                return Err(Fault { code: 402 });
            }

            match (element, attribute) {
                ("dc:date", None) => filter.date = true,

                ("upnp:albumArtURI", None) => filter.art = true,

                ("", Some("childCount")) => filter.child_count = true,

                ("res", Some("duration")) => {
                    filter.res = true;
                    filter.duration = true;
                }

                ("res", None | Some("protocolInfo" | "size" | "resolution")) => filter.res = true,

                _ => {}
            }
        }

        Ok(filter)
    }

    pub fn date(&self) -> bool {
        self.date
    }

    pub fn art(&self) -> bool {
        self.art
    }

    pub fn res(&self) -> bool {
        self.res
    }

    pub fn duration(&self) -> bool {
        self.duration
    }

    pub fn child_count(&self) -> bool {
        self.child_count
    }
}

/// Projected metadata; callers own ID validation, resource selection and ordering.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct Object {
    pub id: String,
    pub parent_id: String,
    pub title: String,
    pub class: String,
    pub date: Option<String>,
    pub art: Option<String>,
    pub child_count: Option<usize>,
    pub resources: Vec<Resource>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct Resource {
    pub uri: String,
    pub mime: String,
    pub duration: Option<String>,
    /// Explicitly established byte-seek support, not inferred from the MIME.
    pub byte_seek: bool,
}

pub fn didl(objects: &[Object], filter: &Filter) -> Result<String, Fault> {
    didl_bounded(objects, filter, SOAP_RESPONSE_BYTES)
}

fn didl_bounded(objects: &[Object], filter: &Filter, limit: usize) -> Result<String, Fault> {
    let mut xml = Xml::new(limit, true);
    xml.raw("<DIDL-Lite xmlns=\"urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:upnp=\"urn:schemas-upnp-org:metadata-1-0/upnp/\">")?;

    for object in objects {
        let container =
            object.class == "object.container" || object.class.starts_with("object.container.");

        let tag = if container { "container" } else { "item" };
        xml.raw("<")?;
        xml.raw(tag)?;
        xml.raw(" id=\"")?;
        xml.text(&object.id)?;
        xml.raw("\" parentID=\"")?;
        xml.text(&object.parent_id)?;
        xml.raw("\" restricted=\"1\"")?;

        if container
            && filter.child_count()
            && let Some(count) = object.child_count
        {
            xml.raw(" childCount=\"")?;
            xml.raw(&count.to_string())?;
            xml.raw("\"")?;
        }

        xml.raw(">")?;
        xml.element("dc:title", &object.title)?;
        xml.element("upnp:class", &object.class)?;

        if filter.date()
            && let Some(date) = &object.date
        {
            xml.element("dc:date", date)?;
        }

        if filter.art()
            && let Some(art) = &object.art
        {
            xml.element("upnp:albumArtURI", art)?;
        }

        if filter.res() {
            for resource in &object.resources {
                xml.raw("<res protocolInfo=\"http-get:*:")?;
                xml.text(&resource.mime)?;
                xml.raw(if resource.byte_seek {
                    ":DLNA.ORG_OP=01\""
                } else {
                    ":*\""
                })?;

                if filter.duration()
                    && let Some(duration) = &resource.duration
                {
                    xml.raw(" duration=\"")?;
                    xml.text(duration)?;
                    xml.raw("\"")?;
                }

                xml.raw(">")?;
                xml.text(&resource.uri)?;
                xml.raw("</res>")?;
            }
        }

        xml.raw("</")?;
        xml.raw(tag)?;
        xml.raw(">")?;
    }

    xml.raw("</DIDL-Lite>")?;

    Ok(xml.value)
}

pub fn scpd(service: Service) -> &'static str {
    match service {
        Service::ContentDirectory => CONTENT_DIRECTORY_SCPD,
        Service::ConnectionManager => CONNECTION_MANAGER_SCPD,
    }
}

const CONTENT_DIRECTORY_SCPD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
<specVersion><major>1</major><minor>0</minor></specVersion>
<actionList>
<action><name>GetSearchCapabilities</name><argumentList>
<argument><name>SearchCaps</name><direction>out</direction><relatedStateVariable>SearchCapabilities</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetSortCapabilities</name><argumentList>
<argument><name>SortCaps</name><direction>out</direction><relatedStateVariable>SortCapabilities</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetSystemUpdateID</name><argumentList>
<argument><name>Id</name><direction>out</direction><relatedStateVariable>SystemUpdateID</relatedStateVariable></argument>
</argumentList></action>
<action><name>Browse</name><argumentList>
<argument><name>ObjectID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_ObjectID</relatedStateVariable></argument>
<argument><name>BrowseFlag</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_BrowseFlag</relatedStateVariable></argument>
<argument><name>Filter</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Filter</relatedStateVariable></argument>
<argument><name>StartingIndex</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Index</relatedStateVariable></argument>
<argument><name>RequestedCount</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>
<argument><name>SortCriteria</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_SortCriteria</relatedStateVariable></argument>
<argument><name>Result</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Result</relatedStateVariable></argument>
<argument><name>NumberReturned</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>
<argument><name>TotalMatches</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Count</relatedStateVariable></argument>
<argument><name>UpdateID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_UpdateID</relatedStateVariable></argument>
</argumentList></action>
</actionList>
<serviceStateTable>
<stateVariable sendEvents="no"><name>SearchCapabilities</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>SortCapabilities</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="yes"><name>SystemUpdateID</name><dataType>ui4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_ObjectID</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_BrowseFlag</name><dataType>string</dataType><allowedValueList><allowedValue>BrowseMetadata</allowedValue><allowedValue>BrowseDirectChildren</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_Filter</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_Index</name><dataType>ui4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_Count</name><dataType>ui4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_SortCriteria</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_Result</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_UpdateID</name><dataType>ui4</dataType></stateVariable>
</serviceStateTable>
</scpd>"#;

const CONNECTION_MANAGER_SCPD: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<scpd xmlns="urn:schemas-upnp-org:service-1-0">
<specVersion><major>1</major><minor>0</minor></specVersion>
<actionList>
<action><name>GetProtocolInfo</name><argumentList>
<argument><name>Source</name><direction>out</direction><relatedStateVariable>SourceProtocolInfo</relatedStateVariable></argument>
<argument><name>Sink</name><direction>out</direction><relatedStateVariable>SinkProtocolInfo</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetCurrentConnectionIDs</name><argumentList>
<argument><name>ConnectionIDs</name><direction>out</direction><relatedStateVariable>CurrentConnectionIDs</relatedStateVariable></argument>
</argumentList></action>
<action><name>GetCurrentConnectionInfo</name><argumentList>
<argument><name>ConnectionID</name><direction>in</direction><relatedStateVariable>A_ARG_TYPE_ConnectionID</relatedStateVariable></argument>
<argument><name>RcsID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_RcsID</relatedStateVariable></argument>
<argument><name>AVTransportID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_AVTransportID</relatedStateVariable></argument>
<argument><name>ProtocolInfo</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_ProtocolInfo</relatedStateVariable></argument>
<argument><name>PeerConnectionManager</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_ConnectionManager</relatedStateVariable></argument>
<argument><name>PeerConnectionID</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_ConnectionID</relatedStateVariable></argument>
<argument><name>Direction</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_Direction</relatedStateVariable></argument>
<argument><name>Status</name><direction>out</direction><relatedStateVariable>A_ARG_TYPE_ConnectionStatus</relatedStateVariable></argument>
</argumentList></action>
</actionList>
<serviceStateTable>
<stateVariable sendEvents="yes"><name>SourceProtocolInfo</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="yes"><name>SinkProtocolInfo</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="yes"><name>CurrentConnectionIDs</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_ConnectionStatus</name><dataType>string</dataType><allowedValueList><allowedValue>OK</allowedValue><allowedValue>ContentFormatMismatch</allowedValue><allowedValue>InsufficientBandwidth</allowedValue><allowedValue>UnreliableChannel</allowedValue><allowedValue>Unknown</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_ConnectionManager</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_Direction</name><dataType>string</dataType><allowedValueList><allowedValue>Input</allowedValue><allowedValue>Output</allowedValue></allowedValueList></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_ProtocolInfo</name><dataType>string</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_ConnectionID</name><dataType>i4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_AVTransportID</name><dataType>i4</dataType></stateVariable>
<stateVariable sendEvents="no"><name>A_ARG_TYPE_RcsID</name><dataType>i4</dataType></stateVariable>
</serviceStateTable>
</scpd>"#;

#[cfg(test)]
mod tests {
    use quick_xml::NsReader;

    use super::*;

    fn request(action: &str, arguments: &str) -> String {
        format!(
            "<s:Envelope xmlns:s=\"{SOAP}\"><s:Body><u:{action} xmlns:u=\"{CONTENT_DIRECTORY}\">{arguments}</u:{action}></s:Body></s:Envelope>"
        )
    }

    fn parse(xml: &str, action: &str) -> Result<Action, Fault> {
        parse_action(
            xml.as_bytes(),
            &format!("\"{CONTENT_DIRECTORY}#{action}\""),
            Service::ContentDirectory,
        )
    }

    fn browse(arguments: &str) -> Result<Action, Fault> {
        parse(&request("Browse", arguments), "Browse")
    }

    const BROWSE_ARGS: &str = "<ObjectID>0</ObjectID><BrowseFlag>BrowseDirectChildren</BrowseFlag><Filter>*</Filter><StartingIndex>0</StartingIndex><RequestedCount>0</RequestedCount><SortCriteria/>";

    fn object() -> Object {
        Object {
            id: "album:a:asset:b".into(),
            parent_id: "album:a".into(),
            title: "A&B".into(),
            class: "object.item.videoItem".into(),
            date: Some("2026-01-02".into()),
            art: Some("http://192.0.2.1/preview?a=1&b=2".into()),
            child_count: None,
            resources: vec![
                Resource {
                    uri: "http://192.0.2.1/original?a=1&b=2".into(),
                    mime: "video/quicktime".into(),
                    duration: Some("123:04:05.006".into()),
                    byte_seek: false,
                },
                Resource {
                    uri: "http://192.0.2.1/playback".into(),
                    mime: "video/mp4".into(),
                    duration: None,
                    byte_seek: false,
                },
            ],
        }
    }

    fn assert_xml(xml: &str) {
        let mut reader = NsReader::from_str(xml);
        reader.config_mut().expand_empty_elements = true;
        let mut depth = 0;
        let mut roots = 0;

        loop {
            let (namespace, event) = reader.read_resolved_event().unwrap();
            assert!(!matches!(namespace, ResolveResult::Unknown(_)));

            match event {
                Event::Start(element) => {
                    for attr in element.attributes() {
                        attr.unwrap().unescape_value().unwrap();
                    }

                    if depth == 0 {
                        roots += 1;
                    }

                    depth += 1;
                }

                Event::End(_) => depth -= 1,

                Event::Eof => break,

                _ => {}
            }
        }

        assert_eq!(depth, 0);
        assert_eq!(roots, 1);
    }

    #[test]
    fn byte_seek_is_explicit_per_resource_and_does_not_add_other_dlna_claims() {
        let mut item = object();
        let baseline = didl(&[item.clone()], &Filter::parse("*").unwrap()).unwrap();
        assert!(!baseline.contains("DLNA.ORG_"));
        item.resources[1].byte_seek = true;

        let xml = didl(&[item.clone()], &Filter::parse("*").unwrap()).unwrap();
        assert!(xml.contains("http-get:*:video/mp4:DLNA.ORG_OP=01"));
        assert!(xml.contains("http-get:*:video/quicktime:*"));
        assert_eq!(xml.matches("DLNA.ORG_").count(), 1);
        assert_xml(&xml);

        let encoded =
            action_response(Service::ContentDirectory, "Browse", &[("Result", &xml)]).unwrap();
        assert!(encoded.contains("http-get:*:video/mp4:DLNA.ORG_OP=01"));
        assert_xml(&encoded);

        let empty = didl(&[item.clone()], &Filter::parse("").unwrap()).unwrap();
        assert!(!empty.contains("<res"));
        assert!(!empty.contains("DLNA.ORG_"));

        let resource = didl(&[item], &Filter::parse("res").unwrap()).unwrap();
        assert!(resource.contains("http-get:*:video/mp4:DLNA.ORG_OP=01"));
        assert!(!resource.contains("duration="));
    }

    #[test]
    fn soap_accepts_namespace_aliases_default_namespace_and_empty_actions() {
        let xml = request("GetSortCapabilities", "")
            .replace("<s:", "<soap:")
            .replace("</s:", "</soap:")
            .replace("xmlns:s=", "xmlns:soap=");

        assert!(parse(&xml, "GetSortCapabilities").is_ok());

        let encoded_namespace = xml
            .replace("/envelope/", "/envelope&#47;")
            .replace("ContentDirectory:1", "ContentDirectory:&#49;");

        assert!(parse(&encoded_namespace, "GetSortCapabilities").is_ok());

        for declaration in [
            "<?xml version='1.0'?>",
            "<?xml version='1.0' encoding='utf-8' standalone='yes'?>",
            "\u{feff}<?xml version='1.0' encoding='UTF-8'?>",
        ] {
            assert!(
                parse(&format!("{declaration}{xml}"), "GetSortCapabilities").is_ok(),
                "{declaration:?}"
            );
        }

        let xml = format!(
            "<Envelope xmlns=\"{SOAP}\"><Header/><Body><GetSortCapabilities xmlns=\"{CONTENT_DIRECTORY}\"/></Body></Envelope>"
        );

        assert_eq!(
            parse(&xml, "GetSortCapabilities").unwrap().name(),
            "GetSortCapabilities"
        );

        let xml = format!(
            "<Envelope xmlns=\"{SOAP}\"><Body><Browse xmlns=\"{CONTENT_DIRECTORY}\">{}</Browse></Body></Envelope>",
            BROWSE_ARGS
                .replace("<ObjectID>", "<ObjectID xmlns=\"\">")
                .replace("<BrowseFlag>", "<BrowseFlag xmlns=\"\">")
                .replace("<Filter>", "<Filter xmlns=\"\">")
                .replace("<StartingIndex>", "<StartingIndex xmlns=\"\">")
                .replace("<RequestedCount>", "<RequestedCount xmlns=\"\">")
                .replace("<SortCriteria/>", "<SortCriteria xmlns=\"\"/>")
        );

        assert!(parse(&xml, "Browse").is_ok());
    }

    #[test]
    fn soap_namespace_accepts_literal_and_referenced_xml_binding() {
        let base = request("GetSortCapabilities", "");

        for namespace in [
            "http://www.w3.org/XML/1998/namespace",
            "http://www.w3.org/XML/1998/namespac&#101;",
            "http://www.w3.org/XML/1998/namespac&#x65;",
        ] {
            let xml = base.replace("<s:Body>", &format!("<s:Body xmlns:xml=\"{namespace}\">"));

            assert!(parse(&xml, "GetSortCapabilities").is_ok(), "{namespace}");
        }
    }

    #[test]
    fn soap_namespace_rejects_reserved_aliases_after_decoding() {
        let base = request("GetSortCapabilities", "");

        for namespace in [
            "http://www.w3.org/XML/1998/namespace",
            "http://www.w3.org/XML/1998/namespac&#101;",
            "http://www.w3.org/2000/xmlns/",
            "http://www.w3.org/2000/xmlns&#47;",
        ] {
            let xml = base.replace("<s:Body>", &format!("<s:Body xmlns:alias=\"{namespace}\">"));

            assert_eq!(
                parse(&xml, "GetSortCapabilities"),
                Err(Fault { code: 402 }),
                "{namespace}"
            );
        }

        for declaration in [
            "xmlns:xml=\"urn:wrong&#49;\"",
            "xmlns:xml=\"http://www.w3.org/2000/xmlns&#47;\"",
            "xmlns:xmlns=\"http://www.w3.org/2000/xmlns&#47;\"",
            "xmlns:xmlns=\"urn:wrong&#49;\"",
        ] {
            let xml = base.replace("<s:Body>", &format!("<s:Body {declaration}>"));

            assert_eq!(
                parse(&xml, "GetSortCapabilities"),
                Err(Fault { code: 402 }),
                "{declaration}"
            );
        }
    }

    #[test]
    fn soap_namespace_rejects_reserved_default_bindings() {
        let base = request("GetSortCapabilities", "");

        for namespace in [
            "http://www.w3.org/XML/1998/namespace",
            "http://www.w3.org/XML/1998/namespac&#101;",
            "http://www.w3.org/2000/xmlns/",
            "http://www.w3.org/2000/xmlns&#47;",
        ] {
            let xml = base.replace("<s:Body>", &format!("<s:Body xmlns=\"{namespace}\">"));

            assert_eq!(
                parse(&xml, "GetSortCapabilities"),
                Err(Fault { code: 402 }),
                "{namespace}"
            );
        }
    }

    #[test]
    fn soap_namespace_decodes_aliases_once_and_restores_scopes() {
        let xml = format!(
            "<s:Envelope xmlns:s=\"{SOAP}\"><s:Header xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope&#47;\" xmlns:extra=\"urn:test?a=1&amp;b=2\"/><s:Body p:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding&#47;\" xmlns:p=\"http://schemas.xmlsoap.org/soap/envelope&#47;\"><u:GetSortCapabilities xmlns:u=\"urn:schemas-upnp-org:service:ContentDirectory:&#49;\"/></s:Body></s:Envelope>"
        );

        assert!(parse(&xml, "GetSortCapabilities").is_ok());

        let aliased_body = xml
            .replace("<s:Body ", "<p:Body ")
            .replace("</s:Body>", "</p:Body>");

        assert!(parse(&aliased_body, "GetSortCapabilities").is_ok());

        let shadowed = xml.replace(
            "<s:Header xmlns:s=",
            "<p:Header xmlns:s=\"urn:shadow?a=1&amp;b=2\" xmlns:p=",
        );

        assert!(parse(&shadowed, "GetSortCapabilities").is_ok());

        let leaked = xml
            .replace("urn:test?a=1&amp;b=2", SOAP)
            .replace("<s:Body ", "<extra:Body ")
            .replace("</s:Body>", "</extra:Body>");

        assert_eq!(
            parse(&leaked, "GetSortCapabilities"),
            Err(Fault { code: 402 })
        );

        let double_escaped = xml.replace("ContentDirectory:&#49;", "ContentDirectory:&amp;#49;");

        assert_eq!(
            parse(&double_escaped, "GetSortCapabilities"),
            Err(Fault { code: 402 })
        );

        let duplicate = xml.replace(
            "<s:Body p:encodingStyle=",
            "<s:Body s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" p:encodingStyle=",
        );

        assert_eq!(
            parse(&duplicate, "GetSortCapabilities"),
            Err(Fault { code: 402 })
        );
    }

    #[test]
    fn soap_decodes_text_cdata_comments_and_references_without_trimming() {
        // Polish, Japanese and supplementary-plane text exercise UTF-8, not ASCII-only escaping.
        let args = BROWSE_ARGS.replace(
            "<ObjectID>0</ObjectID>",
            "<ObjectID> Łódź 東京 &#x10400;&#32;&amp;&lt;&gt;&quot;&apos;<![CDATA[<&]]><!--split--> z </ObjectID>",
        );

        let Action::Browse(arguments) = browse(&args).unwrap() else {
            panic!("expected Browse");
        };

        assert_eq!(arguments.object_id, " Łódź 東京 𐐀 &<>\"'<& z ");
        assert_eq!(arguments.sort, None);
    }

    #[test]
    fn soap_rejects_namespace_spoofing_and_action_ambiguity() {
        let valid = request("Browse", BROWSE_ARGS);

        for xml in [
            valid.replace(SOAP, "urn:wrong"),
            valid.replace(CONTENT_DIRECTORY, CONNECTION_MANAGER),
            valid.replace("xmlns:u=", "xmlns:unused="),
            valid.replace("<ObjectID>", "<ObjectID xmlns=\"urn:wrong\">"),
            valid.replace("<ObjectID>", "<x:ObjectID>").replace("</ObjectID>", "</x:ObjectID>"),
            valid.replace("<s:Body>", "<Body>").replace("</s:Body>", "</Body>"),
            valid.replace("</u:Browse>", "</u:Browse><u:Browse xmlns:u=\"urn:schemas-upnp-org:service:ContentDirectory:1\"/>"),
            valid.replace("</s:Body>", "</s:Body><s:Body/>"),
            format!("{valid}{valid}"),
            valid.replace("</s:Envelope>", "<s:Header/></s:Envelope>"),
            valid.replace("<s:Body>", "<s:Header><s:Body/></s:Header><s:Body>"),
            valid.replace("<Filter>*</Filter>", "<Filter>*</Filter><Filter/>"),
            valid.replace("<ObjectID>0</ObjectID>", "<ObjectID><nested/></ObjectID>"),
            valid.replace("<s:Body>", "<s:Body>unexpected"),
            valid.replace("</u:Browse>", "</u:Search>"),
            valid.replace("</s:Envelope>", ""),
        ] {
            assert_eq!(parse(&xml, "Browse"), Err(Fault { code: 402 }), "{xml}");
        }

        for header in [
            format!("{CONTENT_DIRECTORY}#Search"),
            format!("{CONNECTION_MANAGER}#Browse"),
            format!("\"{CONTENT_DIRECTORY}#Browse"),
            format!("{CONTENT_DIRECTORY}#Browse\""),
            format!("{CONTENT_DIRECTORY}#Browse#Browse"),
            String::new(),
        ] {
            assert_eq!(
                parse_action(valid.as_bytes(), &header, Service::ContentDirectory),
                Err(Fault { code: 402 })
            );
        }
    }

    #[test]
    fn soap_rejects_dtd_entities_bad_characters_attributes_and_declarations() {
        for text in [
            "&custom;",
            "&#0;",
            "&#xFFFF;",
            "&#xD800;",
            "&#x110000;",
            "&#;",
            "&amp",
            "raw & text",
            "]]>",
            "\u{1}",
        ] {
            let args = BROWSE_ARGS.replace(
                "<ObjectID>0</ObjectID>",
                &format!("<ObjectID>{text}</ObjectID>"),
            );

            assert!(browse(&args).is_err(), "{text}");
        }

        let valid = request("GetSortCapabilities", "");

        for xml in [
            format!("<!DOCTYPE s:Envelope [<!ENTITY custom 'expanded'>]>{valid}"),
            format!("<!DOCTYPE s:Envelope SYSTEM 'file:///etc/passwd'>{valid}"),
            valid.replace("<s:Body>", "<s:Body xmlns:x=\"&custom;\">"),
            valid.replace("<s:Body>", "<s:Body xmlns:x=\"x\" xmlns:x=\"y\">"),
            valid.replace("<s:Body>", "<s:Body xmlns:x=\"<bad\">"),
            valid.replace("<s:Body>", "<s:Body x:a=\"value\">"),
            valid.replace("<s:Body>", &format!("<s:Body xmlns:x=\"{SOAP}\" s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" x:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">")),
            valid.replace("<s:Body>", "<s:Body><!-- bad -- comment -->"),
            format!("<?xml version='1.1'?>{valid}"),
            format!("<?xml version='1.0' encoding='ISO-8859-1'?>{valid}"),
            format!("<?xml version='1.0' version='1.0'?>{valid}"),
            format!("<?xml version='1.0' nonsense='yes'?>{valid}"),
            format!("<?xml version='1.0' standalone='maybe'?>{valid}"),
            format!("<?xml version='1.0' standalone='yes' encoding='utf-8'?>{valid}"),
            format!(" <?xml version='1.0'?>{valid}"),
            format!("<!--prolog--><?xml version='1.0'?>{valid}"),
            format!("{valid}<?xml version='1.0'?>"),
        ] {
            assert!(parse(&xml, "GetSortCapabilities").is_err(), "{xml}");
        }

        assert!(parse_action(&[0xff], "bad", Service::ContentDirectory).is_err());
    }

    #[test]
    fn soap_bounds_body_and_rejects_deep_argument_trees() {
        let base = request("GetSortCapabilities", "");
        let exact = format!("{}{}", base, " ".repeat(SOAP_BODY_BYTES - base.len()));
        assert!(parse(&exact, "GetSortCapabilities").is_ok());

        assert_eq!(
            parse(&(exact + " "), "GetSortCapabilities"),
            Err(Fault { code: 501 })
        );

        assert_eq!(
            parse_action(
                base.as_bytes(),
                &"x".repeat(HEADER_BYTES + 1),
                Service::ContentDirectory
            ),
            Err(Fault { code: 501 })
        );

        let nested = format!(
            "{}x{}",
            "<nested>".repeat(XML_DEPTH + 1),
            "</nested>".repeat(XML_DEPTH + 1)
        );

        let args = BROWSE_ARGS.replace(
            "<ObjectID>0</ObjectID>",
            &format!("<ObjectID>{nested}</ObjectID>"),
        );

        assert!(browse(&args).is_err());
    }

    #[test]
    fn soap_browse_validates_shape_and_fault_precedence() {
        let Action::Browse(arguments) = browse(BROWSE_ARGS).unwrap() else {
            panic!("expected Browse");
        };

        assert_eq!(arguments.object_id, "0");
        assert!(!arguments.metadata);
        assert_eq!(arguments.starting_index, 0);
        assert_eq!(arguments.requested_count, 0);
        assert_eq!(arguments.sort, None);
        assert_eq!(arguments.filter, Filter::parse("*").unwrap());

        for argument in [
            "<ObjectID>0</ObjectID>",
            "<BrowseFlag>BrowseDirectChildren</BrowseFlag>",
            "<Filter>*</Filter>",
            "<StartingIndex>0</StartingIndex>",
            "<RequestedCount>0</RequestedCount>",
            "<SortCriteria/>",
        ] {
            let args = BROWSE_ARGS
                .replace(argument, "")
                .replace("<SortCriteria/>", "<SortCriteria>bad</SortCriteria>");

            assert_eq!(browse(&args), Err(Fault { code: 402 }));
            assert_eq!(browse(&(args + "<Extra/>")), Err(Fault { code: 402 }));
        }

        let args = BROWSE_ARGS.replace("<Filter>*</Filter>", "<Filter>res,,</Filter>");
        assert_eq!(browse(&args), Err(Fault { code: 402 }));
        let args = args.replace("<SortCriteria/>", "<SortCriteria>bad</SortCriteria>");
        assert_eq!(browse(&args), Err(Fault { code: 709 }));

        let args = args
            .replace("<StartingIndex>0", "<StartingIndex>1")
            .replace("BrowseDirectChildren", "BrowseMetadata");

        assert_eq!(browse(&args), Err(Fault { code: 402 }));

        let args = BROWSE_ARGS
            .replace("<ObjectID>0", "<ObjectID>not-an-id")
            .replace("<StartingIndex>0", "<StartingIndex>4294967295")
            .replace("<RequestedCount>0", "<RequestedCount>4294967295");

        let Action::Browse(arguments) = browse(&args).unwrap() else {
            panic!("expected Browse");
        };

        assert_eq!(arguments.object_id, "not-an-id");
        assert_eq!(arguments.starting_index, u32::MAX);
        assert_eq!(arguments.requested_count, u32::MAX);
    }

    #[test]
    fn soap_validates_action_inputs_and_sort_syntax() {
        assert_eq!(
            parse(&request("Search", ""), "Search"),
            Err(Fault { code: 401 })
        );

        assert_eq!(
            parse(
                &request("GetSortCapabilities", "<Extra/>"),
                "GetSortCapabilities"
            ),
            Err(Fault { code: 402 })
        );

        for value in ["", "-1", "+1", " 1", "1 ", "4294967296", "1.0"] {
            for name in ["StartingIndex", "RequestedCount"] {
                let args = BROWSE_ARGS.replace(
                    &format!("<{name}>0</{name}>"),
                    &format!("<{name}>{value}</{name}>"),
                );

                assert_eq!(browse(&args), Err(Fault { code: 402 }), "{args}");
            }
        }

        assert!(
            browse(&BROWSE_ARGS.replace("<RequestedCount>0", "<RequestedCount>4294967295")).is_ok()
        );

        for (sort, expected) in [
            ("", None),
            ("+dc:date", Some(false)),
            ("-dc:date", Some(true)),
        ] {
            let args = BROWSE_ARGS.replace(
                "<SortCriteria/>",
                &format!("<SortCriteria>{sort}</SortCriteria>"),
            );

            let Action::Browse(arguments) = browse(&args).unwrap() else {
                panic!("expected Browse");
            };

            assert_eq!(arguments.sort, expected);
        }

        for sort in [
            "dc:date",
            "+dc:title",
            "+dc:date,-dc:date",
            "+dc:date,+dc:date",
            " +dc:date",
            "*",
        ] {
            for flag in ["BrowseDirectChildren", "BrowseMetadata"] {
                let args = BROWSE_ARGS
                    .replace(
                        "<SortCriteria/>",
                        &format!("<SortCriteria>{sort}</SortCriteria>"),
                    )
                    .replace("BrowseDirectChildren", flag);

                assert_eq!(browse(&args), Err(Fault { code: 709 }));
            }
        }

        assert_eq!(
            browse(&BROWSE_ARGS.replace("<Filter>*</Filter>", "")),
            Err(Fault { code: 402 })
        );

        assert_eq!(
            browse(&BROWSE_ARGS.replace("BrowseDirectChildren", "Unknown")),
            Err(Fault { code: 402 })
        );

        let args = BROWSE_ARGS
            .replace("BrowseDirectChildren", "BrowseMetadata")
            .replace("<StartingIndex>0", "<StartingIndex>1");

        assert_eq!(browse(&args), Err(Fault { code: 402 }));

        for (value, expected) in [
            ("0", Ok(Action::GetCurrentConnectionInfo(0))),
            ("+1", Ok(Action::GetCurrentConnectionInfo(1))),
            (
                "-2147483648",
                Ok(Action::GetCurrentConnectionInfo(i32::MIN)),
            ),
            ("2147483647", Ok(Action::GetCurrentConnectionInfo(i32::MAX))),
            ("2147483648", Err(Fault { code: 402 })),
            ("-2147483649", Err(Fault { code: 402 })),
            ("", Err(Fault { code: 402 })),
            (" 0", Err(Fault { code: 402 })),
            ("bad", Err(Fault { code: 402 })),
        ] {
            let xml = request(
                "GetCurrentConnectionInfo",
                &format!("<ConnectionID>{value}</ConnectionID>"),
            )
            .replace(CONTENT_DIRECTORY, CONNECTION_MANAGER);

            assert_eq!(
                parse_action(
                    xml.as_bytes(),
                    &format!("{CONNECTION_MANAGER}#GetCurrentConnectionInfo"),
                    Service::ConnectionManager
                ),
                expected,
                "{value}"
            );
        }
    }

    #[test]
    fn soap_returns_typed_static_actions_and_rejects_wrong_service_actions() {
        for (service, action) in [
            (Service::ContentDirectory, Action::GetSearchCapabilities),
            (Service::ContentDirectory, Action::GetSortCapabilities),
            (Service::ContentDirectory, Action::GetSystemUpdateId),
            (Service::ConnectionManager, Action::GetProtocolInfo),
            (Service::ConnectionManager, Action::GetCurrentConnectionIds),
        ] {
            let name = action.name();
            let xml = request(name, "").replace(CONTENT_DIRECTORY, service.namespace());

            assert_eq!(
                parse_action(
                    xml.as_bytes(),
                    &format!("{}#{name}", service.namespace()),
                    service
                ),
                Ok(action)
            );
        }

        for (service, name) in [
            (Service::ContentDirectory, "GetProtocolInfo"),
            (Service::ConnectionManager, "Browse"),
            (Service::ContentDirectory, "Search"),
        ] {
            let xml = request(name, "<Extra/>").replace(CONTENT_DIRECTORY, service.namespace());

            assert_eq!(
                parse_action(
                    xml.as_bytes(),
                    &format!("{}#{name}", service.namespace()),
                    service
                ),
                Err(Fault { code: 401 })
            );
        }
    }

    #[test]
    fn filters_select_only_requested_available_properties() {
        assert_eq!(Filter::parse(" \t ").unwrap(), Filter::default());
        let all = Filter::parse("*").unwrap();
        assert!(all.date() && all.art() && all.res() && all.duration() && all.child_count());

        for selector in ["res", "res@protocolInfo", "res@size", "res@resolution"] {
            let filter = Filter::parse(selector).unwrap();
            assert!(filter.res());
            assert!(!filter.duration());
        }

        let filter =
            Filter::parse(" dc:date, upnp:albumArtURI , res@duration , @childCount ").unwrap();

        assert_eq!(filter, all);

        assert_eq!(Filter::parse("dc:title,upnp:class,@id,@parentID,@restricted,dc:unknown,res@unknown,unknown:property").unwrap(), Filter::default());

        assert_eq!(
            Filter::parse("dc:date,dc:date").unwrap(),
            Filter::parse("dc:date").unwrap()
        );

        for invalid in [
            ",",
            "res,",
            ",res",
            "res,,dc:date",
            "@",
            "res@",
            "res@@size",
            "dc:",
            ":date",
            "dc:date:extra",
            "res @size",
            "<res>",
            "res/size",
            "dc:*",
            "res@*",
        ] {
            assert_eq!(
                Filter::parse(invalid),
                Err(Fault { code: 402 }),
                "{invalid}"
            );
        }
    }

    #[test]
    fn didl_keeps_mandatory_fields_first_and_resource_order_truthful() {
        let objects = [object()];
        let minimal = didl(&objects, &Filter::parse("").unwrap()).unwrap();
        assert_xml(&minimal);
        assert!(minimal.contains("restricted=\"1\"><dc:title>A&amp;B</dc:title><upnp:class>object.item.videoItem</upnp:class>"));

        for absent in ["<res", "<dc:date>", "<upnp:albumArtURI", "childCount="] {
            assert!(!minimal.contains(absent));
        }

        let full = didl(&objects, &Filter::parse("*").unwrap()).unwrap();
        assert_xml(&full);
        assert!(full.contains("<dc:date>2026-01-02</dc:date>"));

        assert!(
            full.contains(
                "<upnp:albumArtURI>http://192.0.2.1/preview?a=1&amp;b=2</upnp:albumArtURI>"
            )
        );

        assert!(
            full.contains(
                "protocolInfo=\"http-get:*:video/quicktime:*\" duration=\"123:04:05.006\""
            )
        );

        assert!(full.contains(
            "<res protocolInfo=\"http-get:*:video/mp4:*\">http://192.0.2.1/playback</res>"
        ));

        assert!(full.find("/original").unwrap() < full.find("/playback").unwrap());

        for absent in ["size=", "resolution=", "DLNA.", "childCount="] {
            assert!(!full.contains(absent));
        }

        let bare_res = didl(&objects, &Filter::parse("res").unwrap()).unwrap();
        assert!(bare_res.contains("<res protocolInfo="));
        assert!(!bare_res.contains("duration="));

        let mut root = object();
        root.class = "object.container".into();
        root.child_count = Some(12);
        root.resources.clear();
        let xml = didl(&[root.clone()], &Filter::parse("@childCount").unwrap()).unwrap();
        assert!(xml.contains("<container "));
        assert!(xml.contains("childCount=\"12\"><dc:title>"));

        root.class = "object.container.album".into();
        root.child_count = None;
        root.date = None;
        root.art = None;
        let xml = didl(&[root], &Filter::parse("*").unwrap()).unwrap();
        assert!(xml.contains("<container "));
        assert!(!xml.contains("childCount="));
        assert!(!xml.contains("<dc:date>"));
        assert!(!xml.contains("<upnp:albumArtURI>"));
    }

    #[test]
    fn serialization_sanitizes_xml_and_escapes_exactly_two_layers() {
        assert_eq!(
            escape_text("Łódź 東京 𐐀 &<>\"'\u{0}\u{b}\u{ffff}\r\r\n\t\n"),
            "Łódź 東京 𐐀 &amp;&lt;&gt;&quot;&apos;\u{fffd}\u{fffd}\u{fffd}&#13;&#13;\n\t\n"
        );

        let mut object = object();
        object.id = "\" injected=\"yes<&".into();
        object.title = "A&B <title> Łódź 東京 𐐀\u{1}".into();
        object.resources[0].mime = "video/mp4\" bad=\"value".into();
        let xml = didl(&[object], &Filter::parse("*").unwrap()).unwrap();
        assert_xml(&xml);
        assert!(xml.contains("id=\"&quot; injected=&quot;yes&lt;&amp;\""));

        let response = action_response(
            Service::ContentDirectory,
            "Browse",
            &[("Result", &xml), ("NumberReturned", "1")],
        )
        .unwrap();

        assert_xml(&response);
        assert!(response.contains("A&amp;amp;B &amp;lt;title&amp;gt; Łódź 東京 𐐀\u{fffd}"));
        assert!(!response.contains("A&amp;amp;amp;B"));
        assert!(response.contains("<NumberReturned>1</NumberReturned>"));

        assert!(response.contains(&format!(
            "<u:BrowseResponse xmlns:u=\"{CONTENT_DIRECTORY}\">"
        )));

        assert_eq!(
            action_response(Service::ContentDirectory, "Browse", &[("bad:name", "x")]),
            Err(Fault { code: 402 })
        );

        assert_eq!(
            action_response(
                Service::ContentDirectory,
                "Browse",
                &[("Result", "x"), ("Result", "y")]
            ),
            Err(Fault { code: 402 })
        );

        assert_eq!(
            action_response(Service::ContentDirectory, "Bad><xml", &[]),
            Err(Fault { code: 401 })
        );
    }

    #[test]
    fn both_serialization_layers_fail_at_the_bound_without_partial_results() {
        let mut item = object();
        item.title.push_str("\rline\r\nnext");
        let objects = [item];
        let filter = Filter::parse("*").unwrap();
        let xml = didl(&objects, &filter).unwrap();
        let encoded_size = escape_text(&xml).len();
        assert!(encoded_size > xml.len());
        assert_eq!(didl_bounded(&objects, &filter, encoded_size).unwrap(), xml);

        assert_eq!(
            didl_bounded(&objects, &filter, encoded_size - 1),
            Err(Fault { code: 501 })
        );

        assert_eq!(
            didl_bounded(&objects, &filter, xml.len()),
            Err(Fault { code: 501 })
        );

        let args = [("Result", xml.as_str())];
        let response = action_response(Service::ContentDirectory, "Browse", &args).unwrap();

        assert_eq!(
            action_response_bounded(Service::ContentDirectory, "Browse", &args, response.len())
                .unwrap(),
            response
        );

        assert_eq!(
            action_response_bounded(
                Service::ContentDirectory,
                "Browse",
                &args,
                response.len() - 1
            ),
            Err(Fault { code: 501 })
        );

        let mut writer = Xml::new(8, false);
        assert_eq!(writer.text("&&"), Err(Fault { code: 501 }));
        assert_eq!(writer.value, "&amp;");
        assert!(writer.value.len() <= writer.limit);

        let mut writer = Xml::new(8, true);
        assert_eq!(writer.text("&"), Err(Fault { code: 501 }));
        assert!(writer.value.is_empty());

        let mut writer = Xml::new(3, false);
        assert_eq!(writer.text("𐐀"), Err(Fault { code: 501 }));
        assert!(writer.value.is_empty());
    }

    #[test]
    fn descriptions_and_faults_have_correct_namespaces_and_fixed_contracts() {
        let uuid = uuid::Uuid::parse_str("7B37DF49-B75D-4BCB-89A6-0C917A934643").unwrap();
        let device = device_description("Łódź & <photos>\u{0}\rline\r\nnext", uuid);
        assert_xml(&device);

        assert!(device.contains(
            "<friendlyName>Łódź &amp; &lt;photos&gt;\u{fffd}&#13;line&#13;\nnext</friendlyName>"
        ));

        assert!(device.contains("<UDN>uuid:7b37df49-b75d-4bcb-89a6-0c917a934643</UDN>"));
        assert!(device.contains("<major>1</major><minor>0</minor>"));

        for service in ["content-directory", "connection-manager"] {
            for (element, route) in [
                ("SCPDURL", "scpd.xml"),
                ("controlURL", "control"),
                ("eventSubURL", "events"),
            ] {
                assert!(
                    device.contains(&format!("<{element}>/upnp/{service}/{route}</{element}>"))
                );
            }
        }

        for (code, description) in [
            (401, "Invalid Action"),
            (402, "Invalid Args"),
            (501, "Action Failed"),
            (701, "No Such Object"),
            (706, "Invalid Connection Reference"),
            (709, "Unsupported or Invalid Sort Criteria"),
            (710, "No Such Container"),
        ] {
            let xml = fault_xml(Fault { code });
            assert_xml(&xml);
            assert!(xml.contains("<faultcode>s:Client</faultcode>"));
            assert!(xml.contains("<UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\">"));

            assert!(xml.contains(&format!(
                "<errorCode>{code}</errorCode><errorDescription>{description}</errorDescription>"
            )));
        }

        for service in [Service::ContentDirectory, Service::ConnectionManager] {
            let xml = scpd(service);
            assert_xml(xml);
            assert!(xml.contains("<scpd xmlns=\"urn:schemas-upnp-org:service-1-0\">"));

            for absent in [
                "<name>Search</name>",
                "<name>CreateObject</name>",
                "<name>DestroyObject</name>",
                "<name>PrepareForConnection</name>",
                "<name>ConnectionComplete</name>",
                "ContainerUpdateIDs",
            ] {
                assert!(!xml.contains(absent));
            }

            let expected: &[&str] = match service {
                Service::ContentDirectory => &["SystemUpdateID"],

                Service::ConnectionManager => &[
                    "SourceProtocolInfo",
                    "SinkProtocolInfo",
                    "CurrentConnectionIDs",
                ],
            };

            assert_eq!(xml.matches("sendEvents=\"yes\"").count(), expected.len());

            for name in expected {
                assert!(xml.contains(&format!(
                    "<stateVariable sendEvents=\"yes\"><name>{name}</name>"
                )));
            }

            let mut reader = NsReader::from_str(xml);
            let mut stack = Vec::new();
            let mut actions = Vec::new();
            let mut states = BTreeSet::new();
            let mut references = BTreeSet::new();

            loop {
                match reader.read_event().unwrap() {
                    Event::Start(element) => stack
                        .push(String::from_utf8(element.local_name().as_ref().to_vec()).unwrap()),

                    Event::End(_) => {
                        stack.pop();
                    }

                    Event::Text(text) => {
                        let text = text.decode().unwrap().into_owned();

                        if stack
                            .last()
                            .is_some_and(|name| name == "relatedStateVariable")
                        {
                            references.insert(text.clone());
                        }

                        if stack.last().is_some_and(|name| name == "name") {
                            match stack[stack.len() - 2].as_str() {
                                "action" => actions.push(text),

                                "stateVariable" => {
                                    states.insert(text);
                                }

                                _ => {}
                            }
                        }
                    }

                    Event::Eof => break,

                    _ => {}
                }
            }

            assert!(references.is_subset(&states));

            assert_eq!(
                actions.len(),
                if service == Service::ContentDirectory {
                    4
                } else {
                    3
                }
            );

            for action in actions {
                assert!(service.inputs(&action).is_ok());
            }
        }
    }
}
