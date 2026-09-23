use std::collections::BTreeMap;

use quick_xml::{
    Reader,
    events::{BytesStart, Event},
    name::{Namespace, NamespaceResolver, PrefixDeclaration, ResolveResult},
};

use crate::catalog::{BrowseMode, Object, SortOrder};

pub(crate) const SOAP_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

pub(crate) const MEDIA_SERVER: &str = "urn:schemas-upnp-org:device:MediaServer:1";
pub const CONTENT_DIRECTORY: &str = "urn:schemas-upnp-org:service:ContentDirectory:1";
pub(crate) const CONNECTION_MANAGER: &str = "urn:schemas-upnp-org:service:ConnectionManager:1";
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

            _ => Err(Fault::InvalidAction),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Fault {
    InvalidAction,
    InvalidArgs,
    ActionFailed,
    NoSuchObject,
    InvalidConnectionReference,
    InvalidSortCriteria,
    NoSuchContainer,
}

impl Fault {
    pub fn code(self) -> u16 {
        match self {
            Self::InvalidAction => 401,
            Self::InvalidArgs => 402,
            Self::ActionFailed => 501,
            Self::NoSuchObject => 701,
            Self::InvalidConnectionReference => 706,
            Self::InvalidSortCriteria => 709,
            Self::NoSuchContainer => 710,
        }
    }
}

pub(crate) fn fault_xml(fault: Fault) -> String {
    let description = match fault {
        Fault::InvalidAction => "Invalid Action",
        Fault::InvalidArgs => "Invalid Args",
        Fault::ActionFailed => "Action Failed",
        Fault::NoSuchObject => "No Such Object",
        Fault::InvalidConnectionReference => "Invalid Connection Reference",
        Fault::InvalidSortCriteria => "Unsupported or Invalid Sort Criteria",
        Fault::NoSuchContainer => "No Such Container",
    };

    format!(
        "{ENVELOPE_START}<s:Fault><faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring><detail><UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\"><errorCode>{}</errorCode><errorDescription>{description}</errorDescription></UPnPError></detail></s:Fault>{ENVELOPE_END}",
        fault.code()
    )
}

pub fn device_description(friendly_name: &str, uuid: uuid::Uuid) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root xmlns=\"urn:schemas-upnp-org:device-1-0\"><specVersion><major>1</major><minor>0</minor></specVersion><device><deviceType>{MEDIA_SERVER}</deviceType><friendlyName>{}</friendlyName><manufacturer>immich-dlna-proxy</manufacturer><modelName>immich-dlna-proxy</modelName><modelNumber>{}</modelNumber><UDN>uuid:{uuid}</UDN><serviceList><service><serviceType>{CONTENT_DIRECTORY}</serviceType><serviceId>urn:upnp-org:serviceId:ContentDirectory</serviceId><SCPDURL>/upnp/content-directory/scpd.xml</SCPDURL><controlURL>/upnp/content-directory/control</controlURL><eventSubURL>/upnp/content-directory/events</eventSubURL></service><service><serviceType>{CONNECTION_MANAGER}</serviceType><serviceId>urn:upnp-org:serviceId:ConnectionManager</serviceId><SCPDURL>/upnp/connection-manager/scpd.xml</SCPDURL><controlURL>/upnp/connection-manager/control</controlURL><eventSubURL>/upnp/connection-manager/events</eventSubURL></service></serviceList></device></root>",
        escape_text(friendly_name),
        env!("CARGO_PKG_VERSION")
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Action {
    Browse {
        query: BrowseArguments,
        filter: Filter,
    },
    GetSearchCapabilities,
    GetSortCapabilities,
    GetSystemUpdateId,
    GetProtocolInfo,
    GetCurrentConnectionIds,
    GetCurrentConnectionInfo(i32),
}

// Wire arguments retain ObjectID text until request admission and catalog resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BrowseArguments {
    pub(crate) object_id: String,
    pub(crate) mode: BrowseMode,
    pub(crate) sort: SortOrder,
}

impl Action {
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Browse { .. } => "Browse",
            Self::GetSearchCapabilities => "GetSearchCapabilities",
            Self::GetSortCapabilities => "GetSortCapabilities",
            Self::GetSystemUpdateId => "GetSystemUpdateID",
            Self::GetProtocolInfo => "GetProtocolInfo",
            Self::GetCurrentConnectionIds => "GetCurrentConnectionIDs",
            Self::GetCurrentConnectionInfo(_) => "GetCurrentConnectionInfo",
        }
    }
}

fn action_arguments(
    service: Service,
    name: &str,
    mut arguments: BTreeMap<String, String>,
) -> Result<Action, Fault> {
    let invalid = Fault::InvalidArgs;
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

        _ => return Err(Fault::InvalidAction),
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
        "" => SortOrder::Catalog,
        "+dc:date" => SortOrder::DateAscending,
        "-dc:date" => SortOrder::DateDescending,
        _ => return Err(Fault::InvalidSortCriteria),
    };

    let filter = Filter::parse(&arguments["Filter"])?;

    Ok(Action::Browse {
        query: BrowseArguments {
            object_id: arguments.remove("ObjectID").ok_or(invalid)?,
            mode: if metadata {
                BrowseMode::Metadata
            } else {
                BrowseMode::DirectChildren {
                    starting_index,
                    requested_count,
                }
            },
            sort,
        },
        filter,
    })
}

pub(crate) fn xml_char(c: char) -> bool {
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
            std::str::from_utf8(namespace.into_inner()).map_err(|_| Fault::InvalidArgs)
        }

        ResolveResult::Unbound => Ok(""),

        ResolveResult::Unknown(_) => Err(Fault::InvalidArgs),
    }
}

pub(crate) fn parse_action(
    body: &[u8],
    soap_action: &str,
    service: Service,
) -> Result<Action, Fault> {
    let invalid = Fault::InvalidArgs;

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
                let qualified = element.name();
                let spelling = std::str::from_utf8(qualified.as_ref()).map_err(|_| invalid)?;

                if !qname(spelling) {
                    return Err(invalid);
                }

                // Open a scope without raw bindings; validate and add decoded declarations below.
                namespaces
                    .push(&BytesStart::new("scope"))
                    .map_err(|_| invalid)?;

                let mut ordinary = None;

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
                    } else if ordinary.replace((attribute.key, value)).is_some() {
                        return Err(invalid);
                    }
                }

                let (resolved, local) = namespaces.resolve_element(qualified);
                let namespace = resolved_namespace(resolved)?;
                let local = std::str::from_utf8(local.as_ref()).map_err(|_| invalid)?;

                if let Some((key, value)) = ordinary {
                    let (resolved, local) = namespaces.resolve_attribute(key);
                    let ns = resolved_namespace(resolved)?;

                    if ns != SOAP
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
            return Err(Fault::ActionFailed);
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

pub(crate) fn action_response(
    service: Service,
    action: &'static str,
    args: &[(&'static str, &str)],
) -> Result<String, Fault> {
    action_response_bounded(service, action, args, SOAP_RESPONSE_BYTES)
}

fn action_response_bounded(
    service: Service,
    action: &'static str,
    args: &[(&'static str, &str)],
    limit: usize,
) -> Result<String, Fault> {
    let mut xml = Xml::new(limit, false);
    xml.raw(ENVELOPE_START)?;
    xml.raw("<u:")?;
    xml.raw(action)?;
    xml.raw("Response xmlns:u=\"")?;
    xml.raw(service.namespace())?;
    xml.raw("\">")?;

    for &(name, value) in args {
        xml.element(name, value)?;
    }

    xml.raw("</u:")?;
    xml.raw(action)?;
    xml.raw("Response>")?;
    xml.raw(ENVELOPE_END)?;

    Ok(xml.value)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
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
                return Err(Fault::InvalidArgs);
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

    pub(crate) fn res(&self) -> bool {
        self.res
    }
}

pub fn didl(objects: &[Object], filter: &Filter) -> Result<String, Fault> {
    didl_bounded(objects, filter, SOAP_RESPONSE_BYTES)
}

fn didl_bounded(objects: &[Object], filter: &Filter, limit: usize) -> Result<String, Fault> {
    let mut xml = Xml::new(limit, true);
    xml.raw("<DIDL-Lite xmlns=\"urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:upnp=\"urn:schemas-upnp-org:metadata-1-0/upnp/\">")?;

    for object in objects {
        let container = matches!(
            object.kind,
            crate::catalog::ObjectKind::Root { .. } | crate::catalog::ObjectKind::Album { .. }
        );

        let tag = if container { "container" } else { "item" };
        xml.raw("<")?;
        xml.raw(tag)?;
        xml.raw(" id=\"")?;
        xml.text(&object.id().to_string())?;
        xml.raw("\" parentID=\"")?;

        xml.text(
            &object
                .parent_id()
                .map_or_else(|| "-1".into(), |id| id.to_string()),
        )?;

        xml.raw("\" restricted=\"1\"")?;

        if container
            && filter.child_count
            && let Some(count) = object.child_count()
        {
            xml.raw(" childCount=\"")?;
            xml.raw(&count.to_string())?;
            xml.raw("\"")?;
        }

        xml.raw(">")?;
        xml.element("dc:title", &object.title)?;
        xml.element("upnp:class", object.class())?;

        if filter.date
            && let Some(date) = &object.date
        {
            xml.element("dc:date", date)?;
        }

        if filter.art
            && let Some(art) = &object.art
        {
            xml.element("upnp:albumArtURI", art)?;
        }

        if filter.res() {
            for resource in object.resources() {
                xml.raw("<res protocolInfo=\"http-get:*:")?;
                xml.text(&resource.mime)?;
                xml.raw(if resource.byte_seek {
                    ":DLNA.ORG_OP=01\""
                } else {
                    ":*\""
                })?;

                if filter.duration
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

pub(crate) fn event_body(service: Service, system_update_id: u32) -> String {
    let properties = match service {
        Service::ContentDirectory => {
            format!("<e:property><SystemUpdateID>{system_update_id}</SystemUpdateID></e:property>")
        }

        Service::ConnectionManager => "<e:property><SourceProtocolInfo>http-get:*:*:*</SourceProtocolInfo></e:property><e:property><SinkProtocolInfo></SinkProtocolInfo></e:property><e:property><CurrentConnectionIDs>0</CurrentConnectionIDs></e:property>".into(),
    };

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><e:propertyset xmlns:e=\"urn:schemas-upnp-org:event-1-0\">{properties}</e:propertyset>"
    )
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
mod tests;
