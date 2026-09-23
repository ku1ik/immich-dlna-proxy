use std::collections::BTreeSet;

use quick_xml::NsReader;

use super::*;
use crate::catalog::{ObjectKind, Resource};
use uuid::Uuid;

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
        title: "A&B".into(),
        date: Some("2026-01-02".into()),
        art: Some("http://192.0.2.1/preview?a=1&b=2".into()),
        kind: ObjectKind::Video {
            album: Uuid::from_u128(10),
            asset: Uuid::from_u128(11),
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
        },
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
fn event_propertyset_namespaces_and_static_values_are_correct() {
    for (service, expected) in [
        (
            Service::ContentDirectory,
            vec![("SystemUpdateID", "4294967295")],
        ),
        (
            Service::ConnectionManager,
            vec![
                ("SourceProtocolInfo", "http-get:*:*:*"),
                ("SinkProtocolInfo", ""),
                ("CurrentConnectionIDs", "0"),
            ],
        ),
    ] {
        let body = event_body(service, u32::MAX);
        let mut reader = NsReader::from_str(&body);
        let mut variables = Vec::new();

        loop {
            match reader.read_resolved_event().unwrap() {
                (namespace, Event::Start(start)) => {
                    let name = String::from_utf8(start.local_name().as_ref().to_vec()).unwrap();

                    if matches!(name.as_str(), "propertyset" | "property") {
                        assert_eq!(
                            namespace,
                            ResolveResult::Bound(quick_xml::name::Namespace(
                                b"urn:schemas-upnp-org:event-1-0"
                            ))
                        );
                    } else {
                        assert_eq!(namespace, ResolveResult::Unbound);
                        variables.push((name, String::new()));
                    }
                }

                (_, Event::Text(text)) => variables
                    .last_mut()
                    .unwrap()
                    .1
                    .push_str(&text.decode().unwrap()),

                (_, Event::Eof) => break,

                _ => {}
            }
        }

        assert_eq!(
            variables,
            expected
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn byte_seek_is_explicit_per_resource_and_does_not_add_other_dlna_claims() {
    let mut item = object();

    let ObjectKind::Video { resources, .. } = &mut item.kind else {
        panic!("expected video");
    };

    resources[1].byte_seek = true;

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
            Err(Fault::InvalidArgs),
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
            Err(Fault::InvalidArgs),
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
            Err(Fault::InvalidArgs),
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
        Err(Fault::InvalidArgs)
    );

    let double_escaped = xml.replace("ContentDirectory:&#49;", "ContentDirectory:&amp;#49;");

    assert_eq!(
        parse(&double_escaped, "GetSortCapabilities"),
        Err(Fault::InvalidArgs)
    );

    let duplicate = xml.replace(
        "<s:Body p:encodingStyle=",
        "<s:Body s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\" p:encodingStyle=",
    );

    assert_eq!(
        parse(&duplicate, "GetSortCapabilities"),
        Err(Fault::InvalidArgs)
    );
}

#[test]
fn soap_decodes_text_cdata_comments_and_references_without_trimming() {
    // Polish, Japanese and supplementary-plane text exercise UTF-8, not ASCII-only escaping.
    let args = BROWSE_ARGS.replace(
        "<ObjectID>0</ObjectID>",
        "<ObjectID> Łódź 東京 &#x10400;&#32;&amp;&lt;&gt;&quot;&apos;<![CDATA[<&]]><!--split--> z </ObjectID>",
    );

    let Action::Browse { query, .. } = browse(&args).unwrap() else {
        panic!("expected Browse");
    };

    assert_eq!(query.object_id, " Łódź 東京 𐐀 &<>\"'<& z ");
}

#[test]
fn soap_rejects_namespace_spoofing_and_action_ambiguity() {
    let valid = request("Browse", BROWSE_ARGS);

    for xml in [
        valid.replace(SOAP, "urn:wrong"),
        valid.replace(CONTENT_DIRECTORY, CONNECTION_MANAGER),
        valid.replace("xmlns:u=", "xmlns:unused="),
        valid.replace("<ObjectID>", "<ObjectID xmlns=\"urn:wrong\">"),
        valid
            .replace("<ObjectID>", "<x:ObjectID>")
            .replace("</ObjectID>", "</x:ObjectID>"),
        valid
            .replace("<s:Body>", "<Body>")
            .replace("</s:Body>", "</Body>"),
        valid.replace(
            "</u:Browse>",
            "</u:Browse><u:Browse xmlns:u=\"urn:schemas-upnp-org:service:ContentDirectory:1\"/>",
        ),
        valid.replace("</s:Body>", "</s:Body><s:Body/>"),
        format!("{valid}{valid}"),
        valid.replace("</s:Envelope>", "<s:Header/></s:Envelope>"),
        valid.replace("<s:Body>", "<s:Header><s:Body/></s:Header><s:Body>"),
        valid.replace("<Filter>*</Filter>", "<Filter>*</Filter><Filter/>"),
        valid.replace("<s:Body>", "<s:Body>unexpected"),
        valid.replace("</u:Browse>", "</u:Search>"),
        valid.replace("</s:Envelope>", ""),
    ] {
        assert_eq!(parse(&xml, "Browse"), Err(Fault::InvalidArgs), "{xml}");
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
            Err(Fault::InvalidArgs)
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
fn soap_rejects_nested_argument_elements() {
    for nested in ["<nested/>", "<nested>x</nested>"] {
        let args = BROWSE_ARGS.replace(
            "<ObjectID>0</ObjectID>",
            &format!("<ObjectID>{nested}</ObjectID>"),
        );

        assert_eq!(browse(&args), Err(Fault::InvalidArgs), "{nested}");
    }
}

#[test]
fn soap_browse_validates_shape_and_fault_precedence() {
    let Action::Browse { query, filter } = browse(BROWSE_ARGS).unwrap() else {
        panic!("expected Browse");
    };

    assert_eq!(query.object_id, "0");
    assert_eq!(
        query.mode,
        BrowseMode::DirectChildren {
            starting_index: 0,
            requested_count: 0
        }
    );

    assert_eq!(query.sort, SortOrder::Catalog);
    assert_eq!(filter, Filter::parse("*").unwrap());

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

        assert_eq!(browse(&args), Err(Fault::InvalidArgs));
    }

    assert_eq!(
        browse(&format!("{BROWSE_ARGS}<Extra/>")),
        Err(Fault::InvalidArgs)
    );

    // Preserve the argument count to check required names before sort values.
    let args = BROWSE_ARGS
        .replace("<Filter>*</Filter>", "<Extra/>")
        .replace("<SortCriteria/>", "<SortCriteria>bad</SortCriteria>");

    assert_eq!(browse(&args), Err(Fault::InvalidArgs));

    let args = BROWSE_ARGS.replace("<Filter>*</Filter>", "<Filter>res,,</Filter>");
    assert_eq!(browse(&args), Err(Fault::InvalidArgs));
    let args = args.replace("<SortCriteria/>", "<SortCriteria>bad</SortCriteria>");
    assert_eq!(browse(&args), Err(Fault::InvalidSortCriteria));

    let args = args
        .replace("<StartingIndex>0", "<StartingIndex>1")
        .replace("BrowseDirectChildren", "BrowseMetadata");

    assert_eq!(browse(&args), Err(Fault::InvalidArgs));

    let args = BROWSE_ARGS
        .replace("<ObjectID>0", "<ObjectID>not-an-id")
        .replace("<StartingIndex>0", "<StartingIndex>4294967295")
        .replace("<RequestedCount>0", "<RequestedCount>4294967295");

    let Action::Browse { query, .. } = browse(&args).unwrap() else {
        panic!("expected Browse");
    };

    assert_eq!(query.object_id, "not-an-id");
    assert_eq!(
        query.mode,
        BrowseMode::DirectChildren {
            starting_index: u32::MAX,
            requested_count: u32::MAX
        }
    );
}

#[test]
fn metadata_browse_validates_wire_pagination_before_discarding_it() {
    let metadata = BROWSE_ARGS.replace("BrowseDirectChildren", "BrowseMetadata");

    for count in ["0", "1", "99", "4294967295"] {
        let args = metadata.replace("<RequestedCount>0", &format!("<RequestedCount>{count}"));

        let Action::Browse { query, .. } = browse(&args).unwrap() else {
            panic!("expected Browse");
        };

        assert_eq!(query.mode, BrowseMode::Metadata);
    }

    for count in ["", "-1", "+1", " 1", "1 ", "4294967296", "1.0"] {
        let args = metadata.replace("<RequestedCount>0", &format!("<RequestedCount>{count}"));
        assert_eq!(browse(&args), Err(Fault::InvalidArgs), "{args}");
    }

    for start in ["1", "4294967295"] {
        let args = metadata.replace("<StartingIndex>0", &format!("<StartingIndex>{start}"));
        assert_eq!(browse(&args), Err(Fault::InvalidArgs), "{args}");
    }
}

#[test]
fn soap_validates_action_inputs_and_sort_syntax() {
    assert_eq!(
        parse(&request("Search", ""), "Search"),
        Err(Fault::InvalidAction)
    );

    assert_eq!(
        parse(
            &request("GetSortCapabilities", "<Extra/>"),
            "GetSortCapabilities"
        ),
        Err(Fault::InvalidArgs)
    );

    for value in ["", "-1", "+1", " 1", "1 ", "4294967296", "1.0"] {
        for name in ["StartingIndex", "RequestedCount"] {
            let args = BROWSE_ARGS.replace(
                &format!("<{name}>0</{name}>"),
                &format!("<{name}>{value}</{name}>"),
            );

            assert_eq!(browse(&args), Err(Fault::InvalidArgs), "{args}");
        }
    }

    for (sort, expected) in [
        ("", SortOrder::Catalog),
        ("+dc:date", SortOrder::DateAscending),
        ("-dc:date", SortOrder::DateDescending),
    ] {
        let args = BROWSE_ARGS.replace(
            "<SortCriteria/>",
            &format!("<SortCriteria>{sort}</SortCriteria>"),
        );

        let Action::Browse { query, .. } = browse(&args).unwrap() else {
            panic!("expected Browse");
        };

        assert_eq!(query.sort, expected);
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

            assert_eq!(browse(&args), Err(Fault::InvalidSortCriteria));
        }
    }

    assert_eq!(
        browse(&BROWSE_ARGS.replace("BrowseDirectChildren", "Unknown")),
        Err(Fault::InvalidArgs)
    );

    let args = BROWSE_ARGS
        .replace("BrowseDirectChildren", "BrowseMetadata")
        .replace("<StartingIndex>0", "<StartingIndex>1");

    assert_eq!(browse(&args), Err(Fault::InvalidArgs));

    for (value, expected) in [
        ("0", Ok(Action::GetCurrentConnectionInfo(0))),
        ("+1", Ok(Action::GetCurrentConnectionInfo(1))),
        (
            "-2147483648",
            Ok(Action::GetCurrentConnectionInfo(i32::MIN)),
        ),
        ("2147483647", Ok(Action::GetCurrentConnectionInfo(i32::MAX))),
        ("2147483648", Err(Fault::InvalidArgs)),
        ("-2147483649", Err(Fault::InvalidArgs)),
        ("", Err(Fault::InvalidArgs)),
        (" 0", Err(Fault::InvalidArgs)),
        ("bad", Err(Fault::InvalidArgs)),
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
            Err(Fault::InvalidAction)
        );
    }
}

#[test]
fn filters_select_only_requested_available_properties() {
    assert_eq!(Filter::parse(" \t ").unwrap(), Filter::default());
    let all = Filter::parse("*").unwrap();
    assert!(all.date && all.art && all.res() && all.child_count);
    assert_eq!(all.resources, ResourceSelection::WithDuration);

    for selector in ["res", "res@protocolInfo", "res@size", "res@resolution"] {
        let filter = Filter::parse(selector).unwrap();
        assert!(filter.res());
        assert_eq!(filter.resources, ResourceSelection::Basic);

        for selectors in [
            format!("{selector},res@duration"),
            format!("res@duration,{selector}"),
            format!("res@duration,{selector},res@duration,{selector}"),
        ] {
            assert_eq!(
                Filter::parse(&selectors).unwrap(),
                Filter::parse("res@duration").unwrap(),
                "{selectors}"
            );
        }

        for selectors in [format!("*,{selector}"), format!("{selector},*")] {
            assert_eq!(Filter::parse(&selectors).unwrap(), all, "{selectors}");
        }
    }

    let filter = Filter::parse(" dc:date, upnp:albumArtURI , res@duration , @childCount ").unwrap();

    assert_eq!(filter, all);

    assert_eq!(
        Filter::parse(
            "dc:title,upnp:class,@id,@parentID,@restricted,dc:unknown,res@unknown,unknown:property"
        )
        .unwrap(),
        Filter::default()
    );

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
        assert_eq!(Filter::parse(invalid), Err(Fault::InvalidArgs), "{invalid}");
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
        full.contains("<upnp:albumArtURI>http://192.0.2.1/preview?a=1&amp;b=2</upnp:albumArtURI>")
    );

    assert!(
        full.contains("protocolInfo=\"http-get:*:video/quicktime:*\" duration=\"123:04:05.006\"")
    );

    assert!(
        full.contains(
            "<res protocolInfo=\"http-get:*:video/mp4:*\">http://192.0.2.1/playback</res>"
        )
    );

    assert!(full.find("/original").unwrap() < full.find("/playback").unwrap());

    for absent in ["size=", "resolution=", "DLNA.", "childCount="] {
        assert!(!full.contains(absent));
    }

    let bare_res = didl(&objects, &Filter::parse("res").unwrap()).unwrap();
    assert!(bare_res.contains("<res protocolInfo="));
    assert!(!bare_res.contains("duration="));

    let mut root = object();

    root.kind = ObjectKind::Root {
        child_count: Some(12),
    };

    let xml = didl(&[root.clone()], &Filter::parse("@childCount").unwrap()).unwrap();
    assert!(xml.contains("<container "));
    assert!(xml.contains("childCount=\"12\"><dc:title>"));

    root.kind = ObjectKind::Album {
        id: Uuid::from_u128(10),
        child_count: None,
    };

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
    object.title = "A&B <title> Łódź 東京 𐐀\u{1}".into();

    let ObjectKind::Video { resources, .. } = &mut object.kind else {
        panic!("expected video");
    };

    resources[0].mime = "video/mp4\" bad=\"value".into();
    let xml = didl(&[object], &Filter::parse("*").unwrap()).unwrap();
    assert_xml(&xml);
    assert!(xml.contains("video/mp4&quot; bad=&quot;value"));

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
        Err(Fault::ActionFailed)
    );

    assert_eq!(
        didl_bounded(&objects, &filter, xml.len()),
        Err(Fault::ActionFailed)
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
        Err(Fault::ActionFailed)
    );

    let mut writer = Xml::new(8, false);
    assert_eq!(writer.text("&&"), Err(Fault::ActionFailed));
    assert_eq!(writer.value, "&amp;");
    assert!(writer.value.len() <= writer.limit);

    let mut writer = Xml::new(8, true);
    assert_eq!(writer.text("&"), Err(Fault::ActionFailed));
    assert!(writer.value.is_empty());

    let mut writer = Xml::new(3, false);
    assert_eq!(writer.text("𐐀"), Err(Fault::ActionFailed));
    assert!(writer.value.is_empty());
}

#[test]
fn device_description_has_identity_and_service_routes() {
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
            assert!(device.contains(&format!("<{element}>/upnp/{service}/{route}</{element}>")));
        }
    }
}

#[test]
fn faults_have_upnp_namespace_codes_and_descriptions() {
    for (fault, code, description) in [
        (Fault::InvalidAction, 401, "Invalid Action"),
        (Fault::InvalidArgs, 402, "Invalid Args"),
        (Fault::ActionFailed, 501, "Action Failed"),
        (Fault::NoSuchObject, 701, "No Such Object"),
        (
            Fault::InvalidConnectionReference,
            706,
            "Invalid Connection Reference",
        ),
        (
            Fault::InvalidSortCriteria,
            709,
            "Unsupported or Invalid Sort Criteria",
        ),
        (Fault::NoSuchContainer, 710, "No Such Container"),
    ] {
        let xml = fault_xml(fault);
        assert_xml(&xml);
        assert!(xml.contains("<faultcode>s:Client</faultcode>"));
        assert!(xml.contains("<UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\">"));

        assert!(xml.contains(&format!(
            "<errorCode>{code}</errorCode><errorDescription>{description}</errorDescription>"
        )));
    }
}

#[test]
fn scpds_advertise_only_implemented_actions_and_event_variables() {
    for (service, expected, action_count) in [
        (Service::ContentDirectory, &["SystemUpdateID"][..], 4),
        (
            Service::ConnectionManager,
            &[
                "SourceProtocolInfo",
                "SinkProtocolInfo",
                "CurrentConnectionIDs",
            ],
            3,
        ),
    ] {
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
                Event::Start(element) => {
                    stack.push(String::from_utf8(element.local_name().as_ref().to_vec()).unwrap())
                }

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
        assert_eq!(actions.len(), action_count);

        for action in actions {
            assert!(service.inputs(&action).is_ok());
        }
    }
}
