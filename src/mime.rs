/// A validated, lowercase concrete MIME type without parameters.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(transparent)]
pub struct MimeType(String);

impl MimeType {
    /// Validate the complete input, then retain only its normalized type/subtype.
    pub fn parse(value: &str) -> Option<Self> {
        parse(value).map(|mime| Self(mime.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validate a concrete MIME type and its parameters, preserving type/subtype case.
/// Accept spaces and tabs around parameter `=`.
pub(crate) fn parse(value: &str) -> Option<&str> {
    fn token(byte: u8) -> bool {
        byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
    }

    let (mime, mut rest) = value.split_once(';').unwrap_or((value, ""));
    let mime = mime.trim_matches([' ', '\t']);
    let (kind, subtype) = mime.split_once('/')?;

    if kind.is_empty()
        || subtype.is_empty()
        || kind == "*"
        || subtype == "*"
        || !kind.bytes().chain(subtype.bytes()).all(token)
    {
        return None;
    }

    while !rest.is_empty() {
        rest = rest.trim_start_matches([' ', '\t']);

        if rest.is_empty() {
            break;
        }

        if let Some(next) = rest.strip_prefix(';') {
            rest = next;
            continue;
        }

        let end = rest.bytes().take_while(|&byte| token(byte)).count();

        if end == 0 {
            return None;
        }

        rest = rest[end..].trim_start_matches([' ', '\t']);
        rest = rest.strip_prefix('=')?;
        rest = rest.trim_start_matches([' ', '\t']);

        if let Some(quoted) = rest.strip_prefix('"') {
            let mut bytes = quoted.bytes();
            let mut consumed = 0;

            loop {
                let byte = bytes.next()?;
                consumed += 1;

                match byte {
                    b'"' => break,

                    b'\\' => {
                        let escaped = bytes.next()?;
                        consumed += 1;

                        if escaped != b'\t' && !(32..=126).contains(&escaped) {
                            return None;
                        }
                    }

                    b'\t' | 32..=126 => {}
                    _ => return None,
                }
            }

            rest = &quoted[consumed..];
        } else {
            let end = rest.bytes().take_while(|&byte| token(byte)).count();

            if end == 0 {
                return None;
            }

            rest = &rest[end..];
        }

        rest = rest.trim_start_matches([' ', '\t']);

        if !rest.is_empty() {
            rest = rest.strip_prefix(';')?;
        }
    }

    Some(mime)
}

#[cfg(test)]
mod tests {
    use super::{MimeType, parse};

    #[test]
    fn catalog_mime_normalizes_and_serializes_as_a_string() {
        let mime = MimeType::parse(" VIDEO/X-Custom+MP4 ; codecs=\"avc1\"").unwrap();
        assert_eq!(mime.as_str(), "video/x-custom+mp4");
        assert_eq!(mime, MimeType::parse("video/x-custom+mp4").unwrap());

        assert_eq!(
            serde_json::to_string(&mime).unwrap(),
            "\"video/x-custom+mp4\""
        );
    }

    #[test]
    fn mime_grammar_preserves_case_and_consumes_all_parameters() {
        for (value, expected) in [
            ("image/jpeg", "image/jpeg"),
            (" IMAGE/JPEG \t", "IMAGE/JPEG"),
            (" IMAGE/JPEG ; q=90", "IMAGE/JPEG"),
            ("image/jpeg;", "image/jpeg"),
            ("image/jpeg; \t", "image/jpeg"),
            ("image/jpeg; x=1;", "image/jpeg"),
            ("image/jpeg; x=1; \t", "image/jpeg"),
            ("image/jpeg;;x=1", "image/jpeg"),
            ("image/jpeg;;q=90;", "image/jpeg"),
            ("image/jpeg; ;\t; q=90; ;\t", "image/jpeg"),
            ("text/xml;; charset=utf-8;;", "text/xml"),
            ("video/mp4; codecs=avc1", "video/mp4"),
            ("image/jpeg; note=\"semi;colon\"", "image/jpeg"),
            ("image/jpeg; note=\"a;\\\"b\"; x=y", "image/jpeg"),
            ("video/MP4; codecs=\"avc1\\\"test\"", "video/MP4"),
            ("TEXT/XML; Charset=utf-8", "TEXT/XML"),
            ("text/xml; a=\"with;semicolon\"; b=token", "text/xml"),
            ("text/xml; a=\"escaped\\\"quote\"", "text/xml"),
            ("text/xml; a=\"\"; a=\"\\\\\t\\\t ~\" \t", "text/xml"),
            (
                "application/vnd.a+b; !#$%&'*+-.^_`|~=token",
                "application/vnd.a+b",
            ),
        ] {
            assert_eq!(parse(value), Some(expected), "{value:?}");

            assert_eq!(
                MimeType::parse(value).unwrap().as_str(),
                expected.to_ascii_lowercase(),
                "{value:?}"
            );
        }
    }

    #[test]
    fn mime_grammar_rejects_malformed_types_parameters_and_controls() {
        for value in [
            "",
            "image",
            "image/",
            "/jpeg",
            "*/mp4",
            "video/*",
            "*/*",
            "image/jpeg/extra",
            "image/jpeg, image/png",
            "image /jpeg",
            "image/jpeg; x",
            "image/jpeg; x=",
            "image/jpeg; x=\"unterminated",
            "image/jpeg; x=\"v\"oops",
            "image/jpeg; =x",
            "image/jpeg; x=a b",
            "image/jpeg; x=\"dangling\\",
            "video/mp4\r\nsecret",
            "\nimage/jpeg",
            "image/jpeg\n",
            "image/jpeg; x=\"bad\nvalue\"",
            "image/jpeg; x=\"bad\rvalue\"",
            "image/jpeg; x=\"\0\"",
            "image/jpeg; x=\"\u{7f}\"",
            "image/jpeg; x=\"\\\n\"",
            "image/jpeg; x=\"\\\u{7f}\"",
            "image/jpeg; x=\"\u{80}\"",
            "image/jpeg; x=\"\\\u{80}\"",
            "image/jpeg;\nx=1",
            "image/jpeg; x=1\n",
            "image/jpeg; x\n=1",
            "image/jpeg; x=\n1",
            "image/jpeg; x=\u{a0}1",
            "image/jpeg;\r",
            "image/jpeg; \t\n",
            "image/jpeg;;\0",
            "image/jpeg;;\u{b}",
            "image/jpeg; x=1;\u{c}",
            "image/jpeg; x=1;;\u{7f}",
            "image/jpeg; ;\u{a0}",
            "image/jpeg; ;\u{2003}",
        ] {
            assert_eq!(parse(value), None, "{value:?}");
            assert_eq!(MimeType::parse(value), None, "{value:?}");
        }
    }

    #[test]
    fn mime_parameter_whitespace_accepts_spaces_and_tabs() {
        for value in [
            "text/xml; charset =utf-8",
            "text/xml; charset= utf-8",
            "text/xml; charset \t=\t \"utf-8\"; a = b",
            "text/xml;; charset \t=\t \"utf-8\"; ; a = b;",
        ] {
            assert_eq!(parse(value), Some("text/xml"), "{value:?}");
        }
    }
}
