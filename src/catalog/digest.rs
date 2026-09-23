use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Digest([u8; 32]);

impl Digest {
    pub(super) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl FromStr for Digest {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        const INVALID: &str = "revision digest must be 64 lowercase hexadecimal characters";

        fn nibble(byte: u8) -> Result<u8, &'static str> {
            match byte {
                b'0'..=b'9' => Ok(byte - b'0'),
                b'a'..=b'f' => Ok(byte - b'a' + 10),
                _ => Err(INVALID),
            }
        }

        if value.len() != 64 {
            return Err(INVALID);
        }

        let mut bytes = [0; 32];

        for (byte, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
            *byte = nibble(pair[0])? * 16 + nibble(pair[1])?;
        }

        Ok(Self(bytes))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }

        Ok(())
    }
}

impl Serialize for Digest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}
