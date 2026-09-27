#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Digest([u8; 32]);

impl Digest {
    pub(super) fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}
