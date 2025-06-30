// Both the enums here are strictly defined in Protocol.md.
use num_derive::{FromPrimitive, ToPrimitive};

#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive, ToPrimitive)]
#[repr(u32)]
pub enum QunetConnectionError {
    Custom = 0,
    FragmentationDisallowed = 1,
    QdbChunkTooLong = 2,
    QdbInvalidChunk = 3,
    QdbUnavailable = 4,
    ZeroLengthStreamMessage = 5,
    StreamMessageTooLong = 6,
    InternalServerError = 7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive, ToPrimitive)]
#[repr(u32)]
pub enum QunetHandshakeError {
    Custom = 0,
    VersionTooOld = 1,
    VersionTooNew = 2,
    UnknownConnectionId = 3,
}
