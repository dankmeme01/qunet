// Both the enums here are strictly defined in Protocol.md.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    ServerShutdown = 8,
    RateLimitExceeded = 9,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum QunetHandshakeError {
    Custom = 0,
    VersionTooOld = 1,
    VersionTooNew = 2,
    UnknownConnectionId = 3,
    DuplicateConnection = 4,
}

impl QunetConnectionError {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::Custom),
            1 => Some(Self::FragmentationDisallowed),
            2 => Some(Self::QdbChunkTooLong),
            3 => Some(Self::QdbInvalidChunk),
            4 => Some(Self::QdbUnavailable),
            5 => Some(Self::ZeroLengthStreamMessage),
            6 => Some(Self::StreamMessageTooLong),
            7 => Some(Self::InternalServerError),
            8 => Some(Self::ServerShutdown),
            9 => Some(Self::RateLimitExceeded),
            _ => None,
        }
    }

    pub fn to_u32(self) -> u32 {
        self as u32
    }
}

impl TryFrom<u32> for QunetConnectionError {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        QunetConnectionError::from_u32(value).ok_or(())
    }
}

impl QunetHandshakeError {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(Self::Custom),
            1 => Some(Self::VersionTooOld),
            2 => Some(Self::VersionTooNew),
            3 => Some(Self::UnknownConnectionId),
            4 => Some(Self::DuplicateConnection),
            _ => None,
        }
    }

    pub fn to_u32(self) -> u32 {
        self as u32
    }
}

impl TryFrom<u32> for QunetHandshakeError {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        QunetHandshakeError::from_u32(value).ok_or(())
    }
}
