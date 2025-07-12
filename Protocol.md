# Qunet protocol

Table of contents:

* [Encoding rules](#encoding-rules)
* * [VarInt / VarUint](#varint--varuint)
* * [String / StringVar / StringU8 / StringU16](#string--stringvar--stringu8--stringu16)
* * [Option](#option)
* [Qunet Database (qdb)](#qunet-database)
* * [Header](#header)
* * [Sections](#sections)
* * * [Dict section (ID 3)](#dict-section-id-3)
* [Data encoding](#data-encoding)
* [Socket Protocol](#socket-protocol)
* * [Qunet Message](#qunet-message)
* [Transport Protocols](#transport-protocols)
* * [TCP](#tcp)
* * [WebSockets](#websockets)
* * [UDP](#udp)
* * [QUIC](#quic)
* [Message Examples](#message-examples)
* * [Stream based](#stream-based)
* * [UDP](#udp-1)

# Encoding rules

Qunet databases, and basic messages are encoded with simple binary serializations. Application data is typically encoded with special qunet datatypes.

Rules that apply for all types of serialization:
* Booleans are encoded as `00` for `false`, `01` for `true`, when encoded as 1 byte. During decoding, applications may treat values other than `00` and `01` as either invalid values or as truthy values.
* Integers are encoded in little-endian, as opposed to traditional big-endian
* Strings are encoded with a `u16` length prefix, unless specified otherwise

## VarInt / VarUint

VarInt is a special type that is encoded using [LEB128](https://en.wikipedia.org/wiki/LEB128). It can be as small as 1 byte for values <= 127, but allows for arbitrarily large values. In qunet however, the maximum value a VarInt can represent is the same as `i64` can (for VarUint, `u64`)

## String / StringVar / StringU8 / StringU16

By default, length of a string is encoded as a `u16` prefix. `String` is simply an alias to `StringU16`, whereas `StringU8` uses a `u8` prefix and `StringVar` uses a `VarUint` prefix.

## Option

Option is a type that is prefixed by a boolean (`u8`), and then by the value itself if the boolean is `true`.

# Qunet Database

A qunet database consists of a header, and one or more sections with data. It is designed to be backwards and forwards compatible within the same major versions,

## Header

Header is encoded as follows:

* Magic (`0xa3 0xdb 0xdb 0x11`)
* Qunet version (`u16`)
* Count of sections (`u16`)
* Each section is then encoded as:
* * Section type (`u16`)
* * Section options (`u16`) - bitmask, see below
* * Section offset (`u32`) - this indicates the section start, it must be counted from the start of the file. It must be divisible by 16.
* * Section size (`u32`)

The header may contain sections with unrecognized types, they should be simply ignored.

## Sections

Each sections contains data specific to the section type. When decoding sections, the application must **not** read past the end of the section. It is also completely allowed to have arbitrary data before, between and after sections and applications should not assume all sections come sequentially. This has 2 primary purposes: padding and the possibility of adding new sections in the future.

Each section **must** start on a 16-byte boundary.

### Section options

Section options are a `u16` bitmask. Currently, it is reserved and there are no options.

## Zstd dict section (ID 3)

This section contains a zstd compression dictionary that will be used for messages compressed with zstd.

Structure:
* Compression level (`i32`)
* Dict data (rest of the section)

# Data encoding

Data encoding is left completely to the application.

# Socket Protocol

It is important to make a distinction here between **qunet transports** and **the qunet message protocol**. An underlying transport protocol may reshape the data in various ways during the transit, for example to ensure reliability or provide encryption. For more information, see [Transport Protocols](#transport-protocols) section, but the next parts define **specifically** the **qunet message protocol**. After a qunet message is encoded, it is passed to the transport layer and what happens next is up to the implementation of the transport protocol.

## Qunet Message

A qunet message consists of a header and optional data.

The header consists of a single `u8` defining the type of the message, and may have additional fields depending on the message type. It is important to note the difference between a header extension and message data, for example if a message is compressed, all fields in the header must be stored before the data, uncompressed.

Message types:

* 0 - Reserved
* 1 - [Ping](#ping)
* 2 - [Pong](#pong)
* 3 - [Keepalive](#keepalive)
* 4 - [KeepaliveResponse](#keepaliveresponse)
* 5 - [HandshakeStart](#handshakestart)
* 6 - [HandshakeFinishPartial](#handshakefinishpartial)
* 7 - [HandshakeFailure](#handshakefailure)
* 8 - [ClientClose](#clientclose)
* 9 - [ServerClose](#serverclose)
* 10 - [ClientReconnect](#clientreconnect)
* 11 - [ConnectionError](#connectionerror)
* 12 - [QdbChunkRequest](#qdbchunkrequest)
* 13 - [QdbChunkResponse](#qdbchunkresponse)
* 14 - [AckMessages](#ackmessages)
* 64 - QdbgToggle
* 65 - QdbgReport
* 128-255 (`1xxxxxxx` in binary) - Data

## Ping

This message is sent by unconnected clients to see if the server is up, and fetch certain data such as ping, supported protocols, and potentially other application level data. Connected clients should use the [Keepalive](#keepalive) message instead.

Message structure:
* Ping ID (`u32`)
* Flags (`u8`) - a bitmask, see below

The flags bitmask is defined as:
* Bit 0 (least significant) is "don't want protocols", set it to `1` to make the server omit supported protocols in the response.
* All other bits should be set to 0

## Pong

This message is sent by the server in response to the [Ping](#ping) message.

Message structure:
* Ping ID (`u32`)
* Protocols length (`u8`) - this is a list of supported protocols on the server, can be 0
* For each protocol:
* * Protocol ID (`u8`) - these are described in the start of the [Socket Protocol](#socket-protocol) section.
* * Port (`u16`) - the port that should be used when connecting with this protocol
* Application-specific data length (`u16`)
* Application-specific data

## Keepalive

This message is sent by connected clients to check if the server is alive, test the current latency, keep any NAT or firewall rules alive, and potentially receive other application level data.

Message structure:
* Timestamp (`u64`) - a timestamp in milliseconds that will be echoed back by the server
* Flags (`u8`) - a bitmask, currently unused and should be 0

## KeepaliveResponse

This message is sent by the server in response to the [Keepalive](#keepalive) message.

Message structure:
* Timestamp (`u64`) - this should be exactly the same value that the client sent in a Ping message
* Application-specific data length (`u16`)
* Application-specific data

## HandshakeStart

This message is sent by the client when it wants to establish a new connection.

Message structure:
* Qunet major version (`u16`)
* Qdb chunk size limit (`u16`) - preferrably leave as 0
* Qdb hash (`[u8; 16]`) - truncated blake3 hash of the cached qunet database, all zeros if no database is cached

## HandshakeFinishPartial

This message is sent by the server in response to a [HandshakeStart](#handshakestart) or [ClientReconnect](#clientreconnect) message to complete the handshake.

If the qunet database needs to be sent and it does not fit in one message, it will be split up and sent as multiple messages. The client may request missing chunks via the [RequestQdbChunk](#requestqdbchunk) message.

Message structure:
* Connection ID (`u64`)
* Qdb present (`bool`)
* If qdb is present:
* * Qdb uncompressed size (`u32`) - uncompressed size of the qdb
* * Qdb full size (`u32`) - full size of the qdb
* * Qdb offset (`u32`) - offset of the qdb chunk
* * Qdb size (`u32`) - size of the qdb chunk
* * Qdb data (byte array) - chunk of the zstd compressed qunet database data

## HandshakeFailure

This message is sent by the server in response to a [HandshakeStart](#handshakestart) or [ClientReconnect](#clientreconnect) message to indicate that an error occurred and the handshake cannot proceed, the client should immediately close the socket after receiving this message.

Message structure:
* Error code (`u32`) - an error code indicating the issue, see table below
* Error message (`String`) - **only present if error code == 0**, a custom error message saying what the issue is

Error codes:

* 1 - Client qunet version is too old
* 2 - Client qunet version is too new
* 3 - Reconnect failed, unknown connection ID
* 4 - Duplicate connection detected from the same address

## ClientClose

This message is sent by the client when it wants to gracefully close the connection.

Message structure:
* Flags (`u8`) - a bitmask

The flags bitmask is defined as:
* Bit 0 (least significant) - "don't terminate" flag, if set to 0 then the session is terminated completely. If set to 1, the socket is still closed but the client session is not completely terminated, and the client is able to reconnect (whether using the same protocol or any other protocol) in order to recover the session without losing any active state. This is the same mechanism that is used automatically during non-graceful disconnects.

## ServerClose

This message is sent by the server when it wants to close a client connection, either gracefully (because the server might be shutting down), or due to a critical error.

Message structure:
* Error code (`u32`) - an error code indicating the issue
* Error message (`String`) - **only present if error code == 0**, a custom error message saying what the issue is

Error codes here are identical to the ones in [ConnectionError](#connectionerror).

## ClientReconnect

This message is sent by the client instead of [HandshakeStart](#handshakestart) when it wants to recover an existing session, for example after a broken TCP connection. Some protocols will automatically recover when any message arrives and sending this message is not necessary for those.

Message structure:
* Connection ID (`u64`)

## ConnectionError

This message can be sent by either the client or the server when a connection error occurs. For example, if the client or the server sends a fragmented message when the other end is configured to reject those, this error should be returned. Connection errors are typically not critical, if a critical error occurs, a `ServerClose` message should be sent instead.

Message structure:
* Error code (`u32`) - an error code indicating the issue, see table below

Error codes:

* 1 - Fragmentation not allowed
* 2 - Requested QDB chunk is too long
* 3 - Requested QDB chunk is invalid (offset/length are out of bounds)
* 4 - Client requested a QDB chunk but a QDB isn't available
* 5 - Protocol violation: client sent a malformed zero-length stream message
* 6 - Protocol violation: client sent a stream message that exceeds the maximum allowed length
* 7 - Internal server error - this is not client's fault

## QdbChunkRequest

Message structure:
* Starting offset (`u32`)
* Size (`u32`)

## QdbChunkResponse

Message structure:
* Qdb offset (`u32`) - offset of the qdb chunk
* Qdb size (`u32`) - size of the qdb chunk
* Qdb data (byte array) - chunk of the zstd compressed qunet database data

## AckMessages

Message structure:
* ACK count (`u16`)
* For each ACK:
* * Reliable message ID (`u16`)

## Data

This is a special message type for application data. It covers values from 128 to 255, aka all values with the most significant bit being `1`. The other 7 bits are used for flags:

* Bits 0 and 1 (least significant) - compression algorithm, `00` - uncompressed, `01` - zstd, `10` - lz4, `11` - reserved, must not be used
* All the other bits should be set to 0 by the qunet protocol, however they **may be modified by the transport layer**. For example the UDP transport reuses some bits for fragmentation or reliability information.

If compression is enabled, the **Compression** extension is included right after the qunet header byte. Structure:
* Uncompressed size (`u32`) - size of the uncompressed payload, does not include any headers.

After the header, application data follows. If compression is enabled, the payload is first compressed and only then passed to the transport protocol. No headers are compressed.

# Transport Protocols

Qunet currently defines three transport protocols (protocol ID is in parantheses of each of them):

* Raw TCP (1)
* Raw UDP (2)
* QUIC (3)
* WebSockets (4)

The recommended default port for for Qunet applications is 4340. Obviously all of the transports cannot be bound to the same port at the same time, but using port 4340 for UDP (and keeping pings enabled) will be helpful for clients trying to connect.

The next sections describe protocol-specific quirks.

## TCP

Each message (both client -> server and server -> client) should be prefixed with a `u32` which holds the full length of the message. This header, as well as the qunet message header, are included only once for a single message, letting the underlying network stack fragment the data.

The only exception is `HandshakeStart`, this message does NOT include a length prefix.

## WebSockets

As WebSockets are a simple framing layer over TCP, this protocol is almost identical to TCP. All messages must be encoded as `Binary` WebSocket messages, pings should not be used and instead qunet [Keepalive](#keepalive) message should be used.

Unlike TCP, we do **not** add an message length prefix, since the WebSocket protocol already does it.

## UDP

As UDP is used not only for persistent connections, but also for pings from unconnected sockets, this part is split into two parts.

### UDP (unconnected)

Messages using the unconnected UDP transport are sent without any modification, it is the standard qunet header + data.

### UDP (conencted)

The UDP transport layer modifies the qunet header to add the following flags for Data messages:

* Bit 5 (0 being least significant, 7 being most significant) - whether a **Fragmentation Information** extension follows this header
* Bit 4 - whether a **Reliability Information** extension follows this header

Additionally, when fragmenting, this transport *may* set the compression/reliability bits to zeroes and omit the **Compression** or **Reliability** header extensions for all but one fragment, as this data is redundant.

If the **Reliability** bit is set, the following header is encoded after the qunet header:

* Reliable message ID (`u16`) - this should be unique per each reliable message. Use zero if this is not a reliable message and just used for ACKing.
* ACK count (`u16`) - how many messages to acknowledge. This typically can be up to 8 in one message.
* For each ACKed message:
* * Message ID (`u16`) - message to acknowledge

If the **Fragmentation** bit is set, the following header is encoded after the qunet header (or after the reliability header, if one is present):

* Fragmented message ID (`u16`) - this should be unique per qunet message, exists to differentiate fragments of unrelated messages
* Fragment index (`u16`) - only the lower 15 bits of this should be used (so maximum 32768 fragments), the top bit indicates if this is the last fragment

Additionally, right after the qunet header and before UDP-specific extensions, the **Connection ID** (`u64`) must be included (**only for client -> server packets**). This applies to every message type except [HandshakeStart](#handshakestart), connection ID is completely omitted during the handshake.

#### Framing

The implementation of this transport protocol should manually handle fragmentation of large packets, as well as reliability.

If a message is longer than a specific preset limit (this can be the MTU of the link layer, or for safety a slightly lower number), the message should be split up into fragments. Each fragment includes the qunet header, with the **Fragmentation** bit set to 1, and followed by the **Fragmentation Information** header, which should contain a message ID (must be the same for all fragments), index of the fragment (starts from 0, increments for each fragment, top bit must be set for the last fragment), and the fragment offset, which indicates where to put this fragment.

Once all the fragments have arrived, the message can be reassambled and decoded. If all the fragments don't arrive after a specified period of time, the message can be discarded. Clients and servers can also be configured to completely reject fragmented messages and return a [ConnectionError](#connectionerror) when receiving one.

## QUIC

One bidirectional stream is used for the connection, and it must be opened by the client as soon as the QUIC handshake is completed. If any unidirectional streams are opened, or any additional bidirectional streams are opened, the other end may terminate the connection or simply ignore them. ALPN must include `qunet1` as the protocol, or connections may be silently dropped.

As for the data flow inside the stream, rules identical to ones in the [TCP section](#tcp) apply.

# Message Examples

This sections shows a couple of examples for how messages can be encoded. For simplicity sake, encoding is shown using C-like types, not raw bytes.

## Stream-based

Bytestream-based protocols like TCP and QUIC have identical message structure (obviously ignoring TCP/UDP/QUIC headers, only counting actual application data), so they are covered under one section.

HandshakeStart message structure (c -> s):
```
uint8_t   type = MSG_HANDSHAKE_START (0x05)
uint16_t  qunetMajor = 1
uint16_t  fragLimit = 0
byte[16]  qdbHash = {0, 0, ...}
```

HandshakeFinishPartial message structure (s -> c). Note that we have a guarantee that `chunkSize == compressedSize` and `chunkOffset == 0`
```
uint32_t  msgLength        = ... (total length of all fields except msgLength)
uint8_t   type             = MSG_HANDSHAKE_FINISH (0x06)
uint64_t  connectionId     = 0x1234567812345678
bool      qdbPresent       = true (0x1)
uint32_t  uncompressedSize = 12345
uint32_t  compressedSize   = 1234
uint32_t  chunkOffset      = 0
uint32_t  chunkSize        = 1234
uint8_t[1234] qdbData      = ...
```

## UDP

HandshakeStart message structure (c -> s):
```
uint8_t   type = MSG_HANDSHAKE_START (0x05)
uint16_t  qunetMajor = 1
uint16_t  fragLimit = 0
byte[16]  qdbHash = {0, 0, ...}
```

HandshakeFinishPartial message structure (s -> c):
```
uint8_t   type             = MSG_HANDSHAKE_FINISH (0x06)
uint64_t  connectionId     = 0x1234567812345678
bool      qdbPresent       = true (0x1)
uint32_t  uncompressedSize = 12345
uint32_t  compressedSize   = 1234
uint32_t  chunkOffset      = 234
uint32_t  chunkSize        = 1000
uint8_t[1000] qdbData      = ...
```

Keepalive message structure (c -> s):
```
uint8_t   type         = MSG_KEEPALIVE (0x03)
uint64_t  connectionId = 0x1234567812345678
uint64_t  timestamp    = 1751224984476
uint8_t   flags        = 0x00
```
