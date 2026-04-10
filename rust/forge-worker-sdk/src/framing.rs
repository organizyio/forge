//! Length-prefixed framing: `[u32 BE length][u8 kind][payload]`.
//!
//! The `FrameCodec` is a Tokio [`Decoder`] + [`Encoder`] that converts raw byte
//! streams into typed [`Frame`] values.  Both MessagePack (default) and JSON
//! (debug) encodings are supported; the choice is set once at construction and
//! must match the Go supervisor's `transport.Conn` encoding.
//!
//! Frame kind constants align with `go/transport`:
//! - `KIND_REQUEST  = 1` — Go → Rust
//! - `KIND_RESPONSE = 2` — Rust → Go
//! - `KIND_EVENT    = 3` — Rust → Go (async, unsolicited)

use std::str::FromStr;

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{WireEvent, WireRequest, WireResponse};

// ─── CONSTANTS ────────────────────────────────────────────────────────────────

pub const KIND_REQUEST:  u8 = 1;
pub const KIND_RESPONSE: u8 = 2;
pub const KIND_EVENT:    u8 = 3;

/// Hard cap matching `transport.Conn` read loop (64 MiB).
pub const MAX_FRAME_PAYLOAD: usize = 64 * 1024 * 1024;

// ─── ENCODING ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Msgpack,
    Json,
}

impl Encoding {
    /// Wire string reported in `capabilities` and aligned with the Go supervisor `--encoding` flag.
    pub fn wire_name(self) -> &'static str {
        match self {
            Encoding::Msgpack => "msgpack",
            Encoding::Json => "json",
        }
    }
}

impl FromStr for Encoding {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(if s.eq_ignore_ascii_case("json") {
            Encoding::Json
        } else {
            Encoding::Msgpack
        })
    }
}

// ─── FRAME ───────────────────────────────────────────────────────────────────

/// A decoded wire frame.
#[derive(Debug)]
pub enum Frame {
    Request(WireRequest),
    Response(WireResponse),
    Event(WireEvent),
}

// ─── CODEC ───────────────────────────────────────────────────────────────────

/// Stateless codec; encoding is fixed at construction.
pub struct FrameCodec {
    encoding: Encoding,
}

impl FrameCodec {
    pub fn new(encoding: Encoding) -> Self { Self { encoding } }
    pub fn msgpack() -> Self { Self::new(Encoding::Msgpack) }
    pub fn json()    -> Self { Self::new(Encoding::Json) }
}

// ─── DECODER ─────────────────────────────────────────────────────────────────

impl Decoder for FrameCodec {
    type Item  = Frame;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least the 5-byte header
        if src.len() < 5 {
            return Ok(None);
        }

        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        let kind   = src[4];

        if length > MAX_FRAME_PAYLOAD {
            return Err(anyhow::anyhow!("frame payload too large: {} bytes (max {})", length, MAX_FRAME_PAYLOAD));
        }

        // Wait for the full payload
        if src.len() < 5 + length {
            src.reserve(5 + length - src.len());
            return Ok(None);
        }

        // Consume header
        src.advance(5);
        let payload = src.split_to(length);

        let frame = match kind {
            KIND_REQUEST  => Frame::Request(self.unmarshal(&payload)?),
            KIND_RESPONSE => Frame::Response(self.unmarshal(&payload)?),
            KIND_EVENT    => Frame::Event(self.unmarshal(&payload)?),
            k => return Err(anyhow::anyhow!("unknown frame kind: {}", k)),
        };

        Ok(Some(frame))
    }
}

// ─── ENCODER ─────────────────────────────────────────────────────────────────

impl Encoder<Frame> for FrameCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let (kind, payload) = match &frame {
            Frame::Request(r)  => (KIND_REQUEST,  self.marshal(r)?),
            Frame::Response(r) => (KIND_RESPONSE, self.marshal(r)?),
            Frame::Event(e)    => (KIND_EVENT,    self.marshal(e)?),
        };

        dst.reserve(5 + payload.len());
        dst.put_u32(payload.len() as u32); // 4-byte BE length
        dst.put_u8(kind);                  // 1-byte kind
        dst.put_slice(&payload);
        Ok(())
    }
}

// ─── HELPERS ─────────────────────────────────────────────────────────────────

impl FrameCodec {
    fn marshal<T: serde::Serialize>(&self, v: &T) -> anyhow::Result<Vec<u8>> {
        match self.encoding {
            Encoding::Msgpack => rmp_serde::to_vec_named(v).map_err(Into::into),
            Encoding::Json    => serde_json::to_vec(v).map_err(Into::into),
        }
    }

    fn unmarshal<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> anyhow::Result<T> {
        match self.encoding {
            Encoding::Msgpack => rmp_serde::from_slice(data).map_err(Into::into),
            Encoding::Json    => serde_json::from_slice(data).map_err(Into::into),
        }
    }
}
