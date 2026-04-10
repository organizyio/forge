//! [`FrameCodec`] encode/decode roundtrips over an in-memory duplex stream.

use bytes::{BufMut, BytesMut};
use forge_worker_sdk::framing::{Encoding, Frame, FrameCodec};
use forge_worker_sdk::protocol::{WireRequest, WireResponse};
use forge_worker_sdk::MAX_FRAME_PAYLOAD;
use futures::{SinkExt, StreamExt};
use tokio::io::duplex;
use tokio_util::codec::{Decoder, Framed};

async fn roundtrip_request(encoding: Encoding) {
    let (a, b) = duplex(64 * 1024);
    let mut send = Framed::new(a, FrameCodec::new(encoding));
    let mut recv = Framed::new(b, FrameCodec::new(encoding));

    let req = WireRequest {
        id: "rid-1".into(),
        method: "ping".into(),
        params: None,
    };

    send.send(Frame::Request(req.clone())).await.unwrap();
    let frame = recv.next().await.unwrap().unwrap();
    match &frame {
        Frame::Request(got) => {
            assert_eq!(got.id, req.id);
            assert_eq!(got.method, req.method);
        }
        _ => panic!("expected Request frame, got {:?}", frame),
    }
}

async fn roundtrip_response(encoding: Encoding) {
    let (a, b) = duplex(64 * 1024);
    let mut send = Framed::new(a, FrameCodec::new(encoding));
    let mut recv = Framed::new(b, FrameCodec::new(encoding));

    let res = WireResponse {
        id: "resp-1".into(),
        ok: true,
        error: None,
        payload: Some(serde_json::json!({ "pong": true })),
    };

    send.send(Frame::Response(res.clone())).await.unwrap();
    let frame = recv.next().await.unwrap().unwrap();
    match &frame {
        Frame::Response(got) => {
            assert_eq!(got.id, res.id);
            assert_eq!(got.ok, res.ok);
            assert_eq!(got.payload, res.payload);
        }
        _ => panic!("expected Response frame, got {:?}", frame),
    }
}

#[tokio::test]
async fn json_request_roundtrip() {
    roundtrip_request(Encoding::Json).await;
}

#[tokio::test]
async fn json_response_roundtrip() {
    roundtrip_response(Encoding::Json).await;
}

#[tokio::test]
async fn msgpack_request_roundtrip() {
    roundtrip_request(Encoding::Msgpack).await;
}

#[tokio::test]
async fn msgpack_response_roundtrip() {
    roundtrip_response(Encoding::Msgpack).await;
}

#[test]
fn decode_rejects_oversized_length() {
    let mut buf = BytesMut::new();
    buf.put_u32((MAX_FRAME_PAYLOAD as u32).saturating_add(1));
    // KIND_REQUEST
    buf.put_u8(1u8);

    let mut codec = FrameCodec::json();
    let err = codec.decode(&mut buf).unwrap_err();
    assert!(err.to_string().contains("too large"));
}
