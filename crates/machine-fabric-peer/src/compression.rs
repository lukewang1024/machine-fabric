use crate::connection::Frame;
use anyhow::{Context, Result, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use machine_fabric_protocol::Response;
#[cfg(test)]
use machine_fabric_protocol::RpcError;
use std::io::{BufWriter, Read, Write};

// Each response has its own gzip dictionary. Requests and handshake frames stay
// unchanged; old peers omit the capability and receive ordinary responses.
const MAX_GZIP_RESPONSE: usize = 32 * 1024 * 1024;
const MIN_GZIP_RESPONSE: usize = 64 * 1024;

struct ResponseSize(usize);
impl Write for ResponseSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|n| *n <= MAX_GZIP_RESPONSE)
            .ok_or_else(|| std::io::Error::other("response exceeds compression budget"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn encode_response_frame(id: String, response: Response, negotiated: bool) -> Frame {
    if negotiated && response.ok {
        let mut size = ResponseSize(0);
        if serde_json::to_writer(&mut size, &response).is_ok() && size.0 >= MIN_GZIP_RESPONSE {
            let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
            let mut buffered = BufWriter::with_capacity(64 * 1024, &mut gzip);
            let serialized = serde_json::to_writer(&mut buffered, &response)
                .map_err(std::io::Error::other)
                .and_then(|()| buffered.flush());
            let _ = buffered.into_parts();
            if serialized.is_ok()
                && let Ok(bytes) = gzip.finish()
            {
                // Include base64/envelope overhead; incompressible responses
                // use the existing format without changing the tool result.
                if bytes.len().div_ceil(3) * 4 + 256 < size.0 {
                    return Frame::ResponseGzip {
                        id,
                        expanded_bytes: size.0,
                        payload: STANDARD.encode(bytes),
                    };
                }
            }
        }
    }
    Frame::Response { id, response }
}

pub(crate) fn decode_response_frame(
    frame: Frame,
    negotiated: bool,
    maximum: usize,
) -> Result<Frame> {
    let Frame::ResponseGzip {
        id,
        expanded_bytes,
        payload,
    } = frame
    else {
        return Ok(frame);
    };
    if !negotiated
        || expanded_bytes > maximum
        || !(MIN_GZIP_RESPONSE..=MAX_GZIP_RESPONSE).contains(&expanded_bytes)
        || payload.len() > MAX_GZIP_RESPONSE.div_ceil(3) * 4
    {
        return Err(anyhow!("invalid or unnegotiated compressed peer response"));
    }
    let compressed = STANDARD
        .decode(payload)
        .context("decode compressed peer response base64")?;
    let mut bytes = Vec::new();
    GzDecoder::new(compressed.as_slice())
        .take(expanded_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .context("decode compressed peer response gzip")?;
    if bytes.len() != expanded_bytes {
        return Err(anyhow!("compressed peer response length mismatch"));
    }
    let response: Response =
        serde_json::from_slice(&bytes).context("decode compressed peer response JSON")?;
    if !response.ok {
        return Err(anyhow!("compressed error response is not supported"));
    }
    Ok(Frame::Response { id, response })
}

#[cfg(test)]
mod compression_tests {
    use super::*;
    fn decode_response_frame(frame: Frame, negotiated: bool) -> Result<Frame> {
        super::decode_response_frame(frame, negotiated, MAX_GZIP_RESPONSE)
    }

    fn response() -> Response {
        Response::success(
            "request-identity",
            serde_json::json!({
                "stateId":"fresh-state", "outline": "@e1 中文🙂\n".repeat(12000),
                "actions":[{"ref":"@e1","outcome":"unknown-effect"}], "image":"unchanged"
            }),
        )
    }
    fn assert_same(original: &Response, frame: Frame) {
        let Frame::Response { id, response } = decode_response_frame(frame, true).unwrap() else {
            panic!("expected response");
        };
        assert_eq!(id, "routing-identity");
        assert_eq!(
            serde_json::to_value(original).unwrap(),
            serde_json::to_value(response).unwrap()
        );
    }
    #[test]
    fn unicode_refs_ids_and_unknown_action_outcomes_roundtrip_exactly() {
        let original = response();
        let encoded = encode_response_frame("routing-identity".into(), original.clone(), true);
        assert!(matches!(encoded, Frame::ResponseGzip { .. }));
        assert!(
            serde_json::to_vec(&encoded).unwrap().len()
                < serde_json::to_vec(&original).unwrap().len() / 10
        );
        assert_same(&original, encoded);
    }
    #[test]
    fn unnegotiated_small_and_error_responses_keep_old_format() {
        assert!(matches!(
            encode_response_frame("id".into(), response(), false),
            Frame::Response { .. }
        ));
        let small = Response::success("id", serde_json::json!({"ok":true}));
        assert!(matches!(
            encode_response_frame("id".into(), small, true),
            Frame::Response { .. }
        ));
        let error = Response::failure("id", RpcError::new("INPUT_UNKNOWN", "x".repeat(100000)));
        assert!(matches!(
            encode_response_frame("id".into(), error, true),
            Frame::Response { .. }
        ));
    }
    #[test]
    fn incompressible_responses_keep_original_format_and_result() {
        let mut seed = 0x123456789abcdef_u64;
        let text: String = (0..200000)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                char::from(33 + (seed % 94) as u8)
            })
            .collect();
        let original = Response::success("request-identity", serde_json::json!({"text":text}));
        let frame = encode_response_frame("routing-identity".into(), original.clone(), true);
        assert!(matches!(frame, Frame::Response { .. }));
        assert_same(&original, frame);
    }

    #[test]
    fn compressed_frames_reject_unnegotiated_length_mismatch_and_corruption() {
        let frame = encode_response_frame("id".into(), response(), true);
        assert!(decode_response_frame(frame, false).is_err());
        let Frame::ResponseGzip {
            id,
            expanded_bytes,
            payload,
        } = encode_response_frame("id".into(), response(), true)
        else {
            panic!();
        };
        assert!(
            decode_response_frame(
                Frame::ResponseGzip {
                    id: id.clone(),
                    expanded_bytes: expanded_bytes - 1,
                    payload: payload.clone()
                },
                true
            )
            .is_err()
        );
        let mut gzip = STANDARD.decode(&payload).unwrap();
        gzip.truncate(gzip.len() - 4);
        assert!(
            decode_response_frame(
                Frame::ResponseGzip {
                    id,
                    expanded_bytes,
                    payload: STANDARD.encode(gzip)
                },
                true
            )
            .is_err()
        );
    }
    #[test]
    fn oversized_declarations_and_expansion_bombs_are_bounded() {
        let frame = Frame::ResponseGzip {
            id: "id".into(),
            expanded_bytes: MAX_GZIP_RESPONSE + 1,
            payload: String::new(),
        };
        assert!(decode_response_frame(frame, true).is_err());
        let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
        gzip.write_all(&vec![b'x'; MIN_GZIP_RESPONSE * 8]).unwrap();
        let frame = Frame::ResponseGzip {
            id: "id".into(),
            expanded_bytes: MIN_GZIP_RESPONSE,
            payload: STANDARD.encode(gzip.finish().unwrap()),
        };
        assert!(decode_response_frame(frame, true).is_err());
        let mut size = ResponseSize(MAX_GZIP_RESPONSE - 1);
        assert!(size.write_all(b"xx").is_err());
    }
}
