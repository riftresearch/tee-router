pub(crate) struct LimitedResponseBody {
    pub(crate) text: String,
    pub(crate) truncated: bool,
}

const RESPONSE_BODY_ERROR_PREVIEW_CHARS: usize = 4 * 1024;

pub(crate) async fn read_limited_response_text(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<LimitedResponseBody, reqwest::Error> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)?
    {
        if !append_limited_body_chunk(&mut body, chunk.as_ref(), max_bytes) {
            return Ok(LimitedResponseBody {
                text: String::new(),
                truncated: true,
            });
        }
    }

    Ok(LimitedResponseBody {
        text: String::from_utf8_lossy(&body).into_owned(),
        truncated: false,
    })
}

fn append_limited_body_chunk(body: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> bool {
    if body.len().saturating_add(chunk.len()) > max_bytes {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

pub(crate) fn response_body_error_preview(value: &str) -> String {
    let mut end = 0;
    let mut count = 0;
    for (index, ch) in value.char_indices() {
        if count == RESPONSE_BODY_ERROR_PREVIEW_CHARS {
            return format!(
                "{}...<truncated {} chars>",
                &value[..end],
                value[index..].chars().count()
            );
        }
        end = index + ch.len_utf8();
        count += 1;
    }
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_limited_body_chunk_rejects_chunks_past_the_limit_without_mutating() {
        let mut body = b"abcd".to_vec();

        assert!(!append_limited_body_chunk(&mut body, b"ef", 5));
        assert_eq!(body, b"abcd");
    }

    #[test]
    fn append_limited_body_chunk_accepts_chunks_at_the_limit() {
        let mut body = b"abcd".to_vec();

        assert!(append_limited_body_chunk(&mut body, b"ef", 6));
        assert_eq!(body, b"abcdef");
    }

    #[test]
    fn response_body_error_preview_truncates_large_values() {
        let value = "a".repeat(RESPONSE_BODY_ERROR_PREVIEW_CHARS + 9);

        let preview = response_body_error_preview(&value);

        assert_eq!(
            preview.len(),
            RESPONSE_BODY_ERROR_PREVIEW_CHARS + "...<truncated 9 chars>".len()
        );
        assert!(preview.ends_with("...<truncated 9 chars>"));
    }

    #[test]
    fn response_body_error_preview_does_not_split_utf8_codepoints() {
        let value = format!(
            "{}{}",
            "a".repeat(RESPONSE_BODY_ERROR_PREVIEW_CHARS - 1),
            "é".repeat(2)
        );

        let preview = response_body_error_preview(&value);

        assert!(preview.starts_with(&format!(
            "{}é",
            "a".repeat(RESPONSE_BODY_ERROR_PREVIEW_CHARS - 1)
        )));
        assert!(preview.ends_with("...<truncated 1 chars>"));
    }
}
