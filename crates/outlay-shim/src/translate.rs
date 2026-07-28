//! Pure NIP-01 frame parsing + synthesis — the testable seam (mirrors outlay's
//! `map_notification`). outlay's `subscribe` chunks are already complete NIP-01
//! frames and are forwarded verbatim, so translation here is only:
//! inbound client frame → dispatch decision, and publish/error results → OK/NOTICE.

use serde_json::Value;

/// A parsed inbound NIP-01 frame from a vanilla client.
#[derive(Debug, Clone)]
pub enum ClientMsg {
    /// `["REQ", sub, f1, f2, …]`
    Req { sub_id: String, filters: Vec<Value> },
    /// `["CLOSE", sub]`
    Close { sub_id: String },
    /// `["EVENT", <event>]` — a client→relay publish (the 2-element form).
    Publish(Value),
}

/// Parse one inbound text frame. `None` if it isn't a recognized NIP-01 client
/// message (caller emits a NOTICE).
pub fn parse_client_frame(text: &str) -> Option<ClientMsg> {
    let arr: Vec<Value> = serde_json::from_str(text).ok()?;
    let kind = arr.first()?.as_str()?;
    match kind {
        "REQ" => {
            let sub_id = arr.get(1)?.as_str()?.to_owned();
            let filters = arr.iter().skip(2).cloned().collect();
            Some(ClientMsg::Req { sub_id, filters })
        }
        "CLOSE" => Some(ClientMsg::Close {
            sub_id: arr.get(1)?.as_str()?.to_owned(),
        }),
        "EVENT" => Some(ClientMsg::Publish(arr.get(1)?.clone())),
        _ => None,
    }
}

/// `["OK", event_id, accepted, message]`.
pub fn ok_frame(event_id: &str, accepted: bool, message: &str) -> String {
    serde_json::json!(["OK", event_id, accepted, message]).to_string()
}

/// `["NOTICE", message]`.
pub fn notice_frame(message: &str) -> String {
    serde_json::json!(["NOTICE", message]).to_string()
}

/// `["CLOSED", sub_id, message]` (NIP-01 NOTICE-of-close on a subscription).
pub fn closed_frame(sub_id: &str, message: &str) -> String {
    serde_json::json!(["CLOSED", sub_id, message]).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_req_with_filters() {
        let m = parse_client_frame(r#"["REQ","sub1",{"kinds":[1]},{"authors":["abc"]}]"#).unwrap();
        match m {
            ClientMsg::Req { sub_id, filters } => {
                assert_eq!(sub_id, "sub1");
                assert_eq!(filters.len(), 2);
            }
            _ => panic!("expected Req"),
        }
    }

    #[test]
    fn parses_close() {
        match parse_client_frame(r#"["CLOSE","sub1"]"#).unwrap() {
            ClientMsg::Close { sub_id } => assert_eq!(sub_id, "sub1"),
            _ => panic!("expected Close"),
        }
    }

    #[test]
    fn parses_publish_event() {
        match parse_client_frame(r#"["EVENT",{"id":"x","kind":1,"pubkey":"p"}]"#).unwrap() {
            ClientMsg::Publish(e) => assert_eq!(e["id"], "x"),
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn req_without_sub_id_is_none() {
        assert!(parse_client_frame(r#"["REQ"]"#).is_none());
    }

    #[test]
    fn unknown_and_malformed_are_none() {
        assert!(parse_client_frame(r#"["NOTICE","hi"]"#).is_none());
        assert!(parse_client_frame("not json").is_none());
        assert!(parse_client_frame(r#"[]"#).is_none());
    }

    #[test]
    fn frame_synthesis() {
        assert_eq!(ok_frame("abc", true, "ok"), r#"["OK","abc",true,"ok"]"#);
        assert_eq!(notice_frame("bad"), r#"["NOTICE","bad"]"#);
        assert_eq!(closed_frame("s", "done"), r#"["CLOSED","s","done"]"#);
    }
}
