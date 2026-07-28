//! `/<pubkey>` path parsing: validate a path segment as a server-pubkey
//! reference (hex / npub / nprofile) and detect relay hints.
//!
//! The raw string is returned verbatim — `NostrClientTransportConfig::
//! with_server_pubkey` accepts hex / npub / nprofile and runs relay resolution
//! itself. We only parse enough to (a) reject garbage early with a clean error,
//! and (b) decide whether to leave `relay_urls` empty (nprofile hints win) or
//! fill it from `OUTLAY_SHIM_RELAY_URLS` (design/shim.md §3, §6).

use nostr_sdk::nips::nip19::Nip19;
use nostr_sdk::{FromBech32, PublicKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPath {
    /// The exact string to pass to `with_server_pubkey`.
    pub raw: String,
    /// True only for an nprofile carrying ≥1 relay hint.
    pub has_relay_hints: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum PathError {
    #[error("not a server pubkey (expected 64-char hex, npub, or nprofile)")]
    NotAPubkey,
    #[error("invalid pubkey: {0}")]
    Invalid(String),
}

/// Validate a `/<segment>` path. Accepts hex, npub, and nprofile; rejects note /
/// nevent / naddr / nsec (not a server identity) and all garbage.
pub fn parse_path(raw: &str) -> Result<ParsedPath, PathError> {
    let s = raw.trim();
    if s.is_empty() {
        return Err(PathError::NotAPubkey);
    }
    // 64-char hex pubkey.
    let looks_hex = s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit());
    if looks_hex {
        PublicKey::from_hex(s).map_err(|e| PathError::Invalid(e.to_string()))?;
        return Ok(ParsedPath {
            raw: s.to_owned(),
            has_relay_hints: false,
        });
    }
    // npub / nprofile (or some other bech32 entity we reject).
    match Nip19::from_bech32(s) {
        Ok(Nip19::Pubkey(_)) => Ok(ParsedPath {
            raw: s.to_owned(),
            has_relay_hints: false,
        }),
        Ok(Nip19::Profile(p)) => Ok(ParsedPath {
            raw: s.to_owned(),
            has_relay_hints: !p.relays.is_empty(),
        }),
        Ok(_) => Err(PathError::NotAPubkey),
        Err(e) => Err(PathError::Invalid(e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr_sdk::nips::nip19::Nip19Profile;
    use nostr_sdk::Keys;
    use nostr_sdk::RelayUrl;
    use nostr_sdk::ToBech32;

    #[test]
    fn hex_pubkey_accepted_no_hints() {
        let hex = Keys::generate().public_key().to_hex();
        let p = parse_path(&hex).unwrap();
        assert_eq!(p.raw, hex);
        assert!(!p.has_relay_hints);
    }

    #[test]
    fn npub_accepted_no_hints() {
        let npub = Keys::generate().public_key().to_bech32().expect("npub");
        let p = parse_path(&npub).unwrap();
        assert!(!p.has_relay_hints);
    }

    #[test]
    fn nprofile_with_hint_reports_hints() {
        let pk = Keys::generate().public_key();
        let profile = Nip19Profile::new(
            pk,
            vec![RelayUrl::parse("wss://relay.contextvm.org").unwrap()],
        );
        let s = profile.to_bech32().expect("nprofile");
        let p = parse_path(&s).unwrap();
        assert!(p.has_relay_hints, "nprofile with a relay hint => hints");
    }

    #[test]
    fn nprofile_without_hint_reports_no_hints() {
        let pk = Keys::generate().public_key();
        let profile = Nip19Profile::new(pk, Vec::<RelayUrl>::new());
        let s = profile.to_bech32().expect("nprofile");
        let p = parse_path(&s).unwrap();
        assert!(!p.has_relay_hints, "nprofile with no relays => no hints");
    }

    #[test]
    fn rejects_non_pubkey_entities_and_garbage() {
        // valid bech32 but the wrong entity (a note) => NotAPubkey
        let note_id = nostr_sdk::EventId::from_byte_array([0u8; 32]);
        let note_str = note_id.to_bech32().expect("note");
        assert!(matches!(parse_path(&note_str), Err(PathError::NotAPubkey)));
        // empty / whitespace => NotAPubkey
        assert!(matches!(parse_path(""), Err(PathError::NotAPubkey)));
        assert!(matches!(parse_path("   "), Err(PathError::NotAPubkey)));
        // garbage (not valid bech32) => Invalid
        assert!(matches!(
            parse_path("not-a-pubkey"),
            Err(PathError::Invalid(_))
        ));
    }

    #[test]
    fn trims_whitespace() {
        let hex = Keys::generate().public_key().to_hex();
        let p = parse_path(&format!("  {hex}  ")).unwrap();
        assert_eq!(p.raw, hex);
    }
}
