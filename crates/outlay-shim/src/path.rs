//! `/<pubkey>` path parsing: validate a path segment as a server-pubkey
//! reference (hex / npub / nprofile) and detect relay hints.
//!
//! The transport (`transport::build_client`) passes `hex` to
//! `with_server_pubkey` and supplies relay URLs itself — from the nprofile
//! hints when present, else `OUTLAY_SHIM_CVM_RELAYS`. So here we only parse
//! enough to (a) reject garbage early with a clean error, and (b) surface the
//! nprofile's relay hints for the transport to rewrite/dial (design/shim.md
//! §3, §6).
//!
//! `hex` is the canonical lowercase pubkey — the transport cache keys on it so
//! hex / npub / nprofile (with differing relay hints) of the same identity
//! collapse to one shared transport.

use nostr_sdk::nips::nip19::Nip19;
use nostr_sdk::{FromBech32, PublicKey};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedPath {
    /// Lowercase hex pubkey — canonical identity for the transport cache, and
    /// the value passed to `with_server_pubkey`. The shim owns relay resolution
    /// (`transport::build_client`), so the nprofile's embedded hints are not
    /// handed to the SDK — they live in `relay_hints` below.
    pub hex: String,
    /// Relay hints from an nprofile, normalized (no trailing slash). Empty for
    /// hex / npub. The transport rewrites any that point at the shim's own
    /// public URL to the colocated loopback relay; the rest are dialed as-is.
    pub relay_hints: Vec<String>,
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
        let pk = PublicKey::from_hex(s).map_err(|e| PathError::Invalid(e.to_string()))?;
        return Ok(ParsedPath {
            hex: pk.to_hex(),
            relay_hints: Vec::new(),
        });
    }
    // npub / nprofile (or some other bech32 entity we reject).
    match Nip19::from_bech32(s) {
        Ok(Nip19::Pubkey(pk)) => Ok(ParsedPath {
            hex: pk.to_hex(),
            relay_hints: Vec::new(),
        }),
        Ok(Nip19::Profile(p)) => Ok(ParsedPath {
            hex: p.public_key.to_hex(),
            // Normalize via RelayUrl so hint comparison is trailing-slash- and
            // default-port-tolerant (matches `transport::norm_url`).
            relay_hints: p
                .relays
                .iter()
                .map(|r| r.as_str_without_trailing_slash().to_owned())
                .collect(),
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
        assert_eq!(p.hex, hex);
        assert!(p.relay_hints.is_empty());
    }

    #[test]
    fn npub_accepted_no_hints() {
        let pk = Keys::generate().public_key();
        let npub = pk.to_bech32().expect("npub");
        let p = parse_path(&npub).unwrap();
        assert_eq!(p.hex, pk.to_hex());
        assert!(p.relay_hints.is_empty());
    }

    #[test]
    fn nprofile_with_hint_surfaces_normalized_hints() {
        let pk = Keys::generate().public_key();
        let profile = Nip19Profile::new(
            pk,
            vec![RelayUrl::parse("wss://relay.contextvm.org").unwrap()],
        );
        let s = profile.to_bech32().expect("nprofile");
        let p = parse_path(&s).unwrap();
        assert_eq!(p.hex, pk.to_hex());
        // Normalized: trailing slash stripped, ready to compare against self URLs.
        assert_eq!(p.relay_hints, vec!["wss://relay.contextvm.org"]);
    }

    #[test]
    fn nprofile_without_hint_reports_no_hints() {
        let pk = Keys::generate().public_key();
        let profile = Nip19Profile::new(pk, Vec::<RelayUrl>::new());
        let s = profile.to_bech32().expect("nprofile");
        let p = parse_path(&s).unwrap();
        assert_eq!(p.hex, pk.to_hex());
        assert!(
            p.relay_hints.is_empty(),
            "nprofile with no relays => no hints"
        );
    }

    // The transport cache keys on `hex` so that hex / npub / nprofile encodings
    // (and nprofiles with differing relay hints) of ONE identity share a single
    // transport.
    #[test]
    fn all_encodings_share_hex_key() {
        let pk = Keys::generate().public_key();
        let hex = pk.to_hex();
        let npub = pk.to_bech32().expect("npub");
        let with_hints = Nip19Profile::new(
            pk,
            vec![RelayUrl::parse("wss://relay.contextvm.org").unwrap()],
        )
        .to_bech32()
        .expect("nprofile");
        let no_hints = Nip19Profile::new(pk, Vec::<RelayUrl>::new())
            .to_bech32()
            .expect("nprofile");
        for input in [hex.as_str(), &npub, &with_hints, &no_hints] {
            assert_eq!(parse_path(input).unwrap().hex, hex, "input: {input}");
        }
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
        assert_eq!(p.hex, hex);
    }
}
