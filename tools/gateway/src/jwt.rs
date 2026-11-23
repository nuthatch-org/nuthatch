//! JWS compact-form verification (RFC 7515/7519) for the four algorithms an OIDC provider signs
//! with in practice: RS256, ES256, EdDSA (Ed25519) and HS256. Signatures are checked with `ring`.
//!
//! The algorithm a token may use is fixed by the key that verifies it, never by the token alone: an
//! RSA key only verifies RS256, an HMAC secret only HS256, and `alg: none` is refused. That closes
//! the algorithm-confusion attacks (an RSA public key replayed as an HMAC secret).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ring::{hmac, signature};
use serde_json::{Map, Value};

/// Why a token was refused. Each has a stable name, sent to the caller and logged; none carries the
/// token or any key material.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    Missing,
    Malformed,
    UnsupportedAlg,
    UnknownKey,
    BadSignature,
    NoExpiry,
    Expired,
    NotYetValid,
    WrongIssuer,
    WrongAudience,
}

impl Refusal {
    pub fn code(self) -> &'static str {
        match self {
            Refusal::Missing => "token_missing",
            Refusal::Malformed => "token_malformed",
            Refusal::UnsupportedAlg => "token_alg_unsupported",
            Refusal::UnknownKey => "token_key_unknown",
            Refusal::BadSignature => "token_signature_invalid",
            Refusal::NoExpiry => "token_without_expiry",
            Refusal::Expired => "token_expired",
            Refusal::NotYetValid => "token_not_yet_valid",
            Refusal::WrongIssuer => "token_wrong_issuer",
            Refusal::WrongAudience => "token_wrong_audience",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Alg {
    Rs256,
    Es256,
    EdDsa,
    Hs256,
}

#[derive(Clone)]
enum Material {
    Rsa {
        n: Vec<u8>,
        e: Vec<u8>,
    },
    /// Uncompressed SEC1 point, `04 || x || y`.
    P256(Vec<u8>),
    Ed25519(Vec<u8>),
    Hmac(Vec<u8>),
}

impl Material {
    fn alg(&self) -> Alg {
        match self {
            Material::Rsa { .. } => Alg::Rs256,
            Material::P256(_) => Alg::Es256,
            Material::Ed25519(_) => Alg::EdDsa,
            Material::Hmac(_) => Alg::Hs256,
        }
    }

    fn verify(&self, msg: &[u8], sig: &[u8]) -> bool {
        match self {
            Material::Rsa { n, e } => signature::RsaPublicKeyComponents { n, e }
                .verify(&signature::RSA_PKCS1_2048_8192_SHA256, msg, sig)
                .is_ok(),
            Material::P256(pk) => {
                signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, pk)
                    .verify(msg, sig)
                    .is_ok()
            }
            Material::Ed25519(pk) => signature::UnparsedPublicKey::new(&signature::ED25519, pk)
                .verify(msg, sig)
                .is_ok(),
            Material::Hmac(secret) => {
                hmac::verify(&hmac::Key::new(hmac::HMAC_SHA256, secret), msg, sig).is_ok()
            }
        }
    }
}

#[derive(Clone)]
struct Key {
    kid: Option<String>,
    material: Material,
}

/// The keys tokens are verified against: a JWKS document, or one HS256 secret.
#[derive(Clone, Default)]
pub struct KeySet(Vec<Key>);

impl std::fmt::Debug for KeySet {
    // Key material is never printed, even public keys: the kids and count are enough to debug.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kids: Vec<_> = self.0.iter().map(|k| k.kid.as_deref()).collect();
        write!(f, "KeySet{kids:?}")
    }
}

fn b64(s: &str) -> Option<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(s).ok()
}

impl KeySet {
    pub fn hs256(secret: &[u8]) -> KeySet {
        KeySet(vec![Key {
            kid: None,
            material: Material::Hmac(secret.to_vec()),
        }])
    }

    /// Parse a JWKS document. Keys of a type, curve or use this cannot verify with are skipped (an
    /// IdP's set often carries encryption keys too); a set left with none is an error.
    pub fn from_jwks(doc: &[u8]) -> Result<KeySet, String> {
        let doc: Value =
            serde_json::from_slice(doc).map_err(|e| format!("JWKS is not JSON: {e}"))?;
        let keys = doc["keys"].as_array().ok_or("JWKS has no `keys` array")?;
        let mut out = Vec::new();
        for jwk in keys {
            if jwk.get("use").is_some_and(|u| u != "sig") {
                continue;
            }
            let field = |k: &str| jwk[k].as_str().and_then(b64);
            let material = match (jwk["kty"].as_str(), jwk["crv"].as_str()) {
                (Some("RSA"), _) => match (field("n"), field("e")) {
                    (Some(n), Some(e)) => Material::Rsa { n, e },
                    _ => continue,
                },
                (Some("EC"), Some("P-256")) => match (field("x"), field("y")) {
                    (Some(x), Some(y)) if x.len() == 32 && y.len() == 32 => {
                        Material::P256([&[4u8][..], &x, &y].concat())
                    }
                    _ => continue,
                },
                (Some("OKP"), Some("Ed25519")) => match field("x") {
                    Some(x) if x.len() == 32 => Material::Ed25519(x),
                    _ => continue,
                },
                _ => continue,
            };
            // A JWK that names its algorithm is held to it.
            let declared = jwk["alg"].as_str();
            if declared.is_some_and(|a| alg_named(a) != Some(material.alg())) {
                continue;
            }
            out.push(Key {
                kid: jwk["kid"].as_str().map(str::to_string),
                material,
            });
        }
        if out.is_empty() {
            return Err(
                "JWKS has no signing key this gateway can use (RSA, EC P-256, Ed25519)".into(),
            );
        }
        Ok(KeySet(out))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn alg_named(a: &str) -> Option<Alg> {
    match a {
        "RS256" => Some(Alg::Rs256),
        "ES256" => Some(Alg::Es256),
        "EdDSA" => Some(Alg::EdDsa),
        "HS256" => Some(Alg::Hs256),
        _ => None,
    }
}

/// What every accepted token must say about itself.
#[derive(Debug, Clone)]
pub struct Expect {
    pub issuer: String,
    pub audience: String,
    /// Clock skew allowed on `exp` and `nbf`, in seconds.
    pub leeway: u64,
}

/// A verified token's claims.
#[derive(Debug, Clone)]
pub struct Claims(pub Map<String, Value>);

impl Claims {
    /// OAuth 2.0's space-separated `scope` (RFC 8693 §4.2), or the `scp` array some providers send.
    pub fn scopes(&self) -> Vec<String> {
        let mut out = Vec::new();
        for claim in ["scope", "scp"] {
            match self.0.get(claim) {
                Some(Value::String(s)) => out.extend(s.split_whitespace().map(str::to_string)),
                Some(Value::Array(a)) => {
                    out.extend(a.iter().filter_map(Value::as_str).map(str::to_string))
                }
                _ => {}
            }
        }
        out
    }
}

/// Verify `token` against `keys` at unix time `now`. The signature is checked before any claim is
/// read, so a forged token learns nothing about which claims would have passed.
pub fn verify(token: &str, keys: &KeySet, expect: &Expect, now: u64) -> Result<Claims, Refusal> {
    let mut parts = token.split('.');
    let (Some(h), Some(p), Some(s), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(Refusal::Malformed);
    };
    let header: Value = b64(h)
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or(Refusal::Malformed)?;
    let sig = b64(s).ok_or(Refusal::Malformed)?;
    // RFC 7515 §4.1.11: an extension this verifier does not implement must be refused.
    if header.get("crit").is_some() {
        return Err(Refusal::UnsupportedAlg);
    }
    let alg = header["alg"]
        .as_str()
        .and_then(alg_named)
        .ok_or(Refusal::UnsupportedAlg)?;
    let kid = header["kid"].as_str();
    let candidates: Vec<&Key> = keys
        .0
        .iter()
        .filter(|k| k.material.alg() == alg)
        .filter(|k| match (kid, &k.kid) {
            (Some(want), Some(have)) => want == have,
            // A token naming a key is only checked against that key; one naming none, against every
            // key of its algorithm. An HS256 secret has no kid and matches either way.
            (Some(_), None) => matches!(k.material, Material::Hmac(_)),
            (None, _) => true,
        })
        .collect();
    if candidates.is_empty() {
        return Err(Refusal::UnknownKey);
    }
    let signed = &token.as_bytes()[..h.len() + 1 + p.len()];
    if !candidates.iter().any(|k| k.material.verify(signed, &sig)) {
        return Err(Refusal::BadSignature);
    }
    let claims: Map<String, Value> = b64(p)
        .and_then(|b| serde_json::from_slice(&b).ok())
        .ok_or(Refusal::Malformed)?;
    let time = |k: &str| claims.get(k).map(|v| v.as_f64().ok_or(Refusal::Malformed));
    let exp = time("exp").ok_or(Refusal::NoExpiry)??;
    if (now as f64) >= exp + expect.leeway as f64 {
        return Err(Refusal::Expired);
    }
    if let Some(nbf) = time("nbf").transpose()? {
        if ((now + expect.leeway) as f64) < nbf {
            return Err(Refusal::NotYetValid);
        }
    }
    if claims.get("iss").and_then(Value::as_str) != Some(expect.issuer.as_str()) {
        return Err(Refusal::WrongIssuer);
    }
    let aud_ok = match claims.get("aud") {
        Some(Value::String(a)) => *a == expect.audience,
        Some(Value::Array(a)) => a.iter().any(|v| v.as_str() == Some(&expect.audience)),
        _ => false,
    };
    if !aud_ok {
        return Err(Refusal::WrongAudience);
    }
    Ok(Claims(claims))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7515 Appendix A.2: an RS256 JWS and the RSA public key that signed it. Its `exp` is
    /// 1300819380, so the clock is set just before.
    const RFC7515_A2: &str = "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJqb2UiLA0KICJleHAiOjEzMDA4MTkzODAsDQogImh0dHA6Ly9leGFtcGxlLmNvbS9pc19yb290Ijp0cnVlfQ.cC4hiUPoj9Eetdgtv3hF80EGrhuB__dzERat0XF9g2VtQgr9PJbu3XOiZj5RZmh7AAuHIm4Bh-0Qc_lF5YKt_O8W2Fp5jujGbds9uJdbF9CUAr7t1dnZcAcQjbKBYNX4BAynRFdiuB--f_nZLgrnbyTyWzO75vRK5h6xBArLIARNPvkSjtQBMHlb1L07Qe7K0GarZRmB_eSN9383LcOLn6_dO--xi12jzDwusC-eOkHWEsqtFZESc6BfI7noOPqvhJ1phCnvWh6IeYI2w9QOYEUipUTI8np6LbgGY9Fs98rqVt5AXLIhWkWywlVmtVrBp0igcN_IoypGlUPQGe77Rw";
    const RFC7515_A2_JWKS: &str = r#"{"keys":[{"kty":"RSA",
      "n":"ofgWCuLjybRlzo0tZWJjNiuSfb4p4fAkd_wWJcyQoTbji9k0l8W26mPddxHmfHQp-Vaw-4qPCJrcS2mJPMEzP1Pt0Bm4d4QlL-yRT-SFd2lZS-pCgNMsD1W_YpRPEwOWvG6b32690r2jZ47soMZo9wGzjb_7OMg0LOL-bSf63kpaSHSXndS5z5rexMdbBYUsLA9e-KXBdQOS-UTo7WTBEMa2R2CapHg665xsmtdVMTBQY4uDZlxvb3qCo5ZwKh9kG4LT6_I5IhlJH7aGhyxXFvUK-DWNmoudF8NAco9_h9iaGNj8q2ethFkMLs91kzk2PAcDTW9gb54h4FRWyuXpoQ",
      "e":"AQAB"}]}"#;

    fn expect(issuer: &str, audience: &str) -> Expect {
        Expect {
            issuer: issuer.into(),
            audience: audience.into(),
            leeway: 0,
        }
    }

    #[test]
    fn rs256_verifies_the_rfc_7515_example() {
        let keys = KeySet::from_jwks(RFC7515_A2_JWKS.as_bytes()).unwrap();
        // The example has no `aud`, so the signature and expiry pass and the audience is what refuses:
        // proof the RSA signature itself verified.
        assert_eq!(
            verify(RFC7515_A2, &keys, &expect("joe", "x"), 1_300_819_379).unwrap_err(),
            Refusal::WrongAudience
        );
        assert_eq!(
            verify(RFC7515_A2, &keys, &expect("joe", "x"), 1_300_819_380).unwrap_err(),
            Refusal::Expired
        );
        let mut forged = RFC7515_A2.to_string();
        let at = forged.len() - 10;
        let flip = if &forged[at..at + 1] == "A" { "B" } else { "A" };
        forged.replace_range(at..at + 1, flip);
        assert_eq!(
            verify(&forged, &keys, &expect("joe", "x"), 0).unwrap_err(),
            Refusal::BadSignature
        );
    }

    #[test]
    fn an_rsa_key_never_verifies_hs256() {
        // The classic confusion: sign HS256 with the RSA public key's bytes as the secret.
        let keys = KeySet::from_jwks(RFC7515_A2_JWKS.as_bytes()).unwrap();
        let h = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256"}"#);
        let p = URL_SAFE_NO_PAD.encode(br#"{"exp":9999999999}"#);
        let tag = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, RFC7515_A2_JWKS.as_bytes()),
            format!("{h}.{p}").as_bytes(),
        );
        let token = format!("{h}.{p}.{}", URL_SAFE_NO_PAD.encode(tag.as_ref()));
        assert_eq!(
            verify(&token, &keys, &expect("a", "b"), 0).unwrap_err(),
            Refusal::UnknownKey
        );
    }

    #[test]
    fn alg_none_and_crit_are_refused() {
        let keys = KeySet::hs256(b"secret");
        for header in [r#"{"alg":"none"}"#, r#"{"alg":"HS256","crit":["x"]}"#] {
            let token = format!("{}.e30.", URL_SAFE_NO_PAD.encode(header));
            assert_eq!(
                verify(&token, &keys, &expect("a", "b"), 0).unwrap_err(),
                Refusal::UnsupportedAlg,
                "{header}"
            );
        }
        for bad in ["", "a.b", "a.b.c.d", "!!.e30.x"] {
            assert_eq!(
                verify(bad, &keys, &expect("a", "b"), 0).unwrap_err(),
                Refusal::Malformed,
                "{bad}"
            );
        }
    }

    #[test]
    fn a_jwks_key_held_to_its_declared_alg_or_use_is_skipped() {
        let doc = r#"{"keys":[{"kty":"OKP","crv":"Ed25519","x":"11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo","alg":"RS256"},
                              {"kty":"OKP","crv":"Ed25519","x":"11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo","use":"enc"}]}"#;
        assert!(KeySet::from_jwks(doc.as_bytes()).is_err());
    }
}
