//! Reading a contract ABI in the shapes the rest of the toolchain writes it.

use alloy_json_abi::JsonAbi;
use serde_json::Value;

/// Parse an ABI, reading an event without `anonymous` as `anonymous: false`, the Solidity default.
/// viem and wagmi ABIs leave it out and alloy requires it (#2397). The file on disk is never
/// rewritten, so a nest's identity is the same whichever shape it was given in.
pub fn parse(mut abi: Value) -> serde_json::Result<JsonAbi> {
    if let Value::Array(items) = &mut abi {
        for item in items {
            if item.get("type").and_then(Value::as_str) != Some("event") {
                continue;
            }
            if let Value::Object(fields) = item {
                fields.entry("anonymous").or_insert(Value::Bool(false));
            }
        }
    }
    serde_json::from_value(abi)
}

/// [`parse`] from the text of an ABI file.
pub fn parse_str(raw: &str) -> serde_json::Result<JsonAbi> {
    parse(serde_json::from_str(raw)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIEM_EVENT: &str = r#"[{"type":"event","name":"Transfer","inputs":[
        {"name":"from","type":"address","indexed":true},
        {"name":"to","type":"address","indexed":true},
        {"name":"value","type":"uint256","indexed":false}]}]"#;

    #[test]
    fn an_event_without_anonymous_parses_as_not_anonymous() {
        let abi = parse_str(VIEM_EVENT).unwrap();
        let ev = abi.events().next().unwrap();
        assert!(!ev.anonymous);
        assert_eq!(
            format!("0x{}", hex::encode(ev.selector())),
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        );
    }

    #[test]
    fn an_explicitly_anonymous_event_stays_anonymous() {
        let raw = VIEM_EVENT.replace(r#""type":"event","#, r#""type":"event","anonymous":true,"#);
        assert!(parse_str(&raw).unwrap().events().next().unwrap().anonymous);
    }

    #[test]
    fn a_non_abi_still_fails() {
        assert!(parse_str(r#"{"specVersion":"0.0.5"}"#).is_err());
    }
}
