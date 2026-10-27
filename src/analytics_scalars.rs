//! Pure, bounded conversions for event-shaped SQL. Decimal arithmetic must not pass through
//! DOUBLE, and a digest-to-CID conversion must not fetch content or hash the digest again.
use anyhow::{bail, Context, Result};
use num_bigint::BigUint;
use std::sync::Arc;

/// Name, `evaluate` kind and arity of each function `register` defines.
const FUNCTIONS: [(&str, u8, usize); 7] = [
    ("nuthatch_uint256", 1, 1),
    ("nuthatch_mul_div", 2, 3),
    ("nuthatch_cid_v0", 3, 1),
    ("nuthatch_base58_uint256", 4, 1),
    ("nuthatch_uint256_word", 5, 1),
    ("nuthatch_keccak256", 6, 1),
    ("nuthatch_abi_tuple", 7, 2),
];

pub(crate) fn names() -> impl Iterator<Item = &'static str> {
    FUNCTIONS.iter().map(|(name, _, _)| *name)
}

/// A NULL argument answers NULL without reaching `evaluate`.
pub(crate) fn register(engine: &mut burrmill::Engine) {
    for (name, kind, arity) in FUNCTIONS {
        engine.register_text_function(
            name,
            arity,
            Arc::new(move |v: &[&str]| evaluate(kind, v).map_err(|e| format!("{e:#}"))),
        );
    }
}

fn decimal(value: &str) -> Result<BigUint> {
    // 512-bit intermediates, with a little room for event-ledger sums, without admitting
    // arbitrarily large user strings into the bigint allocator.
    if value.is_empty() || value.len() > 160 || !value.bytes().all(|b| b.is_ascii_digit()) {
        bail!("expected an unsigned decimal integer of at most 160 digits");
    }
    BigUint::parse_bytes(value.as_bytes(), 10).context("invalid decimal integer")
}

fn evaluate(kind: u8, values: &[&str]) -> Result<String> {
    match kind {
        1 => {
            let word = values[0];
            if word.len() != 66
                || !word.starts_with("0x")
                || !word[2..].bytes().all(|b| b.is_ascii_hexdigit())
            {
                bail!(
                    "expected one 32-byte ABI uint256 word (received {} characters)",
                    word.len()
                );
            }
            Ok(BigUint::parse_bytes(&word.as_bytes()[2..], 16)
                .context("invalid ABI word")?
                .to_string())
        }
        2 => {
            let numerator = decimal(values[0])? * decimal(values[1])?;
            let denominator = decimal(values[2])?;
            if denominator == BigUint::default() {
                bail!("integer division by zero");
            }
            Ok((numerator / denominator).to_string())
        }
        3 => {
            let word = values[0];
            if word.len() != 66 || !word.starts_with("0x") {
                bail!("expected a 32-byte 0x-prefixed digest");
            }
            let mut digest = [0u8; 32];
            hex::decode_to_slice(&word[2..], &mut digest).context("invalid digest")?;
            Ok(crate::cid::cid_v0_from_digest(&digest))
        }
        4 => {
            let integer = decimal(values[0])?;
            if integer.bits() > 256 {
                bail!("integer exceeds uint256");
            }
            Ok(crate::cid::base58_encode(&integer.to_bytes_be()))
        }
        5 => {
            let integer = decimal(values[0])?;
            if integer.bits() > 256 {
                bail!("integer exceeds uint256");
            }
            Ok(format!("0x{integer:064x}"))
        }
        6 => {
            let value = values[0];
            if !value.starts_with("0x") || value.len() > 2050 {
                bail!("expected at most 1024 hex-encoded bytes");
            }
            let bytes = hex::decode(&value[2..]).context("invalid hex bytes")?;
            Ok(format!("{:#x}", alloy_primitives::keccak256(bytes)))
        }
        7 => {
            use alloy_dyn_abi::{DynSolType, DynSolValue};
            if values[0].len() > 160 || values[1].len() > 131074 {
                bail!("ABI tuple exceeds the type or payload limit");
            }
            let types = values[0]
                .split(',')
                .map(|name| match name {
                    "string" => Ok(DynSolType::String),
                    "bytes" => Ok(DynSolType::Bytes),
                    "address" => Ok(DynSolType::Address),
                    "uint256" => Ok(DynSolType::Uint(256)),
                    "bytes32" => Ok(DynSolType::FixedBytes(32)),
                    "bool" => Ok(DynSolType::Bool),
                    _ => anyhow::bail!("unsupported ABI tuple member"),
                })
                .collect::<Result<Vec<_>>>()?;
            if types.len() > 16 {
                bail!("ABI tuple exceeds 16 members");
            }
            let bytes = hex::decode(
                values[1]
                    .strip_prefix("0x")
                    .context("ABI tuple requires 0x hex")?,
            )?;
            let DynSolValue::Tuple(items) = DynSolType::Tuple(types).abi_decode_params(&bytes)?
            else {
                bail!("ABI value was not a tuple");
            };
            let json = items
                .into_iter()
                .map(|item| match item {
                    DynSolValue::String(s) => serde_json::Value::String(s),
                    DynSolValue::Bytes(b) => serde_json::json!(format!("0x{}", hex::encode(b))),
                    DynSolValue::Address(a) => serde_json::json!(format!("{a:#x}")),
                    DynSolValue::Uint(n, _) => serde_json::json!(n.to_string()),
                    DynSolValue::FixedBytes(b, _) => serde_json::json!(format!("{b:#x}")),
                    DynSolValue::Bool(b) => serde_json::json!(b),
                    _ => unreachable!("bounded flat ABI types"),
                })
                .collect::<Vec<_>>();
            Ok(serde_json::to_string(&json)?)
        }
        _ => unreachable!("registered scalar kind"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> burrmill::Engine {
        let mut engine = burrmill::Engine::open_empty().unwrap();
        register(&mut engine);
        engine
    }

    fn one(engine: &burrmill::Engine, sql: &str) -> std::result::Result<Option<String>, String> {
        let batches = engine.sql(sql).map_err(|e| e.to_string())?;
        let rows = burrmill::df::encode::rows(&batches[0]).map_err(|e| e.to_string())?;
        Ok(rows[0]["v"].as_str().map(str::to_string))
    }

    #[test]
    fn a_case_guard_does_not_decode_an_empty_predeployment_word() {
        let engine = engine();
        let word = format!("0x{:064x}", 16_083_151);
        let sql = format!(
            "SELECT DISTINCT CASE WHEN reverted OR result = '0x' THEN 0 \
             ELSE CAST(nuthatch_uint256(result) AS INTEGER) END AS value \
             FROM (VALUES ('0x', false), ('{word}', false)) t(result, reverted) ORDER BY value"
        );
        let batches = engine.sql(&sql).unwrap();
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|b| burrmill::df::encode::rows(b).unwrap())
            .map(|r| r["value"].as_i64().unwrap())
            .collect();
        assert_eq!(values, vec![0, 16_083_151]);
    }

    #[test]
    fn sql_scalars_preserve_full_width_and_nulls_and_refuse_bad_inputs() {
        let engine = engine();
        let max = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        let value = |call: String| one(&engine, &format!("SELECT {call} AS v")).unwrap();
        assert_eq!(
            value(format!("nuthatch_mul_div('{max}', '{max}', '{max}')")).as_deref(),
            Some(max)
        );
        assert_eq!(
            value(format!("nuthatch_uint256('0x{}')", "f".repeat(64))).as_deref(),
            Some(max)
        );
        assert_eq!(
            value("nuthatch_mul_div('11','3','2')".into()).as_deref(),
            Some("16")
        );
        assert_eq!(value("nuthatch_mul_div(NULL,'3','2')".into()), None);
        assert_eq!(
            value(format!("nuthatch_cid_v0('0x{}')", "00".repeat(32))).as_deref(),
            Some("QmNLei78zWmzUdbeRB3CiUfAizWUrbeeZh5K1rhAQKCh51")
        );
        for call in [
            "nuthatch_uint256('0x01')",
            "nuthatch_cid_v0('0x00')",
            "nuthatch_mul_div('1','2','0')",
            "nuthatch_mul_div('-1','2','3')",
            "nuthatch_abi_tuple('int8', '0x')",
        ] {
            assert!(
                one(&engine, &format!("SELECT {call} AS v")).is_err(),
                "{call}"
            );
        }
        assert!(decimal(&"9".repeat(161)).is_err());
        assert_eq!(
            value("nuthatch_base58_uint256('256')".into()).as_deref(),
            Some("5R")
        );
        assert_eq!(
            value("nuthatch_base58_uint256('0')".into()).as_deref(),
            Some("1")
        );
        assert!(evaluate(4, &[&"9".repeat(79)]).is_err());
        assert_eq!(evaluate(5, &["256"]).unwrap(), format!("0x{:064x}", 256));
        assert_eq!(
            evaluate(6, &["0x"]).unwrap(),
            "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
        let tuple = alloy_dyn_abi::DynSolValue::Tuple(vec![
            alloy_dyn_abi::DynSolValue::String("https://indexer.example".into()),
            alloy_dyn_abi::DynSolValue::String("u10".into()),
            alloy_dyn_abi::DynSolValue::Address(alloy_primitives::Address::ZERO),
        ]);
        let encoded = format!("0x{}", hex::encode(tuple.abi_encode_params()));
        let decoded: serde_json::Value =
            serde_json::from_str(&evaluate(7, &["string,string,address", &encoded]).unwrap())
                .unwrap();
        assert_eq!(
            decoded,
            serde_json::json!([
                "https://indexer.example",
                "u10",
                "0x0000000000000000000000000000000000000000"
            ])
        );
        assert_eq!(
            value("TRY(nuthatch_abi_tuple('string,string,address', '0x'))".into()),
            None
        );
        // More than one batch, including NULLs.
        let count = engine
            .sql(
                "SELECT count(*) AS n FROM (SELECT nuthatch_mul_div(CASE WHEN i % 2 = 0 THEN NULL \
                 ELSE CAST(i AS VARCHAR) END, '3', '3') AS v, i FROM range(10000) t(i)) \
                 WHERE v = CAST(i AS VARCHAR)",
            )
            .unwrap();
        let rows = burrmill::df::encode::rows(&count[0]).unwrap();
        assert_eq!(rows[0]["n"].as_u64(), Some(5000));
    }
}
