//! RFC-0075 S0 spike: does dbsp 0.320.0 build for wasm32-unknown-unknown, and can one worker step a
//! batch there? The circuit is a map plus an integrated aggregate, so a spine (the part of dbsp that
//! wants a merger thread) is on the path.

use dbsp::{IndexedZSetReader, OrdZSet, Runtime, ZWeight};

/// Builds a one-worker circuit, feeds it three batches with a retraction in the middle, and
/// answers the running sum of `doubled` after each step, plus the final `count_by_parity` relation.
pub fn run_steps() -> Result<(Vec<Vec<(u64, ZWeight)>>, Vec<((u64, i64), ZWeight)>), String> {
    let (mut circuit, (input, doubled_out, parity_out)) = Runtime::init_circuit(1, |circuit| {
        let (stream, handle) = circuit.add_input_zset::<u64>();
        let doubled = stream.map(|x| x * 2);
        // An integral keeps a spine; the aggregate reads it incrementally.
        let parity = stream
            .map_index(|x| (x % 2, *x as i64))
            .aggregate_linear(|v| *v)
            .integrate();
        Ok((handle, doubled.output(), parity.output()))
    })
    .map_err(|e| format!("init_circuit: {e}"))?;

    let mut steps = Vec::new();
    for (step, batch) in [
        vec![(1u64, 1 as ZWeight), (2, 1), (3, 1)],
        vec![(2, -1), (4, 1)],
        vec![(5, 1), (6, 1), (7, 1), (8, 1)],
    ]
    .into_iter()
    .enumerate()
    {
        for (k, w) in batch {
            input.push(k, w);
        }
        circuit
            .transaction()
            .map_err(|e| format!("step {step}: {e}"))?;
        let out: OrdZSet<u64> = doubled_out.consolidate();
        steps.push(out.iter().map(|(k, (), w)| (k, w)).collect());
    }
    let parity = parity_out.consolidate();
    let parity: Vec<((u64, i64), ZWeight)> = parity
        .iter()
        .map(|(k, v, w)| ((k, v), w))
        .collect();
    circuit.kill().map_err(|_| "kill".to_string())?;
    Ok((steps, parity))
}

/// Many small transactions through an indexed aggregate, so the spine's merger and its backpressure
/// are on the path: `steps` transactions of `rows` rows each, every fourth a retraction of an earlier
/// row. Answers the final group count and the sum over every group, which a clean fold must match.
pub fn run_stress(steps: u64, rows: u64) -> Result<(usize, i64), String> {
    let (mut circuit, (input, out)) = Runtime::init_circuit(1, |circuit| {
        let (stream, handle) = circuit.add_input_zset::<(u64, u64)>();
        let sums = stream
            .map_index(|(k, v)| (*k % 1024, *v as i64))
            .aggregate_linear(|v| *v)
            .integrate();
        Ok((handle, sums.output()))
    })
    .map_err(|e| format!("init_circuit: {e}"))?;
    let mut seed = 0x9e3779b97f4a7c15u64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let mut live: Vec<(u64, u64)> = Vec::new();
    let mut expect: i64 = 0;
    let mut rel = None;
    for step in 0..steps {
        for _ in 0..rows {
            if step % 4 == 3 && !live.is_empty() {
                // Reduced in u64 so a 32-bit target picks the same row as a 64-bit one.
                let i = (next() % live.len() as u64) as usize;
                let (k, v) = live.swap_remove(i);
                input.push((k, v), -1);
                expect -= v as i64;
            } else {
                let k = next();
                let v = next() % 1000;
                live.push((k, v));
                input.push((k, v), 1);
                expect += v as i64;
            }
        }
        circuit
            .transaction()
            .map_err(|e| format!("step {step}: {e}"))?;
        // Drain the output each step, as a view does; the integral's last emission is the relation.
        rel = Some(out.consolidate());
    }
    let rel = rel.ok_or("no steps")?;
    let mut groups = 0usize;
    let mut total = 0i64;
    for (_k, v, w) in rel.iter() {
        groups += 1;
        total += v * w;
    }
    circuit.kill().map_err(|_| "kill".to_string())?;
    if total != expect {
        return Err(format!("sum {total} != expected {expect}"));
    }
    Ok((groups, total))
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use wasm_bindgen::prelude::*;

    #[wasm_bindgen]
    extern "C" {
        #[wasm_bindgen(js_namespace = console)]
        fn error(s: &str);
    }

    #[wasm_bindgen]
    pub fn spike_run() -> Result<String, JsError> {
        // dbsp wraps whatever hook is installed when its runtime starts; this one makes a panic's
        // message and location visible in Node, which `unreachable` alone does not.
        std::panic::set_hook(Box::new(|info| error(&format!("panic: {info}"))));
        let (steps, parity) = super::run_steps().map_err(|e| JsError::new(&e))?;
        Ok(format!("{steps:?} {parity:?}"))
    }

    #[wasm_bindgen]
    pub fn spike_stress(steps: u32, rows: u32) -> Result<String, JsError> {
        let (groups, total) =
            super::run_stress(steps as u64, rows as u64).map_err(|e| JsError::new(&e))?;
        Ok(format!("groups={groups} total={total}"))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn native_answer() {
        let (steps, parity) = super::run_steps().unwrap();
        assert_eq!(steps[0], vec![(2, 1), (4, 1), (6, 1)]);
        assert_eq!(steps[1], vec![(4, -1), (8, 1)]);
        assert_eq!(
            parity,
            vec![((0, 18), 1), ((1, 16), 1)],
            "{parity:?}"
        );
    }

    #[test]
    fn native_stress() {
        let (groups, total) = super::run_stress(2_000, 100).unwrap();
        eprintln!("native stress: groups={groups} total={total}");
        assert_eq!(groups, 1024);
    }
}
