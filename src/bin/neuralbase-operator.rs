// SPDX-License-Identifier: Apache-2.0
use neuralbase::operator::{reconcile, DesiredTopology, Observation, MAX_OPERATOR_INPUT};
use serde::Deserialize;
use std::io::{Read, Write};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    desired: DesiredTopology,
    observed: Observation,
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() != Some("plan") || std::env::args().len() != 2 {
        return Err("usage: neuralbase-operator plan < observation.json".into());
    }
    let mut bytes = Vec::new();
    std::io::stdin()
        .take((MAX_OPERATOR_INPUT + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_OPERATOR_INPUT {
        return Err("planner input exceeds 256 KiB".into());
    }
    let input: Input = serde_json::from_slice(&bytes)?;
    let plan = reconcile(&input.desired, &input.observed)?;
    serde_json::to_writer(std::io::stdout(), &plan)?;
    std::io::stdout().write_all(b"\n")?;
    Ok(())
}
