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
    let args: Vec<_> = std::env::args().collect();
    #[cfg(unix)]
    if args.len() == 3 && matches!(args[1].as_str(), "admin" | "ready") {
        return admin(&args[2], args[1] == "ready");
    }
    if args.get(1).map(String::as_str) != Some("plan") || args.len() != 2 {
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

#[cfg(unix)]
fn admin(path: &str, readiness: bool) -> Result<(), Box<dyn std::error::Error>> {
    use neuralbase::operator_admin::{read_config, AdminCommand, AdminRequest, AdminResponse};
    use std::os::unix::net::UnixStream;
    use std::path::Path;
    use std::time::Duration;
    let node = read_config(Path::new(path))?;
    let command = if readiness {
        AdminCommand::Status
    } else {
        let mut input = Vec::new();
        std::io::stdin().take(8193).read_to_end(&mut input)?;
        if input.len() > 8192 {
            return Err("management command exceeds 8 KiB".into());
        }
        serde_json::from_slice(&input)?
    };
    let request = serde_json::to_vec(&AdminRequest {
        cluster: node.topology.cluster.clone(),
        command,
    })?;
    if request.len() > 8192 {
        return Err("management request exceeds 8 KiB".into());
    }
    let mut stream = UnixStream::connect(&node.socket)?;
    stream.set_read_timeout(Some(Duration::from_secs(13)))?;
    stream.set_write_timeout(Some(Duration::from_secs(13)))?;
    stream.write_all(&(request.len() as u32).to_be_bytes())?;
    stream.write_all(&request)?;
    let mut size = [0; 4];
    stream.read_exact(&mut size)?;
    let size = u32::from_be_bytes(size) as usize;
    if size > MAX_OPERATOR_INPUT {
        return Err("management response exceeds 256 KiB".into());
    }
    let mut data = vec![0; size];
    stream.read_exact(&mut data)?;
    let response: AdminResponse = serde_json::from_slice(&data)?;
    if response.node != node {
        return Err("management incarnation/configuration mismatch".into());
    }
    if let Some(error) = response.error {
        return Err(error.into());
    }
    if readiness {
        let status = response.status.ok_or("missing readiness status")?;
        if !status.ready
            || !status.committed.voters.contains(&node.id)
            || status.committed.is_joint()
            || status.transition_pending
        {
            return Err("not a ready finalized voter".into());
        }
    } else {
        std::io::stdout().write_all(&data)?;
        std::io::stdout().write_all(b"\n")?;
    }
    Ok(())
}
