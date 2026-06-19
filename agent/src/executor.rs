use crate::artifacts;
use crate::control_client::controlv1;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::process::Command;
use tokio::task::JoinSet;
use tonic::transport::Channel;

pub enum ExecutionOutcome {
    Cancelled,
    ResultReady {
        shard_id: String,
        output_csv: Vec<u8>,
        sha256: String,
    },
}

struct WorkDirGuard {
    path: PathBuf,
}

impl WorkDirGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for WorkDirGuard {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_dir_all(&self.path) {
            if self.path.exists() {
                tracing::warn!(path = %self.path.display(), error = %err, "failed to remove shard work directory");
            }
        }
    }
}

pub fn build_args(header: &[String], row: &[String]) -> Vec<String> {
    let mut args = Vec::with_capacity(header.len() * 2);
    for (name, value) in header.iter().zip(row.iter()) {
        args.push(format!("--{}", name));
        args.push(value.clone());
    }
    args
}

pub async fn execute_shard(
    client: &mut controlv1::control_service_client::ControlServiceClient<Channel>,
    tx: &tokio::sync::mpsc::Sender<controlv1::AgentMessage>,
    agent_id: &str,
    token: &str,
    shard_id: &str,
    bundle_artifact_id: &str,
    input_csv_artifact_id: &str,
    shard_index: i32,
    total_shards: i32,
    cancel_token: &crate::control_client::CancelToken,
    parallelism: usize,
) -> Result<ExecutionOutcome, Box<dyn std::error::Error + Send + Sync>> {
    // Phase 1: Fetch artifacts
    tracing::info!(
        shard_id,
        shard_index,
        total_shards,
        "fetching artifacts for shard"
    );
    let cache_dir = artifacts::artifact_cache_dir();
    let bundle_path =
        artifacts::fetch_and_save_artifact(client, bundle_artifact_id, &cache_dir, token, agent_id)
            .await?;

    let csv_path = artifacts::fetch_and_save_artifact_with_shard(
        client,
        input_csv_artifact_id,
        &cache_dir,
        token,
        agent_id,
        shard_index,
        total_shards,
    )
    .await?;

    // Phase 2: Extract bundle zip
    let work_dir = cache_dir.join(format!("work-{}", shard_id));
    let _work_dir_guard = WorkDirGuard::new(work_dir.clone());
    if work_dir.exists() {
        std::fs::remove_dir_all(&work_dir)?;
    }
    std::fs::create_dir_all(&work_dir)?;
    extract_zip(&bundle_path, &work_dir)?;
    tracing::info!(path = %work_dir.display(), "extracted bundle");

    let (script_dir, run_sh, install_sh) = locate_bundle_scripts(&work_dir)?;

    // Phase 3: Send ShardStarted
    let _ = tx
        .send(controlv1::AgentMessage {
            payload: Some(controlv1::agent_message::Payload::ShardStarted(
                controlv1::ShardStarted {
                    shard_id: shard_id.to_string(),
                },
            )),
        })
        .await;

    // Phase 4: Run install.sh
    if let Some(install_sh) = &install_sh {
        tracing::info!(script = %install_sh.display(), working_dir = %script_dir.display(), "running install.sh");
        let install_output = Command::new("bash")
            .current_dir(&script_dir)
            .arg(install_sh)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await?;

        let install_stderr = String::from_utf8_lossy(&install_output.stderr)
            .trim()
            .to_string();
        if !install_output.status.success() {
            let code = install_output.status.code().unwrap_or(-1);
            return Err(format!("install.sh failed (exit {code}): {install_stderr}").into());
        } else if !install_stderr.is_empty() {
            tracing::warn!(stderr = %install_stderr, "install.sh stderr output");
        } else {
            tracing::info!("install.sh completed");
        }
    }

    // Phase 5: Parse shard CSV (server already sliced this shard's rows)
    let csv_content = std::fs::read_to_string(&csv_path)?;
    let mut csv_reader = csv::Reader::from_reader(csv_content.as_bytes());
    let headers: Vec<String> = csv_reader
        .headers()?
        .iter()
        .map(|s| s.to_string())
        .collect();
    let mut skipped = 0usize;
    let rows: Vec<Vec<String>> = csv_reader
        .records()
        .filter_map(|r| match r {
            Ok(row) => Some(row.iter().map(|s| s.to_string()).collect()),
            Err(e) => {
                skipped += 1;
                tracing::warn!(error = %e, "skipping malformed CSV row");
                None
            }
        })
        .collect();
    if skipped > 0 {
        tracing::warn!(skipped, total = skipped + rows.len(), "CSV rows skipped");
    }
    tracing::info!(row_count = rows.len(), "parsed shard CSV");

    // Phase 6: Execute each row via run.sh (with parallelism)
    let parallelism = parallelism.max(1);
    tracing::info!(
        shard_id,
        row_count = rows.len(),
        parallelism,
        "executing shard"
    );
    let mut succeeded = 0usize;
    let mut failed = 0usize;

    let output_dir = work_dir.join("output");
    std::fs::create_dir_all(&output_dir)?;

    if parallelism <= 1 {
        // Sequential execution
        for (row_index, row) in rows.iter().enumerate() {
            if cancel_token.is_cancelled() {
                tracing::info!(shard_id, "shard cancelled by server, stopping execution");
                return Ok(ExecutionOutcome::Cancelled);
            }
            let output_path = output_dir.join(format!("row-{}.csv", row_index));
            let mut args = build_args(&headers, row);
            args.push("--output_path".to_string());
            args.push(output_path.to_string_lossy().to_string());

            tracing::debug!(row_index, total = rows.len(), script = %run_sh.display(), args = ?&args, "executing row");
            let exit_code = run_single_row(&script_dir, &run_sh, &args).await;
            if exit_code == 0 {
                tracing::debug!(row_index, exit_code = exit_code, "row completed");
                succeeded += 1;
            } else {
                failed += 1;
            }
        }
    } else {
        // Parallel execution
        let mut tasks: JoinSet<(usize, i32)> = JoinSet::new();
        for (row_index, row) in rows.iter().enumerate() {
            if cancel_token.is_cancelled() {
                tracing::info!(shard_id, "shard cancelled by server, stopping execution");
                tasks.abort_all();
                return Ok(ExecutionOutcome::Cancelled);
            }
            // Wait for a slot if at max concurrency
            while tasks.len() >= parallelism {
                if let Some(result) = tasks.join_next().await {
                    match result {
                        Ok((idx, code)) => {
                            if code == 0 {
                                succeeded += 1;
                            } else {
                                failed += 1;
                                if code != 0 {
                                    tracing::warn!(
                                        row_index = idx,
                                        exit_code = code,
                                        "row execution failed"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "row task panicked");
                            failed += 1;
                        }
                    }
                }
            }
            let output_path = output_dir.join(format!("row-{}.csv", row_index));
            let mut args = build_args(&headers, row);
            args.push("--output_path".to_string());
            args.push(output_path.to_string_lossy().to_string());

            let script_dir = script_dir.clone();
            let run_sh = run_sh.clone();
            tracing::debug!(row_index, total = rows.len(), "spawning row task");
            tasks.spawn(async move {
                tracing::debug!(row_index, "row task started");
                let exit_code = run_single_row(&script_dir, &run_sh, &args).await;
                tracing::debug!(row_index, exit_code = exit_code, "row task finished");
                (row_index, exit_code)
            });
        }
        // Drain remaining tasks
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((idx, code)) => {
                    if code == 0 {
                        succeeded += 1;
                    } else {
                        failed += 1;
                        if code != 0 {
                            tracing::warn!(
                                row_index = idx,
                                exit_code = code,
                                "row execution failed"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "row task panicked");
                    failed += 1;
                }
            }
        }
    }

    // Phase 7: Merge row output CSVs into shard-level output
    let output_csv_bytes = merge_row_csvs(&output_dir)?;

    // Phase 8: Return shard output to the control loop so it can keep one
    // pending result in memory until the server fetches and acknowledges it.
    tracing::info!(
        shard_id,
        succeeded,
        failed,
        "shard execution finished with result ready"
    );

    Ok(ExecutionOutcome::ResultReady {
        shard_id: shard_id.to_string(),
        sha256: sha256_hex(&output_csv_bytes),
        output_csv: output_csv_bytes,
    })
}

async fn run_single_row(script_dir: &Path, run_sh: &Path, args: &[String]) -> i32 {
    tracing::debug!(script = %run_sh.display(), working_dir = %script_dir.display(), args = ?args, "spawning bash run.sh");
    let output = Command::new("bash")
        .current_dir(script_dir)
        .arg(run_sh)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;

    match output {
        Ok(out) => {
            let code = out.status.code().unwrap_or(-1);
            if code != 0 {
                let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
                let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if !stderr.is_empty() {
                    tracing::warn!(exit_code = code, detail = %stderr, "row failed with output");
                } else if !stdout.is_empty() {
                    tracing::warn!(exit_code = code, detail = %stdout, "row failed with output");
                } else {
                    tracing::warn!(exit_code = code, "row failed with output");
                }
            }
            code
        }
        Err(e) => {
            tracing::error!(error = %e, "row failed to spawn");
            -1
        }
    }
}

fn locate_bundle_scripts(
    work_dir: &Path,
) -> Result<(PathBuf, PathBuf, Option<PathBuf>), Box<dyn std::error::Error + Send + Sync>> {
    let mut script_dir = work_dir.to_path_buf();
    let mut run_sh = work_dir.join("run.sh");
    let mut install_sh = work_dir.join("install.sh");

    if run_sh.exists() {
        let install = if install_sh.exists() {
            Some(install_sh)
        } else {
            None
        };
        return Ok((script_dir, run_sh, install));
    }

    if let Ok(entries) = std::fs::read_dir(work_dir) {
        let subdirs: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .map(|e| e.path())
            .collect();
        if subdirs.len() == 1 {
            let candidate_dir = &subdirs[0];
            let candidate_run = candidate_dir.join("run.sh");
            if candidate_run.exists() {
                script_dir = candidate_dir.clone();
                run_sh = candidate_run;
                install_sh = candidate_dir.join("install.sh");
                let install = if install_sh.exists() {
                    Some(install_sh)
                } else {
                    None
                };
                return Ok((script_dir, run_sh, install));
            }
        }
    }

    fn find_first(dir: &Path, name: &str, depth: usize) -> Option<PathBuf> {
        if depth == 0 {
            return None;
        }
        let entries = std::fs::read_dir(dir).ok()?;
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_file() && path.file_name().and_then(|s| s.to_str()) == Some(name) {
                return Some(path);
            }
            if path.is_dir() {
                if let Some(found) = find_first(&path, name, depth - 1) {
                    return Some(found);
                }
            }
        }
        None
    }

    if let Some(found_run) = find_first(work_dir, "run.sh", 4) {
        script_dir = found_run.parent().unwrap_or(work_dir).to_path_buf();
        run_sh = found_run;
        let found_install = find_first(&script_dir, "install.sh", 2);
        return Ok((script_dir, run_sh, found_install));
    }

    Err("bundle missing run.sh".into())
}

fn merge_row_csvs(
    output_dir: &std::path::Path,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let mut csv_files: Vec<_> = std::fs::read_dir(output_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "csv"))
        .collect();
    csv_files.sort_by(|a, b| {
        let a_name = a.file_name().to_string_lossy().to_string();
        let b_name = b.file_name().to_string_lossy().to_string();
        match (row_csv_index(&a_name), row_csv_index(&b_name)) {
            (Some(a_idx), Some(b_idx)) => a_idx.cmp(&b_idx).then_with(|| a_name.cmp(&b_name)),
            _ => a_name.cmp(&b_name),
        }
    });

    if csv_files.is_empty() {
        return Ok(Vec::new());
    }

    let mut merged = Vec::new();
    for (i, entry) in csv_files.iter().enumerate() {
        let content = std::fs::read(entry.path())?;
        if content.is_empty() {
            continue;
        }
        if i == 0 {
            merged.extend_from_slice(&content);
        } else {
            if let Some(pos) = content.iter().position(|&b| b == b'\n') {
                merged.extend_from_slice(&content[pos + 1..]);
            } else {
                continue;
            }
        }
    }

    if !merged.is_empty() && *merged.last().unwrap() != b'\n' {
        merged.push(b'\n');
    }

    Ok(merged)
}

fn extract_zip(
    zip_path: &std::path::Path,
    dest_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = std::fs::File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let outpath = match entry.enclosed_name() {
            Some(p) => dest_dir.join(p),
            None => continue,
        };
        if entry.name().ends_with('/') {
            std::fs::create_dir_all(&outpath)?;
        } else {
            if let Some(parent) = outpath.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let mut outfile = std::fs::File::create(&outpath)?;
            std::io::copy(&mut entry, &mut outfile)?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Some(mode) = entry.unix_mode() {
                std::fs::set_permissions(
                    &outpath,
                    std::fs::Permissions::from_mode(sanitize_unix_mode(mode)),
                )?;
            }
        }
    }

    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{:02x}", byte));
    }
    out
}

fn row_csv_index(file_name: &str) -> Option<usize> {
    let suffix = file_name.strip_prefix("row-")?.strip_suffix(".csv")?;
    suffix.parse().ok()
}

#[cfg(unix)]
fn sanitize_unix_mode(mode: u32) -> u32 {
    mode & 0o755
}

#[cfg(test)]
mod tests {
    use std::fs;

    #[test]
    fn build_args_maps_underscores_to_dashes() {
        let header = vec!["sample_id".to_string(), "group".to_string()];
        let row = vec!["s1".to_string(), "A".to_string()];
        let args = super::build_args(&header, &row);
        assert_eq!(args, vec!["--sample_id", "s1", "--group", "A"]);
    }

    #[test]
    fn merge_row_csvs_sorts_by_numeric_row_index() {
        let temp_dir = std::env::temp_dir().join(format!("merge-row-csvs-{}", std::process::id()));
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir).unwrap();
        }
        fs::create_dir_all(&temp_dir).unwrap();

        fs::write(temp_dir.join("row-10.csv"), "id,value\n10,z\n").unwrap();
        fs::write(temp_dir.join("row-2.csv"), "id,value\n2,b\n").unwrap();
        fs::write(temp_dir.join("row-1.csv"), "id,value\n1,a\n").unwrap();

        let merged = super::merge_row_csvs(&temp_dir).unwrap();
        assert_eq!(
            String::from_utf8(merged).unwrap(),
            "id,value\n1,a\n2,b\n10,z\n"
        );

        fs::remove_dir_all(&temp_dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn sanitize_unix_mode_strips_elevated_and_world_writable_bits() {
        assert_eq!(super::sanitize_unix_mode(0o4777), 0o755);
        assert_eq!(super::sanitize_unix_mode(0o2666), 0o644);
    }
}
