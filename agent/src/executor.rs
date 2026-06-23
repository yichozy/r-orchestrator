use crate::oss;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::process::Command;

pub enum ExecutionOutcome {
    Cancelled,
    ResultReady {
        shard_id: String,
        output_oss_key: String,
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

pub fn cache_dir() -> PathBuf {
    let dir = std::env::var("RORCHESTRATOR_ARTIFACT_CACHE_DIR")
        .unwrap_or_else(|_| "/workspace/cache/artifacts".to_string());
    let path = PathBuf::from(dir);
    let _ = std::fs::create_dir_all(&path);
    path
}

pub async fn execute_shard(
    bundle_download_url: &str,
    output_upload_url: &str,
    output_oss_key: &str,
    shard_id: &str,
    script_name: &str,
    cancel_token: &crate::control_client::CancelToken,
) -> Result<ExecutionOutcome, Box<dyn std::error::Error + Send + Sync>> {
    // Phase 1: Download bundle
    tracing::info!(shard_id, "downloading bundle for shard");
    let cache = cache_dir();
    let bundle_path = cache.join(format!("bundle-{}.zip", shard_id));
    oss::download_file(bundle_download_url, &bundle_path).await?;

    // Phase 2: Extract bundle
    let work_dir = cache.join(format!("work-{}", shard_id));
    let _work_dir_guard = WorkDirGuard::new(work_dir.clone());
    if work_dir.exists() {
        std::fs::remove_dir_all(&work_dir)?;
    }
    std::fs::create_dir_all(&work_dir)?;
    extract_zip(&bundle_path, &work_dir)?;
    tracing::info!(path = %work_dir.display(), "extracted bundle");

    // Phase 3: Run install.sh
    let install_sh = work_dir.join("install.sh");
    if install_sh.exists() {
        if cancel_token.is_cancelled() {
            return Ok(ExecutionOutcome::Cancelled);
        }
        tracing::info!(script = %install_sh.display(), "running install.sh");
        let output = Command::new("bash")
            .current_dir(&work_dir)
            .arg(&install_sh)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await?;

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if !output.status.success() {
            let code = output.status.code().unwrap_or(-1);
            return Err(format!("install.sh failed (exit {code}): {stderr}").into());
        } else if !stderr.is_empty() {
            tracing::warn!(stderr = %stderr, "install.sh stderr output");
        } else {
            tracing::info!("install.sh completed");
        }
    }

    // Phase 4: Run cmd/{script_name}
    let script_path = work_dir.join("cmd").join(script_name);
    if !script_path.exists() {
        return Err(format!("script not found: {}", script_path.display()).into());
    }

    if cancel_token.is_cancelled() {
        return Ok(ExecutionOutcome::Cancelled);
    }
    tracing::info!(shard_id, script = %script_path.display(), "executing script");

    let output = Command::new("bash")
        .current_dir(&work_dir)
        .arg(&script_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    if !output.status.success() {
        let code = output.status.code().unwrap_or(-1);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(
            format!("script failed (exit {code}): stderr={stderr} stdout={stdout}").into(),
        );
    }
    tracing::info!(shard_id, "script completed");

    // Phase 5: Collect output files and create output zip
    let output_dir = work_dir.join("output");
    let output_zip_path = cache.join(format!("output-{}.zip", shard_id));
    if output_dir.exists() {
        create_output_zip(&output_dir, &output_zip_path)?;
    } else {
        create_empty_zip(&output_zip_path)?;
    }

    // Phase 6: Compute SHA256 and upload to OSS
    let zip_bytes = std::fs::read(&output_zip_path)?;
    let sha256 = sha256_hex(&zip_bytes);

    oss::upload_file(output_upload_url, &output_zip_path).await?;

    // Phase 7: Return result
    tracing::info!(shard_id, %output_oss_key, sha256, "shard execution finished");
    Ok(ExecutionOutcome::ResultReady {
        shard_id: shard_id.to_string(),
        output_oss_key: output_oss_key.to_string(),
        sha256,
    })
}

fn create_output_zip(
    output_dir: &Path,
    zip_path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = std::fs::File::create(zip_path)?;
    let mut zip_writer = zip::ZipWriter::new(file);
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);

    fn add_dir(
        dir: &Path,
        base: &Path,
        writer: &mut zip::ZipWriter<std::fs::File>,
        options: zip::write::SimpleFileOptions,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                add_dir(&path, base, writer, options)?;
            } else {
                let name = path.strip_prefix(base)?.to_string_lossy().to_string();
                tracing::debug!("adding to output zip: {}", name);
                writer.start_file(name, options)?;
                let mut f = std::fs::File::open(&path)?;
                std::io::copy(&mut f, writer)?;
            }
        }
        Ok(())
    }

    add_dir(output_dir, output_dir, &mut zip_writer, options)?;
    zip_writer.finish()?;
    Ok(())
}

fn create_empty_zip(zip_path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = std::fs::File::create(zip_path)?;
    let zip_writer = zip::ZipWriter::new(file);
    zip_writer.finish()?;
    Ok(())
}

fn extract_zip(
    zip_path: &Path,
    dest_dir: &Path,
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

#[cfg(unix)]
fn sanitize_unix_mode(mode: u32) -> u32 {
    mode & 0o755
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    #[test]
    fn sanitize_unix_mode_strips_elevated_and_world_writable_bits() {
        assert_eq!(super::sanitize_unix_mode(0o4777), 0o755);
        assert_eq!(super::sanitize_unix_mode(0o2666), 0o644);
    }
}
