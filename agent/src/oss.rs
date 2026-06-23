use std::path::Path;

pub async fn download_file(url: &str, local_path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!(%url, "downloading file");

    let response = reqwest::get(url).await?;
    if !response.status().is_success() {
        return Err(format!("download failed: {} for url {}", response.status(), url).into());
    }
    let bytes = response.bytes().await?;

    if let Some(parent) = local_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(local_path, &bytes)?;
    tracing::info!(path = %local_path.display(), size = bytes.len(), "downloaded file");
    Ok(())
}

pub async fn upload_file(url: &str, local_path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let bytes = std::fs::read(local_path)?;
    tracing::info!(%url, size = bytes.len(), "uploading file");

    let client = reqwest::Client::new();
    let response = client
        .put(url)
        .header("Content-Type", "application/octet-stream")
        .body(bytes)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!("upload failed: {} for url {}", response.status(), url).into());
    }
    tracing::info!(path = %local_path.display(), "uploaded file");
    Ok(())
}
