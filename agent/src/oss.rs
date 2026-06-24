use std::path::Path;
use std::time::Duration;

fn build_client() -> Result<reqwest::Client, Box<dyn std::error::Error + Send + Sync>> {
    Ok(reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .build()?)
}

pub async fn download_file(url: &str, local_path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::info!(%url, "downloading file");

    let client = build_client()?;
    let response = client.get(url).send().await?;
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

    let client = build_client()?;
    let response = client.put(url).body(bytes).send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("upload failed: {status} body: {body}").into());
    }
    tracing::info!(path = %local_path.display(), "uploaded file");
    Ok(())
}
