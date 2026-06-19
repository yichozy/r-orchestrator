use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

pub fn artifact_cache_dir() -> PathBuf {
    let dir = std::env::var("RORCHESTRATOR_ARTIFACT_CACHE_DIR")
        .unwrap_or_else(|_| "/workspace/cache/artifacts".to_string());
    let path = PathBuf::from(dir);
    let _ = std::fs::create_dir_all(&path);
    path
}

fn validate_artifact_id(
    artifact_id: &str,
) -> Result<&str, Box<dyn std::error::Error + Send + Sync>> {
    let file_name = Path::new(artifact_id)
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("invalid artifact id: {artifact_id}"))?;
    if file_name != artifact_id {
        return Err(format!("artifact id must be a plain file name, got: {artifact_id}").into());
    }
    Ok(file_name)
}

pub async fn fetch_and_save_artifact(
    client: &mut crate::control_client::controlv1::control_service_client::ControlServiceClient<
        tonic::transport::Channel,
    >,
    artifact_id: &str,
    cache_dir: &Path,
    token: &str,
    agent_id: &str,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    use crate::control_client::controlv1::{FetchArtifactChunk, FetchArtifactRequest};
    use tonic::Request;
    use tonic::metadata::MetadataValue;

    let safe_id = validate_artifact_id(artifact_id)?;

    let mut request = Request::new(FetchArtifactRequest {
        artifact_id: artifact_id.to_string(),
        shard_index: 0,
        total_shards: 0,
    });
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {}", token))?,
    );
    request
        .metadata_mut()
        .insert("agent-id", MetadataValue::try_from(agent_id)?);

    let response = client.fetch_artifact(request).await?.into_inner();

    let file_path = cache_dir.join(safe_id);
    let mut file = tokio::fs::File::create(&file_path).await?;
    let mut stream = response;
    while let Some(chunk) = stream.message().await? {
        let chunk: FetchArtifactChunk = chunk;
        file.write_all(&chunk.data).await?;
    }
    file.flush().await?;

    Ok(file_path)
}

pub async fn fetch_and_save_artifact_with_shard(
    client: &mut crate::control_client::controlv1::control_service_client::ControlServiceClient<
        tonic::transport::Channel,
    >,
    artifact_id: &str,
    cache_dir: &Path,
    token: &str,
    agent_id: &str,
    shard_index: i32,
    total_shards: i32,
) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    use crate::control_client::controlv1::{FetchArtifactChunk, FetchArtifactRequest};
    use tonic::Request;
    use tonic::metadata::MetadataValue;

    let safe_id = validate_artifact_id(artifact_id)?;

    let mut request = Request::new(FetchArtifactRequest {
        artifact_id: artifact_id.to_string(),
        shard_index,
        total_shards,
    });
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {}", token))?,
    );
    request
        .metadata_mut()
        .insert("agent-id", MetadataValue::try_from(agent_id)?);

    let response = client.fetch_artifact(request).await?.into_inner();

    let file_path = cache_dir.join(format!(
        "{}-shard{}of{}",
        safe_id, shard_index, total_shards
    ));
    let mut file = tokio::fs::File::create(&file_path).await?;
    let mut stream = response;
    while let Some(chunk) = stream.message().await? {
        let chunk: FetchArtifactChunk = chunk;
        file.write_all(&chunk.data).await?;
    }
    file.flush().await?;

    Ok(file_path)
}
