use std::path::Path;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha1::Sha1;

type HmacSha1 = Hmac<Sha1>;

#[derive(Clone)]
pub struct OSSConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key_id: String,
    pub access_key_secret: String,
}

impl OSSConfig {
    pub fn from_env() -> Self {
        Self {
            endpoint: std::env::var("ALIYUN_OSS_ENDPOINT")
                .unwrap_or_else(|_| "oss-cn-hangzhou-internal.aliyuncs.com".to_string()),
            bucket: std::env::var("ALIYUN_OSS_BUCKET").unwrap_or_default(),
            access_key_id: std::env::var("ALIYUN_OSS_ACCESS_KEY").unwrap_or_default(),
            access_key_secret: std::env::var("ALIYUN_OSS_ACCESS_SECRET").unwrap_or_default(),
        }
    }

    fn sign_url(&self, method: &str, key: &str, expires_in_secs: u64) -> String {
        let expires = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + expires_in_secs;

        let resource = format!("/{}{}", self.bucket, key);
        let string_to_sign = format!("{}\n\n\n{}\n{}", method, expires, resource);

        let mut mac =
            HmacSha1::new_from_slice(self.access_key_secret.as_bytes()).expect("HMAC key is valid");
        mac.update(string_to_sign.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());

        format!(
            "https://{}.{}/{}?OSSAccessKeyId={}&Signature={}&Expires={}",
            self.bucket,
            self.endpoint,
            key,
            percent_encode(&self.access_key_id),
            percent_encode(&signature),
            expires
        )
    }
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.as_bytes() {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            _ => {
                out.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    out
}

pub async fn download_file(
    config: &OSSConfig,
    oss_key: &str,
    local_path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = config.sign_url("GET", oss_key, 3600);
    tracing::info!(oss_key, %url, "downloading from OSS");

    let response = reqwest::get(&url).await?;
    if !response.status().is_success() {
        return Err(format!(
            "OSS download failed: {} for key {}",
            response.status(),
            oss_key
        )
        .into());
    }
    let bytes = response.bytes().await?;

    if let Some(parent) = local_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(local_path, &bytes)?;
    tracing::info!(
        oss_key,
        path = %local_path.display(),
        size = bytes.len(),
        "downloaded from OSS"
    );
    Ok(())
}

pub async fn upload_file(
    config: &OSSConfig,
    local_path: &Path,
    oss_key: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = config.sign_url("PUT", oss_key, 3600);
    tracing::info!(oss_key, %url, "uploading to OSS");

    let bytes = std::fs::read(local_path)?;
    let client = reqwest::Client::new();
    let response = client
        .put(&url)
        .header("Content-Type", "application/octet-stream")
        .body(bytes)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!(
            "OSS upload failed: {} for key {}",
            response.status(),
            oss_key
        )
        .into());
    }
    tracing::info!(oss_key, path = %local_path.display(), "uploaded to OSS");
    Ok(())
}
