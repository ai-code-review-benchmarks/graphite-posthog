use std::path::Path;
use std::time::Instant;

use super::{CheckpointConfig, CheckpointUploader};
use crate::rocksdb::deduplication_store::DeduplicationStore;

use anyhow::Result;
use metrics;
use tracing::{error, info, warn};

const CHECKPOINT_UPLOAD_DURATION_HISTOGRAM: &str = "checkpoint_upload_duration_seconds";

#[derive(Debug)]
pub struct CheckpointExporter {
    config: CheckpointConfig,
    uploader: Box<dyn CheckpointUploader>,
}

impl CheckpointExporter {
    pub fn new(config: CheckpointConfig, uploader: Box<dyn CheckpointUploader>) -> Self {
        Self { config, uploader }
    }

    // returns the remote key prefix for this checkpoint or an error
    pub async fn export_checkpoint(
        &self,
        local_checkpoint_path: &Path,
        checkpoint_name: &str,
        store: &DeduplicationStore,
        is_full_upload: bool,
    ) -> Result<String> {
        let start_time = Instant::now();

        let remote_key_prefix = if is_full_upload {
            format!("{}/full/{}", self.config.s3_key_prefix, checkpoint_name)
        } else {
            format!(
                "{}/incremental/{}",
                self.config.s3_key_prefix, checkpoint_name
            )
        };

        // Upload to remote storage in background
        if self.uploader.is_available().await {
            let upload_start = Instant::now();

            match self
                .uploader
                .upload_checkpoint_dir(&local_checkpoint_path, &remote_key_prefix)
                .await
            {
                Ok(uploaded_files) => {
                    let upload_duration = upload_start.elapsed();
                    metrics::histogram!(CHECKPOINT_UPLOAD_DURATION_HISTOGRAM)
                        .record(upload_duration.as_secs_f64());

                    // TODO(eli): stat this
                    info!(
                        local_checkpoint_path,
                        remote_key_prefix,
                        uploaded_files_count = uploaded_files.len(),
                        elapsed_time = upload_duration,
                        "Export successful: checkpoint ({} type) uploaded",
                        if is_full_upload {
                            "full"
                        } else {
                            "incremental"
                        },
                    );
                }

                Err(e) => {
                    // TODO(eli): stat this
                    error!(
                        local_checkpoint_path,
                        remote_key_prefix, "Export failed: uploading checkpoint: {}", e
                    );
                    return Err(e);
                }
            };

            return Ok(remote_key_prefix);
        } else {
            // TODO(eli): stat this
            warn!(
                local_checkpoint_path,
                remote_key_prefix, "Export failed: uploader not available"
            );
            return Err(anyhow::anyhow!("Uploader not available"));
        }
    }
}
