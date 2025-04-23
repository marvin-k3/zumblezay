use clap::Parser;
use std::path::PathBuf;

/// Common command-line arguments that can be shared between different apps
#[derive(Parser, Debug, Clone)]
pub struct CommonArgs {
    /// Path to the events database
    #[arg(long, env = "EVENTS_DB")]
    pub events_db: PathBuf,

    /// Path to the zumblezay database
    #[arg(long, env = "ZUMBLEZAY_DB")]
    pub zumblezay_db: PathBuf,

    /// Path to the cache database
    #[arg(long, env = "CACHE_DB")]
    pub cache_db: PathBuf,

    /// Maximum number of concurrent tasks
    #[arg(long, default_value = "3")]
    pub max_concurrent_tasks: usize,

    /// OpenAI API key
    #[arg(long, env = "OPENAI_API_KEY")]
    pub openai_api_key: Option<String>,

    /// OpenAI API base URL
    #[arg(long, env = "OPENAI_API_BASE")]
    pub openai_api_base: Option<String>,

    /// RunPod API key
    #[arg(long, env = "RUNPOD_API_KEY")]
    pub runpod_api_key: Option<String>,

    /// Whisper API URL
    #[arg(
        long,
        default_value = "http://localhost:9000/asr",
        env = "WHISPER_URL"
    )]
    pub whisper_url: String,

    /// Transcription service to use (whisper-local, openai)
    #[arg(long, default_value = "whisper-local")]
    pub transcription_service: String,

    /// Default model for summaries
    #[arg(long, default_value = "anthropic-claude-haiku")]
    pub default_summary_model: String,

    /// Video path original prefix
    #[arg(long, default_value = "/data")]
    pub video_path_original_prefix: String,

    /// Video path replacement prefix
    #[arg(long, default_value = "/path/to/replacement")]
    pub video_path_replacement_prefix: String,

    /// Timezone
    #[arg(long, env = "TIMEZONE")]
    pub timezone: Option<String>,

    /// Signing secret
    #[arg(long, env = "SIGNING_SECRET")]
    pub signing_secret: Option<String>,
}
