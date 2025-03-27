use std::sync::Once;
use tracing;
use tracing_subscriber;

static INIT: Once = Once::new();
static mut LOGGING_INITIALIZED: bool = false;

/// Initialize test logging in a thread-safe way.
/// This function ensures that logging is initialized only once across all tests,
/// even when multiple test files are running in parallel.
pub fn init_test_logging() {
    unsafe {
        if !LOGGING_INITIALIZED {
            INIT.call_once(|| {
                // Initialize the tracing subscriber only once
                let subscriber = tracing_subscriber::fmt()
                    .with_env_filter(
                        tracing_subscriber::EnvFilter::from_default_env(),
                    )
                    .with_test_writer()
                    .finish();

                // Set as global default
                tracing::subscriber::set_global_default(subscriber)
                    .expect("Failed to set tracing subscriber");

                LOGGING_INITIALIZED = true;
            });
        }
    }
}
