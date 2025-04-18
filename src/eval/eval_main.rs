use anyhow::Result;
use zumblezay::eval::run_app;

#[tokio::main]
async fn main() -> Result<()> {
    run_app().await
}
