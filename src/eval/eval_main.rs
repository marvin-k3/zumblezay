use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    zumblezay::eval::main().await
}
