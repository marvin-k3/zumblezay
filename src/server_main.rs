use anyhow::Result;

use zumblezay::app::serve;

#[tokio::main]
async fn main() -> Result<()> {
    serve().await
}
