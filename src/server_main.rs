use anyhow::Result;

use zumblezay::app::serve;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    serve().await
}
