use std::time::Duration;

use anyhow::Result;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    sleep(Duration::from_secs(1)).await;
    Ok(())
}
