mod cli;

use pgmold::model;
use pgmold::parser;
use pgmold::util;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::run().await
}
