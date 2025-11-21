mod apply;
mod cli;
mod diff;
mod drift;
mod lint;
mod model;
mod parser;
mod pg;
mod util;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::run().await
}
