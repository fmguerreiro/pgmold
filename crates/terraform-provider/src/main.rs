use terraform_provider_pgmold::PgmoldProvider;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tf_provider::serve("pgmold", PgmoldProvider::default()).await?;
    Ok(())
}
