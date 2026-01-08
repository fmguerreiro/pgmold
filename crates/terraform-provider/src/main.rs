use terraform_provider_pgmold::PgmoldProvider;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tf_provider::serve("pgmold", PgmoldProvider::default()).await?;
    Ok(())
}
