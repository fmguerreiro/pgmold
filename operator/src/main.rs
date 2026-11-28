use std::{convert::Infallible, sync::Arc};

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{
    runtime::{controller::Action, watcher, Controller},
    Api, Client,
};

async fn reconcile(config_map: Arc<ConfigMap>, _: Arc<()>) -> Result<Action, Infallible> {
    tracing::info!(name = %config_map.metadata.name.as_deref().unwrap_or_default(), "reconciled ConfigMap");
    Ok(Action::await_change())
}

fn error_policy(_: Arc<ConfigMap>, error: &Infallible, _: Arc<()>) -> Action {
    match *error {}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_ansi(false).init();
    let client = Client::try_default().await?;
    let config_maps: Api<ConfigMap> = Api::default_namespaced(client);
    Controller::new(config_maps, watcher::Config::default())
        .run(reconcile, error_policy, Arc::new(()))
        .for_each(|result| async move {
            if let Err(error) = result {
                tracing::error!(%error, "controller error");
            }
        })
        .await;
    Ok(())
}
