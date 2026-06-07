//! `t9n storage` — operator/verification commands for the storage backend.

use tutankhamun_server::storage::{self, StorageRegistry};

use crate::{StorageArgs, StorageCommand};

pub(crate) fn run(args: &StorageArgs) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    match &args.command {
        StorageCommand::Check { url, prefix } => runtime.block_on(check(url, prefix.as_deref())),
    }
}

async fn check(url: &str, prefix: Option<&str>) -> anyhow::Result<()> {
    let registry = StorageRegistry::from_url(url)?;
    storage::check(&*registry.store(), prefix).await.map(|_| ())
}
