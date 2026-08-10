/*
 * SPDX-FileCopyrightText: 2026 Copyright (c) Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use async_trait::async_trait;
use cda_interfaces::{ReloadContext, Updateable, runtime_update_api::ReloadError};
use tokio::sync::RwLock;

pub struct ConfigReloadable<C> {
    config: Arc<RwLock<C>>,
}

impl<C> ConfigReloadable<C> {
    #[must_use]
    pub fn new(config: Arc<RwLock<C>>) -> Self {
        Self { config }
    }
}

#[async_trait]
impl<C, U, G, F> Updateable<U, G, F> for ConfigReloadable<C>
where
    C: serde::de::DeserializeOwned + Send + Sync + 'static,
    U: Send + Sync + 'static,
    G: Send + Sync + 'static,
    F: Send + Sync + 'static,
{
    async fn on_reload(&self, context: &ReloadContext<U, G, F>) -> Result<(), ReloadError> {
        let ReloadContext::Configuration(context) = context else {
            return Ok(());
        };
        let content = tokio::fs::read_to_string(&context.config_path)
            .await
            .map_err(|error| ReloadError::General(format!("Failed to read config: {error}")))?;
        let parsed = toml::from_str(&content)
            .map_err(|error| ReloadError::General(format!("Failed to parse config: {error}")))?;
        *self.config.write().await = parsed;
        Ok(())
    }

    fn priority(&self) -> u32 {
        10
    }

    fn name(&self) -> &'static str {
        "configuration"
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Deserialize)]
    struct Config {
        value: u8,
    }

    #[tokio::test]
    async fn reads_and_replaces_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.toml");
        std::fs::write(&path, "value = 2\n").unwrap();
        let config = Arc::new(RwLock::new(Config { value: 1 }));
        let reloadable = ConfigReloadable::new(Arc::clone(&config));
        let context =
            ReloadContext::Configuration(cda_interfaces::ConfigReloadContext { config_path: path });

        Updateable::<(), (), ()>::on_reload(&reloadable, &context)
            .await
            .unwrap();

        assert_eq!(config.read().await.value, 2);
    }
}
