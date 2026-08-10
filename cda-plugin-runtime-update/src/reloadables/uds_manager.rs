/*
 * SPDX-FileCopyrightText: 2026 Copyright (c) Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use async_trait::async_trait;
use cda_interfaces::{ReloadContext, Shutdown, Updateable, runtime_update_api::ReloadError};
use tokio::sync::RwLock;

pub struct UdsManagerReloadable<U> {
    uds_manager: Arc<RwLock<U>>,
}

impl<U> UdsManagerReloadable<U> {
    #[must_use]
    pub fn new(uds_manager: Arc<RwLock<U>>) -> Self {
        Self { uds_manager }
    }
}

#[async_trait]
impl<U, G, F> Updateable<U, G, F> for UdsManagerReloadable<U>
where
    U: Shutdown + Send + Sync + 'static,
    G: Send + Sync + 'static,
    F: Send + Sync + 'static,
{
    async fn on_reload(&self, context: &ReloadContext<U, G, F>) -> Result<(), ReloadError> {
        let ReloadContext::Database(context) = context else {
            return Ok(());
        };
        let replacement = context
            .uds_manager
            .lock()
            .await
            .take()
            .ok_or_else(|| ReloadError::General("UDS manager already installed".to_owned()))?;
        let old = std::mem::replace(&mut *self.uds_manager.write().await, replacement);
        old.shutdown().await;
        Ok(())
    }

    fn priority(&self) -> u32 {
        20
    }

    fn name(&self) -> &'static str {
        "UDS manager"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use async_trait::async_trait;
    use cda_interfaces::{
        DatabaseReloadContext, FunctionalDescriptionConfig, HashMap, Shutdown, Updateable,
    };

    use super::*;

    struct Manager {
        id: u8,
        stopped: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Shutdown for Manager {
        async fn shutdown(&self) {
            self.stopped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn replaces_manager_and_shuts_down_old_instance() {
        let stopped = Arc::new(AtomicBool::new(false));
        let manager = Arc::new(RwLock::new(Manager {
            id: 1,
            stopped: Arc::clone(&stopped),
        }));
        let reloadable = UdsManagerReloadable::new(Arc::clone(&manager));
        let context = ReloadContext::Database(Box::new(DatabaseReloadContext {
            uds_manager: tokio::sync::Mutex::new(Some(Manager {
                id: 2,
                stopped: Arc::new(AtomicBool::new(false)),
            })),
            diagnostic_gateway: tokio::sync::Mutex::new(Some(())),
            file_managers: tokio::sync::Mutex::new(Some(HashMap::<String, u8>::default())),
            ecu_names: Vec::new(),
            functional_group_config: FunctionalDescriptionConfig::default(),
            mdd_paths: Vec::new(),
        }));

        Updateable::<Manager, (), u8>::on_reload(&reloadable, &context)
            .await
            .unwrap();

        assert_eq!(manager.read().await.id, 2);
        assert!(stopped.load(Ordering::SeqCst));
    }
}
