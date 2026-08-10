/*
 * SPDX-FileCopyrightText: 2026 Copyright (c) Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use async_trait::async_trait;
use cda_interfaces::{ReloadContext, Shutdown, Updateable, runtime_update_api::ReloadError};
use tokio::sync::Mutex;

pub struct GatewayReloadable<G> {
    gateway: Arc<Mutex<G>>,
}

impl<G> GatewayReloadable<G> {
    #[must_use]
    pub fn new(gateway: Arc<Mutex<G>>) -> Self {
        Self { gateway }
    }
}

#[async_trait]
impl<U, G, F> Updateable<U, G, F> for GatewayReloadable<G>
where
    U: Send + Sync + 'static,
    G: Shutdown + Send + Sync + 'static,
    F: Send + Sync + 'static,
{
    async fn on_reload(&self, context: &ReloadContext<U, G, F>) -> Result<(), ReloadError> {
        let ReloadContext::Database(context) = context else {
            return Ok(());
        };
        let replacement = context
            .diagnostic_gateway
            .lock()
            .await
            .take()
            .ok_or_else(|| {
                ReloadError::General("diagnostic gateway already installed".to_owned())
            })?;
        let old = std::mem::replace(&mut *self.gateway.lock().await, replacement);
        old.shutdown().await;
        Ok(())
    }

    fn priority(&self) -> u32 {
        10
    }

    fn name(&self) -> &'static str {
        "diagnostic gateway"
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use async_trait::async_trait;
    use cda_interfaces::{
        ConfigReloadContext, DatabaseReloadContext, FunctionalDescriptionConfig, HashMap, Shutdown,
        Updateable,
    };

    use super::*;

    struct Gateway {
        id: u8,
        stopped: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Shutdown for Gateway {
        async fn shutdown(&self) {
            self.stopped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn replaces_gateway_and_shuts_down_old_instance() {
        let stopped = Arc::new(AtomicBool::new(false));
        let gateway = Arc::new(Mutex::new(Gateway {
            id: 1,
            stopped: Arc::clone(&stopped),
        }));
        let reloadable = GatewayReloadable::new(Arc::clone(&gateway));
        let context = ReloadContext::Database(Box::new(DatabaseReloadContext {
            uds_manager: Mutex::new(Some(())),
            diagnostic_gateway: Mutex::new(Some(Gateway {
                id: 2,
                stopped: Arc::new(AtomicBool::new(false)),
            })),
            file_managers: Mutex::new(Some(HashMap::<String, u8>::default())),
            ecu_names: Vec::new(),
            functional_group_config: FunctionalDescriptionConfig::default(),
            mdd_paths: Vec::new(),
        }));

        Updateable::<(), Gateway, u8>::on_reload(&reloadable, &context)
            .await
            .unwrap();

        assert_eq!(gateway.lock().await.id, 2);
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn ignores_configuration_reload() {
        let gateway = Arc::new(Mutex::new(Gateway {
            id: 1,
            stopped: Arc::new(AtomicBool::new(false)),
        }));
        let reloadable = GatewayReloadable::new(Arc::clone(&gateway));
        let context = ReloadContext::Configuration(ConfigReloadContext {
            config_path: PathBuf::default(),
        });

        Updateable::<(), Gateway, ()>::on_reload(&reloadable, &context)
            .await
            .unwrap();

        assert_eq!(gateway.lock().await.id, 1);
    }
}
