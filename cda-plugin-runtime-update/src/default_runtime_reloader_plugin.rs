/*
 * SPDX-FileCopyrightText: 2026 Copyright (c) Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use cda_interfaces::{
    ConfigReloadContext, DatabaseReloadContext, ReloadContext, Shutdown, UdsQuery,
    UpdateableRegistry,
    runtime_update_api::{ReloadError, RuntimeReloaderPlugin, VehicleComponentFactory},
};
use tokio::sync::{Mutex, RwLock};

/// Reload handler that creates new components and delegates their installation to a registry.
pub struct DefaultRuntimeReloaderPlugin<Config, Uds, Gateway, VehicleFactory>
where
    Config: Send + Sync + 'static,
    Uds: UdsQuery + Shutdown,
    Gateway: Shutdown,
    VehicleFactory: VehicleComponentFactory<Config, Uds, Gateway>,
{
    config: Arc<RwLock<Config>>,
    factory: Arc<VehicleFactory>,
    registry: UpdateableRegistry<Uds, Gateway, VehicleFactory::FileManager>,
}

impl<Config, Uds, Gateway, VehicleFactory>
    DefaultRuntimeReloaderPlugin<Config, Uds, Gateway, VehicleFactory>
where
    Config: Send + Sync + 'static,
    Uds: UdsQuery + Shutdown,
    Gateway: Shutdown,
    VehicleFactory: VehicleComponentFactory<Config, Uds, Gateway>,
{
    #[must_use]
    pub fn new(
        config: Arc<RwLock<Config>>,
        factory: Arc<VehicleFactory>,
        registry: UpdateableRegistry<Uds, Gateway, VehicleFactory::FileManager>,
    ) -> Self {
        Self {
            config,
            factory,
            registry,
        }
    }
}

#[async_trait]
impl<Config, Uds, Gateway, VehicleFactory> RuntimeReloaderPlugin
    for DefaultRuntimeReloaderPlugin<Config, Uds, Gateway, VehicleFactory>
where
    Config: Clone + Send + Sync + 'static,
    Uds: UdsQuery + Shutdown + Send + Sync + 'static,
    Gateway: Shutdown + Send + Sync + 'static,
    VehicleFactory: VehicleComponentFactory<Config, Uds, Gateway>,
{
    async fn reload_databases(&self, mdd_paths: Vec<PathBuf>) -> Result<(), ReloadError> {
        let config = self.config.read().await.clone();
        let components = self.factory.create(&config, &mdd_paths).await?;
        let ecu_names = components.uds_manager.get_physical_ecus().await;
        let context = ReloadContext::Database(Box::new(DatabaseReloadContext {
            uds_manager: Mutex::new(Some(components.uds_manager)),
            diagnostic_gateway: Mutex::new(Some(components.diagnostic_gateway)),
            file_managers: Mutex::new(Some(components.file_managers)),
            ecu_names,
            functional_group_config: components.functional_group_config,
            mdd_paths,
        }));
        self.registry.notify_all(&context).await
    }

    async fn reload_configuration(&self, config_path: PathBuf) -> Result<(), ReloadError> {
        self.registry
            .notify_all(&ReloadContext::Configuration(ConfigReloadContext {
                config_path,
            }))
            .await
    }
}
