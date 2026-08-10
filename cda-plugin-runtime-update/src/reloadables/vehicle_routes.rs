/*
 * SPDX-FileCopyrightText: 2026 Copyright (c) Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use cda_interfaces::{
    ReloadContext, SchemaProvider, UdsEcu, Updateable, datatypes::ComponentsConfig,
    file_manager::FileManager, runtime_update_api::ReloadError,
};
use cda_plugin_communication_management::lifecycle::access::CommunicationAccess;
use cda_plugin_security::SecurityPluginLoader;
use cda_sovd::{RouteHandle, SovdLockStateProvider, dynamic_router::DynamicRouter};
use tokio::sync::RwLock;

pub struct VehicleRouteReloadable<U, SecurityLoader> {
    dynamic_router: DynamicRouter,
    vehicle_route_handle: RouteHandle,
    flash_files_path: String,
    components_config: ComponentsConfig,
    lock_provider: Arc<SovdLockStateProvider>,
    communication_access: Arc<dyn CommunicationAccess>,
    uds_manager: Arc<RwLock<U>>,
    _phantom: PhantomData<SecurityLoader>,
}

impl<U, SecurityLoader> VehicleRouteReloadable<U, SecurityLoader> {
    #[must_use]
    pub fn new(
        dynamic_router: DynamicRouter,
        vehicle_route_handle: RouteHandle,
        flash_files_path: String,
        components_config: ComponentsConfig,
        lock_provider: Arc<SovdLockStateProvider>,
        communication_access: Arc<dyn CommunicationAccess>,
        uds_manager: Arc<RwLock<U>>,
    ) -> Self {
        Self {
            dynamic_router,
            vehicle_route_handle,
            flash_files_path,
            components_config,
            lock_provider,
            communication_access,
            uds_manager,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<U, G, F, SecurityLoader> Updateable<U, G, F> for VehicleRouteReloadable<U, SecurityLoader>
where
    U: UdsEcu + SchemaProvider + Clone + Send + Sync + 'static,
    G: Send + Sync + 'static,
    F: FileManager + Send + Sync + 'static,
    SecurityLoader: SecurityPluginLoader,
{
    async fn on_reload(&self, context: &ReloadContext<U, G, F>) -> Result<(), ReloadError> {
        let ReloadContext::Database(context) = context else {
            return Ok(());
        };
        let file_managers =
            context.file_managers.lock().await.take().ok_or_else(|| {
                ReloadError::General("file managers already installed".to_owned())
            })?;
        let current_locks = self.lock_provider.current_locks().await;
        let uds_manager = self.uds_manager.read().await.clone();
        let routes = cda_sovd::build_vehicle_routes::<_, _, SecurityLoader>(
            cda_sovd::VehicleConfig {
                flash_files_path: self.flash_files_path.clone(),
                functional_group_config: context.functional_group_config.clone(),
                components_config: self.components_config.clone(),
            },
            cda_sovd::VehicleResources {
                ecu_uds: uds_manager,
                file_managers,
                locks: current_locks,
                communication_access: Arc::clone(&self.communication_access),
            },
        )
        .await;
        self.dynamic_router
            .replace_routes(&self.vehicle_route_handle, routes)
            .await
            .map_err(|error| ReloadError::ReplacementFailure(error.to_string()))
    }

    fn priority(&self) -> u32 {
        40
    }

    fn name(&self) -> &'static str {
        "vehicle routes"
    }
}
