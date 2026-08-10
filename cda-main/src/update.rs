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
use cda_comm_can::CanDiagGateway;
use cda_comm_doip::DoipDiagGateway;
use cda_core::EcuManager;
use cda_interfaces::{
    HashMap, UpdateableRegistry,
    health::HealthProvider,
    runtime_update_api::{
        ReloadError, RuntimeFilesUpdatePlugin, VehicleComponentFactory, VehicleComponents,
    },
};
use cda_plugin_communication_management::lifecycle::access::CommunicationAccess;
use cda_plugin_runtime_update::{
    DefaultRuntimeUpdatePlugin, DefaultUpdateSecurityHandler,
    default_runtime_reloader_plugin::DefaultRuntimeReloaderPlugin,
    reloadables::{
        ConfigReloadable, GatewayReloadable, LockProviderReloadable, UdsManagerReloadable,
        VehicleRouteReloadable,
    },
};
use cda_plugin_security::{SecurityPlugin, SecurityPluginLoader};
use cda_sovd::SovdLockStateProvider;
use cda_storage::LocalStorage;
use cda_transport_router::DiagnosticTransportRouter;

use crate::{
    AppError, UdsManagerType,
    config::configfile::{Configuration, ConfigurationValidator},
    setup::CdaRuntime,
};

/// Trait for async plugin builders that produce a [`RuntimeFilesUpdatePlugin`].
///
/// Implement this trait (or use a closure via [`update_plugin_fn`]) to provide a
/// custom update plugin to [`crate::Setup::with_update_plugin`].
pub trait UpdatePluginBuilder<SP: SecurityPlugin>: Send {
    /// The concrete plugin type this builder produces.
    type Plugin: RuntimeFilesUpdatePlugin;

    /// Build the plugin from the given runtime context.
    fn build(
        self,
        infra: CdaRuntime<SP>,
    ) -> impl Future<Output = Result<Self::Plugin, AppError>> + Send;
}

/// Wrapper that adapts an async closure into an [`UpdatePluginBuilder`].
///
/// Created via [`update_plugin_fn`].
pub struct UpdatePluginFn<F>(F);

/// Wrap an async closure as an [`UpdatePluginBuilder`].
///
/// # Example
/// ```rust,ignore
/// use opensovd_cda_lib::{Setup, update::update_plugin_fn};
///
/// let setup = Setup::new().with_update_plugin(update_plugin_fn(|infra| async move {
///     Ok(MyPlugin::new(infra))
/// }));
/// ```
pub fn update_plugin_fn<SP, F, Fut, P>(f: F) -> UpdatePluginFn<F>
where
    SP: SecurityPlugin,
    F: FnOnce(CdaRuntime<SP>) -> Fut + Send,
    Fut: Future<Output = Result<P, AppError>> + Send,
    P: RuntimeFilesUpdatePlugin,
{
    UpdatePluginFn(f)
}

impl<SP, F, Fut, P> UpdatePluginBuilder<SP> for UpdatePluginFn<F>
where
    SP: SecurityPlugin,
    F: FnOnce(CdaRuntime<SP>) -> Fut + Send,
    Fut: Future<Output = Result<P, AppError>> + Send,
    P: RuntimeFilesUpdatePlugin,
{
    type Plugin = P;

    async fn build(self, infra: CdaRuntime<SP>) -> Result<P, AppError> {
        self.0(infra).await
    }
}

/// Concrete [`VehicleComponentFactory`] that delegates to
/// [`crate::create_vehicle_components`].
///
/// Used by [`DefaultRuntimeReloaderPlugin`]
/// and called every time the diagnostic databases are reloaded.
pub struct CdaMainVehicleFactory<SP>
where
    SP: SecurityPlugin,
{
    health_providers: Option<HashMap<String, Arc<dyn HealthProvider>>>,
    communication_access: Arc<dyn CommunicationAccess>,
    _phantom: std::marker::PhantomData<SP>,
}

impl<SP> CdaMainVehicleFactory<SP>
where
    SP: SecurityPlugin,
{
    #[must_use]
    pub fn new(
        health_providers: Option<HashMap<String, Arc<dyn HealthProvider>>>,
        communication_access: Arc<dyn CommunicationAccess>,
    ) -> Self {
        Self {
            health_providers,
            communication_access,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<SP>
    VehicleComponentFactory<
        Configuration,
        UdsManagerType<SP>,
        DiagnosticTransportRouter<DoipDiagGateway<EcuManager<SP>>, CanDiagGateway>,
    > for CdaMainVehicleFactory<SP>
where
    SP: SecurityPlugin,
{
    type FileManager = cda_database::FileManager;

    async fn create(
        &self,
        config: &Configuration,
        mdd_paths: &[PathBuf],
    ) -> Result<
        VehicleComponents<
            UdsManagerType<SP>,
            DiagnosticTransportRouter<DoipDiagGateway<EcuManager<SP>>, CanDiagGateway>,
            Self::FileManager,
        >,
        ReloadError,
    > {
        let crate_components = crate::create_vehicle_components::<SP>(
            config,
            mdd_paths,
            self.health_providers.as_ref(),
            Arc::clone(&self.communication_access),
        )
        .await
        .map_err(|e| {
            ReloadError::ReplacementFailure(format!("Failed to create new vehicle components: {e}"))
        })?;

        Ok(VehicleComponents {
            uds_manager: crate_components.uds_manager,
            diagnostic_gateway: crate_components.diagnostic_gateway,
            file_managers: crate_components.file_managers,
            functional_group_config: config.functional_description.clone(),
        })
    }
}

/// Registers the runtime update routes on the dynamic router using the provided plugin.
///
/// Wraps the plugin in
/// [`ExclusiveRuntimePlugin`](cda_interfaces::runtime_update_api::ExclusiveRuntimePlugin) for
/// read/write mutual exclusion and
/// mounts the HTTP endpoints by delegating to [`cda_sovd::add_runtime_update_routes`].
/// The caller is responsible for constructing the plugin before calling this function.
pub async fn add_runtime_update_routes<S, P>(
    dynamic_router: &cda_sovd::dynamic_router::DynamicRouter,
    plugin: P,
    lock_provider: Arc<SovdLockStateProvider>,
    upload_body_limit_bytes: usize,
    retry_after_seconds: u64,
) where
    S: SecurityPluginLoader,
    P: RuntimeFilesUpdatePlugin,
{
    let service = Arc::new(plugin.with_exclusive_access());
    cda_sovd::add_runtime_update_routes::<S, _, SovdLockStateProvider>(
        dynamic_router,
        service,
        lock_provider,
        upload_body_limit_bytes,
        retry_after_seconds,
    )
    .await;
}

/// Creates the default runtime update plugin using the standard CDA components.
///
/// This helper function eliminates code duplication between `run()` and `run_with_config()`.
/// It builds a fully configured `DefaultRuntimeUpdatePlugin` with all the standard
/// CDA infrastructure components.
///
/// # Arguments
/// - `infra`: The runtime infrastructure containing all CDA components
///
/// # Errors
/// Returns [`AppError::RuntimeUpdateError`] if plugin initialization fails.
pub async fn create_default_update_plugin<SP, SL>(
    infra: CdaRuntime<SP>,
) -> Result<impl RuntimeFilesUpdatePlugin, AppError>
where
    SP: SecurityPlugin,
    SL: SecurityPluginLoader,
{
    let health_for_factory = infra.health.clone();
    let factory = Arc::new(CdaMainVehicleFactory::<SP>::new(
        health_for_factory,
        Arc::clone(&infra.communication_access),
    ));

    let mut registry = UpdateableRegistry::new();
    registry.register(Arc::new(GatewayReloadable::new(Arc::clone(&infra.gateway))));
    registry.register(Arc::new(UdsManagerReloadable::new(Arc::clone(
        &infra.uds_manager,
    ))));
    registry.register(Arc::new(LockProviderReloadable::new(Arc::clone(
        &infra.lock_provider,
    ))));
    registry.register(Arc::new(VehicleRouteReloadable::<_, SL>::new(
        infra.dynamic_router.clone(),
        infra.vehicle_route_handle.clone(),
        infra.flash_files_path.clone(),
        infra.components_config.clone(),
        Arc::clone(&infra.lock_provider),
        Arc::clone(&infra.communication_access),
        Arc::clone(&infra.uds_manager),
    )));
    registry.register(Arc::new(ConfigReloadable::new(Arc::clone(&infra.config))));

    let reloader_plugin = Arc::new(DefaultRuntimeReloaderPlugin::new(
        Arc::clone(&infra.config),
        factory,
        registry,
    ));

    let storage = Arc::new(LocalStorage::new(&infra.storage_dir).map_err(|e| {
        AppError::InitializationFailed(format!("Failed to init storage, error={e:?}"))
    })?);
    Ok(DefaultRuntimeUpdatePlugin::new(
        storage,
        reloader_plugin,
        Arc::new(DefaultUpdateSecurityHandler::new(Arc::clone(
            &infra.lock_provider,
        ))),
        Arc::clone(&infra.lock_provider),
        infra.mdd_decompress,
        infra.communication_disable,
        infra.http_protections,
        infra.update_retry_after_seconds,
        infra.post_update_mode,
        ConfigurationValidator::new(),
    ))
}
