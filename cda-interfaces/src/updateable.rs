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
use tokio::sync::Mutex;

use crate::{FunctionalDescriptionConfig, HashMap, runtime_update_api::ReloadError};

/// What triggered a runtime reload.
pub enum ReloadContext<Uds, Gateway, File> {
    /// MDD database files changed.
    Database(Box<DatabaseReloadContext<Uds, Gateway, File>>),
    /// The application configuration changed.
    Configuration(ConfigReloadContext),
}

/// Newly created vehicle state to install after an MDD change.
pub struct DatabaseReloadContext<Uds, Gateway, File> {
    /// Newly created UDS manager, consumed by its updateable.
    pub uds_manager: Mutex<Option<Uds>>,
    /// Newly created diagnostic gateway, consumed by its updateable.
    pub diagnostic_gateway: Mutex<Option<Gateway>>,
    /// File managers keyed by ECU name, consumed during route replacement.
    pub file_managers: Mutex<Option<HashMap<String, File>>>,
    /// ECU names derived from the new UDS manager.
    pub ecu_names: Vec<String>,
    /// Functional-group configuration loaded with the new components.
    pub functional_group_config: FunctionalDescriptionConfig,
    /// MDD paths used to create the new components.
    pub mdd_paths: Vec<PathBuf>,
}

/// Configuration reload context.
pub struct ConfigReloadContext {
    /// Path to the newly active configuration file.
    pub config_path: PathBuf,
}

/// A component that installs its own state in response to reload events.
#[async_trait]
pub trait Updateable<Uds, Gateway, File>: Send + Sync + 'static
where
    Uds: Send + Sync,
    Gateway: Send + Sync,
    File: Send + Sync,
{
    async fn on_reload(
        &self,
        context: &ReloadContext<Uds, Gateway, File>,
    ) -> Result<(), ReloadError>;

    /// Call order. Lower values execute first.
    fn priority(&self) -> u32;

    fn name(&self) -> &'static str;
}

/// Priority-ordered collection of reloadable components.
pub struct UpdateableRegistry<Uds, Gateway, File> {
    components: Vec<Arc<dyn Updateable<Uds, Gateway, File>>>,
}

impl<Uds, Gateway, File> UpdateableRegistry<Uds, Gateway, File>
where
    Uds: Send + Sync + 'static,
    Gateway: Send + Sync + 'static,
    File: Send + Sync + 'static,
{
    #[must_use]
    pub fn new() -> Self {
        Self {
            components: Vec::new(),
        }
    }

    pub fn register(&mut self, component: Arc<dyn Updateable<Uds, Gateway, File>>) {
        self.components.push(component);
        self.components
            .sort_by_key(|component| component.priority());
    }

    /// Notifies every component in ascending priority order.
    ///
    /// # Errors
    ///
    /// Returns the first component error without notifying later components.
    pub async fn notify_all(
        &self,
        context: &ReloadContext<Uds, Gateway, File>,
    ) -> Result<(), ReloadError> {
        for component in &self.components {
            tracing::debug!(
                name = component.name(),
                priority = component.priority(),
                "reload notify"
            );
            component.on_reload(context).await?;
        }
        Ok(())
    }
}

impl<Uds, Gateway, File> Default for UpdateableRegistry<Uds, Gateway, File>
where
    Uds: Send + Sync + 'static,
    Gateway: Send + Sync + 'static,
    File: Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::*;

    struct Recorder {
        priority: u32,
        calls: Arc<Mutex<Vec<u32>>>,
        fail: bool,
    }

    #[async_trait]
    impl Updateable<(), (), ()> for Recorder {
        async fn on_reload(&self, _context: &ReloadContext<(), (), ()>) -> Result<(), ReloadError> {
            self.calls.lock().unwrap().push(self.priority);
            if self.fail {
                return Err(ReloadError::General("failed".to_owned()));
            }
            Ok(())
        }

        fn priority(&self) -> u32 {
            self.priority
        }

        fn name(&self) -> &'static str {
            "recorder"
        }
    }

    fn context() -> ReloadContext<(), (), ()> {
        ReloadContext::Configuration(ConfigReloadContext {
            config_path: PathBuf::new(),
        })
    }

    #[tokio::test]
    async fn notifies_in_priority_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = UpdateableRegistry::new();
        for priority in [30, 10, 20] {
            registry.register(Arc::new(Recorder {
                priority,
                calls: Arc::clone(&calls),
                fail: false,
            }));
        }

        registry.notify_all(&context()).await.unwrap();

        assert_eq!(*calls.lock().unwrap(), [10, 20, 30]);
    }

    #[tokio::test]
    async fn stops_after_first_error() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut registry = UpdateableRegistry::new();
        for (priority, fail) in [(10, false), (20, true), (30, false)] {
            registry.register(Arc::new(Recorder {
                priority,
                calls: Arc::clone(&calls),
                fail,
            }));
        }

        assert!(registry.notify_all(&context()).await.is_err());
        assert_eq!(*calls.lock().unwrap(), [10, 20]);
    }
}
