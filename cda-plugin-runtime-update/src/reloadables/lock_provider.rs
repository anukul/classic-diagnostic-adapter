/*
 * SPDX-FileCopyrightText: 2026 Copyright (c) Contributors to the Eclipse Foundation
 *
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::Arc;

use async_trait::async_trait;
use cda_interfaces::{ReloadContext, Updateable, runtime_update_api::ReloadError};
use cda_sovd::SovdLockStateProvider;

pub struct LockProviderReloadable {
    lock_provider: Arc<SovdLockStateProvider>,
}

impl LockProviderReloadable {
    #[must_use]
    pub fn new(lock_provider: Arc<SovdLockStateProvider>) -> Self {
        Self { lock_provider }
    }
}

#[async_trait]
impl<U, G, F> Updateable<U, G, F> for LockProviderReloadable
where
    U: Send + Sync + 'static,
    G: Send + Sync + 'static,
    F: Send + Sync + 'static,
{
    async fn on_reload(&self, context: &ReloadContext<U, G, F>) -> Result<(), ReloadError> {
        let ReloadContext::Database(context) = context else {
            return Ok(());
        };
        self.lock_provider
            .update_entries(context.ecu_names.clone())
            .await
            .map_err(|error| ReloadError::General(error.to_string()))
    }

    fn priority(&self) -> u32 {
        30
    }

    fn name(&self) -> &'static str {
        "lock provider"
    }
}
