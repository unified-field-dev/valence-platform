//! Create and drive [`crate::ValenceIterRun`] rows without hand-building model constructors.
//!
//! Hosts call [`IterService::start`] after [`super::dispatch::register_iter_dispatch`] (Chronon
//! `run_now`).

use chrono::Utc;
use uuid::Uuid;

use crate::{ValenceIterRun, ValenceIterRunStatus};
use valence::{Actor, Error, Model, Result, Valence};

use super::dispatch::run_iter_orchestrator_now_for_registered_backend;
use super::orchestrator::run_valence_iter_orchestrator_for_tests;

fn default_initiated_by(v: &Valence) -> String {
    match v.actor() {
        Actor::User { user_id } => user_id.clone(),
        Actor::ServiceUser { service_name } => service_name.clone(),
        Actor::System { operation } => operation.clone(),
        Actor::Anonymous => "anonymous".to_string(),
    }
}

/// Optional fields for [`IterService::create_run`] / [`IterService::start`].
#[derive(Debug, Clone, Default)]
pub struct IterRunOptions {
    initiated_by: Option<String>,
    target_row_id: Option<String>,
    run_id: Option<String>,
}

impl IterRunOptions {
    /// Audit string stored on `ValenceIterRun.initiated_by`.
    #[must_use]
    pub fn initiated_by(mut self, value: impl Into<String>) -> Self {
        self.initiated_by = Some(value.into());
        self
    }

    /// When set, the orchestrator scans this row only.
    #[must_use]
    pub fn target_row_id(mut self, value: impl Into<String>) -> Self {
        self.target_row_id = Some(value.into());
        self
    }

    /// Caller-chosen run id; otherwise a new UUID.
    #[must_use]
    pub fn run_id(mut self, value: impl Into<String>) -> Self {
        self.run_id = Some(value.into());
        self
    }
}

/// Helpers for starting Valence Iter runs.
pub struct IterService;

impl IterService {
    /// Insert a `pending` iter run and return its id. Does not start the orchestrator.
    ///
    /// Prefer [`Self::start`] when Chronon is registered. Use this when you need the row before
    /// `run_now`.
    ///
    /// # Errors
    ///
    /// Empty `iter_name` / `target_table`, or Valence persist errors.
    pub async fn create_run(
        v: &Valence,
        iter_name: &str,
        target_table: &str,
        opts: IterRunOptions,
    ) -> Result<String> {
        if iter_name.is_empty() {
            return Err(Error::Validation("iter_name must be non-empty".into()));
        }
        if target_table.is_empty() {
            return Err(Error::Validation("target_table must be non-empty".into()));
        }

        let run_id = opts.run_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let initiated_by = opts.initiated_by.unwrap_or_else(|| default_initiated_by(v));

        let run = ValenceIterRun::new(
            iter_name.to_string(),
            target_table.to_string(),
            ValenceIterRunStatus::Pending,
            0,
            0,
            0,
            0,
            0,
            None,
            None,
            None,
            Utc::now(),
            initiated_by,
            opts.target_row_id,
        )?;
        ValenceIterRun::upsert(&run_id, run, v).await?;
        Ok(run_id)
    }

    /// Create a pending run and Chronon `run_now` `valence-iter-orchestrator`.
    ///
    /// Requires [`super::dispatch::register_iter_dispatch`] at host boot. Returns the `run_id`
    /// after Chronon accepts the job; does not wait for Boson row work to finish.
    ///
    /// # Errors
    ///
    /// Create validation / persist errors, or Chronon not registered / `run_now` failure.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    /// use valence_platform::iter::dispatch::register_iter_dispatch;
    /// use valence_platform::iter::run_service::{IterRunOptions, IterService};
    ///
    /// register_iter_dispatch(Arc::clone(&chronon_backend));
    ///
    /// let run_id = IterService::start(
    ///     &valence,
    ///     "MarkProcessedIter",
    ///     "demo_note",
    ///     IterRunOptions::default(),
    /// )
    /// .await?;
    /// ```
    pub async fn start(
        v: &Valence,
        iter_name: &str,
        target_table: &str,
        opts: IterRunOptions,
    ) -> Result<String> {
        let run_id = Self::create_run(v, iter_name, target_table, opts).await?;
        run_iter_orchestrator_now_for_registered_backend(&run_id).await?;
        Ok(run_id)
    }

    /// Drive an existing pending run with the inline row worker (test harness; no Chronon/Boson).
    ///
    /// # Errors
    ///
    /// Propagates orchestrator failures (including missing run).
    #[doc(hidden)]
    pub async fn start_for_tests(v: &Valence, run_id: &str) -> Result<()> {
        run_valence_iter_orchestrator_for_tests(v.clone(), run_id.to_string())
            .await
            .map_err(|e| Error::Internal(e.to_string()))
    }

    /// Create a pending run, drive it inline, and return the terminal [`ValenceIterRun`].
    ///
    /// For tests and examples without Chronon. Hosts should use [`Self::start`].
    ///
    /// # Errors
    ///
    /// Create validation / persist errors, orchestrator failures, or missing run after drive.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use valence_platform::iter::run_service::{IterRunOptions, IterService};
    ///
    /// let run = IterService::run_for_tests(
    ///     &valence,
    ///     "MarkProcessedIter",
    ///     "demo_note",
    ///     IterRunOptions::default(),
    /// )
    /// .await?;
    /// ```
    #[doc(hidden)]
    pub async fn run_for_tests(
        v: &Valence,
        iter_name: &str,
        target_table: &str,
        opts: IterRunOptions,
    ) -> Result<ValenceIterRun> {
        let run_id = Self::create_run(v, iter_name, target_table, opts).await?;
        Self::start_for_tests(v, &run_id).await?;
        ValenceIterRun::get(&run_id, v)
            .await?
            .ok_or_else(|| Error::NotFound(format!("iter run {run_id} disappeared")))
    }
}
