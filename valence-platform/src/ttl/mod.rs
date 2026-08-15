//! Deferred schema TTL: host registration and budgeted Chronon sweep.
//!
//! At boot, call [`sweep::register_ttl_service`], then optionally
//! [`sweep::resync_valence_ttl_sweep_job_cron_if_present`]. Tests and embedded hosts can run one
//! discover+enqueue tick with [`sweep::run_valence_ttl_sweep_inline`].
//!
//! # Examples
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use valence_platform::ttl::sweep::{
//!     register_ttl_service, resync_valence_ttl_sweep_job_cron_if_present,
//! };
//!
//! register_ttl_service(Arc::clone(&chronon_backend));
//! resync_valence_ttl_sweep_job_cron_if_present(chronon_backend.as_ref(), &valence).await?;
//! ```
//!
//! # Chronon tick
//!
//! After registration, the `valence-ttl-sweep` job calls [`sweep::sweep_expired_ttl_rows`] each
//! tick: discover expired Deferred rows under a budget, queue deletes into the deletion DAG, and
//! return. Physical delete follows the deletion path already wired at boot.

pub mod sweep;
