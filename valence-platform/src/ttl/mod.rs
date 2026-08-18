//! Deferred schema TTL: host registration and budgeted Chronon sweep.
//!
//! At boot, call [`sweep::register_ttl_service`], then optionally
//! [`sweep::resync_valence_ttl_sweep_job_cron_if_present`]. Tests and embedded hosts can run one
//! discover+enqueue tick with [`sweep::run_valence_ttl_sweep_inline`].
//!
//! # Examples
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use chronon_coordinator::ChrononCoordinatorBackend;
//! use valence_platform::ttl::sweep::register_ttl_service;
//!
//! fn wire_ttl(chronon: Arc<dyn ChrononCoordinatorBackend>) {
//!     register_ttl_service(chronon);
//!     // Optional: resync_valence_ttl_sweep_job_cron_if_present after default jobs exist.
//! }
//! ```
//!
//! # Chronon tick
//!
//! After registration, the `valence-ttl-sweep` job calls [`sweep::sweep_expired_ttl_rows`] each
//! tick: discover expired Deferred rows under a budget, queue deletes into the deletion DAG, and
//! return. Physical delete follows the deletion path already wired at boot.

pub mod sweep;
