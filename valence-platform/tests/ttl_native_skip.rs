//! Sweeper includes only Deferred capabilities.

#![allow(missing_docs)]

use valence::BackendTtlCapability;
use valence_platform::ttl::sweep::ttl_capability_included_in_deferred_sweep;

#[test]
fn k_ttl_native_deferred_included_native_and_unsupported_skipped() {
    assert!(ttl_capability_included_in_deferred_sweep(
        BackendTtlCapability::Deferred
    ));
    assert!(
        !ttl_capability_included_in_deferred_sweep(BackendTtlCapability::SupportedNative),
        "Redis/Mongo must not be Deferred-swept"
    );
    assert!(
        !ttl_capability_included_in_deferred_sweep(BackendTtlCapability::Unsupported),
        "IndraDB Unsupported must not be Deferred-swept"
    );
}
