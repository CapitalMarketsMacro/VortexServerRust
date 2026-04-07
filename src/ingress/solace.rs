//! Solace PubSub+ ingress.
//!
//! ## Status: scaffolded, not yet wired up
//!
//! Solace's native protocol is SMF, which requires `libsolclient` (the
//! official C library shipped by Solace as the "Solace Messaging API for C").
//! There is no pure-Rust SMF client. The two community options are:
//!
//! - [`solace-rs`](https://crates.io/crates/solace-rs) — wraps libsolclient
//!   via FFI; needs the C library installed system-wide.
//! - [`rsolace`](https://crates.io/crates/rsolace) — newer wrapper, same
//!   external dependency.
//!
//! Pulling either in changes the build prerequisites for everyone, so we
//! defer that until you have the C library installed and confirm which
//! crate to standardize on.
//!
//! ## How to wire it up later
//!
//! 1. Add the chosen Solace crate to `Cargo.toml`.
//! 2. Replace `validate_config` with a real `connect` that opens an SMF
//!    session using `SolaceConfig` and returns a `SolaceContext`.
//! 3. Add a `start` function mirroring `nats::start`: subscribe to the
//!    topic, receive the first message synchronously to seed the table,
//!    then spawn a per-table consumer task that keeps streaming.
//! 4. Update `ingress::mod::spawn_consumers` to call `solace::start`
//!    instead of returning the not-implemented error.

use anyhow::anyhow;

use crate::config::SolaceConfig;

/// Validate that a Solace transport block is well-formed. Currently this is
/// the only Solace-related work we do at startup — actual connection is not
/// implemented.
pub fn validate_config(_cfg: &SolaceConfig) -> anyhow::Result<()> {
    Err(anyhow!(
        "Solace transport is configured but not yet wired up. \
         See src/ingress/solace.rs for the implementation TODO."
    ))
}
