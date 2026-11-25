//! Identifier newtypes shared across the domain core; also mints row identities.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

macro_rules! id_newtype {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

pub(crate) use id_newtype;

/// A fresh identity for one stored row. The counter keeps it unique within the
/// process and the nanosecond stamp across processes.
pub fn mint(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{nanos:x}{count:x}")
}

id_newtype!(MindId);
id_newtype!(SpaceId);
id_newtype!(ContextId);
id_newtype!(SubjectId);

#[cfg(test)]
mod tests {
    use super::mint;

    #[test]
    fn minted_ids_carry_their_prefix_and_never_repeat() {
        let first = mint("spc");
        let second = mint("spc");
        assert!(first.starts_with("spc_"));
        assert!(second.starts_with("spc_"));
        assert_ne!(first, second);
    }
}
