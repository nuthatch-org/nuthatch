//! RFC-0075 S6: a tab nest's memory ceiling, 2 GiB on a desktop and 600 MiB on a phone, held with
//! a cursor and a statement running together. One statement at a time takes a quarter, the node's
//! one-permit share of a cursor's 2 GiB (512 MiB), through Burrmill's pool; the cursor and its
//! entities take the rest. Declared entity state is refused at admission as a node refuses a mount,
//! and a window is refused once the module's linear memory has reached the ceiling, checked before its
//! fetch and again before its commit. That is a wall between steps, not an allocator cap: what one
//! window decodes, folds and seals can pass the ceiling before the next check stops the cursor.

use std::fmt;

pub const DESKTOP_CEILING_MIB: u32 = 2048;
pub const PHONE_CEILING_MIB: u32 = 600;

// The node's admission charge, from `src/runtime.rs`; `tests/native.rs` holds the two equal.
pub const ENTITY_RSS_BYTES_PER_ROW: u64 = 3_200;
pub const ENTITY_CIRCUIT_RSS_MB: u64 = 8;

/// What admission charges one entity, as `runtime::entity_allowance_bytes` does for one that reads
/// no offchain table, which a tab has none of.
pub fn entity_allowance_bytes(max_rows: u64) -> u64 {
    (ENTITY_CIRCUIT_RSS_MB * 1024 * 1024)
        .saturating_add(max_rows.saturating_mul(ENTITY_RSS_BYTES_PER_ROW))
}

/// The module's linear memory in bytes. It never shrinks, so it is the high-water mark.
pub fn linear_memory() -> u64 {
    #[cfg(target_arch = "wasm32")]
    return core::arch::wasm32::memory_size(0) as u64 * 65_536;
    #[cfg(not(target_arch = "wasm32"))]
    0
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TabBudget {
    pub ceiling: u64,
}

impl Default for TabBudget {
    fn default() -> Self {
        Self::mib(DESKTOP_CEILING_MIB)
    }
}

/// A refusal, named, as the node names `MountRefusal::OverBudget`.
#[derive(Debug, PartialEq, Eq)]
pub enum OverBudget {
    Entities {
        declared: u64,
        cursor_share: u64,
        ceiling: u64,
    },
    Memory {
        linear: u64,
        ceiling: u64,
    },
}

const MIB: u64 = 1024 * 1024;

impl fmt::Display for OverBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OverBudget::Entities {
                declared,
                cursor_share,
                ceiling,
            } => write!(
                f,
                "over the tab's memory budget: its entities' declared state would put the cursor at \
                 ~{} MiB against the {} MiB its {} MiB ceiling leaves beside one statement - lower \
                 their max_rows, or run this nest on a node",
                declared.div_ceil(MIB),
                cursor_share / MIB,
                ceiling / MIB
            ),
            OverBudget::Memory { linear, ceiling } => write!(
                f,
                "over the tab's memory budget: the module holds {} MiB against a {} MiB ceiling, so \
                 the cursor stops here rather than be killed by the browser - run this nest on a node",
                linear / MIB,
                ceiling / MIB
            ),
        }
    }
}

impl std::error::Error for OverBudget {}

impl TabBudget {
    pub fn mib(ceiling_mib: u32) -> Self {
        Self {
            ceiling: u64::from(ceiling_mib) * MIB,
        }
    }

    /// One statement's working memory, the pool Burrmill refuses it over.
    pub fn query_bytes(&self) -> u64 {
        self.ceiling / 4
    }

    /// What the cursor, its store and its entities may hold.
    pub fn cursor_bytes(&self) -> u64 {
        self.ceiling - self.query_bytes()
    }

    /// Admit entities declaring `max_rows` each, or refuse the lot.
    pub fn admit_entities(
        &self,
        max_rows: impl IntoIterator<Item = u64>,
    ) -> Result<(), OverBudget> {
        let declared = max_rows
            .into_iter()
            .map(entity_allowance_bytes)
            .fold(0u64, u64::saturating_add);
        if declared > self.cursor_bytes() {
            return Err(OverBudget::Entities {
                declared,
                cursor_share: self.cursor_bytes(),
                ceiling: self.ceiling,
            });
        }
        Ok(())
    }

    /// Before a window: refuse once the module's memory has reached the ceiling.
    pub fn check_memory(&self, linear: u64) -> Result<(), OverBudget> {
        if linear >= self.ceiling {
            return Err(OverBudget::Memory {
                linear,
                ceiling: self.ceiling,
            });
        }
        Ok(())
    }
}
