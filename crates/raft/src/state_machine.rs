use concord_crdt::CrdtState;
use serde::{Deserialize, Serialize};

use crate::log::Command;

/// The CRDT-backed state machine that Raft commits get applied to.
///
/// Each committed log entry carries a CrdtOp which is applied to the
/// underlying CrdtState. Because CRDT ops are commutative/associative/
/// idempotent by construction, applying them in committed-log order is
/// correct, and snapshot + replay is safe.
pub struct StateMachine {
    state: CrdtState,
    last_applied: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    pub state: CrdtState,
    pub last_applied: u64,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            state: CrdtState::new(),
            last_applied: 0,
        }
    }

    pub fn apply(&mut self, index: u64, command: &Command) {
        if index <= self.last_applied {
            return;
        }
        match command {
            Command::CrdtOp(op) => {
                self.state.apply(op);
            }
            Command::Noop => {}
        }
        self.last_applied = index;
    }

    pub fn state(&self) -> &CrdtState {
        &self.state
    }

    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    pub fn snapshot(&self) -> StateMachineSnapshot {
        StateMachineSnapshot {
            state: self.state.clone(),
            last_applied: self.last_applied,
        }
    }

    pub fn restore(&mut self, snapshot: StateMachineSnapshot) {
        self.state = snapshot.state;
        self.last_applied = snapshot.last_applied;
    }
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}
