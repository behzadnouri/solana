//! State for durable transaction nonces.

mod current;
pub use current::{Data, DurableNonce, State};
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum Versions {
    Current(Box<State>),
}

impl Versions {
    pub fn new_current(state: State, separate_domains: bool) -> Self {
        Self::Current(Box::new(state))
    }

    pub fn convert_to_current(self) -> State {
        match self {
            Self::Current(state) => *state,
        }
    }

    pub fn separate_domains(&self) -> bool {
        false // XXX
    }
}
