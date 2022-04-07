use {
    solana_sdk::{
        account::AccountSharedData,
        stake::state::{Delegation, StakeState},
    },
};

pub(crate) struct StakeAccount(AccountSharedData, StakeState);

impl StakeAccount {
    pub(crate) fn delegation(&self) -> Option<Delegation> {
        self.1.delegation()
    }
}

