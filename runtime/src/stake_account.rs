use {
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        account_utils::StateMut,
        instruction::InstructionError,
        pubkey::Pubkey,
        stake::state::{Delegation, StakeState},
    },
    thiserror::Error,
};

pub(crate) struct StakeAccount(AccountSharedData, StakeState);

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("Invalid stake account owner: {owner:?}")]
    InvalidOwner { owner: Pubkey },
    #[error(transparent)]
    InstructionError(#[from] InstructionError),
}

impl StakeAccount {
    pub(crate) fn delegation(&self) -> Option<Delegation> {
        self.1.delegation()
    }

    pub(crate) fn stake_state(&self) -> &StakeState {
        &self.1
    }
}

impl TryFrom<AccountSharedData> for StakeAccount {
    type Error = Error;
    fn try_from(account: AccountSharedData) -> Result<Self, Self::Error> {
        if account.owner() != &solana_stake_program::id() {
            return Err(Error::InvalidOwner {
                owner: *account.owner(),
            });
        }
        let stake_state = account.state()?;
        Ok(Self(account, stake_state))
    }
}

impl From<StakeAccount> for (AccountSharedData, StakeState) {
    fn from(stake_account: StakeAccount) -> Self {
        (stake_account.0, stake_account.1)
    }
}
