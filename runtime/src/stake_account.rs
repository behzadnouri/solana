use {
    solana_frozen_abi::abi_example::AbiExample,
    solana_sdk::{
        account::{Account, AccountSharedData, ReadableAccount},
        account_utils::StateMut,
        instruction::InstructionError,
        pubkey::Pubkey,
        stake::state::{Delegation, StakeState},
    },
    thiserror::Error,
};

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StakeAccount(AccountSharedData, StakeState);

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error(transparent)]
    InstructionError(#[from] InstructionError),
    #[error("Invalid stake account owner: {owner:?}")]
    InvalidOwner { owner: Pubkey },
}

impl StakeAccount {
    pub(crate) fn lamports(&self) -> u64 {
        self.0.lamports()
    }

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

impl AbiExample for StakeAccount {
    fn example() -> Self {
        use solana_sdk::stake::state::{Meta, Stake};
        let stake_state = StakeState::Stake(Meta::example(), Stake::example());
        let mut account = Account::example();
        account.data.resize(256, 0u8);
        account.owner = solana_stake_program::id();
        let _ = account.set_state(&stake_state).unwrap();
        Self::try_from(AccountSharedData::from(account)).unwrap()
    }
}
