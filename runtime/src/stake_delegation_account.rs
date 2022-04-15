#[cfg(RUSTC_WITH_SPECIALIZATION)]
use solana_frozen_abi::abi_example::AbiExample;
use {
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        account_utils::StateMut,
        pubkey::Pubkey,
        stake::state::{Delegation, StakeState},
    },
    solana_stake_program::stake_state::{Meta, Stake},
    thiserror::Error,
};

/// A stake delegation account and its state deserialized from the account.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StakeDelegationAccount(AccountSharedData, Meta, Stake);

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid account data for active stake")]
    InvalidAccountData,
    #[error("Invalid stake account owner: {owner:?}")]
    InvalidOwner { owner: Pubkey },
}

impl StakeDelegationAccount {
    #[inline]
    pub(crate) fn lamports(&self) -> u64 {
        self.0.lamports()
    }

    #[inline]
    pub(crate) fn delegation(&self) -> &Delegation {
        &self.2.delegation
    }
}

impl TryFrom<AccountSharedData> for StakeDelegationAccount {
    type Error = Error;
    fn try_from(account: AccountSharedData) -> Result<Self, Self::Error> {
        if account.owner() != &solana_stake_program::id() {
            return Err(Error::InvalidOwner {
                owner: *account.owner(),
            });
        }
        if let Ok(StakeState::Stake(meta, stake)) = account.state() {
            Ok(Self(account, meta, stake))
        } else {
            Err(Error::InvalidAccountData)
        }
    }
}

impl From<StakeDelegationAccount> for (StakeState, AccountSharedData) {
    fn from(stake_account: StakeDelegationAccount) -> Self {
        (
            StakeState::Stake(stake_account.1, stake_account.2),
            stake_account.0,
        )
    }
}

#[cfg(RUSTC_WITH_SPECIALIZATION)]
impl AbiExample for StakeDelegationAccount {
    fn example() -> Self {
        use solana_sdk::{
            account::Account,
            stake::state::{Meta, Stake},
        };
        let stake_state = StakeState::Stake(Meta::example(), Stake::example());
        let mut account = Account::example();
        account.data.resize(256, 0u8);
        account.owner = solana_stake_program::id();
        let _ = account.set_state(&stake_state).unwrap();
        Self::try_from(AccountSharedData::from(account)).unwrap()
    }
}
