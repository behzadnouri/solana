use {
    bincode::ErrorKind,
    serde::{de::DeserializeOwned, Serialize},
    std::{
        any::{Any, TypeId},
        collections::{hash_map::Entry, HashMap},
        sync::RwLock,
    },
};

type CacheEntry = Option<Box<dyn Any + Send + Sync>>;

#[derive(Debug, Default, AbiExample)]
pub(crate) struct AccountData {
    data: Vec<u8>,
    cache: RwLock<HashMap<TypeId, CacheEntry>>,
}

impl AccountData {
    pub(crate) fn encode<T>(value: T) -> bincode::Result<Self>
    where
        T: Send + Sync + Serialize + 'static,
    {
        let data = bincode::serialize(&value)?;
        let mut cache = HashMap::<_, CacheEntry>::new();
        cache.insert(TypeId::of::<T>(), Some(Box::new(value)));
        let cache = RwLock::new(cache);
        Ok(Self { data, cache })
    }

    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }

    /// Deserializes and returns value of type T stored in the buffer.
    pub(crate) fn get<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + DeserializeOwned + 'static,
    {
        let cast = |any: &CacheEntry| -> Option<T> {
            Some(any.as_ref()?.downcast_ref::<T>().unwrap().clone())
        };
        let key = TypeId::of::<T>();
        if let Some(any) = self.cache.read().unwrap().get(&key) {
            return cast(any);
        }
        match self.cache.write().unwrap().entry(key) {
            Entry::Vacant(entry) => {
                let value = bincode::deserialize::<T>(&self.data).ok();
                entry.insert(Some(Box::new(value.clone()?)));
                value
            }
            Entry::Occupied(entry) => cast(entry.get()),
        }
    }

    // Serializes the value and stores it into the inner data buffer
    // **without** resizing the buffer. The call will fail if the buffer
    // is too small to serialize the value into.
    pub(crate) fn emplace<T>(&mut self, value: T) -> bincode::Result<()>
    where
        T: Send + Sync + Serialize + 'static,
    {
        // Despite what is claimed in the docs, serialize_into will corrupt
        // buffer when it fails.
        let data = bincode::serialize(&value)?;
        if data.len() > self.data.len() {
            return Err(Box::new(ErrorKind::SizeLimit));
        }
        self.data[..data.len()].copy_from_slice(&data);
        let mut cache = self.cache.write().unwrap();
        cache.clear(); // Invalidate existing cache.
        cache.insert(TypeId::of::<T>(), Some(Box::new(value)));
        Ok(())
    }
}

impl Clone for AccountData {
    fn clone(&self) -> Self {
        Self::from(self.data.clone()) // Cache of Any cannot be cloned.
    }
}

impl PartialEq<AccountData> for AccountData {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data // Disregard cache!
    }
}

impl Eq for AccountData {}

impl From<Vec<u8>> for AccountData {
    fn from(data: Vec<u8>) -> Self {
        Self {
            data,
            cache: RwLock::default(),
        }
    }
}

impl From<AccountData> for Vec<u8> {
    fn from(account_data: AccountData) -> Self {
        account_data.data
    }
}

impl AsMut<Vec<u8>> for AccountData {
    fn as_mut(&mut self) -> &mut Vec<u8> {
        // Invalidate the cache since the data may be mutated.
        self.cache.write().unwrap().clear();
        &mut self.data
    }
}

impl AsRef<[u8]> for AccountData {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::hash,
        rand::Rng,
        solana_program::sysvar::{clock::Clock, slot_hashes::SlotHashes},
        std::iter::repeat_with,
    };

    fn new_rand_sysvar_clock<R: Rng>(rng: &mut R) -> Clock {
        Clock {
            slot: rng.gen(),
            epoch_start_timestamp: rng.gen(),
            epoch: rng.gen(),
            leader_schedule_epoch: rng.gen(),
            unix_timestamp: rng.gen(),
        }
    }

    fn new_rand_sysvar_slot_hashes<R: Rng>(rng: &mut R, size: usize) -> SlotHashes {
        let slot_hashes: Vec<_> = repeat_with(|| (rng.gen(), hash::new_rand(rng)))
            .take(size)
            .collect();
        SlotHashes::new(&slot_hashes)
    }

    #[test]
    fn test_account_data() {
        let mut rng = rand::thread_rng();
        let clock = new_rand_sysvar_clock(&mut rng);
        let data = bincode::serialize(&clock).unwrap();
        let mut account_data = AccountData::from(data);
        assert_eq!(account_data.get::<Clock>().unwrap(), clock);
        assert_eq!(account_data.get::<Clock>().unwrap(), clock);
        assert_eq!(account_data.get::<SlotHashes>(), None);

        let slot_hashes = new_rand_sysvar_slot_hashes(&mut rng, 20);
        let data = account_data.data.clone();
        // Should fail because of small buffer.
        match *account_data.emplace(slot_hashes.clone()).unwrap_err() {
            ErrorKind::SizeLimit => (),
            err => panic!("unexpected error: {:?}", err),
        };
        // But should not corrupt the buffer.
        assert_eq!(data, account_data.data);
        assert_eq!(account_data.get::<Clock>().unwrap(), clock);

        let size = bincode::serialized_size(&slot_hashes).unwrap();
        account_data.as_mut().resize(size as usize, 0u8);
        // Cache is invalidated because of mutable access to the bytes array.
        assert!(account_data.cache.read().unwrap().is_empty());

        account_data.emplace(slot_hashes.clone()).unwrap();
        assert_eq!(account_data.get::<SlotHashes>().unwrap(), slot_hashes);
        assert_ne!(account_data.get::<Clock>(), Some(clock));
        assert_eq!(account_data.get::<SlotHashes>().unwrap(), slot_hashes);
    }
}
