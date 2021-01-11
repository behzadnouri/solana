use bincode::Options as _;
use serde::{de::DeserializeOwned, Serialize};
use std::io::{Read, Write};
use thiserror::Error;

#[derive(Deserialize, Serialize)]
enum Encoded {
    Bincode(Vec<u8>),
    Zstd(Vec<u8>),
}

#[derive(Clone, Copy)]
pub enum Options {
    Bincode,
    Zstd { level: i32 },
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("(de)serialization error")]
    SerializationError(#[from] bincode::Error),
}

impl Encoded {
    fn len(&self) -> usize {
        match self {
            Encoded::Bincode(bytes) => bytes.len(),
            Encoded::Zstd(bytes) => bytes.len(),
        }
    }
}

fn encode_zstd(data: &[u8], level: i32) -> std::io::Result<Vec<u8>> {
    let mut buffer = Vec::with_capacity(data.len() * 2);
    let mut encoder = zstd::stream::write::Encoder::new(&mut buffer, level)?;
    encoder.write_all(data)?;
    encoder.finish()?;
    Ok(buffer)
}

pub fn encode<T: Serialize>(obj: &T, options: Options) -> bincode::Result<Vec<u8>> {
    let bytes = bincode::options().serialize(obj)?;
    let encoded = match options {
        Options::Bincode => None,
        Options::Zstd { level } => encode_zstd(&bytes, level).map(Encoded::Zstd).ok(),
    };
    let encoded = match encoded {
        Some(encoded) if encoded.len() < bytes.len() => encoded,
        _ => Encoded::Bincode(bytes),
    };
    bincode::options().serialize(&encoded)
}

pub fn decode<T: DeserializeOwned>(
    bytes: &[u8],
    limit: usize, // Limit maximum number of bytes decoded.
) -> Result<T, Error> {
    let encoded = bincode::options().deserialize_from(bytes)?;
    let decoder: Box<dyn Read> = match &encoded {
        Encoded::Bincode(bytes) => Box::new(&bytes[..]),
        Encoded::Zstd(bytes) => Box::new(zstd::stream::read::Decoder::new(&bytes[..])?),
    };
    Ok(bincode::options()
        .with_limit(limit as u64)
        .deserialize_from(decoder)?)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rand::Rng;
    use solana_ledger::{
        blockstore_meta::DuplicateSlotProof,
        entry::Entry,
        shred::{Shred, Shredder},
    };
    use solana_sdk::{hash, pubkey::Pubkey, signature::Keypair, system_transaction};
    use std::sync::Arc;

    pub fn new_rand_shred<R: Rng>(
        rng: &mut R,
        next_shred_index: u32,
        shredder: &Shredder,
    ) -> Shred {
        let entries: Vec<_> = std::iter::repeat_with(|| {
            let tx = system_transaction::transfer(
                &Keypair::new(),       // from
                &Pubkey::new_unique(), // to
                rng.gen(),             // lamports
                hash::new_rand(rng),   // recent blockhash
            );
            Entry::new(
                &hash::new_rand(rng), // prev_hash
                1,                    // num_hashes,
                vec![tx],             // transactions
            )
        })
        .take(5)
        .collect();
        let (mut data_shreds, _coding_shreds, _last_shred_index) = shredder.entries_to_shreds(
            &entries,
            true, // is_last_in_slot
            next_shred_index,
        );
        data_shreds.swap_remove(0)
    }

    #[test]
    fn test_encode_round_trip() {
        let mut rng = rand::thread_rng();
        let leader = Arc::new(Keypair::new());
        let (slot, parent_slot, fec_rate, reference_tick, version) =
            (53084024, 53084023, 0.0, 0, 0);
        let shredder =
            Shredder::new(slot, parent_slot, fec_rate, leader, reference_tick, version).unwrap();
        let next_shred_index = rng.gen();
        let shred1 = new_rand_shred(&mut rng, next_shred_index, &shredder);
        let shred2 = new_rand_shred(&mut rng, next_shred_index, &shredder);
        let proof = DuplicateSlotProof {
            shred1: shred1.payload,
            shred2: shred2.payload,
        };
        let options = vec![
            Options::Bincode,
            Options::Zstd { level: 0 },
            Options::Zstd { level: 9 },
        ];
        for opts in options {
            let bytes = encode(&proof, opts).unwrap();
            let other: DuplicateSlotProof = decode(&bytes[..], 4096).unwrap();
            assert_eq!(proof.shred1, other.shred1);
            assert_eq!(proof.shred2, other.shred2);
        }
    }
}
