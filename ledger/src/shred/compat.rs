#![allow(dead_code)]

use {
    crate::shred::{
        ErasureSetId, Error, ShredFlags, ShredId, ShredType, DATA_SHRED_SIZE_RANGE,
        ENCODED_PAYLOAD_SIZE, MAX_DATA_SHREDS_PER_FEC_BLOCK, MAX_DATA_SHREDS_PER_SLOT,
        SHRED_DATA_OFFSET, SHRED_PAYLOAD_SIZE, SIZE_OF_CODING_SHRED_HEADERS, SIZE_OF_SIGNATURE,
    },
    serde::{Deserialize, Serialize},
    solana_perf::packet::Packet,
    solana_sdk::{
        clock::Slot,
        hash::hashv,
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
struct ShredCommonHeader {
    signature: Signature,
    shred_type: ShredType,
    slot: Slot,
    index: u32,
    version: u16,
    fec_set_index: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
struct DataShredHeader {
    common_header: ShredCommonHeader,
    parent_offset: u16,
    flags: ShredFlags,
    size: u16, // common shred header + data shred header + data
}

#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
struct CodingShredHeader {
    common_header: ShredCommonHeader,
    num_data_shreds: u16,
    num_coding_shreds: u16,
    position: u16,
}

#[derive(Clone, Debug, PartialEq)]
struct Shred<T> {
    header: T,
    payload: Vec<u8>,
}

trait ShredHeader {
    fn common_header(&self) -> &ShredCommonHeader;
    fn common_header_mut(&mut self) -> &mut ShredCommonHeader;
}

impl<T: ShredHeader> Shred<T> {
    pub fn signature(&self) -> Signature {
        self.header.common_header().signature
    }

    pub fn slot(&self) -> Slot {
        self.header.common_header().slot
    }

    pub fn index(&self) -> u32 {
        self.header.common_header().index
    }

    pub fn version(&self) -> u16 {
        self.header.common_header().version
    }

    pub fn fec_set_index(&self) -> u32 {
        self.header.common_header().fec_set_index
    }

    // Identifier for the erasure coding set that the shred belongs to.
    pub(crate) fn erasure_set(&self) -> ErasureSetId {
        ErasureSetId(self.slot(), self.fec_set_index())
    }

    pub fn seed(&self, leader_pubkey: Pubkey) -> [u8; 32] {
        hashv(&[
            &self.slot().to_le_bytes(),
            &self.index().to_le_bytes(),
            &leader_pubkey.to_bytes(),
        ])
        .to_bytes()
    }

    #[inline]
    pub fn payload(&self) -> &Vec<u8> {
        &self.payload
    }

    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }

    pub fn sign(&mut self, keypair: &Keypair) {
        let signature = keypair.sign_message(&self.payload[SIZE_OF_SIGNATURE..]);
        bincode::serialize_into(&mut self.payload[..SIZE_OF_SIGNATURE], &signature).unwrap();
        self.header.common_header_mut().signature = signature;
    }

    pub fn verify(&self, pubkey: &Pubkey) -> bool {
        self.signature()
            .verify(pubkey.as_ref(), &self.payload[SIZE_OF_SIGNATURE..])
    }

    pub fn copy_to_packet(&self, packet: &mut Packet) {
        let size = self.payload.len();
        packet.data[..size].copy_from_slice(&self.payload[..]);
        packet.meta.size = size;
    }

    // Only for tests.
    pub fn set_index(&mut self, index: u32) {
        self.header.common_header_mut().index = index;
        bincode::serialize_into(&mut self.payload[..], self.header.common_header()).unwrap();
    }

    // Only for tests.
    pub fn set_slot(&mut self, slot: Slot) {
        self.header.common_header_mut().slot = slot;
        bincode::serialize_into(&mut self.payload[..], self.header.common_header()).unwrap();
    }
}

impl Shred<DataShredHeader> {
    pub const fn shred_type(&self) -> ShredType {
        ShredType::Data
    }

    /// Unique identifier for each shred.
    pub fn id(&self) -> ShredId {
        ShredId(self.slot(), self.index(), self.shred_type())
    }

    pub fn parent(&self) -> Result<Slot, Error> {
        let slot = self.slot();
        let parent_offset = self.header.parent_offset;
        if parent_offset == 0 && slot != 0 {
            return Err(Error::InvalidParentOffset {
                slot,
                parent_offset,
            });
        }
        slot.checked_sub(Slot::from(parent_offset))
            .ok_or(Error::InvalidParentOffset {
                slot,
                parent_offset,
            })
    }

    pub(crate) fn data(&self) -> Result<&[u8], Error> {
        let size = usize::from(self.header.size);
        if size > self.payload.len() || !DATA_SHRED_SIZE_RANGE.contains(&size) {
            return Err(Error::InvalidDataSize {
                size: self.header.size,
                payload: self.payload.len(),
            });
        }
        Ok(&self.payload[SHRED_DATA_OFFSET..size])
    }

    // Possibly trimmed payload;
    // Should only be used when storing shreds to blockstore.
    pub(crate) fn bytes_to_store(&self) -> &[u8] {
        // Payload will be padded out to SHRED_PAYLOAD_SIZE.
        // But only need to store the bytes within data_header.size.
        &self.payload[..self.header.size as usize]
    }

    // Possibly zero pads bytes stored in blockstore.
    pub(crate) fn resize_stored_shred(mut shred: Vec<u8>) -> Result<Vec<u8>, Error> {
        // XXX Need a place for the dispatcher code?
        // XXX maybe still do at least a debug check of type!
        // let shred_type = match shred.get(OFFSET_OF_SHRED_TYPE) {
        //     None => return Err(Error::InvalidPayloadSize(shred.len())),
        //     Some(shred_type) => match ShredType::try_from(*shred_type) {
        //         Err(_) => return Err(Error::InvalidShredType),
        //         Ok(shred_type) => shred_type,
        //     },
        // };
        if !(SHRED_DATA_OFFSET..SHRED_PAYLOAD_SIZE).contains(&shred.len()) {
            return Err(Error::InvalidPayloadSize(shred.len()));
        }
        shred.resize(SHRED_PAYLOAD_SIZE, 0u8);
        Ok(shred)
    }

    // Returns true if the shred passes sanity checks.
    pub fn sanitize(&self) -> Result<(), Error> {
        // XXX Need a place for this!
        // if self.payload().len() != SHRED_PAYLOAD_SIZE {
        //     return Err(Error::InvalidPayloadSize(self.payload.len()));
        // }
        // if self.erasure_shard_index().is_none() {
        //     let headers: Box<dyn Debug> = match self.shred_type() {
        //         ShredType::Data => Box::new((self.common_header, self.data_header)),
        //         ShredType::Code => Box::new((self.common_header, self.coding_header)),
        //     };
        //     return Err(Error::InvalidErasureShardIndex(headers));
        // }
        if self.index() as usize >= MAX_DATA_SHREDS_PER_SLOT {
            return Err(Error::InvalidDataShredIndex(self.index()));
        }
        let _parent = self.parent()?;
        let size = usize::from(self.header.size);
        if size > self.payload.len() || !DATA_SHRED_SIZE_RANGE.contains(&size) {
            return Err(Error::InvalidDataSize {
                size: self.header.size,
                payload: self.payload.len(),
            });
        }
        let flags = self.header.flags;
        if flags.intersects(ShredFlags::LAST_SHRED_IN_SLOT)
            && !flags.contains(ShredFlags::DATA_COMPLETE_SHRED)
        {
            return Err(Error::InvalidShredFlags(self.header.flags.bits()));
        }
        Ok(())
    }

    // Returns the shard index within the erasure coding set.
    pub(crate) fn erasure_shard_index(&self) -> Option<usize> {
        let index = self.index().checked_sub(self.fec_set_index())?;
        usize::try_from(index).ok()
    }

    // Returns the portion of the shred's payload which is erasure coded.
    pub(crate) fn erasure_shard(self) -> Result<Vec<u8>, Error> {
        if self.payload.len() != SHRED_PAYLOAD_SIZE {
            return Err(Error::InvalidPayloadSize(self.payload.len()));
        }
        let mut shard = self.payload;
        shard.resize(ENCODED_PAYLOAD_SIZE, 0u8);
        Ok(shard)
    }

    // Like Shred::erasure_shard but returning a slice.
    pub(crate) fn erasure_shard_as_slice(&self) -> Result<&[u8], Error> {
        if self.payload.len() != SHRED_PAYLOAD_SIZE {
            return Err(Error::InvalidPayloadSize(self.payload.len()));
        }
        Ok(&self.payload[..ENCODED_PAYLOAD_SIZE])
    }

    pub fn last_in_slot(&self) -> bool {
        self.header.flags.contains(ShredFlags::LAST_SHRED_IN_SLOT)
    }

    pub fn data_complete(&self) -> bool {
        self.header.flags.contains(ShredFlags::DATA_COMPLETE_SHRED)
    }

    pub(crate) fn reference_tick(&self) -> u8 {
        (self.header.flags & ShredFlags::SHRED_TICK_REFERENCE_MASK).bits()
    }

    // Only for tests.
    pub fn set_last_in_slot(&mut self) {
        self.header.flags |= ShredFlags::LAST_SHRED_IN_SLOT;
        bincode::serialize_into(&mut self.payload[..], &self.header).unwrap();
    }
}

impl Shred<CodingShredHeader> {
    pub const fn shred_type(&self) -> ShredType {
        ShredType::Code
    }

    /// Unique identifier for each shred.
    pub fn id(&self) -> ShredId {
        ShredId(self.slot(), self.index(), self.shred_type())
    }

    pub(crate) fn first_coding_index(&self) -> Option<u32> {
        let position = u32::from(self.header.position);
        self.index().checked_sub(position)
    }

    // Returns true if the shred passes sanity checks.
    pub fn sanitize(&self) -> Result<(), Error> {
        // XXX need a place for this! sanitize_common?!
        // if self.payload().len() != SHRED_PAYLOAD_SIZE {
        //     return Err(Error::InvalidPayloadSize(self.payload.len()));
        // }
        // if self.erasure_shard_index().is_none() {
        //     let headers: Box<dyn Debug> = match self.shred_type() {
        //         ShredType::Data => Box::new((self.common_header, self.data_header)),
        //         ShredType::Code => Box::new((self.common_header, self.coding_header)),
        //     };
        //     return Err(Error::InvalidErasureShardIndex(headers));
        // }
        let num_coding_shreds = u32::from(self.header.num_coding_shreds);
        if num_coding_shreds > 8 * MAX_DATA_SHREDS_PER_FEC_BLOCK {
            return Err(Error::InvalidNumCodingShreds(self.header.num_coding_shreds));
        }
        Ok(())
    }

    // Returns the shard index within the erasure coding set.
    pub(crate) fn erasure_shard_index(&self) -> Option<usize> {
        // Assert that the last shred index in the erasure set does not
        // overshoot u32.
        self.fec_set_index()
            .checked_add(u32::from(self.header.num_data_shreds.checked_sub(1)?))?;
        self.first_coding_index()?
            .checked_add(u32::from(self.header.num_coding_shreds.checked_sub(1)?))?;
        let num_data_shreds = usize::from(self.header.num_data_shreds);
        let num_coding_shreds = usize::from(self.header.num_coding_shreds);
        let position = usize::from(self.header.position);
        let fec_set_size = num_data_shreds.checked_add(num_coding_shreds)?;
        let index = position.checked_add(num_data_shreds)?;
        (index < fec_set_size).then(|| index)
    }

    // Returns the portion of the shred's payload which is erasure coded.
    pub(crate) fn erasure_shard(self) -> Result<Vec<u8>, Error> {
        if self.payload.len() != SHRED_PAYLOAD_SIZE {
            return Err(Error::InvalidPayloadSize(self.payload.len()));
        }
        let mut shard = self.payload;
        // SIZE_OF_CODING_SHRED_HEADERS bytes at the beginning of the coding
        // shreds contains the header and is not part of erasure coding.
        shard.drain(..SIZE_OF_CODING_SHRED_HEADERS);
        Ok(shard)
    }

    // Like Shred::erasure_shard but returning a slice.
    pub(crate) fn erasure_shard_as_slice(&self) -> Result<&[u8], Error> {
        if self.payload.len() != SHRED_PAYLOAD_SIZE {
            return Err(Error::InvalidPayloadSize(self.payload.len()));
        }
        Ok(&self.payload[SIZE_OF_CODING_SHRED_HEADERS..])
    }

    // Returns true if the erasure coding of the two shreds mismatch.
    pub(crate) fn erasure_mismatch(&self, other: &Self) -> bool {
        let CodingShredHeader {
            common_header: _,
            num_data_shreds,
            num_coding_shreds,
            position: _,
        } = self.header;
        num_coding_shreds != other.header.num_coding_shreds
            || num_data_shreds != other.header.num_data_shreds
            || self.first_coding_index() != other.first_coding_index()
    }

    pub(crate) fn num_data_shreds(&self) -> u16 {
        self.header.num_data_shreds
    }

    pub(crate) fn num_coding_shreds(&self) -> u16 {
        self.header.num_coding_shreds
    }
}

impl ShredHeader for DataShredHeader {
    #[inline]
    fn common_header(&self) -> &ShredCommonHeader {
        &self.common_header
    }

    fn common_header_mut(&mut self) -> &mut ShredCommonHeader {
        &mut self.common_header
    }
}

impl ShredHeader for CodingShredHeader {
    #[inline]
    fn common_header(&self) -> &ShredCommonHeader {
        &self.common_header
    }

    fn common_header_mut(&mut self) -> &mut ShredCommonHeader {
        &mut self.common_header
    }
}

