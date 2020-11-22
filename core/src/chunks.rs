use serde::ser::Serialize;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::iter::Fuse;

const NUM_CHUNK_BUFFERS: usize = 8;

pub fn split<I, T>(
    data_feed: I,
    max_chunk_size: usize,
) -> Chunks<Fuse<<I as IntoIterator>::IntoIter>, T>
where
    T: Serialize + Debug,
    I: IntoIterator<Item = T>,
{
    Chunks {
        data_feed: data_feed.into_iter().fuse(),
        max_chunk_size: max_chunk_size as u64,
        buffers: VecDeque::with_capacity(NUM_CHUNK_BUFFERS + 1),
    }
}

pub struct Chunks<I, T> {
    data_feed: I,
    max_chunk_size: u64,
    buffers: VecDeque<(u64, Vec<T>)>,
}

impl<I, T> Chunks<I, T>
where
    T: Serialize,
{
    // Returns index of the first buffer which can fit data_size.
    fn get_buffer(&mut self, data_size: u64) -> usize {
        for (index, (buffer_size, _)) in self.buffers.iter().enumerate() {
            if buffer_size + data_size <= self.max_chunk_size {
                return index;
            }
        }
        self.buffers.push_back((0, Vec::new()));
        self.buffers.len() - 1
    }

    // Moves the buffer of the given index towards the
    // front, so that self.buffers stays sorted.
    fn reorder_buffer(&mut self, mut index: usize) {
        let (buffer_size, _) = self.buffers[index];
        while index > 0 && buffer_size > self.buffers[index - 1].0 {
            self.buffers.swap(index, index - 1);
            index -= 1;
        }
    }

    fn pop(&mut self) -> Option<Vec<T>> {
        println!("{:?}", self.buffers.iter().map(|(s, _)| *s).collect::<Vec<_>>());
        self.buffers.pop_front().map(|(_, chunk)| chunk)
    }

    fn push(&mut self, data: T) -> bincode::Result<()> {
        let data_size = bincode::serialized_size(&data)?;
        if data_size > self.max_chunk_size {
            return Err(bincode::Error::new(bincode::ErrorKind::SizeLimit));
        }
        let index = self.get_buffer(data_size);
        let (buffer_size, buffer) = &mut self.buffers[index];
        *buffer_size += data_size;
        buffer.push(data);
        self.reorder_buffer(index);
        Ok(())
    }
}

impl<I, T> Iterator for Chunks<I, T>
where
    T: Serialize,
    I: Iterator<Item = T>,
{
    type Item = Vec<T>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.data_feed.next() {
                None => return self.pop(),
                Some(data) => {
                    if let Err(err) = self.push(data) {
                        error!("data chunk error: {}", err);
                    }
                    if self.buffers.len() > NUM_CHUNK_BUFFERS {
                        return self.pop();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crds_value::CrdsValue;
    use rand::Rng;
    use solana_perf::packet::PACKET_DATA_SIZE;
    use std::collections::HashMap;
    use std::iter::{repeat_with, FromIterator};

    #[test]
    fn test_split_chunks() {
        let mut rng = rand::thread_rng();
        let data_feed: Vec<_> = repeat_with(|| {
            let size = rng.gen_range(1, 8);
            let mut data = vec![0u8; size];
            rng.fill(&mut data[..]);
            data
        })
        .take(1024)
        .collect();
        let mut counts = HashMap::new();
        for data in &data_feed {
            let count = counts.entry(data.clone()).or_insert(0);
            *count += 1;
        }
        let max_chunk_size = 64;
        let mut num_chunks = 0;
        for chunk in split(data_feed, max_chunk_size as usize) {
            num_chunks += 1;
            let mut chunk_size = 0;
            for data in chunk {
                chunk_size += bincode::serialized_size(&data).unwrap();
                let count = counts.entry(data.clone()).or_insert(0);
                *count -= 1;
            }
            assert!(chunk_size <= max_chunk_size);
        }
        assert!(num_chunks < 256);
        // Assert that all the data appeared in the chunks.
        for (_, count) in counts {
            assert_eq!(count, 0);
        }
    }

    #[test]
    fn test_split_chunks_with_crds_values() {
        const NUM_CRDS_VALUES: usize = 2048;
        const CRDS_PAYLOAD_SIZE: usize = PACKET_DATA_SIZE - 44;
        let mut rng = rand::thread_rng();
        let values: Vec<_> = std::iter::repeat_with(|| CrdsValue::new_rand(&mut rng, None))
            .take(NUM_CRDS_VALUES)
            .collect();
        let chunks: Vec<_> = split(values.clone(), CRDS_PAYLOAD_SIZE).collect();
        // Assert that all messages are included in the chunks.
        assert_eq!(NUM_CRDS_VALUES, chunks.iter().map(Vec::len).sum::<usize>());
        let mut values = HashMap::<_, _>::from_iter(values.into_iter().map(|v| (v.label(), v)));
        for chunk in &chunks {
            for value in chunk {
                assert_eq!(*value, values.remove(&value.label()).unwrap());
            }
        }
        assert!(values.is_empty());
        println!("# of chunks: {}", chunks.len());
    }
}
