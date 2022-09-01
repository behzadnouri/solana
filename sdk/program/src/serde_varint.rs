use {
    integer_encoding::{VarInt, VarIntReader},
    serde::{
        de::{Error as _, SeqAccess, Visitor},
        ser::SerializeTuple,
        Deserializer, Serializer,
    },
    std::{
        fmt,
        io::{Error, ErrorKind, Read},
        marker::PhantomData,
    },
};

struct SeqAccessWrapper<T>(T);

struct VarIntVisitor<T> {
    phantom: PhantomData<T>,
}

impl<'a, T> Read for SeqAccessWrapper<T>
where
    T: SeqAccess<'a>,
{
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, Error> {
        match self.0.next_element::<u8>() {
            Ok(Some(byte)) => {
                buffer[0] = byte;
                Ok(1)
            }
            Ok(None) | Err(_) => Err(Error::from(ErrorKind::UnexpectedEof)),
        }
    }
}

impl<'de, T> Visitor<'de> for VarIntVisitor<T>
where
    T: VarInt,
{
    type Value = T;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a VarInt")
    }

    fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut reader = SeqAccessWrapper(seq);
        match reader.read_varint::<T>() {
            Ok(out) => Ok(out),
            Err(err) => Err(A::Error::custom(err)),
        }
    }
}

pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: VarInt,
    S: Serializer,
{
    let mut buffer = [0u8; 10];
    debug_assert!(value.required_space() <= buffer.len());
    let size = value.encode_var(&mut buffer);
    let mut seq = serializer.serialize_tuple(size)?;
    for byte in &buffer[..size] {
        seq.serialize_element(byte)?;
    }
    seq.end()
}

pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: VarInt,
{
    deserializer.deserialize_tuple(
        10,
        VarIntVisitor {
            phantom: PhantomData::default(),
        },
    )
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct Dummy {
        #[serde(with = "super")]
        a: u32,
        b: u64,
        #[serde(with = "super")]
        c: u64,
        d: u32,
    }

    #[test]
    fn test_serde_varint() {
        let dummy = Dummy {
            a: 698,
            b: 370,
            c: 146,
            d: 796,
        };
        let bytes = bincode::serialize(&dummy).unwrap();
        assert_eq!(bytes.len(), 16);
        let other: Dummy = bincode::deserialize(&bytes).unwrap();
        assert_eq!(other, dummy);
    }

    #[test]
    fn test_serde_varint_rand() {
        let mut rng = rand::thread_rng();
        for _ in 0..1000 {
            let dummy = Dummy {
                a: rng.gen(),
                b: rng.gen(),
                c: rng.gen(),
                d: rng.gen(),
            };
            let bytes = bincode::serialize(&dummy).unwrap();
            let other: Dummy = bincode::deserialize(&bytes).unwrap();
            assert_eq!(other, dummy);
        }
    }
}
