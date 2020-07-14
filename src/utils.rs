use bytes::BytesMut;
use std::convert::TryInto;

#[cfg(feature = "prostmsg")]
use prost::Message;

use crate::CustomStorageError;

pub fn read_be_u64(input: &mut &[u8]) -> u64 {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<u64>());
    *input = rest;
    u64::from_be_bytes(int_bytes.try_into().unwrap())
}

pub fn decode<V>(v: &[u8]) -> Result<V, CustomStorageError>
where
    V: Message + Default,
{
    Ok(V::decode(v)?)
}

pub fn encode<V>(v: V) -> Result<BytesMut, CustomStorageError>
where
    V: Message,
{
    let mut buf = bytes::BytesMut::with_capacity(v.encoded_len());
    V::encode(&v, &mut buf)?;
    Ok(buf)
}
