use std::{borrow::Cow, marker::PhantomData};

use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};

pub trait Encoder<'a> {
    type Item: 'a;
    fn to_bytes(item: &'a Self::Item) -> Option<Cow<'a, [u8]>>;
}

pub trait Decoder<'a> {
    type Item: 'a;
    fn from_bytes(src: &'a [u8]) -> Option<Self::Item>;
}

pub struct BincodeConfig<T>(PhantomData<T>);

impl<T> BincodeConfig<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for BincodeConfig<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<'a, T: 'a> Encoder<'a> for BincodeConfig<T>
where
    T: Serialize,
{
    type Item = T;

    fn to_bytes(item: &'a T) -> Option<Cow<[u8]>> {
        serialize(item).map(Cow::Owned).ok()
    }
}

impl<'a, T: 'a> Decoder<'a> for BincodeConfig<T>
where
    T: Deserialize<'a> + Clone,
{
    type Item = T;

    fn from_bytes(src: &'a [u8]) -> Option<T> {
        deserialize(src).ok()
    }
}
