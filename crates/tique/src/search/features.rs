use byteorder::LittleEndian;
use serde::{Deserialize, Serialize};
use zerocopy::{AsBytes, ByteSliceMut, LayoutVerified, U16};

// FIXME Feature::values().length if this were Java. Macro this out?
pub const NUM_FEATURES: usize = 8;
pub const BYTE_SIZE: usize = NUM_FEATURES * 16;

const UNSET_FEATURE: u16 = std::u16::MAX;

type FeatureValue = U16<LittleEndian>;
type Features = [FeatureValue; NUM_FEATURES];

#[derive(Serialize, Deserialize, Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum Feature {
    NumIngredients = 0,

    Calories,
    FatContent,
    ProteinContent,
    CarbContent,

    CookTime,
    PrepTime,
    TotalTime,
    // Remember to update NUM_FEATURES
}

// FIXME Learn yourself some macros already
impl std::fmt::Display for Feature {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Feature::NumIngredients => "num_ingredients",

            Feature::Calories => "calories",
            Feature::FatContent => "fat",
            Feature::ProteinContent => "protein",
            Feature::CarbContent => "carb",

            Feature::CookTime => "cook_time",
            Feature::PrepTime => "prep_time",
            Feature::TotalTime => "total_time",
        })
    }
}

#[derive(Debug)]
pub struct FeatureVector<B: ByteSliceMut>(LayoutVerified<B, Features>);

pub struct BytesVector;
pub type BytesFeatureVector<'a> = FeatureVector<&'a mut [u8]>;
type ParseOption<'a> = Option<(BytesFeatureVector<'a>, &'a [u8])>;

impl<'a> BytesVector {
    pub fn parse(src: &'a mut Vec<u8>) -> ParseOption {
        let (inner, rest) = LayoutVerified::new_from_prefix(src.as_mut_slice())?;

        Some((FeatureVector(inner), rest))
    }

    pub fn init(bfv: &'a mut BytesFeatureVector) {
        for b in bfv.0.iter_mut() {
            *b = FeatureValue::new(UNSET_FEATURE);
        }
    }

    pub fn new_buf() -> Vec<u8> {
        vec![std::u8::MAX; NUM_FEATURES * 2]
    }
}

impl<B: ByteSliceMut> FeatureVector<B> {
    pub fn get(&self, feature: &Feature) -> Option<FeatureValue> {
        let idx = *feature as usize;
        assert!(idx < NUM_FEATURES);

        let FeatureVector(inner) = self;

        let value = inner[idx];
        if value == FeatureValue::new(UNSET_FEATURE) {
            None
        } else {
            Some(value)
        }
    }

    pub fn set(&mut self, feature: &Feature, value: u16) {
        let idx = *feature as usize;
        assert!(idx < NUM_FEATURES);
        let FeatureVector(inner) = self;

        inner[idx] = FeatureValue::new(value);
    }

    pub fn as_bytes(&self) -> &[u8] {
        let FeatureVector(inner) = self;
        inner.as_bytes()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn example_usage() {
        let mut buf = BytesVector::new_buf();

        let (mut features, _rest) = BytesVector::parse(&mut buf).unwrap();

        // Just to minimize typing
        let A = &Feature::NumIngredients;
        let B = &Feature::FatContent;
        let C = &Feature::PrepTime;

        features.set(A, 10);
        features.set(B, 60);

        assert_eq!(Some(FeatureValue::new(10)), features.get(A));
        assert_eq!(Some(FeatureValue::new(60)), features.get(B));
        assert_eq!(None, features.get(C));

        let mut bytes = features.as_bytes();
        assert_eq!(NUM_FEATURES * 2, bytes.len());

        let mut from_bytes_buf = BytesVector::new_buf();
        from_bytes_buf.copy_from_slice(&bytes[..]);

        let opt = BytesVector::parse(&mut from_bytes_buf);

        assert!(opt.is_some());

        let (mut from_bytes, _rest) = opt.unwrap();

        assert_eq!(bytes, from_bytes.as_bytes());
    }

}
