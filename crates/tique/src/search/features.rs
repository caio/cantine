use byteorder::LittleEndian;
use serde::{Deserialize, Serialize};
use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, LayoutVerified, U16};

// FIXME Feature::values().length if this were Java. Macro this out?
pub const NUM_FEATURES: usize = 8;
const UNSET_FEATURE: u16 = std::u16::MAX;

pub const EMPTY_BUFFER: [u8; NUM_FEATURES * 2] = [std::u8::MAX; NUM_FEATURES * 2];

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
pub struct FeatureVector<B: ByteSlice>(LayoutVerified<B, Features>);

impl<B: ByteSlice> FeatureVector<B> {
    pub fn parse(src: B) -> Option<FeatureVector<B>> {
        let (inner, _) = LayoutVerified::new_from_prefix(src)?;
        Some(FeatureVector(inner))
    }

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

    pub fn as_bytes(&self) -> &[u8] {
        let FeatureVector(inner) = self;
        inner.as_bytes()
    }
}

impl<B: ByteSliceMut> FeatureVector<B> {
    pub fn set(&mut self, feature: &Feature, value: u16) {
        let idx = *feature as usize;
        assert!(idx < NUM_FEATURES);
        let FeatureVector(inner) = self;

        inner[idx] = FeatureValue::new(value);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn example_usage() {
        let mut buf = EMPTY_BUFFER.to_vec();

        let mut features = FeatureVector::parse(buf.as_mut_slice()).unwrap();

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

        let mut from_bytes_buf = EMPTY_BUFFER.to_vec();
        from_bytes_buf.copy_from_slice(&bytes[..]);

        let opt = FeatureVector::parse(from_bytes_buf.as_slice());

        assert!(opt.is_some());

        let from_bytes = opt.unwrap();

        assert_eq!(bytes, from_bytes.as_bytes());
    }

}
