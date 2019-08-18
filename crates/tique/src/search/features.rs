use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use zerocopy::{AsBytes, ByteSlice, ByteSliceMut};

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

    DietKeto,
    DietLowCarb,
    DietVegan,
    DietVegetarian,
    DietPaleo,
}

impl Feature {
    pub const LENGTH: usize = 13;

    pub const VALUES: [Feature; Feature::LENGTH] = [
        Feature::NumIngredients,
        Feature::Calories,
        Feature::FatContent,
        Feature::ProteinContent,
        Feature::CarbContent,
        Feature::CookTime,
        Feature::PrepTime,
        Feature::TotalTime,
        Feature::DietKeto,
        Feature::DietLowCarb,
        Feature::DietVegan,
        Feature::DietVegetarian,
        Feature::DietPaleo,
        // TODO Feature::InstructionsLength
    ];

    // TODO explore using a bitset instead
    pub const UNSET_FEATURE: u16 = std::u16::MAX;

    pub const EMPTY_BUFFER: [u8; Feature::LENGTH * 2] = [std::u8::MAX; Feature::LENGTH * 2];
}

impl Into<usize> for Feature {
    fn into(self) -> usize {
        self as usize
    }
}

impl Into<usize> for &Feature {
    fn into(self) -> usize {
        *self as usize
    }
}

#[derive(Debug)]
pub struct FeatureVector<B: ByteSlice, T>(usize, B, PhantomData<T>);

impl<B, T> FeatureVector<B, T>
where
    B: ByteSlice,
    T: Into<usize>,
{
    pub fn parse(num_features: usize, src: B) -> Option<FeatureVector<B, T>> {
        if num_features == 0 || src.len() < num_features * 2 {
            None
        } else {
            Some(FeatureVector(num_features, src, PhantomData))
        }
    }

    pub fn get(&self, feature: T) -> Option<u16> {
        let idx: usize = feature.into();

        if idx < self.0 {
            let value = (&self.1[idx * 2..]).read_u16::<LittleEndian>().unwrap();
            // FIXME drop unset_feature somehow
            if value == Feature::UNSET_FEATURE {
                None
            } else {
                Some(value)
            }
        } else {
            None
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.1.as_bytes()
    }
}

impl<B: ByteSliceMut, T> FeatureVector<B, T>
where
    T: Into<usize>,
{
    pub fn set(&mut self, feature: T, value: u16) -> Result<(), &'static str> {
        let idx = feature.into();
        if idx < self.0 {
            self.1[idx * 2..idx * 2 + 2].copy_from_slice(value.as_bytes());
            Ok(())
        } else {
            Err("Feature maps to index larger than known buffer")
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn init_ok() {
        let mut buf = Feature::EMPTY_BUFFER.to_vec();
        let vector = FeatureVector::parse(Feature::LENGTH, buf.as_mut_slice()).unwrap();

        for feat in Feature::VALUES.iter() {
            assert_eq!(None, vector.get(feat));
        }
    }

    #[test]
    fn get_set() {
        let mut buf = Feature::EMPTY_BUFFER.to_vec();
        let mut vector = FeatureVector::parse(Feature::LENGTH, buf.as_mut_slice()).unwrap();

        for feat in Feature::VALUES.iter() {
            vector.set(feat, *feat as u16).unwrap();
            assert_eq!(Some(*feat as u16), vector.get(feat));
        }
    }

    #[test]
    fn cannot_set_over_num_features() {
        let mut buf = Feature::EMPTY_BUFFER.to_vec();
        let mut features = FeatureVector::parse(1, buf.as_mut_slice()).unwrap();

        // NumIngredients maps to 0, so it should work
        let a = Feature::NumIngredients;
        // Anything else shouldn't
        let b = Feature::FatContent;
        let c = Feature::PrepTime;

        features.set(a, 10).unwrap();

        assert!(features.set(b, 10).is_err());
        assert!(features.set(c, 10).is_err());
    }

    #[test]
    fn cannot_create_with_num_features_zero() {
        let mut buf = Feature::EMPTY_BUFFER.to_vec();
        let opt_pv: Option<FeatureVector<_, Feature>> = FeatureVector::parse(0, buf.as_mut_slice());
        assert!(opt_pv.is_none());
    }

    #[test]
    fn example_usage() {
        let mut buf = Feature::EMPTY_BUFFER.to_vec();

        let mut features = FeatureVector::parse(Feature::LENGTH, buf.as_mut_slice()).unwrap();

        // Just to minimize typing
        let a = Feature::NumIngredients;
        let b = Feature::FatContent;
        let c = Feature::PrepTime;

        features.set(a, 10).unwrap();
        features.set(b, 60).unwrap();

        assert_eq!(Some(10), features.get(a));
        assert_eq!(Some(60), features.get(b));
        assert_eq!(None, features.get(c));

        let bytes = features.as_bytes();
        assert_eq!(Feature::EMPTY_BUFFER.len(), bytes.len());

        let mut from_bytes_buf = Feature::EMPTY_BUFFER.to_vec();
        from_bytes_buf.copy_from_slice(&bytes[..]);

        let opt: Option<FeatureVector<_, Feature>> =
            FeatureVector::parse(Feature::LENGTH, from_bytes_buf.as_slice());

        assert!(opt.is_some());

        let from_bytes = opt.unwrap();

        assert_eq!(bytes, from_bytes.as_bytes());
    }

}
