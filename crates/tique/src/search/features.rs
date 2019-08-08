use byteorder::LittleEndian;
use serde::{Deserialize, Serialize};
use zerocopy::{AsBytes, ByteSlice, ByteSliceMut, LayoutVerified, U16};

type FeatureValue = U16<LittleEndian>;

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
    ];

    pub const UNSET_FEATURE: u16 = std::u16::MAX;

    pub const EMPTY_BUFFER: [u8; Feature::LENGTH * 2] = [std::u8::MAX; Feature::LENGTH * 2];
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

            Feature::DietKeto => "keto",
            Feature::DietLowCarb => "lowcarb",
            Feature::DietVegan => "vegan",
            Feature::DietVegetarian => "vegetarian",
            Feature::DietPaleo => "paleo",
        })
    }
}

#[derive(Debug)]
pub struct FeatureVector<B: ByteSlice>(LayoutVerified<B, [FeatureValue; Feature::LENGTH]>);

impl<B: ByteSlice> FeatureVector<B> {
    pub fn parse(src: B) -> Option<FeatureVector<B>> {
        let (inner, _) = LayoutVerified::new_from_prefix(src)?;
        Some(FeatureVector(inner))
    }

    pub fn get(&self, feature: &Feature) -> Option<FeatureValue> {
        let idx = *feature as usize;
        assert!(idx < Feature::LENGTH);

        let FeatureVector(inner) = self;

        let value = inner[idx];
        if value == FeatureValue::new(Feature::UNSET_FEATURE) {
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
        assert!(idx < Feature::LENGTH);
        let FeatureVector(inner) = self;

        inner[idx] = FeatureValue::new(value);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn init_ok() {
        let mut buf = Feature::EMPTY_BUFFER.to_vec();
        let vector = FeatureVector::parse(buf.as_mut_slice()).unwrap();

        Feature::VALUES
            .iter()
            .map(|feat| assert_eq!(None, vector.get(feat)));
    }

    #[test]
    fn get_set() {
        let mut buf = Feature::EMPTY_BUFFER.to_vec();
        let mut vector = FeatureVector::parse(buf.as_mut_slice()).unwrap();

        Feature::VALUES.iter().map(|feat| {
            vector.set(feat, *feat as u16);
            assert_eq!(Some(*feat as u16), vector.get(feat).map(|v| v.get()));
        });
    }

    #[test]
    fn example_usage() {
        let mut buf = Feature::EMPTY_BUFFER.to_vec();

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
        assert_eq!(Feature::EMPTY_BUFFER.len(), bytes.len());

        let mut from_bytes_buf = Feature::EMPTY_BUFFER.to_vec();
        from_bytes_buf.copy_from_slice(&bytes[..]);

        let opt = FeatureVector::parse(from_bytes_buf.as_slice());

        assert!(opt.is_some());

        let from_bytes = opt.unwrap();

        assert_eq!(bytes, from_bytes.as_bytes());
    }

}
