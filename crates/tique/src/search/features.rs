use byteorder::{ByteOrder, LittleEndian};
use std::marker::PhantomData;
use zerocopy::{AsBytes, ByteSlice, ByteSliceMut};

pub trait IsUnset<T> {
    fn is_unset(value: T) -> bool;
}

impl IsUnset<u16> for u16 {
    fn is_unset(value: u16) -> bool {
        value == std::u16::MAX
    }
}

#[derive(Debug)]
pub struct FeatureVector<B: ByteSlice, T>(usize, B, PhantomData<T>);

impl<B, T> FeatureVector<B, T>
where
    B: ByteSlice,
    T: Into<usize> + IsUnset<u16>,
{
    pub fn parse(num_features: usize, src: B) -> Option<FeatureVector<B, T>> {
        if num_features == 0 || src.len() < num_features * 2 {
            None
        } else {
            Some(FeatureVector(num_features, src, PhantomData))
        }
    }

    pub fn read_value(&self, buf: &[u8]) -> u16 {
        LittleEndian::read_u16(buf)
    }

    pub fn get(&self, feature: T) -> Option<u16> {
        let idx: usize = feature.into();

        if idx < self.0 {
            let value = self.read_value(&self.1[idx * 2..]);
            if T::is_unset(value) {
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

    const LENGTH: usize = 4;

    const EMPTY_BUFFER: [u8; 8] = [std::u8::MAX; LENGTH * 2];

    // Using usize as a feature
    impl IsUnset<u16> for usize {
        fn is_unset(val: u16) -> bool {
            val == std::u16::MAX
        }
    }

    #[test]
    fn init_ok() {
        let mut buf = EMPTY_BUFFER.to_vec();
        let vector = FeatureVector::parse(LENGTH, buf.as_mut_slice()).unwrap();

        for feat in 0..LENGTH {
            assert_eq!(None, vector.get(feat));
        }
    }

    #[test]
    fn get_set() {
        let mut buf = EMPTY_BUFFER.to_vec();
        let mut vector = FeatureVector::parse(LENGTH, buf.as_mut_slice()).unwrap();

        for feat in 0..LENGTH {
            vector.set(feat, feat as u16).unwrap();
            assert_eq!(Some(feat as u16), vector.get(feat));
        }
    }

    #[test]
    fn cannot_set_over_num_features() {
        let mut buf = EMPTY_BUFFER.to_vec();
        let mut features = FeatureVector::parse(1, buf.as_mut_slice()).unwrap();

        // Feature idx 0 should work
        features.set(0usize, 10).unwrap();

        // Anything else shouldn't
        assert!(features.set(1, 10).is_err());
        assert!(features.set(2, 10).is_err());
    }

    #[test]
    fn cannot_create_with_num_features_zero() {
        let mut buf = EMPTY_BUFFER.to_vec();
        let opt_pv: Option<FeatureVector<_, usize>> = FeatureVector::parse(0, buf.as_mut_slice());
        assert!(opt_pv.is_none());
    }

    #[test]
    fn example_usage() {
        let mut buf = EMPTY_BUFFER.to_vec();

        let mut features = FeatureVector::parse(LENGTH, buf.as_mut_slice()).unwrap();

        features.set(0usize, 10).unwrap();
        features.set(1, 60).unwrap();

        assert_eq!(Some(10), features.get(0));
        assert_eq!(Some(60), features.get(1));
        assert_eq!(None, features.get(2));

        let bytes = features.as_bytes();
        assert_eq!(EMPTY_BUFFER.len(), bytes.len());

        let mut from_bytes_buf = EMPTY_BUFFER.to_vec();
        from_bytes_buf.copy_from_slice(&bytes[..]);

        let opt: Option<FeatureVector<_, usize>> =
            FeatureVector::parse(LENGTH, from_bytes_buf.as_slice());

        assert!(opt.is_some());

        let from_bytes = opt.unwrap();

        assert_eq!(bytes, from_bytes.as_bytes());
    }

}
