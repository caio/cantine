use byteorder::{ByteOrder, NativeEndian};
use std::mem::size_of;
use zerocopy::{AsBytes, ByteSlice, ByteSliceMut};

#[derive(Debug)]
pub struct FeatureVector<B: ByteSlice, T>(usize, B, Option<T>);

pub type FeatureValue = u16;

macro_rules! feature_vector {
    ($t: ty, $reader: expr) => {
        impl<B: ByteSlice> FeatureVector<B, $t> {
            pub fn parse(
                src: B,
                num_features: usize,
                unset_value: Option<$t>,
            ) -> Option<FeatureVector<B, $t>> {
                if num_features == 0 || src.len() < num_features * size_of::<$t>() {
                    None
                } else {
                    Some(FeatureVector(num_features, src, unset_value))
                }
            }

            pub fn get(&self, feature: usize) -> Option<$t> {
                if feature >= self.0 {
                    return None;
                }

                let start_offset = feature * size_of::<$t>();
                let end_offset = start_offset + size_of::<$t>();

                let value = $reader(&self.1[start_offset..end_offset]);

                if let Some(unset) = &self.2 {
                    if unset == &value {
                        None
                    } else {
                        Some(value)
                    }
                } else {
                    Some(value)
                }
            }

            pub fn as_bytes(&self) -> &[u8] {
                self.1.as_bytes()
            }
        }

        impl<B: ByteSliceMut> FeatureVector<B, $t> {
            pub fn set(&mut self, feature: usize, value: $t) -> Result<(), &'static str> {
                if feature < self.0 {
                    let size = size_of::<$t>();
                    let start_offset = feature * size;
                    self.1[start_offset..start_offset + size].copy_from_slice(value.as_bytes());
                    Ok(())
                } else {
                    Err("Feature maps to index larger than known buffer")
                }
            }
        }
    };
}

feature_vector!(u16, NativeEndian::read_u16);
feature_vector!(u32, NativeEndian::read_u32);
feature_vector!(u64, NativeEndian::read_u64);
feature_vector!(i16, NativeEndian::read_i16);
feature_vector!(i32, NativeEndian::read_i32);
feature_vector!(i64, NativeEndian::read_i64);

#[cfg(test)]
mod tests {

    use super::*;
    use std::u16::MAX;

    const LENGTH: usize = 4;
    const UNSET: Option<u16> = Some(MAX);

    fn empty_buf() -> Vec<u8> {
        vec![MAX; LENGTH].as_bytes().into()
    }

    #[test]
    fn init_ok() {
        let mut buf = empty_buf();
        let vector = FeatureVector::<_, u16>::parse(buf.as_mut_slice(), LENGTH, UNSET).unwrap();

        for feat in 0..LENGTH {
            assert_eq!(None, vector.get(feat));
        }
    }

    #[test]
    fn get_set() {
        let mut buf = empty_buf();
        let mut vector = FeatureVector::<_, u16>::parse(buf.as_mut_slice(), LENGTH, UNSET).unwrap();

        for feat in 0..LENGTH as u16 {
            vector.set(feat as usize, feat).unwrap();
            assert_eq!(Some(feat), vector.get(feat as usize));
        }
    }

    #[test]
    fn cannot_set_over_num_features() {
        let mut buf = empty_buf();
        let mut features = FeatureVector::<_, u16>::parse(buf.as_mut_slice(), 1, UNSET).unwrap();

        // Feature idx 0 should work
        features.set(0, 10).unwrap();

        // Anything else shouldn't
        assert!(features.set(1, 10).is_err());
        assert!(features.set(2, 10).is_err());
    }

    #[test]
    fn cannot_create_with_num_features_zero() {
        let mut buf = empty_buf();
        let opt_pv = FeatureVector::<_, u16>::parse(buf.as_mut_slice(), 0, None);
        assert!(opt_pv.is_none());
    }

    #[test]
    fn example_usage() {
        let mut buf = empty_buf();

        let mut features =
            FeatureVector::<_, u16>::parse(buf.as_mut_slice(), LENGTH, UNSET).unwrap();

        features.set(0, 10).unwrap();
        features.set(1, 60).unwrap();

        assert_eq!(Some(10), features.get(0));
        assert_eq!(Some(60), features.get(1));
        assert_eq!(None, features.get(2));

        let bytes = features.as_bytes();
        let mut from_bytes_buf = empty_buf();
        from_bytes_buf.copy_from_slice(&bytes[..]);

        let opt = FeatureVector::<_, u16>::parse(from_bytes_buf.as_slice(), LENGTH, UNSET);

        assert!(opt.is_some());

        let from_bytes = opt.unwrap();

        assert_eq!(bytes, from_bytes.as_bytes());
    }

    #[test]
    fn without_unset_smoke() {
        let mut buf = vec![0u8; LENGTH * 2];

        // When parsing without an unset value
        let mut fv = FeatureVector::<_, u16>::parse(buf.as_mut_slice(), LENGTH, None).unwrap();

        // gets always work
        for feat in 0..LENGTH {
            assert_eq!(fv.get(feat), Some(0));
        }

        assert_eq!(None, fv.get(5));

        fv.set(0, 42).unwrap();
        assert_eq!(Some(42), fv.get(0));
    }

    macro_rules! check_fv_get_set {
        ($t: ty, $vals: expr) => {
            let mut buf = vec![0u8; $vals.len() * size_of::<$t>()];
            let mut fv =
                FeatureVector::<_, $t>::parse(buf.as_mut_slice(), $vals.len(), Some(0)).unwrap();

            for feat in 0..$vals.len() {
                assert_eq!(None, fv.get(feat));
                fv.set(feat, $vals[feat]).unwrap();
                assert_eq!(Some($vals[feat]), fv.get(feat));
            }
        };
    }

    #[test]
    fn get_set_all_supported_types() {
        check_fv_get_set!(u16, [1, 2, 3, 4]);
        check_fv_get_set!(u32, [1, 2, 3, 4]);
        check_fv_get_set!(u64, [1, 2, 3, 4]);
        check_fv_get_set!(i16, [-1, 2, -3, 4]);
        check_fv_get_set!(i32, [-1, 2, -3, 4]);
        check_fv_get_set!(i64, [-1, 2, -3, 4]);
    }
}
