use std::{mem::size_of, ops::RangeInclusive};

use tantivy::{
    query::{BooleanQuery, Occur, Query, RangeQuery},
    schema::{Field, SchemaBuilder, INDEXED},
    Document, Result, TantivyError,
};

use super::{FeatureValue, FeatureVector};

#[derive(Clone)]
pub struct FeatureFields(Vec<Field>, Option<FeatureValue>);

// TODO make it all composable!
//      Interface should be able to:
//         - LOAD from tantivy::Index
//         - NEW from SchemaBuilder, and a prefix so I can actually reliably load?
//         - Persist parameters? Or require them for loading?
impl FeatureFields {
    // TODO try_from(index) -> FeatureFields
    // FIXME what about a default value?
    pub fn create(
        builder: &mut SchemaBuilder,
        prefix: &str,
        num_features: usize,
        unset_value: Option<FeatureValue>,
        // FIXME how to make them sortable? Wire from the outside?
        // DEFAULT VALUE + DEFAULT_MEANS_EMPTY
    ) -> FeatureFields {
        assert!(num_features > 0, "num_features must be >0");

        let mut fields = Vec::with_capacity(1 + num_features);

        fields.push(builder.add_bytes_field(&format!("{}fv", prefix)));
        // one for each feature, for filtering
        for i in 0..num_features {
            fields.push(builder.add_u64_field(&format!("{}feat_{}", prefix, i).as_str(), INDEXED));
        }

        FeatureFields(fields, unset_value)
    }

    pub fn feature_vector(&self) -> Field {
        self.0[0]
    }

    pub fn num_features(&self) -> usize {
        self.0.len() - 1
    }

    pub fn feature(&self, feat: usize) -> Option<Field> {
        if feat < self.num_features() {
            Some(self.0[1 + feat])
        } else {
            None
        }
    }

    pub fn interpret_request(&self, filters: &FilterRequest) -> Result<Box<dyn Query>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        for (feat, range) in filters {
            clauses.push((
                Occur::Must,
                Box::new(RangeQuery::new_u64(
                    self.feature(*feat).ok_or_else(|| {
                        TantivyError::SystemError("Unknown feature in filters".to_owned())
                    })?,
                    // XXX can't this be less awkward?
                    u64::from(*range.start())..u64::from(range.end() + 1),
                )),
            ));
        }

        let bq: BooleanQuery = clauses.into();
        Ok(Box::new(bq))
    }

    // FIXME collector should return the SearchMarker
    // TODO generic instead of u16 <> FeatureValue
    pub fn populate_document(
        &self,
        doc: &mut Document,
        features: Vec<(usize, FeatureValue)>,
    ) -> Result<()> {
        let num_features = self.num_features();

        if num_features != features.len() {
            return Err(TantivyError::InvalidArgument(format!(
                "Expected {} features, got {}",
                num_features,
                features.len()
            )));
        }

        let mut buf = vec![0; num_features * size_of::<u16>()];
        let mut fv: FeatureVector<_, u16> =
            FeatureVector::parse(buf.as_mut_slice(), num_features, None).unwrap();

        for (feat, value) in features {
            fv.set(feat, value).unwrap();
            if let Some(feature) = self.feature(feat) {
                doc.add_u64(feature, u64::from(value));
            } else {
                return Err(TantivyError::InvalidArgument(format!(
                    "Unknown feature {}",
                    feat
                )));
            }
        }

        doc.add_bytes(self.feature_vector(), fv.as_bytes().into());

        Ok(())
    }
}

pub type FilterRequest = Vec<(usize, RangeInclusive<FeatureValue>)>;

#[cfg(test)]
mod tests {

    use super::*;
    use tantivy::schema::Schema;

    fn test_fields(num_features: usize) -> (Schema, FeatureFields) {
        let mut builder = SchemaBuilder::new();
        let fields = FeatureFields::create(&mut builder, "prefix", num_features, None);
        (builder.build(), fields)
    }

    #[test]
    fn can_get_a_field_for_every_known_feature() {
        let num_features = 100;
        let (_, fields) = test_fields(num_features);

        for feat in 0..num_features {
            assert!(fields.feature(feat).is_some())
        }
    }

    #[test]
    fn feature_fields_structure() {
        let num_features = 10;
        let (schema, fields) = test_fields(num_features);
        let mut iter = schema.fields().iter();

        // The stored FeatureVector
        assert_eq!(
            schema.get_field_entry(fields.feature_vector()),
            iter.next().unwrap()
        );

        // Now come feature fields
        for feat in 0..num_features {
            assert_eq!(
                iter.next().unwrap(),
                schema.get_field_entry(fields.feature(feat).unwrap())
            );
        }

        // And there should be nothing else
        assert_eq!(None, iter.next());
    }
}
