use super::raw::{parse_query, FieldNameValidator, RawQuery};

use tantivy::{
    self,
    query::{AllQuery, BooleanQuery, BoostQuery, Occur, PhraseQuery, Query, TermQuery},
    schema::{Field, IndexRecordOption},
    tokenizer::TextAnalyzer,
    Index, Result, Term,
};

pub struct QueryParser {
    state: Vec<(Option<String>, Option<f32>, Interpreter)>,
    default_indices: Vec<usize>,
}

impl QueryParser {
    pub fn new(index: &Index, fields: Vec<Field>) -> Result<Self> {
        let schema = index.schema();

        let mut parser = QueryParser {
            default_indices: (0..fields.len()).collect(),
            state: Vec::with_capacity(fields.len()),
        };

        for field in fields.into_iter() {
            parser.state.push((
                Some(schema.get_field_name(field).to_owned()),
                None,
                Interpreter {
                    field,
                    analyzer: index.tokenizer_for_field(field)?,
                },
            ));
        }

        Ok(parser)
    }

    pub fn set_boost(&mut self, field: Field, boost: Option<f32>) {
        if let Some(row) = self
            .position_by_field(field)
            .map(|pos| self.state.get_mut(pos))
            .flatten()
        {
            row.1 = boost;
        }
    }

    pub fn set_name(&mut self, field: Field, name: Option<String>) {
        if let Some(row) = self
            .position_by_field(field)
            .map(|pos| self.state.get_mut(pos))
            .flatten()
        {
            row.0 = name;
        }
    }

    pub fn set_default_fields(&mut self, fields: Vec<Field>) {
        let mut indices = Vec::with_capacity(fields.len());
        for field in fields.into_iter() {
            if let Some(idx) = self.position_by_field(field) {
                indices.push(idx);
            }
        }
        indices.sort();
        self.default_indices = indices;
    }

    fn position_by_name(&self, field_name: &str) -> Option<usize> {
        self.state
            .iter()
            .position(|(opt_name, _opt_boost, _interpreter)| {
                opt_name
                    .as_ref()
                    .map(|name| name == field_name)
                    .unwrap_or(false)
            })
    }

    fn position_by_field(&self, field: Field) -> Option<usize> {
        self.state
            .iter()
            .position(|(_opt_name, _opt_boost, interpreter)| interpreter.field == field)
    }

    pub fn parse(&self, input: &str) -> Option<Box<dyn Query>> {
        let (_, parsed) = parse_query(input, self).ok()?;

        match parsed.len() {
            0 => None,
            1 => {
                let raw = &parsed[0];
                let query = self.query_from_raw(&raw)?;

                if raw.occur == Occur::MustNot {
                    Some(Box::new(BooleanQuery::from(vec![
                        (Occur::MustNot, query),
                        (Occur::Must, Box::new(AllQuery)),
                    ])))
                } else {
                    Some(query)
                }
            }
            _ => {
                let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

                let mut num_must_not = 0;
                for tok in parsed {
                    if let Some(query) = self.query_from_raw(&tok) {
                        if tok.occur == Occur::MustNot {
                            num_must_not += 1;
                        }

                        subqueries.push((tok.occur, query));
                    }
                }

                // Detect boolean queries with only MustNot clauses
                // and appends a AllQuery otherwise the resulting
                // query will match nothing
                if num_must_not > 1 && num_must_not == subqueries.len() {
                    subqueries.push((Occur::Must, Box::new(AllQuery)));
                }

                match subqueries.len() {
                    0 => None,
                    1 => Some(subqueries.pop().expect("Element always present").1),
                    _ => Some(Box::new(BooleanQuery::from(subqueries))),
                }
            }
        }
    }

    fn query_from_raw(&self, raw_query: &RawQuery) -> Option<Box<dyn Query>> {
        let indices = if let Some(position) = raw_query
            .field_name
            .map(|field_name| self.position_by_name(field_name))
            .flatten()
        {
            vec![position]
        } else {
            self.default_indices.clone()
        };

        let queries: Vec<Box<dyn Query>> = indices
            .into_iter()
            .flat_map(|i| self.state.get(i))
            .flat_map(|(_, boost, interpreter)| {
                interpreter.to_query(raw_query).map(|query| {
                    if let Some(val) = boost {
                        Box::new(BoostQuery::new(query, *val))
                    } else {
                        query
                    }
                })
            })
            .collect();

        match queries.len() {
            0 => None,
            1 => Some(queries.into_iter().nth(0).unwrap()),
            _ => Some(Box::new(BooleanQuery::from(
                queries
                    .into_iter()
                    .map(|q| (Occur::Should, q))
                    .collect::<Vec<_>>(),
            ))),
        }
    }
}

impl FieldNameValidator for QueryParser {
    fn check(&self, field_name: &str) -> bool {
        self.state
            .iter()
            .any(|(opt_name, _opt_boost, _interpreter)| {
                opt_name
                    .as_ref()
                    .map(|name| name == field_name)
                    .unwrap_or(false)
            })
    }
}

struct Interpreter {
    field: Field,
    analyzer: TextAnalyzer,
}

impl Interpreter {
    fn to_query(&self, raw_query: &RawQuery) -> Option<Box<dyn Query>> {
        let mut terms = Vec::new();
        let mut stream = self.analyzer.token_stream(raw_query.input);

        stream.process(&mut |token| {
            terms.push(Term::from_field_text(self.field, &token.text));
        });

        if terms.is_empty() {
            return None;
        }

        let query: Box<dyn Query> = if terms.len() == 1 {
            Box::new(TermQuery::new(
                terms.pop().unwrap(),
                IndexRecordOption::WithFreqs,
            ))
        } else if raw_query.is_phrase {
            Box::new(PhraseQuery::new(terms))
        } else {
            // An analyzer might emit multiple tokens even if the
            // raw parser only got one (say: raw takes "word", but
            // analyzer is actually a char tokenizer)
            Box::new(BooleanQuery::new_multiterms_query(terms))
        };

        Some(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tantivy::tokenizer::TokenizerManager;

    fn test_interpreter() -> Interpreter {
        Interpreter {
            field: Field::from_field_id(0),
            analyzer: TokenizerManager::default().get("en_stem").unwrap(),
        }
    }

    #[test]
    fn empty_raw_is_none() {
        assert!(test_interpreter().to_query(&RawQuery::new("")).is_none());
    }

    #[test]
    fn simple_raw_is_termquery() {
        let query = test_interpreter()
            .to_query(&RawQuery::new("word"))
            .expect("parses to a Some(Query)");

        assert!(query.as_any().downcast_ref::<TermQuery>().is_some());
    }

    #[test]
    fn phrase_raw_is_phrasequery() {
        let query = test_interpreter()
            .to_query(&RawQuery::new("sweet potato").phrase())
            .expect("parses to a Some(Query)");

        assert!(query.as_any().downcast_ref::<PhraseQuery>().is_some());
    }

    #[test]
    fn single_word_raw_phrase_is_termquery() {
        let query = test_interpreter()
            .to_query(&RawQuery::new("single").phrase())
            .expect("parses to a Some(Query)");

        assert!(query.as_any().downcast_ref::<TermQuery>().is_some());
    }

    fn single_field_test_parser() -> QueryParser {
        QueryParser {
            default_indices: vec![0],
            state: vec![(
                None,
                None,
                Interpreter {
                    field: Field::from_field_id(0),
                    analyzer: TokenizerManager::default().get("en_stem").unwrap(),
                },
            )],
        }
    }

    #[test]
    fn empty_query_results_in_none() {
        assert!(single_field_test_parser().parse("").is_none());
    }

    use tantivy::{
        collector::TopDocs,
        doc,
        schema::{SchemaBuilder, TEXT},
        DocAddress,
    };

    #[test]
    fn index_integration() -> Result<()> {
        let mut builder = SchemaBuilder::new();
        let title = builder.add_text_field("title", TEXT);
        let plot = builder.add_text_field("plot", TEXT);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        let doc_across = DocAddress(0, 0);
        writer.add_document(doc!(
            title => "Across the Universe",
            plot => "Musical based on The Beatles songbook and set in the 60s England, \
                    America, and Vietnam. The love story of Lucy and Jude is intertwined \
                    with the anti-war movement and social protests of the 60s."
        ));

        let doc_moulin = DocAddress(0, 1);
        writer.add_document(doc!(
            title => "Moulin Rouge!",
            plot => "A poet falls for a beautiful courtesan whom a jealous duke covets in \
                    this stylish musical, with music drawn from familiar 20th century sources."
        ));

        let doc_once = DocAddress(0, 2);
        writer.add_document(doc!(
            title => "Once",
            plot => "A modern-day musical about a busker and an immigrant and their eventful\
                    week in Dublin, as they write, rehearse and record songs that tell their \
                    love story."
        ));

        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let parser = QueryParser::new(&index, vec![title, plot])?;

        let search = |input, limit| {
            let query = parser.parse(input).expect("given input yields Some()");
            searcher
                .search(&query, &TopDocs::with_limit(limit))
                .expect("working index")
        };

        let found = search("+title:Once musical", 2);
        // Even if "musical" matches every document,
        // there's a MUST query that only one matches
        assert_eq!(1, found.len());
        assert_eq!(doc_once, found[0].1);

        let found = search("\"the beatles\"", 1);
        assert!(!found.is_empty());
        assert_eq!(doc_across, found[0].1);

        // Purely negative queries should work too
        for input in &["-story -love", "-\"love story\""] {
            let found = search(input, 3);
            assert_eq!(1, found.len());
            assert_eq!(doc_moulin, found[0].1);

            let found = search("-music -", 3);
            assert_eq!(1, found.len());
            assert_eq!(doc_moulin, found[0].1);
        }

        Ok(())
    }

    #[test]
    fn field_boosting() -> Result<()> {
        let mut builder = SchemaBuilder::new();
        let field_a = builder.add_text_field("a", TEXT);
        let field_b = builder.add_text_field("b", TEXT);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        writer.add_document(doc!(
            field_a => "bar",
            field_b => "foo baz",
        ));

        writer.add_document(doc!(
            field_a => "foo",
            field_b => "bar",
        ));

        writer.add_document(doc!(
            field_a => "bar",
            field_b => "foo",
        ));

        writer.commit()?;

        let mut parser = QueryParser::new(&index, vec![field_a, field_b])?;

        let input = "foo baz";
        let normal_query = parser.parse(&input).unwrap();

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let found = searcher.search(&normal_query, &TopDocs::with_limit(3))?;
        assert_eq!(3, found.len());
        // the first doc matches perfectly on `field_b`
        assert_eq!(DocAddress(0, 0), found[0].1);

        parser.set_boost(field_a, Some(1.5));
        let boosted_query = parser.parse(&input).unwrap();

        let found = searcher.search(&boosted_query, &TopDocs::with_limit(3))?;
        assert_eq!(3, found.len());
        // the first doc matches perfectly on field_b
        // but now matching on `field_a` is super important
        assert_eq!(DocAddress(0, 1), found[0].1);

        Ok(())
    }
}
