use super::parser::{parse_query, KnownQuery};

use tantivy::{
    self,
    query::{AllQuery, BooleanQuery, Occur, PhraseQuery, Query, TermQuery},
    schema::{Field, IndexRecordOption},
    tokenizer::{BoxedTokenizer, TokenizerManager},
    Term,
};

type Result<T> = super::Result<T>;

struct QueryParser {
    field: Field,
    tokenizer: Box<BoxedTokenizer>,
}

impl QueryParser {
    pub fn new(field: Field) -> QueryParser {
        QueryParser {
            field: field,
            //schema: schema,
            tokenizer: TokenizerManager::default()
                .get("en_stem")
                .expect("cannot happen!"),
        }
    }

    pub fn parse(&self, input: &str) -> Result<Option<Box<dyn Query>>> {
        // XXX custom error module
        let (_, parsed) = parse_query(input)
            .map_err(|e| tantivy::TantivyError::InvalidArgument(format!("{:?}", e)))?;

        Ok(match parsed.len() {
            0 => Some(Box::new(AllQuery)),
            1 => self.query_from_token(&parsed[0])?,
            _ => {
                let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

                for tok in parsed {
                    self.query_from_token(&tok)?
                        .map(|query| subqueries.push((Occur::Must, query)));
                }

                let bq: BooleanQuery = subqueries.into();
                Some(Box::new(bq))
            }
        })
    }

    // FIXME make Result<>
    // FIXME add `accept_phrase: bool`
    fn assemble_query(&self, text: &str) -> Option<Box<dyn Query>> {
        let tokens = self.tokenize(text);

        match &tokens[..] {
            [] => None,
            [(_, term)] => Some(Box::new(TermQuery::new(
                term.clone(),
                IndexRecordOption::WithFreqs,
            ))),
            _ => Some(Box::new(PhraseQuery::new_with_offset(tokens))),
        }
    }

    //Not[Inner] queries are always [MatchAllDocs() - Inner]
    fn negate_query(inner: Box<dyn Query>) -> Box<dyn Query> {
        let subqueries: Vec<(Occur, Box<dyn Query>)> =
            vec![(Occur::MustNot, inner), (Occur::Must, Box::new(AllQuery))];

        let bq: BooleanQuery = subqueries.into();
        Box::new(bq)
    }

    // May result in Ok(None) because the tokenizer might give us nothing
    fn query_from_token(&self, token: &KnownQuery) -> Result<Option<Box<dyn Query>>> {
        match token {
            // FIXME this swallows a potential parse problem where parser.rs
            // found something to be a Term but after applying a tokenizer
            // it becomes a Phrase. Ditto for NotTerm()
            KnownQuery::Term(t) => Ok(self.assemble_query(t)),

            KnownQuery::Phrase(p) => Ok(self.assemble_query(p)),

            KnownQuery::NotTerm(t) => Ok(self
                .assemble_query(t)
                .map(|inner| Self::negate_query(inner))),

            KnownQuery::NotPhrase(p) => Ok(self
                .assemble_query(p)
                .map(|inner| Self::negate_query(inner))),
        }
    }

    fn tokenize(&self, phrase: &str) -> Vec<(usize, Term)> {
        let mut terms: Vec<(usize, Term)> = Vec::new();
        let mut stream = self.tokenizer.token_stream(phrase);

        stream.process(&mut |token| {
            let term = Term::from_field_text(self.field, &token.text);
            terms.push((token.position, term));
        });

        terms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tantivy::{
        self,
        schema::{SchemaBuilder, TEXT},
    };

    fn test_parser() -> QueryParser {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder.add_text_field("text", TEXT);
        QueryParser::new(field)
    }

    fn parsed(input: &str) -> Box<Query> {
        test_parser()
            .parse(input)
            .unwrap()
            .expect("Should have gotten Some(dyn Query)")
    }

    #[test]
    fn can_parse_term_query() {
        assert!(parsed("gula")
            .as_any()
            .downcast_ref::<TermQuery>()
            .is_some());
    }

    #[test]
    fn can_parse_phrase_query() {
        assert!(parsed(" \"gula recipes\" ")
            .as_any()
            .downcast_ref::<PhraseQuery>()
            .is_some());
    }

    #[test]
    fn single_term_phrase_query_becomes_term_query() {
        assert!(parsed(" \"gula\" ")
            .as_any()
            .downcast_ref::<TermQuery>()
            .is_some());
    }

    #[test]
    fn negation_works() {
        let input = vec!["-hunger", "-\"ads and tracking\""];

        for i in input {
            let p = parsed(i);
            let query = p
                .as_any()
                .downcast_ref::<BooleanQuery>()
                .expect("Must be a boolean query");

            let clauses = dbg!(query.clauses());

            assert_eq!(2, clauses.len());
            // XXX First clause is the wrapped {Term,Phrase}Query

            // Second clause is the MatchAllDocs()
            let (occur, inner) = &clauses[1];
            assert_eq!(Occur::Must, *occur);
            assert!(inner.as_any().downcast_ref::<AllQuery>().is_some())
        }
    }
}
