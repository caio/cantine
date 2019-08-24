use crate::search::parser::{parse_query, Token};

use tantivy::{
    self,
    query::{AllQuery, BooleanQuery, Occur, PhraseQuery, Query, TermQuery},
    schema::{Field, IndexRecordOption},
    tokenizer::BoxedTokenizer,
    Term,
};

// TODO stop using tantivy's Result
type Result<T> = super::Result<T>;

pub struct QueryParser {
    field: Field,
    tokenizer: Box<dyn BoxedTokenizer>,
}

impl QueryParser {
    pub fn new(field: Field, tokenizer: Box<dyn BoxedTokenizer>) -> QueryParser {
        QueryParser { field, tokenizer }
    }

    pub fn parse(&self, input: &str) -> Result<Option<Box<dyn Query>>> {
        let (_, parsed) = parse_query(input)
            .map_err(|e| tantivy::TantivyError::InvalidArgument(format!("{:?}", e)))?;

        Ok(match parsed.len() {
            0 => Some(Box::new(AllQuery)),
            1 => self.query_from_token(&parsed[0])?,
            _ => {
                let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

                for tok in parsed {
                    if let Some(query) = self.query_from_token(&tok)? {
                        subqueries.push((Occur::Must, query));
                    }
                }

                let bq: BooleanQuery = subqueries.into();
                Some(Box::new(bq))
            }
        })
    }

    fn assemble_query(&self, text: &str, allow_phrase: bool) -> Result<Option<Box<dyn Query>>> {
        let tokens = self.tokenize(text);

        match &tokens[..] {
            [] => Ok(None),
            [(_, term)] => Ok(Some(Box::new(TermQuery::new(
                term.clone(),
                IndexRecordOption::WithFreqs,
            )))),
            _ => {
                if allow_phrase {
                    Ok(Some(Box::new(PhraseQuery::new_with_offset(tokens))))
                } else {
                    Err(tantivy::TantivyError::InvalidArgument(
                        "More than one token found but allow_phrase is false".to_owned(),
                    ))
                }
            }
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
    fn query_from_token(&self, token: &Token) -> Result<Option<Box<dyn Query>>> {
        match token {
            Token::Term(t) => self.assemble_query(t, false),

            Token::Phrase(p) => self.assemble_query(p, true),

            Token::NotTerm(t) => Ok(self
                .assemble_query(t, false)?
                .map(|inner| Self::negate_query(inner))),

            Token::NotPhrase(p) => Ok(self
                .assemble_query(p, true)?
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

    use tantivy::schema::{SchemaBuilder, TEXT};
    use tantivy::tokenizer::TokenizerManager;

    fn test_parser() -> QueryParser {
        let mut schema_builder = SchemaBuilder::new();
        let field = schema_builder.add_text_field("text", TEXT);
        QueryParser::new(field, TokenizerManager::default().get("en_stem").unwrap())
    }

    fn parsed(input: &str) -> Box<dyn Query> {
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

            let clauses = query.clauses();

            assert_eq!(2, clauses.len());
            // XXX First clause is the wrapped {Term,Phrase}Query

            // Second clause is the MatchAllDocs()
            let (occur, inner) = &clauses[1];
            assert_eq!(Occur::Must, *occur);
            assert!(inner.as_any().downcast_ref::<AllQuery>().is_some())
        }
    }

    #[test]
    fn cannot_assemble_phrase_when_allow_phrase_is_false() {
        assert!(test_parser().assemble_query("hello world", false).is_err());
    }
}
