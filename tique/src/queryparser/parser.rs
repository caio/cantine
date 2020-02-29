use nom::{
    self,
    branch::alt,
    bytes::complete::take_while1,
    character::complete::{char as is_char, multispace0},
    combinator::{map, map_res},
    multi::many0,
    sequence::{delimited, preceded, separated_pair},
    IResult,
};

#[derive(Debug, PartialEq)]
pub struct RawQuery<'a> {
    pub input: &'a str,
    pub is_negated: bool,
    pub is_phrase: bool,
    pub field_name: Option<&'a str>,
}

pub const FIELD_SEP: char = ':';

impl<'a> RawQuery<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            is_negated: false,
            is_phrase: false,
            field_name: None,
        }
    }

    pub fn negated(mut self) -> Self {
        debug_assert!(!self.is_negated);
        self.is_negated = true;
        self
    }

    pub fn phrase(mut self) -> Self {
        debug_assert!(!self.is_phrase);
        self.is_phrase = true;
        self
    }

    pub fn with_field(mut self, name: &'a str) -> Self {
        debug_assert_eq!(None, self.field_name);
        self.field_name = Some(name);
        self
    }
}

pub trait FieldNameValidator {
    fn check(&self, field_name: &str) -> bool;
}

impl<T> FieldNameValidator for Vec<T>
where
    T: for<'a> PartialEq<&'a str>,
{
    fn check(&self, field_name: &str) -> bool {
        self.iter().any(|item| item == &field_name)
    }
}

impl FieldNameValidator for bool {
    fn check(&self, _field_name: &str) -> bool {
        *self
    }
}

pub fn parse_query(input: &str) -> IResult<&str, Vec<RawQuery>> {
    parse_query_with_fields(input, &false)
}

pub fn parse_query_with_fields<'a, C: FieldNameValidator>(
    input: &'a str,
    validator: &'a C,
) -> IResult<&'a str, Vec<RawQuery<'a>>> {
    many0(delimited(
        multispace0,
        alt((
            |input| negated_query(input, validator),
            |input| field_prefixed_query(input, validator),
            any_field_query,
        )),
        multispace0,
    ))(input)
}

fn negated_query<'a, C: FieldNameValidator>(
    input: &'a str,
    validator: &'a C,
) -> IResult<&'a str, RawQuery<'a>> {
    map(
        preceded(
            is_char('-'),
            alt((
                |input| field_prefixed_query(input, validator),
                any_field_query,
            )),
        ),
        |query| query.negated(),
    )(input)
}

fn field_prefixed_query<'a, C: FieldNameValidator>(
    input: &'a str,
    validator: &'a C,
) -> IResult<&'a str, RawQuery<'a>> {
    map_res(
        separated_pair(
            take_while1(|c| c != FIELD_SEP && is_term_char(c)),
            is_char(FIELD_SEP),
            any_field_query,
        ),
        |(name, term)| {
            if validator.check(name) {
                Ok(term.with_field(name))
            } else {
                Err("Invalid field")
            }
        },
    )(input)
}

fn any_field_query(input: &str) -> IResult<&str, RawQuery> {
    alt((parse_phrase, parse_term))(input)
}

fn parse_phrase(input: &str) -> IResult<&str, RawQuery> {
    map(
        delimited(is_char('"'), take_while1(|c| c != '"'), is_char('"')),
        |s| RawQuery::new(s).phrase(),
    )(input)
}

fn parse_term(input: &str) -> IResult<&str, RawQuery> {
    map(take_while1(is_term_char), RawQuery::new)(input)
}

fn is_term_char(c: char) -> bool {
    !(c == ' ' || c == '\t' || c == '\r' || c == '\n')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_extraction() {
        assert_eq!(parse_query("gula"), Ok(("", vec![RawQuery::new("gula")])));
    }

    #[test]
    fn not_term_extraction() {
        assert_eq!(
            parse_query("-ads"),
            Ok(("", vec![RawQuery::new("ads").negated()]))
        )
    }

    #[test]
    fn phrase_extraction() {
        assert_eq!(
            parse_query("\"gula recipes\""),
            Ok(("", vec![RawQuery::new("gula recipes").phrase()]))
        );
    }

    #[test]
    fn not_phrase_extraction() {
        assert_eq!(
            parse_query("-\"ads and tracking\""),
            Ok((
                "",
                vec![RawQuery::new("ads and tracking").negated().phrase()]
            ))
        );
    }

    #[test]
    fn parse_query_works() {
        assert_eq!(
            parse_query(" peanut -\"peanut butter\" -sugar "),
            Ok((
                "",
                vec![
                    RawQuery::new("peanut"),
                    RawQuery::new("peanut butter").phrase().negated(),
                    RawQuery::new("sugar").negated()
                ]
            ))
        );
    }

    #[test]
    fn check_field_behavior() {
        let input = "title:banana ingredient:sugar";

        // No field support: fields end up in the term
        assert_eq!(
            parse_query_with_fields(input, &false),
            Ok((
                "",
                vec![
                    RawQuery::new("title:banana"),
                    RawQuery::new("ingredient:sugar"),
                ]
            ))
        );

        // Any field support: field names are not valitdated at all
        assert_eq!(
            parse_query_with_fields(input, &true),
            Ok((
                "",
                vec![
                    RawQuery::new("banana").with_field("title"),
                    RawQuery::new("sugar").with_field("ingredient"),
                ]
            ))
        );

        // Strict field support: known fields are identified, unknown
        // ones are part of the term
        assert_eq!(
            parse_query_with_fields(input, &vec!["ingredient"]),
            Ok((
                "",
                vec![
                    RawQuery::new("title:banana"),
                    RawQuery::new("sugar").with_field("ingredient"),
                ]
            ))
        );
    }

    #[test]
    fn garbage_handling() {
        assert_eq!(
            parse_query_with_fields("- -field: -\"\" body:\"\"", &true),
            Ok((
                "",
                vec![
                    RawQuery::new("-"),
                    RawQuery::new("field:").negated(),
                    RawQuery::new("\"\"").negated(),
                    RawQuery::new("\"\"").with_field("body"),
                ]
            ))
        );
    }

    #[test]
    fn parse_term_with_field() {
        assert_eq!(
            parse_query_with_fields("title:potato:queen -instructions:mash -body:\"how to fail\" ingredient:\"golden peeler\"", &true),
            Ok((
                "",
                vec![
                    RawQuery::new("potato:queen").with_field("title"),
                    RawQuery::new("mash").with_field("instructions").negated(),
                    RawQuery::new("how to fail").with_field("body").negated().phrase(),
                    RawQuery::new("golden peeler").with_field("ingredient").phrase()
                ]
            ))
        );
    }
}
