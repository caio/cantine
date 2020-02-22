use nom::{
    self,
    branch::alt,
    bytes::complete::take_while1,
    character::complete::{char as is_char, multispace0},
    combinator::map,
    multi::many0,
    sequence::{delimited, preceded, separated_pair},
    IResult,
};

#[derive(Debug, PartialEq)]
pub(crate) struct RawQuery<'a> {
    pub input: &'a str,
    pub is_negated: bool,
    pub is_phrase: bool,
    pub field_name: Option<&'a str>,
}

impl<'a> RawQuery<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            is_negated: false,
            is_phrase: false,
            field_name: None,
        }
    }

    fn negated(mut self) -> Self {
        debug_assert!(!self.is_negated);
        self.is_negated = true;
        self
    }

    fn phrase(mut self) -> Self {
        debug_assert!(!self.is_phrase);
        self.is_phrase = true;
        self
    }

    fn with_field(mut self, name: &'a str) -> Self {
        debug_assert_eq!(None, self.field_name);
        self.field_name = Some(name);
        self
    }
}

pub(crate) fn parse_query(input: &str) -> IResult<&str, Vec<RawQuery>> {
    many0(delimited(
        multispace0,
        alt((negated_query, field_prefixed_query, any_field_query)),
        multispace0,
    ))(input)
}

fn negated_query(input: &str) -> IResult<&str, RawQuery> {
    map(
        preceded(is_char('-'), alt((field_prefixed_query, any_field_query))),
        |query| query.negated(),
    )(input)
}

fn field_prefixed_query(input: &str) -> IResult<&str, RawQuery> {
    map(
        separated_pair(
            take_while1(|c| c != ':' && is_term_char(c)),
            is_char(':'),
            any_field_query,
        ),
        |(name, term)| term.with_field(name),
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
    fn garbage_handling() {
        assert_eq!(
            parse_query("- -field: -\"\" body:\"\""),
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
            parse_query("title:potato:queen -instructions:mash -body:\"how to fail\" ingredient:\"golden peeler\""),
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
