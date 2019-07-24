use nom::{
    self,
    branch::alt,
    bytes::complete::take_while1,
    character::complete::{char as is_char, multispace0},
    combinator::map,
    multi::{many0, many1},
    sequence::{delimited, preceded},
    IResult,
};

#[derive(Debug, PartialEq)]
enum KnownQuery<'a> {
    Negated(Box<KnownQuery<'a>>),
    Phrase(Vec<KnownQuery<'a>>),
    Term(&'a str),
}

fn parse_term(input: &str) -> IResult<&str, KnownQuery> {
    map(take_while1(is_term_char), |t| KnownQuery::Term(t))(input)
}

fn is_term_char(c: char) -> bool {
    // XXX This never allows dangling quotes
    !(c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '"')
}

// XXX no support for escaping the quotes
fn parse_phrase(input: &str) -> IResult<&str, KnownQuery> {
    map(
        delimited(
            is_char('"'),
            many1(delimited(multispace0, parse_term, multispace0)),
            is_char('"'),
        ),
        |p| KnownQuery::Phrase(p),
    )(input)
}

fn parse_negated(input: &str) -> IResult<&str, KnownQuery> {
    map(
        preceded(is_char('-'), alt((parse_phrase, parse_term))),
        |inner| KnownQuery::Negated(Box::new(inner)),
    )(input)
}

fn parse_query(input: &str) -> IResult<&str, Vec<KnownQuery>> {
    many0(delimited(
        multispace0,
        alt((parse_negated, parse_phrase, parse_term)),
        multispace0,
    ))(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::KnownQuery::*;

    #[test]
    fn term_extraction() {
        assert_eq!(parse_term("gula"), Ok(("", Term("gula"))));
    }

    #[test]
    fn phrase_extraction() {
        assert_eq!(
            parse_phrase("\"  gula     recipes  \""),
            Ok(("", Phrase(vec![Term("gula"), Term("recipes")])))
        );
    }

    #[test]
    fn empty_term_not_allowed() {
        assert!(parse_term("").is_err());
    }

    #[test]
    fn empty_phrase_not_allowed() {
        assert!(parse_phrase("\"\"").is_err());
    }

    #[test]
    fn negated_extraction() {
        assert_eq!(
            parse_negated("-hunger"),
            Ok(("", Negated(Box::new(Term("hunger")))))
        );
        assert_eq!(
            parse_negated("-\"ads\""),
            Ok(("", Negated(Box::new(Phrase(vec![Term("ads")])))))
        );
    }

    #[test]
    fn negation_does_not_allow_spaces() {
        assert!(parse_negated("- bacon").is_err());
        assert!(parse_negated("- \"peanut butter\"").is_err());
    }

    #[test]
    fn negation_requires_more_tokens() {
        assert!(parse_negated("-").is_err());
        assert!(parse_negated("-\"\"").is_err());
    }

    #[test]
    fn parse_query_works() {
        assert_eq!(
            parse_query(" peanut -\" peanut butter  \" -sugar "),
            Ok((
                "",
                vec![
                    Term("peanut"),
                    Negated(Box::new(Phrase(vec![Term("peanut"), Term("butter")]))),
                    Negated(Box::new(Term("sugar")))
                ]
            ))
        );
    }

    #[test]
    fn parse_query_accepts_empty_string() {
        assert_eq!(parse_query(""), Ok(("", vec![])));
        assert_eq!(parse_query(" "), Ok((" ", vec![])));
    }

    #[test]
    fn parsed_solo_negation_token_becomes_term() {
        assert_eq!(
            parse_query("- potato"),
            Ok(("", vec![Term("-"), Term("potato")]))
        )
    }
}
