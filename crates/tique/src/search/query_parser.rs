use nom::{
    self,
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::{char as is_char, multispace0},
    combinator::map,
    multi::many0,
    sequence::{delimited, preceded},
    IResult,
};

#[derive(Debug, PartialEq)]
enum KnownQuery<'a> {
    NotPhrase(&'a str),
    NotTerm(&'a str),
    Phrase(&'a str),
    Term(&'a str),
}

fn parse_not_phrase(input: &str) -> IResult<&str, KnownQuery> {
    map(
        delimited(tag("-\""), take_while1(|c| c != '"'), is_char('"')),
        |r| KnownQuery::NotPhrase(r),
    )(input)
}

fn parse_phrase(input: &str) -> IResult<&str, KnownQuery> {
    map(
        delimited(is_char('"'), take_while1(|c| c != '"'), is_char('"')),
        |r| KnownQuery::Phrase(r),
    )(input)
}

fn parse_term(input: &str) -> IResult<&str, KnownQuery> {
    map(take_while1(is_term_char), |r| KnownQuery::Term(r))(input)
}

fn parse_not_term(input: &str) -> IResult<&str, KnownQuery> {
    map(preceded(is_char('-'), take_while1(is_term_char)), |r| {
        KnownQuery::NotTerm(r)
    })(input)
}

fn is_term_char(c: char) -> bool {
    !(c == ' ' || c == '\t' || c == '\r' || c == '\n')
}

fn parse_query(input: &str) -> IResult<&str, Vec<KnownQuery>> {
    many0(delimited(
        multispace0,
        alt((parse_not_phrase, parse_phrase, parse_not_term, parse_term)),
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
    fn not_term_extraction() {
        assert_eq!(parse_not_term("-ads"), Ok(("", NotTerm("ads"))))
    }

    #[test]
    fn phrase_extraction() {
        assert_eq!(
            parse_phrase("\"gula recipes\""),
            Ok(("", Phrase("gula recipes")))
        );
    }

    #[test]
    fn not_phrase_extraction() {
        assert_eq!(
            parse_not_phrase("-\"ads and tracking\""),
            Ok(("", NotPhrase("ads and tracking")))
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
    fn parse_query_works() {
        assert_eq!(
            parse_query(" peanut -\"peanut butter\" -sugar "),
            Ok((
                "",
                vec![Term("peanut"), NotPhrase("peanut butter"), NotTerm("sugar")]
            ))
        );
    }

    #[test]
    fn parse_query_accepts_empty_string() {
        assert_eq!(parse_query(""), Ok(("", vec![])));
        assert_eq!(parse_query(" "), Ok((" ", vec![])));
    }

    #[test]
    fn garbage_is_extracted_as_term() {
        assert_eq!(parse_query("- \""), Ok(("", vec![Term("-"), Term("\"")])));
    }
}
