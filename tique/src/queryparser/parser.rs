use nom::{
    self,
    branch::alt,
    bytes::complete::take_while1,
    character::complete::{char as is_char, multispace0},
    combinator::map,
    multi::many0,
    sequence::{delimited, preceded},
    IResult,
};

#[derive(Debug, PartialEq)]
pub enum Token<'a> {
    Phrase(&'a str, bool),
    Term(&'a str, bool),
}

fn parse_not_phrase(input: &str) -> IResult<&str, Token> {
    map(preceded(is_char('-'), parse_phrase), |t| match t {
        Token::Phrase(inner, false) => Token::Phrase(inner, true),
        _ => unreachable!(),
    })(input)
}

fn parse_phrase(input: &str) -> IResult<&str, Token> {
    map(
        delimited(is_char('"'), take_while1(|c| c != '"'), is_char('"')),
        |s| Token::Phrase(s, false),
    )(input)
}

fn parse_term(input: &str) -> IResult<&str, Token> {
    map(take_while1(is_term_char), |s| Token::Term(s, false))(input)
}

fn parse_not_term(input: &str) -> IResult<&str, Token> {
    map(preceded(is_char('-'), parse_term), |t| match t {
        Token::Term(inner, false) => Token::Term(inner, true),
        _ => unreachable!(),
    })(input)
}

fn is_term_char(c: char) -> bool {
    !(c == ' ' || c == '\t' || c == '\r' || c == '\n')
}

pub fn parse_query(input: &str) -> IResult<&str, Vec<Token>> {
    many0(delimited(
        multispace0,
        alt((parse_not_phrase, parse_phrase, parse_not_term, parse_term)),
        multispace0,
    ))(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::Token::*;

    #[test]
    fn term_extraction() {
        assert_eq!(parse_term("gula"), Ok(("", Term("gula", false))));
    }

    #[test]
    fn not_term_extraction() {
        assert_eq!(parse_not_term("-ads"), Ok(("", Term("ads", true))))
    }

    #[test]
    fn phrase_extraction() {
        assert_eq!(
            parse_phrase("\"gula recipes\""),
            Ok(("", Phrase("gula recipes", false)))
        );
    }

    #[test]
    fn not_phrase_extraction() {
        assert_eq!(
            parse_not_phrase("-\"ads and tracking\""),
            Ok(("", Phrase("ads and tracking", true)))
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
                vec![
                    Term("peanut", false),
                    Phrase("peanut butter", true),
                    Term("sugar", true)
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
    fn garbage_is_extracted_as_term() {
        assert_eq!(
            parse_query("- \""),
            Ok(("", vec![Term("-", false), Term("\"", false)]))
        );
    }
}
