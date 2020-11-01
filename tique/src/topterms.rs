//! Extract keywords and search for similar documents based on the
//! contents of your index.
//!
//! This module implements the same idea as Lucene's MoreLikeThis.
//! You can read more about the idea in the [original's documentation][mlt],
//! but here's a gist of how it works:
//!
//! 1. Counts the words (Terms) from an arbitrary input: may be a string
//!    or the address of a document you already indexed; Then
//!
//! 2. Ranks each word using the frequencies from `1` and information from
//!    the index (how often it appears in the corpus, how many documents have
//!    it)
//!
//! The result is a set of terms that are most relevant to represent your
//! input in relation to your current index. I.e.: it finds words that are
//! important and unique enough to describe your input.
//!
//! [mlt]: http://lucene.apache.org/core/8_4_1/queries/org/apache/lucene/queries/mlt/MoreLikeThis.html
//!
//! # Examples
//!
//! ## Finding Similar Documents
//!
//!```no_run
//! # use tantivy::{DocAddress, Index, Searcher, collector::TopDocs, schema::Field, Result};
//! # use tique::topterms::TopTerms;
//! # fn example(index: &Index, body: Field, title: Field,
//! #   doc_address: DocAddress, searcher: &Searcher) -> Result<()> {
//! let topterms = TopTerms::new(&index, vec![body, title])?;
//! let keywords = topterms.extract_from_doc(10, doc_address);
//!
//! let nearest_neighbors =
//!      searcher.search(&keywords.into_query(), &TopDocs::with_limit(10))?;
//! # Ok(())
//! # }
//!```
//!
//! ## Tuning the Keywords Extration
//!
//! Depending on how your fields are indexed you might find that the results
//! from the keyword extration are not very good. Maybe it includes words
//! that are too uncommon, too small or anything. You can modify how TopDocs
//! works via a custom `KeywordAcceptor` that you can use via the
//! `extract_filtered` and `extract_filtered_from_doc` methods:
//!
//!```no_run
//! # use tantivy::{Index, schema::{Field, Term}, Result};
//! # use tique::topterms::TopTerms;
//! # fn example(index: &Index, fulltext: Field, input: &str) -> Result<()> {
//! let topterms = TopTerms::new(&index, vec![fulltext])?;
//!
//! let keywords = topterms.extract_filtered(
//!      10,
//!      input,
//!      &|term: &Term, term_freq, doc_freq, num_docs| {
//!          // Only words longer than 4 characters and that appear
//!          // in at least 10 documents
//!          term.text().chars().count() > 4 && doc_freq >= 10
//!      }
//! );
//! # Ok(())
//! # }
//!```
//!
use std::{collections::HashMap, str};

use tantivy::{
    query::{BooleanQuery, BoostQuery, Occur, Query, TermQuery},
    schema::{Field, FieldType, IndexRecordOption, Schema},
    tokenizer::TextAnalyzer,
    DocAddress, DocSet, Index, IndexReader, Postings, Result, Searcher, Term,
};

use crate::conditional_collector::topk::{DescendingTopK, TopK};

// Source: Copy-pasta from tantivy::query::bm25::idf
fn idf(doc_freq: u64, doc_count: u64) -> f32 {
    let x = ((doc_count - doc_freq) as f32 + 0.5) / (doc_freq as f32 + 0.5);
    (1f32 + x).ln()
}

/// TopTerms extracts the most relevant Keywords from your index
pub struct TopTerms {
    reader: IndexReader,
    field_tokenizers: Vec<(Field, TextAnalyzer)>,
}

/// Allows tuning the algorithm to pick the top keywords
pub trait KeywordAcceptor {
    /// Decides wether the given Term is an acceptable keyword.
    ///
    /// Tunables:
    ///
    /// * tf: Term frequency. How often has the given term appeared
    ///       in the input (i.e.: what you gave to `TopTerms::extract*`)
    /// * doc_freq: Document frequency: How many documents in the
    ///       index contain this term
    /// * num_docs: How many documents are in the index in total
    fn accept(&self, term: &Term, tf: u32, doc_freq: u64, num_docs: u64) -> bool;
}

impl KeywordAcceptor for () {
    fn accept(&self, _: &Term, _: u32, _: u64, _: u64) -> bool {
        true
    }
}

impl<F> KeywordAcceptor for F
where
    F: Fn(&Term, u32, u64, u64) -> bool,
{
    fn accept(&self, term: &Term, tf: u32, doc_freq: u64, num_docs: u64) -> bool {
        (self)(term, tf, doc_freq, num_docs)
    }
}

impl TopTerms {
    /// Creates a new TopTerms that will extract keywords by looking at
    /// the given index fields
    ///
    /// # Errors
    ///
    /// Will yield an error if the provided fields are unknown or if they
    /// are not `tantivy::schema::TEXT`
    pub fn new(index: &Index, fields: Vec<Field>) -> Result<Self> {
        let mut field_tokenizers = Vec::new();

        for field in fields {
            if field_is_valid(&index.schema(), field) {
                let tok = index.tokenizer_for_field(field)?;
                field_tokenizers.push((field, tok));
            } else {
                let msg = format!(
                    "Field '{}' is not a text field with frequencies (TEXT)",
                    index.schema().get_field_name(field)
                );
                return Err(tantivy::TantivyError::SchemaError(msg));
            }
        }

        Ok(Self {
            reader: index.reader()?,
            field_tokenizers,
        })
    }

    /// Extracts the `limit` most relevant terms from the input
    pub fn extract(&self, limit: usize, input: &str) -> Keywords {
        self.extract_filtered(limit, input, &())
    }

    /// Extracts the `limit` most relevant terms from an indexed document
    pub fn extract_from_doc(&self, limit: usize, addr: DocAddress) -> Keywords {
        self.extract_filtered_from_doc(limit, addr, &())
    }

    /// Same as `extract`, but with support inspect/filter the terms as
    /// they are being picked.
    pub fn extract_filtered<F: KeywordAcceptor>(
        &self,
        limit: usize,
        input: &str,
        acceptor: &F,
    ) -> Keywords {
        let searcher = self.reader.searcher();
        let num_docs = searcher.num_docs();

        let mut keywords = DescendingTopK::new(limit);

        for (field, tokenizer) in &self.field_tokenizers {
            for (term, tf) in termfreq(&input, *field, tokenizer) {
                let doc_freq = searcher.doc_freq(&term);

                if doc_freq > 0 && acceptor.accept(&term, tf, doc_freq, num_docs) {
                    let score = tf as f32 * idf(doc_freq, num_docs);
                    keywords.visit(term, score);
                }
            }
        }

        keywords.into()
    }

    /// Same as `extract_from_doc`, but with support inspect/filter the
    /// terms as they are being picked.
    pub fn extract_filtered_from_doc<F: KeywordAcceptor>(
        &self,
        limit: usize,
        addr: DocAddress,
        acceptor: &F,
    ) -> Keywords {
        let searcher = self.reader.searcher();
        let num_docs = searcher.num_docs();

        let mut keywords = DescendingTopK::new(limit);

        for (field, _tokenizer) in &self.field_tokenizers {
            termfreq_for_doc(&searcher, *field, addr, |term, term_freq| {
                let doc_freq = searcher.doc_freq(&term);
                if acceptor.accept(&term, term_freq, doc_freq, num_docs) {
                    let score = term_freq as f32 * idf(doc_freq, num_docs);
                    keywords.visit(term, score);
                }
            });
        }

        keywords.into()
    }
}

/// Keywords is a collection of Term objects found via TopTerms
#[derive(Clone)]
pub struct Keywords(Vec<(Term, f32)>);

impl Keywords {
    /// Convert into a Query. It can be used as a way to approximate a
    /// nearest neighbors search, so it's expected that results are
    /// similar to the source used to create this Keywords instance.
    pub fn into_query(self) -> BooleanQuery {
        BooleanQuery::new_multiterms_query(self.0.into_iter().map(|(term, _score)| term).collect())
    }

    /// Same as `into_query`, but with terms boosted by their
    /// relative importance. The boost for each term is computed
    /// as `boost_factor * (score / max_score)`.
    /// The `boost_factor` parameter is useful when building more
    /// complex queries; `1.0` is a good default.
    pub fn into_boosted_query(self, boost_factor: f32) -> BooleanQuery {
        let max_score = self.0.first().map_or(0.0, |(_term, score)| *score);

        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        for (term, score) in self.0 {
            let boost = boost_factor * (score / max_score);
            let tq = Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
            clauses.push((Occur::Should, Box::new(BoostQuery::new(tq, boost))));
        }

        BooleanQuery::from(clauses)
    }

    /// Iterates over the terms of this keywords set, more relevant
    /// terms appear first
    pub fn terms(&self) -> impl Iterator<Item = &Term> {
        self.0.iter().map(|(term, _score)| term)
    }

    /// How many terms this set contains
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if this keywords set contains no terms
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Exposes the ordered terms and their scores. Useful if you are
    /// using the keywords for other purposes, like reporting, feeding
    /// into a more complex query, etc.
    pub fn into_sorted_vec(self) -> Vec<(Term, f32)> {
        self.0
    }
}

impl From<DescendingTopK<f32, Term>> for Keywords {
    fn from(src: DescendingTopK<f32, Term>) -> Self {
        Self(src.into_sorted_vec())
    }
}

fn termfreq(input: &str, field: Field, tokenizer: &TextAnalyzer) -> HashMap<Term, u32> {
    let mut termfreq = HashMap::new();

    let mut stream = tokenizer.token_stream(&input);
    while let Some(token) = stream.next() {
        let term = Term::from_field_text(field, &token.text);
        *termfreq.entry(term).or_insert(0) += 1;
    }

    termfreq
}

fn termfreq_for_doc<F>(searcher: &Searcher, field: Field, doc: DocAddress, mut consumer: F)
where
    F: FnMut(Term, u32),
{
    let DocAddress(seg_id, doc_id) = doc;

    let reader = searcher.segment_reader(seg_id);
    let inverted_index = reader.inverted_index(field.clone());
    let mut termstream = inverted_index.terms().stream();

    while let Some((bytes, terminfo)) = termstream.next() {
        if let Ok(text) = str::from_utf8(bytes) {
            let mut postings =
                inverted_index.read_postings_from_terminfo(terminfo, IndexRecordOption::WithFreqs);

            // XXX SegmentPostings::seek crashes debug builds when the target
            //     is before the current position
            if postings.doc() > doc_id {
                continue;
            }

            if postings.seek(doc_id) == doc_id {
                let term = Term::from_field_text(field, text);
                consumer(term, postings.term_freq());
            }
        }
    }
}

fn field_is_valid(schema: &Schema, field: Field) -> bool {
    if let FieldType::Str(opts) = schema.get_field_entry(field).field_type() {
        opts.get_indexing_options()
            .map_or(false, |opts| opts.index_option().has_freq())
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tantivy::{
        doc,
        schema::{SchemaBuilder, INDEXED, STRING, TEXT},
        tokenizer::SimpleTokenizer,
    };

    #[test]
    fn termfreq_counts_correctness() {
        let field = Field::from_field_id(1);
        let freqs = termfreq("a b b c c c d d d d", field, &SimpleTokenizer.into());

        let get = |tok| {
            let term = Term::from_field_text(field, tok);
            freqs.get(&term).cloned().unwrap_or(0)
        };

        assert_eq!(1, get("a"));
        assert_eq!(2, get("b"));
        assert_eq!(3, get("c"));
        assert_eq!(4, get("d"));
        assert_eq!(0, get("e"));
    }

    #[test]
    fn termfreq_from_input_and_doc_are_the_same() -> Result<()> {
        let mut builder = SchemaBuilder::new();

        let body = builder.add_text_field("body", TEXT);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        let text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Maecenas \
                   accumsan et diam id condimentum. Nam ac venenatis sapien. Curabitur \
                   et finibus tellus, non porta velit. Sed ac quam condimentum felis \
                   bibendum dignissim. Fusce venenatis sodales urna porta tincidunt. \
                   Ut nec tortor porttitor, placerat turpis vitae, rutrum eros.";

        writer.add_document(doc!(body => text));
        writer.commit()?;

        let text_termfreq = termfreq(&text, body, &index.tokenizer_for_field(body)?);

        let reader = index.reader()?;
        termfreq_for_doc(&reader.searcher(), body, DocAddress(0, 0), |term, tf| {
            assert_eq!(Some(&tf), text_termfreq.get(&term));
        });

        Ok(())
    }

    #[test]
    fn text_fields_are_valid() {
        let mut builder = SchemaBuilder::new();

        let invalid = builder.add_text_field("string", STRING);
        let also_invalid = builder.add_u64_field("non_str", INDEXED);
        let valid = builder.add_text_field("text", TEXT);

        let schema = builder.build();

        assert!(!field_is_valid(&schema, invalid));
        assert!(!field_is_valid(&schema, also_invalid));
        assert!(field_is_valid(&schema, valid));
    }

    #[test]
    fn topterms_integration() -> Result<()> {
        let mut builder = SchemaBuilder::new();

        let source = builder.add_text_field("source", TEXT);
        let quote = builder.add_text_field("quote", TEXT);

        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        writer.add_document(doc!(
            source => "Marley and Me: Life and Love With the World's Worst Dog",
            quote  => "A person can learn a lot from a dog, even a loopy one like ours. \
                      Marley taught me about living each day with unbridled exuberance \
                      and joy, about seizing the moment and following your heart. He \
                      taught me to appreciate the simple things-a walk in the woods, a \
                      fresh snowfall, a nap in a shaft of winter sunlight. And as he \
                      grew old and achy, he taught me about optimism in the face of \
                      adversity. Mostly, he taught me about friendship and selflessness \
                      and, above all else, unwavering loyalty."
        ));

        writer.add_document(doc!(
            source => "The Case-Book of Sherlock Holmes",
            quote  => "A dog reflects the family life. Whoever saw a frisky dog in a \
                      gloomy family, or a sad dog in a happy one? Snarling people have \
                      snarling dogs, dangerous people have dangerous ones."
        ));

        writer.add_document(doc!(
            source => "The Essential Groucho: Writings For By And About Groucho Marx",
            quote  => "Outside of a dog, a book is man's best friend. \
                      Inside of a dog it's too dark to read."
        ));

        writer.commit()?;

        fn assert_word_found(word: &str, kw: Keywords) {
            let words = kw
                .into_sorted_vec()
                .into_iter()
                .map(|(term, _score)| String::from(term.text()))
                .collect::<Vec<_>>();
            assert!(
                words.iter().any(|w| word == w),
                "Expected to find '{}'. Keywords = {}",
                word,
                words.join(", ")
            )
        }

        let topterms = TopTerms::new(&index, vec![source, quote])?;

        let keyword_filter = |term: &Term, _tf, doc_freq, num_docs| {
            // Only words with more than 3 characters
            term.text().chars().count() > 3
                // that do NOT appear in every document at this field
                && doc_freq < num_docs
        };

        let marley_keywords =
            topterms.extract_filtered_from_doc(5, DocAddress(0, 0), &keyword_filter);

        assert_word_found("marley", marley_keywords);

        let holmes_keywords =
            topterms.extract_filtered_from_doc(5, DocAddress(0, 1), &keyword_filter);

        assert_word_found("dangerous", holmes_keywords);

        let groucho_keywords =
            topterms.extract_filtered_from_doc(5, DocAddress(0, 2), &keyword_filter);

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let similar_to_groucho = searcher.search(
            &groucho_keywords.into_query(),
            &tantivy::collector::TopDocs::with_limit(3),
        )?;

        assert_eq!(
            Some(DocAddress(0, 2)),
            similar_to_groucho.first().map(|x| x.1),
            "expected groucho's to be the most similar to its own keyword set"
        );

        Ok(())
    }
}
