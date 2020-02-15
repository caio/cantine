use std::{collections::HashMap, str};

use tantivy::{
    query::BooleanQuery,
    schema::{Field, IndexRecordOption},
    tokenizer::BoxedTokenizer,
    DocAddress, DocSet, Index, IndexReader, Postings, Result, Searcher, SkipResult, Term,
};

use crate::conditional_collector::topk::{DescendingTopK, TopK};

// Source: Copy-pasta from tantivy::query::bm25::idf
fn idf(doc_freq: u64, doc_count: u64) -> f32 {
    let x = ((doc_count - doc_freq) as f32 + 0.5) / (doc_freq as f32 + 0.5);
    (1f32 + x).ln()
}

fn termfreq(input: &str, field: Field, tokenizer: &BoxedTokenizer) -> HashMap<Term, u32> {
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

            if postings.skip_next(doc_id) == SkipResult::Reached {
                let term = Term::from_field_text(field, text);
                consumer(term, postings.term_freq());
            }
        }
    }
}

pub struct TopTerms {
    reader: IndexReader,
    field_tokenizers: Vec<(Field, BoxedTokenizer)>,
}

pub trait KeywordAcceptor {
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
    pub fn new(index: Index, fields: Vec<Field>) -> Result<Self> {
        let mut field_tokenizers = Vec::new();

        for field in fields.into_iter() {
            let tok = index.tokenizer_for_field(field)?;
            field_tokenizers.push((field, tok));
        }

        Ok(Self {
            reader: index.reader()?,
            field_tokenizers,
        })
    }

    pub fn extract(&self, limit: usize, input: &str) -> Keywords {
        self.extract_filtered(limit, input, ())
    }

    pub fn extract_from_doc(&self, limit: usize, addr: DocAddress) -> Keywords {
        self.extract_filtered_from_doc(limit, addr, ())
    }

    pub fn extract_filtered<F: KeywordAcceptor>(
        &self,
        limit: usize,
        input: &str,
        acceptor: F,
    ) -> Keywords {
        let searcher = self.reader.searcher();
        let num_docs = searcher.num_docs();

        let mut keywords = Keywords::new(limit);

        for (field, tokenizer) in self.field_tokenizers.iter() {
            let termfreq = termfreq(&input, *field, tokenizer);

            for (term, tf) in termfreq.into_iter() {
                let doc_freq = searcher.doc_freq(&term);

                if doc_freq == 0 && acceptor.accept(&term, tf, doc_freq, num_docs) {
                    continue;
                }

                let score = tf as f32 * idf(doc_freq, num_docs);
                keywords.visit(term, score);
            }
        }

        keywords
    }

    pub fn extract_filtered_from_doc<F: KeywordAcceptor>(
        &self,
        limit: usize,
        addr: DocAddress,
        acceptor: F,
    ) -> Keywords {
        let searcher = self.reader.searcher();
        let num_docs = searcher.num_docs();

        let mut keywords = Keywords::new(limit);

        for (field, _tokenizer) in self.field_tokenizers.iter() {
            termfreq_for_doc(&searcher, *field, addr, |term, term_freq| {
                let doc_freq = searcher.doc_freq(&term);
                if acceptor.accept(&term, term_freq, doc_freq, num_docs) {
                    let score = term_freq as f32 * idf(doc_freq, num_docs);
                    keywords.visit(term, score);
                }
            });
        }

        keywords
    }
}

pub struct Keywords(DescendingTopK<f32, Term>);

impl Keywords {
    pub fn new(limit: usize) -> Self {
        Self(DescendingTopK::new(limit))
    }

    pub fn into_sorted_vec(self) -> Vec<(Term, f32)> {
        self.0.into_sorted_vec()
    }

    pub fn into_query(self) -> BooleanQuery {
        BooleanQuery::new_multiterms_query(
            self.0
                .into_vec()
                .into_iter()
                .map(|(term, _score)| term)
                .collect(),
        )
    }

    fn visit(&mut self, term: Term, score: f32) {
        self.0.visit(term, score);
    }

    // TODO into_boosted_query, using the scaled tf/idf scores scaled with
}

#[cfg(test)]
mod tests {
    use super::*;

    use tantivy::{
        doc,
        schema::{SchemaBuilder, TEXT},
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

        let topterms = TopTerms::new(index.clone(), vec![source, quote])?;

        let keyword_filter = |term: &Term, _tf, doc_freq, num_docs| {
            // Only words with more than characters 3
            term.text().chars().count() > 3
                // that do NOT appear in every document at this field
                && doc_freq < num_docs
        };

        let marley_keywords =
            topterms.extract_filtered_from_doc(5, DocAddress(0, 0), keyword_filter);

        assert_word_found("marley", marley_keywords);

        let holmes_keywords =
            topterms.extract_filtered_from_doc(5, DocAddress(0, 1), keyword_filter);

        assert_word_found("dangerous", holmes_keywords);

        let groucho_keywords =
            topterms.extract_filtered_from_doc(5, DocAddress(0, 2), keyword_filter);

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
