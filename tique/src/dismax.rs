use tantivy::{
    self,
    query::{EmptyScorer, Explanation, Query, Scorer, Weight},
    DocId, DocSet, Result, Score, Searcher, SegmentReader, SkipResult, TantivyError,
};

/// A Maximum Disjunction query, as popularized by Lucene/Solr
///
/// A DisMax query is one that behaves as the union of its sub-queries and
/// the resulting documents are scored as the best score over each sub-query
/// plus a configurable increment based on additional matches.
///
/// The final score formula is `score = max + (sum - max) * tiebreaker`,
/// so with a tiebreaker of `0.0` you get only the maximum score and if you
/// turn it up to `1.0` the score ends up being the sum of all scores, just
/// like a plain "should" BooleanQuery would.
///
#[derive(Debug)]
pub struct DisMaxQuery {
    disjuncts: Vec<Box<dyn Query>>,
    tiebreaker: f32,
}

impl DisMaxQuery {
    /// Create a union-like query that picks the best score instead of the sum
    ///
    /// Panics if tiebreaker is not within the `[0,1]` range
    pub fn new(disjuncts: Vec<Box<dyn Query>>, tiebreaker: f32) -> Self {
        assert!((0.0..=1.0).contains(&tiebreaker));
        Self {
            disjuncts,
            tiebreaker,
        }
    }
}

impl Clone for DisMaxQuery {
    fn clone(&self) -> Self {
        Self {
            disjuncts: self.disjuncts.iter().map(|q| q.box_clone()).collect(),
            tiebreaker: self.tiebreaker,
        }
    }
}

impl Query for DisMaxQuery {
    fn weight(&self, searcher: &Searcher, scoring_enabled: bool) -> Result<Box<dyn Weight>> {
        Ok(Box::new(DisMaxWeight::new(
            self.disjuncts
                .iter()
                .map(|d| d.weight(searcher, scoring_enabled))
                .collect::<Result<Vec<_>>>()?,
            self.tiebreaker,
        )))
    }
}

struct DisMaxWeight {
    weights: Vec<Box<dyn Weight>>,
    tiebreaker: f32,
}

impl DisMaxWeight {
    fn new(weights: Vec<Box<dyn Weight>>, tiebreaker: f32) -> Self {
        Self {
            weights,
            tiebreaker,
        }
    }
}

impl Weight for DisMaxWeight {
    fn scorer(&self, reader: &SegmentReader, boost: f32) -> Result<Box<dyn Scorer>> {
        match self.weights.len() {
            0 => Ok(Box::new(EmptyScorer)),
            1 => self.weights.get(0).unwrap().scorer(reader, boost),
            _ => Ok(Box::new(DisMaxScorer::new(
                self.weights
                    .iter()
                    .map(|w| w.scorer(reader, boost))
                    .collect::<Result<Vec<_>>>()?,
                self.tiebreaker,
            ))),
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;

        if scorer.skip_next(doc) != SkipResult::Reached {
            return Err(TantivyError::InvalidArgument("Not a match".to_owned()));
        }

        let mut explanation = Explanation::new(
            format!(
                "DisMaxQuery. Score = max + (sum - max) * {}",
                self.tiebreaker
            ),
            scorer.score(),
        );

        for weight in &self.weights {
            if let Ok(sub_explanation) = weight.explain(reader, doc) {
                explanation.add_detail(sub_explanation);
            }
        }

        Ok(explanation)
    }
}

struct DisMaxScorer {
    scorers: Vec<Box<dyn Scorer>>,
    current: Option<DocId>,
    tiebreaker: f32,
}

impl DisMaxScorer {
    fn new(scorers: Vec<Box<dyn Scorer>>, tiebreaker: f32) -> Self {
        Self {
            scorers,
            tiebreaker,
            current: None,
        }
    }
}

impl Scorer for DisMaxScorer {
    fn score(&mut self) -> Score {
        let mut max = 0.0;
        let mut sum = 0.0;

        debug_assert!(self.current.is_some());
        for scorer in self.scorers.iter_mut() {
            if self.current.map(|d| scorer.doc() == d).unwrap_or(false) {
                let score = scorer.score();
                sum += score;

                if score > max {
                    max = score;
                }
            }
        }

        max + (sum - max) * self.tiebreaker
    }
}

impl DocSet for DisMaxScorer {
    fn advance(&mut self) -> bool {
        let mut next_target = None;
        let mut to_remove = Vec::new();

        for (idx, scorer) in self.scorers.iter_mut().enumerate() {
            // Advance every scorer that's on target or behind
            if self.current.map(|d| d >= scorer.doc()).unwrap_or(true) && !scorer.advance() {
                to_remove.push(idx);
                continue;
            }

            let doc = scorer.doc();
            if next_target.map(|next| doc < next).unwrap_or(true) {
                next_target.replace(doc);
            }
        }

        while let Some(idx) = to_remove.pop() {
            self.scorers.remove(idx);
        }

        if let Some(target) = next_target {
            self.current.replace(target);
            true
        } else {
            false
        }
    }

    fn doc(&self) -> tantivy::DocId {
        debug_assert!(self.current.is_some());
        self.current.unwrap_or(0)
    }

    fn size_hint(&self) -> u32 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{num::Wrapping, ops::Range};

    use tantivy::{
        doc,
        query::TermQuery,
        schema::{IndexRecordOption, SchemaBuilder, TEXT},
        DocAddress, Index, Term,
    };

    // XXX ConstScorer::from(VecDocSet::from(...)), but I can't seem
    //     import tantivy::query::VecDocSet here??
    struct VecScorer {
        doc_ids: Vec<DocId>,
        cursor: Wrapping<usize>,
    }

    impl Scorer for VecScorer {
        fn score(&mut self) -> Score {
            1.0
        }
    }

    impl DocSet for VecScorer {
        fn advance(&mut self) -> bool {
            self.cursor += Wrapping(1);
            self.doc_ids.len() > self.cursor.0
        }

        fn doc(&self) -> DocId {
            self.doc_ids[self.cursor.0]
        }

        fn size_hint(&self) -> u32 {
            self.doc_ids.len() as u32
        }
    }

    fn test_scorer(range: Range<DocId>) -> Box<dyn Scorer> {
        Box::new(VecScorer {
            doc_ids: range.collect(),
            cursor: Wrapping(usize::max_value()),
        })
    }

    #[test]
    fn scorer_advances_as_union() {
        let scorers = vec![
            test_scorer(0..10),
            test_scorer(5..20),
            test_scorer(9..30),
            test_scorer(42..43),
            test_scorer(13..13), // empty docset
        ];

        let mut dismax = DisMaxScorer::new(scorers, 0.0);

        for i in 0..30 {
            assert!(dismax.advance(), "failed advance at i={}", i);
            assert_eq!(i, dismax.doc());
        }

        assert!(dismax.advance());
        assert_eq!(42, dismax.doc());
        assert!(!dismax.advance(), "scorer should have ended by now");
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn tiebreaker() {
        let scorers = vec![test_scorer(4..5), test_scorer(4..6), test_scorer(4..7)];

        // So now the score is the sum of scores for
        // every matching scorer (VecScorer always yields 1)
        let mut dismax = DisMaxScorer::new(scorers, 1.0);

        assert!(dismax.advance());
        assert_eq!(3.0, dismax.score());
        assert!(dismax.advance());
        assert_eq!(2.0, dismax.score());
        assert!(dismax.advance());
        assert_eq!(1.0, dismax.score());
        assert!(!dismax.advance(), "scorer should have ended by now");

        let scorers = vec![test_scorer(7..8), test_scorer(7..8)];

        // With a tiebreaker 0, it actually uses
        // the maximum disjunction
        let mut dismax = DisMaxScorer::new(scorers, 0.0);
        assert!(dismax.advance());
        // So now, even though doc=7 occurs twice, the score is 1
        assert_eq!(1.0, dismax.score());
        assert!(!dismax.advance(), "scorer should have ended by now");
    }

    #[test]
    fn explaination() -> Result<()> {
        let mut builder = SchemaBuilder::new();
        let field = builder.add_text_field("field", TEXT);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        writer.add_document(doc!(field => "foo"));
        writer.add_document(doc!(field => "bar"));
        writer.add_document(doc!(field => "foo bar"));
        writer.add_document(doc!(field => "baz"));
        writer.commit()?;

        let foo_query = TermQuery::new(
            Term::from_field_text(field, "foo"),
            IndexRecordOption::Basic,
        );

        let bar_query = TermQuery::new(
            Term::from_field_text(field, "bar"),
            IndexRecordOption::Basic,
        );

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let dismax = DisMaxQuery::new(vec![Box::new(foo_query), Box::new(bar_query)], 0.0);

        let baz_doc = DocAddress(0, 3);
        assert!(
            dismax.explain(&searcher, baz_doc).is_err(),
            "Shouldn't be able to explain a non-matching doc"
        );

        // Ensure every other doc can be explained
        for doc_id in 0..3 {
            let explanation = dismax.explain(&searcher, DocAddress(0, doc_id))?;
            assert!(explanation.to_pretty_json().contains("DisMaxQuery"));
        }

        Ok(())
    }
}
