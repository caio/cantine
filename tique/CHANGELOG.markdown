# Changelog

## v0.7.0 - 2021-09-11

* Depend on tantivy 0.16+

## v0.6.0 - 2021-06-14

* Depend on tantivy 0.15+
* `contidional_collector::CheckCondition` now takes a `SegmentOrdinal`
  instead of a `SegmentLocalId` following tantivy's changes.
  Migration should be simply a matter of renaming.

## v0.5.0 - 2021-02-07

* Depend on tantivy 0.14+

## v0.4.0 - 2020-03-17

* Stabilized `QueryParser` under the `queryparser` feature
* Added `DisMaxQuery` and `QueryParser::parse_dismax`

## v0.3.0 - 2020-02-19

* Depend on tantivy 0.12+
* Added `topterms::Keywords`

## v0.2.0 - 2020-02-16

* Added new `topterms` module
