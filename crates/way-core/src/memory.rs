//! Corpus recall over an FTS5 index.
//!
//! Two properties make this safe to expose:
//!
//! 1. **Redaction is a SQL predicate inside the FTS join**, not a post-filter.
//!    A restricted row never enters the result buffer, the cardinality, or the
//!    ranking, so a caller cannot infer a denied document's existence from a
//!    short page or a shifted score. A post-filter leaks all three.
//! 2. **The index is pinned to the profile digest.** The mask is derived from
//!    the digest-bound injection list, so answering from an index built under a
//!    different digest would apply a stale policy. That returns
//!    `memory_index_stale` instead.
//!
//! The mask itself is computed in TypeScript by the SAME exported `isRestricted`
//! predicate the injector uses, and passed in. Reimplementing it here would let
//! the two drift, and a redaction drift is a disclosure.

use rusqlite::{Connection, OptionalExtension, params};

use crate::store::{Store, StoreError, StoreResult};

/// Bit `i` denies injection file `i` in the profile's ordered list.
pub type RestrictionMask = u64;

#[derive(Debug, Clone)]
pub struct MemoryHit {
    pub path: String,
    pub snippet: String,
    pub rank: f64,
}

#[derive(Debug)]
pub enum MemoryError {
    /// The FTS5 extension is unavailable in this build.
    Fts5Unavailable,
    /// The index was built under a different profile digest.
    IndexStale { indexed: String, current: String },
    Store(StoreError),
}

impl std::fmt::Display for MemoryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fts5Unavailable => write!(
                formatter,
                "SQLite FTS5 is unavailable; the memory recall index cannot enforce redaction in-query"
            ),
            Self::IndexStale { indexed, current } => write!(
                formatter,
                "memory index was built for profile digest {indexed} but the active digest is {current}"
            ),
            Self::Store(error) => write!(formatter, "{error}"),
        }
    }
}

impl From<StoreError> for MemoryError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<rusqlite::Error> for MemoryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Store(StoreError::from(error))
    }
}

/// Verifies FTS5 at startup so the failure is a named boot error rather than a
/// silent fallback to an unfiltered scan at query time.
pub fn assert_fts5_available(connection: &Connection) -> Result<(), MemoryError> {
    connection
        .execute_batch("CREATE VIRTUAL TABLE IF NOT EXISTS temp.fts5_probe USING fts5(body); DROP TABLE temp.fts5_probe;")
        .map_err(|_| MemoryError::Fts5Unavailable)
}

pub fn ensure_schema(connection: &Connection) -> StoreResult<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS memory_documents (
            file_index INTEGER PRIMARY KEY,
            path TEXT NOT NULL
         );
         CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
            path UNINDEXED,
            body,
            file_index UNINDEXED
         );",
    )?;
    Ok(())
}

/// Replaces the index contents and pins it to `profile_digest`.
pub fn rebuild(
    store: &Store,
    profile_digest: &str,
    documents: &[(i64, String, String)],
) -> Result<usize, MemoryError> {
    let mut connection = store.connection()?;
    assert_fts5_available(&connection)?;
    ensure_schema(&connection)?;
    let transaction = connection.transaction()?;
    transaction.execute("DELETE FROM memory_fts", [])?;
    transaction.execute("DELETE FROM memory_documents", [])?;
    for (file_index, path, body) in documents {
        transaction.execute(
            "INSERT INTO memory_documents(file_index, path) VALUES (?1, ?2)",
            params![file_index, path],
        )?;
        transaction.execute(
            "INSERT INTO memory_fts(path, body, file_index) VALUES (?1, ?2, ?3)",
            params![path, body, file_index],
        )?;
    }
    crate::store::meta_set_tx(&transaction, "memory_index_digest", profile_digest)?;
    transaction.commit()?;
    Ok(documents.len())
}

/// Escapes a user query as a single quoted FTS5 string.
///
/// The result is bound as ONE parameter to `MATCH ?1`; the query text is never
/// interpolated into SQL. Doubling the quote is what stops a caller from
/// closing the string and appending FTS5 operators of their own.
pub fn escape_fts5_query(raw: &str) -> String {
    format!("\"{}\"", raw.replace('"', "\"\""))
}

pub fn search(
    store: &Store,
    profile_digest: &str,
    query: &str,
    mask: RestrictionMask,
    limit: i64,
) -> Result<Vec<MemoryHit>, MemoryError> {
    let connection = store.connection()?;
    assert_fts5_available(&connection)?;
    ensure_schema(&connection)?;

    let indexed: Option<String> = connection
        .query_row("SELECT v FROM gateway_meta WHERE k = 'memory_index_digest'", [], |row| row.get(0))
        .optional()?;
    let indexed = indexed.unwrap_or_default();
    if indexed != profile_digest {
        return Err(MemoryError::IndexStale { indexed, current: profile_digest.to_owned() });
    }

    // The mask predicate lives INSIDE this query, beside the MATCH, so a denied
    // row is never ranked, counted, or buffered. `bm25` is negated-ascending in
    // SQLite, so ORDER BY rank ASC is best-first.
    //
    // An out-of-range file_index is EXCLUDED rather than admitted. Load rejects
    // more than 64 injection files so it should be unreachable, but a bit index
    // the mask cannot address must fail closed: admitting it would make an
    // unmaskable document permanently visible.
    let mut statement = connection.prepare(
        "SELECT path, snippet(memory_fts, 1, '', '', '...', 16) AS snippet, bm25(memory_fts) AS rank
           FROM memory_fts
          WHERE memory_fts MATCH ?1
            AND CAST(file_index AS INTEGER) BETWEEN 0 AND 63
            AND ((?2 >> CAST(file_index AS INTEGER)) & 1) = 0
          ORDER BY rank
          LIMIT ?3",
    )?;
    let escaped = escape_fts5_query(query);
    // The mask is bound as a signed i64 because SQLite has no unsigned type;
    // the bit pattern is preserved and only bit tests are performed on it.
    let rows = statement.query_map(params![escaped, mask as i64, limit], |row| {
        Ok(MemoryHit { path: row.get(0)?, snippet: row.get(1)?, rank: row.get(2)? })
    })?;
    let mut hits = Vec::new();
    for row in rows {
        hits.push(row?);
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seeded() -> (Store, String) {
        let store = Store::default();
        let digest = "sha256:test".to_owned();
        rebuild(
            &store,
            &digest,
            &[
                (0, "SOUL.md".to_owned(), "the operator prefers direct feedback zebra".to_owned()),
                (1, "MEMORY.md".to_owned(), "a private note also mentioning zebra".to_owned()),
            ],
        )
        .unwrap();
        (store, digest)
    }

    #[test]
    fn fts5_is_available_in_this_build() {
        let store = Store::default();
        let connection = store.connection().unwrap();
        assert!(assert_fts5_available(&connection).is_ok());
    }

    #[test]
    fn an_unrestricted_search_returns_both_documents() {
        let (store, digest) = seeded();
        let hits = search(&store, &digest, "zebra", 0, 10).unwrap();
        assert_eq!(hits.len(), 2);
    }

    /// The mask must remove the row from the QUERY, not from the results, so a
    /// caller cannot infer the denied document from cardinality or ranking.
    #[test]
    fn a_masked_document_never_appears() {
        let (store, digest) = seeded();
        // Deny injection file 1 (MEMORY.md).
        let hits = search(&store, &digest, "zebra", 0b10, 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].path, "SOUL.md");
        assert!(!hits[0].snippet.contains("private"));
    }

    /// Rank equality against a control index proves the filter is in-query: a
    /// post-filter would leave the restricted document influencing bm25.
    #[test]
    fn a_masked_search_ranks_identically_to_a_control_index_without_the_document() {
        let (store, digest) = seeded();
        let masked = search(&store, &digest, "zebra", 0b10, 10).unwrap();

        let control = Store::default();
        rebuild(
            &control,
            &digest,
            &[(0, "SOUL.md".to_owned(), "the operator prefers direct feedback zebra".to_owned())],
        )
        .unwrap();
        let control_hits = search(&control, &digest, "zebra", 0, 10).unwrap();

        assert_eq!(masked.len(), control_hits.len());
        assert_eq!(masked[0].path, control_hits[0].path);
        assert_eq!(masked[0].snippet, control_hits[0].snippet);
        assert!(
            (masked[0].rank - control_hits[0].rank).abs() < f64::EPSILON,
            "a post-filter would rank differently: {} vs {}",
            masked[0].rank,
            control_hits[0].rank
        );
    }

    /// An index position the mask cannot address must be excluded, not admitted.
    /// Fail-open here would make an unmaskable document permanently visible.
    #[test]
    fn an_out_of_range_file_index_is_excluded_rather_than_admitted() {
        let store = Store::default();
        let digest = "sha256:oob".to_owned();
        rebuild(
            &store,
            &digest,
            &[
                (0, "SOUL.md".to_owned(), "addressable zebra".to_owned()),
                (64, "OVERFLOW.md".to_owned(), "unaddressable zebra".to_owned()),
                (-1, "NEGATIVE.md".to_owned(), "negative zebra".to_owned()),
            ],
        )
        .unwrap();

        let hits = search(&store, &digest, "zebra", 0, 10).unwrap();
        let paths: Vec<&str> = hits.iter().map(|hit| hit.path.as_str()).collect();
        assert_eq!(paths, vec!["SOUL.md"], "only addressable positions may be returned");
    }

    #[test]
    fn a_stale_index_is_refused_rather_than_answered() {
        let (store, _) = seeded();
        let error = search(&store, "sha256:rotated", "zebra", 0, 10).unwrap_err();
        assert!(matches!(error, MemoryError::IndexStale { .. }));
    }

    #[test]
    fn a_query_cannot_escape_its_quoted_string() {
        let (store, digest) = seeded();
        // Closing the quote and appending an FTS5 operator must not execute.
        let hits = search(&store, &digest, "zebra\" OR body:\"private", 0b10, 10).unwrap();
        for hit in &hits {
            assert_ne!(hit.path, "MEMORY.md", "a masked document must stay unreachable");
        }
        assert_eq!(escape_fts5_query("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn a_rebuild_repins_the_digest_and_replaces_contents() {
        let (store, _) = seeded();
        rebuild(&store, "sha256:next", &[(0, "SOUL.md".to_owned(), "only this remains".to_owned())]).unwrap();
        let hits = search(&store, "sha256:next", "remains", 0, 10).unwrap();
        assert_eq!(hits.len(), 1);
        // The prior contents are gone, not merely shadowed.
        assert!(search(&store, "sha256:next", "zebra", 0, 10).unwrap().is_empty());
    }

    /// FTS5 operators, column filters, and a query that is itself a quote must
    /// stay inside the escaped string. A masked document has to stay unreachable
    /// even when the query is crafted to close the quote or rank a denied row.
    #[test]
    fn crafted_fts5_queries_cannot_observe_a_masked_document() {
        let (store, digest) = seeded();
        for query in [
            "zebra\" OR *",
            "zebra\" OR body:\"private",
            "zebra NEAR private",
            "*private",
            "body:private",
            "private OR zebra",
            "\"",
            "*",
            "NEAR",
            "file_index:1",
        ] {
            let hits = search(&store, &digest, query, 0b10, 10).unwrap();
            for hit in &hits {
                assert_ne!(hit.path, "MEMORY.md", "query {query:?} reached a masked document");
                assert!(
                    !hit.snippet.to_lowercase().contains("private"),
                    "snippet leaked restricted text for {query:?}: {}",
                    hit.snippet
                );
            }
        }
    }

    /// Empty mask, all-ones mask, and bit 63 (the signed-i64 sign bit SQLite
    /// binds) over a 64-file corpus. The last injection file is the boundary.
    #[test]
    fn empty_all_ones_and_bit_63_masks_over_a_64_file_corpus() {
        let store = Store::default();
        let digest = "sha256:bits".to_owned();
        let documents: Vec<(i64, String, String)> = (0..64)
            .map(|index| {
                let body = if index == 63 {
                    "bit63secret unique token".to_owned()
                } else {
                    format!("common token file{index}")
                };
                (index, format!("file-{index}.md"), body)
            })
            .collect();
        rebuild(&store, &digest, &documents).unwrap();

        let visible = search(&store, &digest, "bit63secret", 0, 10).unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].path, "file-63.md");

        let bit63 = 1u64 << 63;
        let masked = search(&store, &digest, "bit63secret", bit63, 10).unwrap();
        assert!(
            masked.is_empty(),
            "bit 63 must deny file-63.md; got {:?}",
            masked.iter().map(|hit| hit.path.as_str()).collect::<Vec<_>>()
        );

        let none = search(&store, &digest, "common", u64::MAX, 10).unwrap();
        assert!(none.is_empty(), "all-ones must deny the entire 64-file corpus");
    }
}
