use urn::Urn;

/// Which streams a stream-level read covers, by **stream id** alone.
///
/// Used by [`EventStore::streams_needing_compaction`](crate::EventStore::streams_needing_compaction)
/// to narrow a sweep to, for example, every stream of one tenant. It is deliberately
/// not a [`StreamFilter`](crate::StreamFilter): that filter's variants select *events*
/// (by metadata, `created`, `aggregate_version`, ...) and have no meaning for a row of
/// the `streams` table, which carries only an id, a type and its versions. The aggregate
/// type is given separately, by the reader.
///
/// Matching is on the id's text as stored (`Urn::to_string`), case-sensitive. `%`, `_`
/// and `\` in a prefix or suffix are matched literally, never as wildcards.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StreamScope {
    /// Every stream.
    #[default]
    All,
    /// Streams whose id starts with this text, e.g. `"urn:bank-account:eu-"`.
    Prefix(String),
    /// Streams whose id ends with this text, e.g. `"@tenant:acme"`. See
    /// [`StreamScope::within`] for the common case of a scoped URN.
    Suffix(String),
}

impl StreamScope {
    /// The streams scoped under `scope` — every id built by
    /// [`ScopedUrn::at`](replay::ScopedUrn::at) with `scope` as the argument, which
    /// renders as `<base>@<scope nid>:<scope nss>`
    /// (`docs/adr/0010-nested-scoped-urns-parse-at-the-first-at-sign.md`).
    ///
    /// ```text
    /// StreamScope::within(&urn:tenant:acme)
    ///     matches  urn:bank-account:acct-1@tenant:acme
    ///     matches  urn:watchlist:main@user:0x78@tenant:acme   (nested under acme)
    ///     misses   urn:bank-account:acct-1@tenant:acme-eu
    /// ```
    pub fn within<S: Clone + Into<Urn>>(scope: &S) -> Self {
        let scope: Urn = scope.clone().into();
        StreamScope::Suffix(format!("@{}:{}", scope.nid(), scope.nss()))
    }

    /// Whether the stream id `id` (its stored text) is in this scope.
    pub(crate) fn matches(&self, id: &str) -> bool {
        match self {
            StreamScope::All => true,
            StreamScope::Prefix(prefix) => id.starts_with(prefix.as_str()),
            StreamScope::Suffix(suffix) => id.ends_with(suffix.as_str()),
        }
    }

    /// The `LIKE` pattern (with `\` as the escape character) selecting this scope, or
    /// `None` for [`StreamScope::All`], which needs no predicate.
    pub(crate) fn like_pattern(&self) -> Option<String> {
        match self {
            StreamScope::All => None,
            StreamScope::Prefix(prefix) => Some(format!("{}%", escape_like(prefix))),
            StreamScope::Suffix(suffix) => Some(format!("%{}", escape_like(suffix))),
        }
    }
}

/// Escapes `LIKE`'s metacharacters so `text` matches only itself.
fn escape_like(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        if matches!(c, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn within_matches_direct_and_nested_scopes_only() {
        let scope = StreamScope::within(&Urn::from_str("urn:tenant:acme").unwrap());

        assert!(scope.matches("urn:bank-account:acct-1@tenant:acme"));
        assert!(scope.matches("urn:watchlist:main@user:0x78@tenant:acme"));
        assert!(!scope.matches("urn:bank-account:acct-1@tenant:acme-eu"));
        assert!(!scope.matches("urn:bank-account:acct-1"));
    }

    #[test]
    fn like_patterns_escape_metacharacters() {
        assert_eq!(StreamScope::All.like_pattern(), None);
        assert_eq!(
            StreamScope::Prefix(r"urn:a:50%_off\".into()).like_pattern(),
            Some(r"urn:a:50\%\_off\\%".to_string())
        );
        assert_eq!(
            StreamScope::Suffix("@t:a_b".into()).like_pattern(),
            Some(r"%@t:a\_b".to_string())
        );
    }
}
