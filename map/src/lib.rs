//! Draws a Replay domain as an event-storming map: aggregates with their commands and events, and the policies that
//! turn an aggregate's event into commands for aggregates. `cargo replay-map` is the command-line front end.
//!
//! It reads the domain's *source* with `syn` rather than anything `define_aggregate!` emits. The macro expands to
//! enums that know nothing about which command yields which event: that link exists only in the match arms of
//! `Aggregate::handle`, `AggregatePolicy::react` and `Policy::react`, which no macro sees. Reading source also needs
//! no build, no runtime and no database.
//!
//! That puts one structural rule on a domain that adopts the map: every `handle` and `react` arm builds its events or
//! commands **inline**, in result position, never through a helper call — `<Agg>Event::<Variant>` in `handle`,
//! `(urn, <Target>Command::<Variant>)` in `AggregatePolicy::react`, and
//! `Dispatch::to::<Agg>(urn, <Agg>Command::<Variant>)` in `Policy::react`. A policy reading several aggregates' events
//! through a `query_events!` wrapper names each as `<Wrapper>::<Agg>Event(<Agg>Event::<Variant>)`. An arm the
//! generator cannot read fails with `file:line`; nothing is skipped.
//!
//! ```no_run
//! use replay_map::{DomainMap, SourceDirs};
//!
//! let markdown = DomainMap::read(&SourceDirs(&["src".into()]))?.to_markdown();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::collections::BTreeMap;
use std::fmt::{self, Write as _};
use std::fs;
use std::path::{Path, PathBuf};

use proc_macro2::{Delimiter, Span, TokenTree};
use quote::ToTokens;
use syn::ext::IdentExt;
use syn::parse::{ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token;
use syn::visit::{self, Visit};
use syn::{
    braced, bracketed, Attribute, Block, Expr, ExprClosure, ExprMatch, ExprReturn, GenericArgument,
    Generics, Ident, ImplItem, Item, ItemImpl, Macro, Meta, Pat, PathArguments, Stmt, Token, Type,
};

/// A domain construct the generator cannot read, at the place it met it.
#[derive(Debug)]
pub struct MapError {
    pub file: String,
    pub line: usize,
    pub message: String,
}

impl std::error::Error for MapError {}

impl fmt::Display for MapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.line {
            // A file or directory that could not be read has no line to point at.
            0 => write!(f, "{}: {}", self.file, self.message),
            line => write!(f, "{}:{line}: {}", self.file, self.message),
        }
    }
}

type Result<T> = std::result::Result<T, MapError>;

/// One source file of the domain, named by the path errors should print.
pub struct Source {
    pub path: String,
    pub text: String,
}

/// Where a domain's source comes from. `DomainMap::read` walks it once per pass and holds one file's text at a time,
/// so a source tree is never in memory whole.
pub trait Sources {
    /// Hands every source file to `visit`, in the same order on every call.
    fn each(
        &self,
        visit: &mut dyn FnMut(&Source) -> std::result::Result<(), MapError>,
    ) -> std::result::Result<(), MapError>;
}

/// Sources already in memory: the caller's, and the caller's to bound.
impl Sources for [Source] {
    fn each(&self, visit: &mut dyn FnMut(&Source) -> Result<()>) -> Result<()> {
        self.iter().try_for_each(visit)
    }
}

/// Every `.rs` file under these directories, in path order so the map does not depend on the file system's, read
/// from disk again on every pass.
pub struct SourceDirs<'a>(pub &'a [PathBuf]);

impl Sources for SourceDirs<'_> {
    fn each(&self, visit: &mut dyn FnMut(&Source) -> Result<()>) -> Result<()> {
        self.0.iter().try_for_each(|dir| each_in(dir, None, visit))
    }
}

/// A directory being walked, and the ones it was reached through: a chain on the call stack, as deep as the walk.
struct Walking<'a> {
    real: PathBuf,
    parent: Option<&'a Walking<'a>>,
}

fn each_in(
    dir: &Path,
    parent: Option<&Walking>,
    visit: &mut dyn FnMut(&Source) -> Result<()>,
) -> Result<()> {
    let real = fs::canonicalize(dir).map_err(|e| unreadable(dir, e))?;
    // A symbolic link back to a directory the walk is inside would read the same files forever.
    if std::iter::successors(parent, |walking| walking.parent).any(|walking| walking.real == real) {
        return Err(MapError {
            file: dir.display().to_string(),
            line: 0,
            message: format!(
                "links back to {}, which is already being read",
                real.display()
            ),
        });
    }
    let walking = Walking { real, parent };
    let mut after = None;
    while let Some(path) = next_entry(dir, after.as_deref())? {
        if path.is_dir() {
            each_in(&path, Some(&walking), visit)?;
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let text = fs::read_to_string(&path).map_err(|e| unreadable(&path, e))?;
            visit(&Source {
                path: path.display().to_string(),
                text,
            })?;
        }
        after = Some(path);
    }
    Ok(())
}

/// The first entry of `dir` in path order after `after`. Rescanning the directory for each entry costs time
/// quadratic in one directory's size, and holds one path instead of its listing.
fn next_entry(dir: &Path, after: Option<&Path>) -> Result<Option<PathBuf>> {
    let mut next: Option<PathBuf> = None;
    for entry in fs::read_dir(dir).map_err(|e| unreadable(dir, e))? {
        let path = entry.map_err(|e| unreadable(dir, e))?.path();
        if after.is_none_or(|after| path.as_path() > after)
            && next.as_ref().is_none_or(|next| path < *next)
        {
            next = Some(path);
        }
    }
    Ok(next)
}

fn unreadable(path: &Path, e: std::io::Error) -> MapError {
    MapError {
        file: path.display().to_string(),
        line: 0,
        message: format!("cannot read: {e}"),
    }
}

/// `(aggregate, variant)`: a command or event, named with the aggregate that declares it.
pub type Ref = (String, String);

#[derive(Debug, PartialEq)]
pub struct Aggregate {
    pub name: String,
    pub commands: Vec<String>,
    pub events: Vec<String>,
    /// `(command, event)`: the command's `handle` arm builds the event.
    pub decisions: Vec<(String, String)>,
}

#[derive(Debug, PartialEq)]
pub struct Policy {
    pub name: String,
    /// One per command-issuing `react` arm: the events it matches, and the commands it issues.
    pub reactions: Vec<(Vec<Ref>, Vec<Ref>)>,
}

#[derive(Debug)]
pub struct DomainMap {
    pub aggregates: Vec<Aggregate>,
    pub policies: Vec<Policy>,
}

impl DomainMap {
    /// Each pass reads and parses one file at a time and drops it before the next, so the source text and syntax tree
    /// held at once are bounded by the largest file rather than the domain; rereading costs time, not memory. What
    /// does grow with the domain is the map itself, its names and edges, which is what this returns.
    pub fn read<S: Sources + ?Sized>(sources: &S) -> Result<Self> {
        // A `#[cfg(test)] mod tests;` keeps its items in another file, which `walk` never sees the attribute of.
        let mut modules = Vec::new();
        sources.each(&mut |source| {
            let file = parse(source)?;
            let dir = Path::new(&source.path).parent().unwrap_or(Path::new(""));
            out_of_line_modules(
                &normalize(Path::new(&source.path)),
                &file.items,
                dir,
                &children_dir(Path::new(&source.path)),
                // A file-level `#![cfg(test)]` gates every module the file declares.
                is_test(&file.attrs),
                &mut modules,
            );
            Ok(())
        })?;
        let domain_file = |source: &Source| -> Result<Option<syn::File>> {
            if test_only(&modules, &normalize(Path::new(&source.path))) {
                return Ok(None);
            }
            let file = parse(source)?;
            Ok((!is_test(&file.attrs)).then_some(file))
        };
        // Declarations first: a policy's arms can only be read once every `query_events!` wrapper is known.
        let mut scan = Scan::default();
        sources.each(&mut |source| match domain_file(source)? {
            Some(file) => walk(&file.items, &mut |item| {
                scan.declaration(&source.path, item)
            }),
            None => Ok(()),
        })?;
        sources.each(&mut |source| match domain_file(source)? {
            Some(file) => walk(&file.items, &mut |item| scan.behaviour(&source.path, item)),
            None => Ok(()),
        })?;
        scan.assemble()
    }

    pub fn to_markdown(&self) -> String {
        let mut out = String::from(concat!(
            "# Domain map\n\n",
            "<!-- Generated by `cargo replay-map`. Do not edit by hand. -->\n\n",
            "Aggregates are yellow, commands blue, events orange and policies purple.\n\n",
            "```mermaid\n",
            // Text is pinned to black so no renderer theme can grey it out, and every fill keeps it at WCAG AAA (7:1 or
            // better). The mid-grey lines stay above 3:1 on a light and a dark page, since the renderer's theme is the
            // reader's.
            "---\n",
            "config:\n",
            "  theme: base\n",
            "  themeVariables:\n",
            "    lineColor: \"#808080\"\n",
            "    primaryTextColor: \"#000000\"\n",
            "    nodeTextColor: \"#000000\"\n",
            "    titleColor: \"#000000\"\n",
            "---\n",
            "flowchart LR\n",
            "    classDef command fill:#9dc3e3,stroke:#4a7599,color:#000\n",
            "    classDef event fill:#eeb07c,stroke:#9c6232,color:#000\n",
            "    classDef policy fill:#c7b3de,stroke:#6f5790,color:#000\n",
        ));
        // Writing to a `String` cannot fail.
        for aggregate in &self.aggregates {
            let name = &aggregate.name;
            let group = node_id("agg", &[name]);
            let _ = writeln!(out, "\n    subgraph {group} [{name}]");
            for command in &aggregate.commands {
                let _ = writeln!(
                    out,
                    "        {}(\"{command}\"):::command",
                    command_id(name, command)
                );
            }
            for event in &aggregate.events {
                let _ = writeln!(
                    out,
                    "        {}(\"{event}\"):::event",
                    event_id(name, event)
                );
            }
            let _ = writeln!(out, "    end");
            let _ = writeln!(
                out,
                "    style {group} fill:#eedc92,stroke:#8f7d33,color:#000"
            );
            for (command, event) in &aggregate.decisions {
                let _ = writeln!(
                    out,
                    "    {} --> {}",
                    command_id(name, command),
                    event_id(name, event)
                );
            }
        }
        for policy in &self.policies {
            for (index, (events, commands)) in policy.reactions.iter().enumerate() {
                let node = node_id("pol", &[&policy.name, &index.to_string()]);
                let _ = writeln!(out, "\n    {node}(\"{}\"):::policy", policy.name);
                for (aggregate, event) in events {
                    let _ = writeln!(out, "    {} --> {node}", event_id(aggregate, event));
                }
                for (aggregate, command) in commands {
                    let _ = writeln!(out, "    {node} --> {}", command_id(aggregate, command));
                }
            }
        }
        out.push_str("```\n");
        out
    }
}

fn command_id(aggregate: &str, command: &str) -> String {
    node_id("cmd", &[aggregate, command])
}

fn event_id(aggregate: &str, event: &str) -> String {
    node_id("evt", &[aggregate, event])
}

/// Joined with `-`, which no Rust identifier contains, so two different name pairs never share a node: `_` would
/// make aggregate `A_B` with command `C` collide with aggregate `A` and command `B_C`.
fn node_id(kind: &str, names: &[&str]) -> String {
    std::iter::once(kind)
        .chain(names.iter().copied())
        .collect::<Vec<_>>()
        .join("-")
}

struct Located<T> {
    file: String,
    line: usize,
    value: T,
}

impl<T> Located<T> {
    fn at(file: &str, span: Span, value: T) -> Self {
        Self {
            file: file.to_owned(),
            line: span.start().line,
            value,
        }
    }

    fn error(&self, message: String) -> MapError {
        MapError {
            file: self.file.clone(),
            line: self.line,
            message,
        }
    }
}

struct AggregateDef {
    name: String,
    commands: Vec<String>,
    events: Vec<String>,
}

/// One match arm: what its pattern matches, and what it builds.
type Arm = Located<(Vec<Ref>, Vec<Ref>)>;

struct Handle {
    aggregate: String,
    arms: Vec<Arm>,
}

struct PolicyDef {
    name: String,
    arms: Vec<Arm>,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Command,
    Event,
}

impl Kind {
    fn suffix(self) -> &'static str {
        match self {
            Kind::Command => "Command",
            Kind::Event => "Event",
        }
    }
}

/// What a `match` in `handle` or `react` is over.
enum Matched {
    /// One aggregate's own command or event enum.
    Enum { aggregate: String, kind: Kind },
    /// A `query_events!` wrapper, whose variants are named after the event types it merges.
    Wrapper { name: String, members: Vec<String> },
}

/// What a `handle` or `react` arm returns.
enum Builds {
    /// `<Agg>Event::<Variant>` values, from `handle`.
    Events(String),
    /// `(urn, <Target>Command::<Variant>)` pairs, from `AggregatePolicy::react`.
    Pairs(String),
    /// `Dispatch::to::<Agg>(urn, <Agg>Command::<Variant>)`, from `Policy::react`.
    Dispatches,
}

#[derive(Default)]
struct Scan {
    aggregates: Vec<Located<AggregateDef>>,
    /// `query_events!` wrapper name → the event types it merges.
    wrappers: BTreeMap<String, Vec<String>>,
    handles: Vec<Located<Handle>>,
    policies: Vec<Located<PolicyDef>>,
}

/// Every item, descending into inline modules, except those compiled only for tests.
fn walk<'a>(items: &'a [Item], visit: &mut impl FnMut(&'a Item) -> Result<()>) -> Result<()> {
    for item in items {
        let attrs = match item {
            Item::Macro(m) => &m.attrs,
            Item::Impl(i) => &i.attrs,
            Item::Mod(m) => &m.attrs,
            _ => continue,
        };
        if is_test(attrs) {
            continue;
        }
        match item {
            Item::Mod(m) => {
                if let Some((_, items)) = &m.content {
                    walk(items, visit)?;
                }
            }
            _ => visit(item)?,
        }
    }
    Ok(())
}

fn parse(source: &Source) -> Result<syn::File> {
    syn::parse_file(&source.text).map_err(|e| {
        error(
            &source.path,
            e.span(),
            format!("expected Rust that parses: {e}"),
        )
    })
}

/// An out-of-line module's file, or the directory its submodules live in, as one `mod` declaration names it.
struct Module {
    path: PathBuf,
    /// Compiled only under test by its own `cfg` or an inline module's around it.
    test: bool,
    /// The file the declaration is in, whose own gate the module inherits.
    declared_in: PathBuf,
}

/// Whether `file` is compiled only under test: every declaration closest to it is test-only, by its own `cfg` or by
/// sitting in a file that is itself test-only. A file a test-only module and a production one both name, as `#[path]`
/// allows, is production's. Read once every file has been seen, so source order does not matter.
fn test_only(modules: &[Module], file: &Path) -> bool {
    gated(modules, file, 0)
}

fn gated(modules: &[Module], file: &Path, depth: usize) -> bool {
    // Deeper than any real module tree: a `#[path]` cycle, which rustc rejects anyway.
    if depth > 64 {
        return false;
    }
    let naming = || modules.iter().filter(|m| file.starts_with(&m.path));
    let Some(closest) = naming().map(|m| m.path.components().count()).max() else {
        return false;
    };
    naming()
        .filter(|m| m.path.components().count() == closest)
        .all(|m| m.test || gated(modules, &m.declared_in, depth + 1))
}

/// Collects into `out` the file of every out-of-line module and the directory its own submodules live in, each with
/// whether it is compiled only under test, by the reference's rules: `scope` is where this scope's `mod m;` lives
/// (`m.rs` or `m/mod.rs`); a `#[path]` resolves against `dir`, the declaring file's directory, at the top level and
/// against `scope` inside an inline module. `gated` marks a scope already inside a test-only module; a gate on
/// `file` itself is resolved later, by `test_only`.
fn out_of_line_modules(
    file: &Path,
    items: &[Item],
    dir: &Path,
    scope: &Path,
    gated: bool,
    out: &mut Vec<Module>,
) {
    for item in items {
        let Item::Mod(m) = item else { continue };
        let test = gated || is_test(&m.attrs);
        let path = path_attr(&m.attrs);
        match (&m.content, path) {
            (Some((_, items)), path) => {
                let inner = scope.join(path.unwrap_or_else(|| m.ident.unraw().to_string()));
                out_of_line_modules(file, items, &inner, &inner, test, out);
            }
            (None, path) => {
                let mut push = |path: PathBuf| {
                    out.push(Module {
                        path,
                        test,
                        declared_in: file.to_path_buf(),
                    })
                };
                match path {
                    Some(path) => {
                        let target = normalize(&dir.join(path));
                        // `#[path = "mod.rs"]` beside the declaring file would claim that file's own directory.
                        let children = children_dir(&target);
                        if !file.starts_with(&children) {
                            push(children);
                        }
                        push(target);
                    }
                    None => {
                        let module = normalize(&scope.join(m.ident.unraw().to_string()));
                        push(module.with_extension("rs"));
                        // `m/mod.rs` and every submodule of either form.
                        push(module);
                    }
                }
            }
        }
    }
}

/// Where the submodules of the module in `file` live: beside a `mod.rs`, `lib.rs` or `main.rs`, under a directory
/// named for any other file.
fn children_dir(file: &Path) -> PathBuf {
    let dir = file.parent().unwrap_or(Path::new(""));
    match file.file_stem().and_then(|stem| stem.to_str()) {
        Some("mod" | "lib" | "main") | None => dir.to_path_buf(),
        Some(stem) => dir.join(stem),
    }
}

fn path_attr(attrs: &[Attribute]) -> Option<String> {
    attrs.iter().find_map(|attr| match &attr.meta {
        Meta::NameValue(nv) if nv.path.is_ident("path") => match &nv.value {
            Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(s),
                ..
            }) => Some(s.value()),
            _ => None,
        },
        _ => None,
    })
}

/// Resolves `.` and `..` without the file system, so a `#[path = "../x.rs"]` matches the path `x.rs` was read under.
fn normalize(path: &Path) -> PathBuf {
    use std::path::Component;

    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir
                if matches!(out.components().next_back(), Some(Component::Normal(_))) =>
            {
                out.pop();
            }
            other => out.push(other),
        }
    }
    out
}

impl Scan {
    fn declaration(&mut self, file: &str, item: &Item) -> Result<()> {
        let Item::Macro(m) = item else { return Ok(()) };
        if is_named(&m.mac.path, "define_aggregate") {
            self.aggregates.push(aggregate(file, &m.mac)?);
        } else if is_named(&m.mac.path, "query_events") {
            let (name, members) = wrapper(file, &m.mac)?;
            if self.wrappers.insert(name.clone(), members).is_some() {
                return Err(error(
                    file,
                    m.mac.path.span(),
                    format!("one `query_events!` named `{name}`"),
                ));
            }
        }
        Ok(())
    }

    fn behaviour(&mut self, file: &str, item: &Item) -> Result<()> {
        let Item::Impl(imp) = item else { return Ok(()) };
        match imp
            .trait_
            .as_ref()
            .and_then(|(_, path, _)| path.segments.last())
        {
            Some(t) if ident_name(&t.ident) == "Aggregate" => self.handles.push(handle(file, imp)?),
            Some(t) if ident_name(&t.ident) == "AggregatePolicy" => {
                let policy = policy(file, imp, &self.wrappers, true)?;
                self.policies.push(policy);
            }
            Some(t) if ident_name(&t.ident) == "Policy" => {
                let policy = policy(file, imp, &self.wrappers, false)?;
                self.policies.push(policy);
            }
            _ => {}
        }
        Ok(())
    }

    fn assemble(self) -> Result<DomainMap> {
        let mut aggregates: BTreeMap<String, (Located<AggregateDef>, Option<Vec<Arm>>)> =
            BTreeMap::new();
        for def in self.aggregates {
            if aggregates.contains_key(&def.value.name) {
                return Err(def.error(format!(
                    "one `define_aggregate!` named `{}`",
                    def.value.name
                )));
            }
            aggregates.insert(def.value.name.clone(), (def, None));
        }

        for handle in self.handles {
            let Some((_, arms)) = aggregates.get_mut(&handle.value.aggregate) else {
                return Err(handle.error(format!(
                    "`impl Aggregate for {}` to name a type declared with `define_aggregate!`",
                    handle.value.aggregate
                )));
            };
            if arms.is_some() {
                return Err(handle.error(format!(
                    "one `impl Aggregate for {}`",
                    handle.value.aggregate
                )));
            }
            *arms = Some(handle.value.arms);
        }

        let declared = |arm: &Arm, refs: &[Ref], kind: Kind| -> Result<()> {
            for (aggregate, variant) in refs {
                let def = aggregates.get(aggregate).map(|(def, _)| &def.value);
                let variants = def.map(|def| match kind {
                    Kind::Command => &def.commands,
                    Kind::Event => &def.events,
                });
                if !variants.is_some_and(|variants| variants.contains(variant)) {
                    return Err(arm.error(format!(
                        "`{aggregate}{}::{variant}` to be declared in `define_aggregate!`",
                        kind.suffix()
                    )));
                }
            }
            Ok(())
        };

        let mut map = DomainMap {
            aggregates: Vec::new(),
            policies: Vec::new(),
        };
        for (def, arms) in aggregates.values() {
            let Some(arms) = arms else {
                return Err(def.error(format!(
                    "an `impl Aggregate for {}` with a `handle`",
                    def.value.name
                )));
            };
            let mut decisions = Vec::new();
            for arm in arms {
                let (commands, events) = &arm.value;
                declared(arm, commands, Kind::Command)?;
                declared(arm, events, Kind::Event)?;
                for (_, command) in commands {
                    for (_, event) in events {
                        push_unique(&mut decisions, (command.clone(), event.clone()));
                    }
                }
            }
            map.aggregates.push(Aggregate {
                name: def.value.name.clone(),
                commands: def.value.commands.clone(),
                events: def.value.events.clone(),
                decisions,
            });
        }

        let mut policies = self.policies;
        policies.sort_by(|a, b| a.value.name.cmp(&b.value.name));
        if let Some(pair) = policies
            .windows(2)
            .find(|pair| pair[0].value.name == pair[1].value.name)
        {
            return Err(pair[1].error(format!("one policy named `{}`", pair[1].value.name)));
        }
        for policy in policies {
            let mut reactions = Vec::new();
            for arm in &policy.value.arms {
                let (events, commands) = &arm.value;
                declared(arm, events, Kind::Event)?;
                declared(arm, commands, Kind::Command)?;
                if !commands.is_empty() {
                    reactions.push(arm.value.clone());
                }
            }
            map.policies.push(Policy {
                name: policy.value.name.clone(),
                reactions,
            });
        }
        Ok(map)
    }
}

fn push_unique<T: PartialEq>(items: &mut Vec<T>, item: T) {
    if !items.contains(&item) {
        items.push(item);
    }
}

fn error(file: &str, span: Span, message: String) -> MapError {
    MapError {
        file: file.to_owned(),
        line: span.start().line,
        message,
    }
}

/// `#[cfg(test)]`, or any `cfg` that holds only under test, such as `#[cfg(all(test, not(target_arch = "wasm32")))]`.
fn is_test(attrs: &[Attribute]) -> bool {
    fn test_only(meta: &Meta) -> bool {
        match meta {
            Meta::Path(path) => path.is_ident("test"),
            Meta::List(list) if list.path.is_ident("all") => list
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .is_ok_and(|all| all.iter().any(test_only)),
            _ => false,
        }
    }
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg") && attr.parse_args::<Meta>().is_ok_and(|meta| test_only(&meta))
    })
}

/// An identifier as the type system reads it: `r#Light` and `Light` are one name, and the macro builds `LightCommand`
/// from either.
fn ident_name(ident: &Ident) -> String {
    ident.unraw().to_string()
}

fn is_named(path: &syn::Path, name: &str) -> bool {
    path.segments
        .last()
        .is_some_and(|segment| ident_name(&segment.ident) == name)
}

fn snippet(tokens: &impl ToTokens) -> String {
    let text = tokens.to_token_stream().to_string();
    match text.char_indices().nth(80) {
        Some((cut, _)) => format!("{}…", &text[..cut]),
        None => text,
    }
}

/// `Name<Generics> { key: value .. }`, where `commands` and `events` are braced lists of variants and every other value
/// (`namespace`, `state`, `service: FileService + LogService { .. }`) runs to the next section and is ignored, as are
/// the generics.
fn aggregate(file: &str, mac: &Macro) -> Result<Located<AggregateDef>> {
    let parser = |input: ParseStream| -> syn::Result<AggregateDef> {
        let name: Ident = input.parse()?;
        input.parse::<Generics>()?;
        let body;
        braced!(body in input);
        let (mut commands, mut events) = (None, None);
        while !body.is_empty() {
            let key: Ident = body.parse()?;
            body.parse::<Token![:]>()?;
            let mut value = Vec::new();
            // `define_aggregate!` makes the comma between sections optional, so a value also ends where the next
            // `key:` starts. Neither ends it inside a service path's generics, `Storage<String, usize>`, whose angle
            // brackets are not token groups.
            let mut angles = 0usize;
            while !body.is_empty() && (angles > 0 || !body.peek(Token![,]) && !next_section(&body))
            {
                let token = body.parse::<TokenTree>()?;
                if let TokenTree::Punct(punct) = &token {
                    // The `>` of `->` and `=>` closes nothing.
                    let arrow = matches!(value.last(), Some(TokenTree::Punct(p))
                        if matches!(p.as_char(), '-' | '=') && p.spacing() == proc_macro2::Spacing::Joint);
                    match punct.as_char() {
                        '<' => angles += 1,
                        '>' if !arrow => angles = angles.saturating_sub(1),
                        _ => {}
                    }
                }
                value.push(token);
            }
            let slot = match ident_name(&key).as_str() {
                "commands" => Some(&mut commands),
                "events" => Some(&mut events),
                _ => None,
            };
            if let Some(slot) = slot {
                let group = match value.as_slice() {
                    [TokenTree::Group(group)] if group.delimiter() == Delimiter::Brace => group,
                    _ => return Err(syn::Error::new(key.span(), format!("`{key}: {{ .. }}`"))),
                };
                // A repeated section adds to the earlier one, as it does in the macro.
                slot.get_or_insert_with(Vec::new)
                    .extend(variant_names.parse2(group.stream())?);
            }
            if body.peek(Token![,]) {
                body.parse::<Token![,]>()?;
            }
        }
        // The macro defaults an omitted section to an empty list.
        Ok(AggregateDef {
            commands: commands.unwrap_or_default(),
            events: events.unwrap_or_default(),
            name: ident_name(&name),
        })
    };
    let def = parser.parse2(mac.tokens.clone()).map_err(|e| {
        error(
            file,
            e.span(),
            format!("expected a `define_aggregate!` this generator can read: {e}"),
        )
    })?;
    Ok(Located::at(file, mac.path.span(), def))
}

fn next_section(input: ParseStream) -> bool {
    input.peek(Ident) && input.peek2(Token![:]) && !input.peek2(Token![::])
}

/// The variant names of a `commands` or `events` block, read as `define_aggregate!` reads them: commas between
/// variants are optional, and each name may carry a braced (or parenthesised) payload the map ignores.
fn variant_names(input: ParseStream) -> syn::Result<Vec<String>> {
    let mut names = Vec::new();
    while !input.is_empty() {
        Attribute::parse_outer(input)?;
        let name: Ident = input.parse()?;
        if input.peek(token::Brace) || input.peek(token::Paren) {
            input.parse::<TokenTree>()?;
        }
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
        }
        names.push(ident_name(&name));
    }
    Ok(names)
}

/// `Name => [AEvent, BEvent]`.
fn wrapper(file: &str, mac: &Macro) -> Result<(String, Vec<String>)> {
    let parser = |input: ParseStream| -> syn::Result<(String, Vec<String>)> {
        let name: Ident = input.parse()?;
        input.parse::<Token![=>]>()?;
        let list;
        bracketed!(list in input);
        let members = Punctuated::<Type, Token![,]>::parse_terminated(&list)?;
        let members = members
            .iter()
            .map(|ty| type_name(ty).ok_or_else(|| syn::Error::new(ty.span(), "a named event type")))
            .collect::<syn::Result<_>>()?;
        Ok((ident_name(&name), members))
    };
    parser.parse2(mac.tokens.clone()).map_err(|e| {
        error(
            file,
            e.span(),
            format!("expected a `query_events!` this generator can read: {e}"),
        )
    })
}

fn handle(file: &str, imp: &ItemImpl) -> Result<Located<Handle>> {
    let aggregate = self_name(file, imp)?;
    let matched = Matched::Enum {
        aggregate: aggregate.clone(),
        kind: Kind::Command,
    };
    let reader = Reader {
        file,
        builds: Builds::Events(aggregate.clone()),
    };
    let arms = arms(file, tail_match(file, imp, "handle")?, &matched, &reader)?;
    Ok(Located::at(
        file,
        imp.self_ty.span(),
        Handle { aggregate, arms },
    ))
}

/// An `impl AggregatePolicy` when `targeted`, otherwise a raw `impl Policy`.
fn policy(
    file: &str,
    imp: &ItemImpl,
    wrappers: &BTreeMap<String, Vec<String>>,
    targeted: bool,
) -> Result<Located<PolicyDef>> {
    let name = self_name(file, imp)?;
    let associated = |wanted: &str| {
        imp.items
            .iter()
            .find_map(|item| match item {
                ImplItem::Type(t) if ident_name(&t.ident) == wanted && !is_test(&t.attrs) => {
                    type_name(&t.ty)
                }
                _ => None,
            })
            .ok_or_else(|| {
                error(
                    file,
                    imp.self_ty.span(),
                    format!("`type {wanted} = <Type>;` in the policy `{name}`"),
                )
            })
    };
    let event = associated("Event")?;
    let matched = match (wrappers.get(&event), event.strip_suffix("Event")) {
        (Some(members), _) => Matched::Wrapper {
            name: event.clone(),
            members: members.clone(),
        },
        (None, Some(aggregate)) => Matched::Enum {
            aggregate: aggregate.to_owned(),
            kind: Kind::Event,
        },
        (None, None) => {
            return Err(error(
                file,
                imp.self_ty.span(),
                format!(
                    "`type Event` of `{name}` to be an aggregate's `<Agg>Event` or a `query_events!` wrapper; found \
                     `{event}`"
                ),
            ));
        }
    };
    let builds = if targeted {
        Builds::Pairs(associated("Target")?)
    } else {
        Builds::Dispatches
    };
    let reader = Reader { file, builds };
    let arms = arms(file, tail_match(file, imp, "react")?, &matched, &reader)?;
    Ok(Located::at(
        file,
        imp.self_ty.span(),
        PolicyDef { name, arms },
    ))
}

fn self_name(file: &str, imp: &ItemImpl) -> Result<String> {
    type_name(&imp.self_ty).ok_or_else(|| {
        error(
            file,
            imp.self_ty.span(),
            format!("a named type; found `{}`", snippet(&imp.self_ty)),
        )
    })
}

fn type_name(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(p) => p
            .path
            .segments
            .last()
            .map(|segment| ident_name(&segment.ident)),
        _ => None,
    }
}

/// The `match` that `fn <name>` ends in.
fn tail_match<'a>(file: &str, imp: &'a ItemImpl, name: &str) -> Result<&'a ExprMatch> {
    let function = imp.items.iter().find_map(|item| match item {
        ImplItem::Fn(f) if ident_name(&f.sig.ident) == name && !is_test(&f.attrs) => Some(f),
        _ => None,
    });
    let Some(function) = function else {
        return Err(error(
            file,
            imp.self_ty.span(),
            format!("`fn {name}` in `impl ... for {}`", snippet(&imp.self_ty)),
        ));
    };
    // A `#[cfg(test)]` statement after the dispatch is not compiled outside test.
    let stmts = &function.block.stmts;
    let last = stmts.iter().rposition(|stmt| !is_test(&outer_attrs(stmt)));
    let (tail, before) = match last {
        Some(last) => (Some(&stmts[last]), &stmts[..last]),
        None => (None, &stmts[..0]),
    };
    let Some(Stmt::Expr(Expr::Match(m), None)) = tail else {
        return Err(error(
            file,
            function.sig.ident.span(),
            format!("`fn {name}` to end in a `match` whose arms name one variant each"),
        ));
    };
    // A `return` before the dispatch, its scrutinee included, would decide outside any arm, where no command or event
    // names it.
    let mut returns = Returns::default();
    before.iter().for_each(|stmt| returns.visit_stmt(stmt));
    returns.visit_expr(&m.expr);
    let early = returns.found.iter().map(Spanned::span);
    if let Some(early) = early.chain(returns.hidden.iter().copied()).next() {
        return Err(error(
            file,
            early,
            format!("`fn {name}` to return only from the arms of its closing `match`"),
        ));
    }
    Ok(m)
}

fn arms(file: &str, body: &ExprMatch, matched: &Matched, reader: &Reader) -> Result<Vec<Arm>> {
    body.arms
        .iter()
        .filter(|arm| !is_test(&arm.attrs))
        .map(|arm| {
            let mut patterns = Vec::new();
            let mut wildcard = false;
            variants(file, &arm.pat, matched, &mut patterns, &mut wildcard)?;
            let mut built = Vec::new();
            if let Some((_, guard)) = &arm.guard {
                reader.exits(guard, &mut built)?;
            }
            reader.exits(&arm.body, &mut built)?;
            reader.expr(&arm.body, &mut built)?;
            // A wildcard names no variant, so there is nothing to draw an edge from.
            if wildcard && !built.is_empty() {
                return Err(error(
                    file,
                    arm.pat.span(),
                    "an arm that builds something to name the variants it matches, not `_`"
                        .to_owned(),
                ));
            }
            let mut unique = Vec::new();
            built
                .into_iter()
                .for_each(|built| push_unique(&mut unique, built));
            Ok(Located::at(file, arm.pat.span(), (patterns, unique)))
        })
        .collect()
}

fn variants(
    file: &str,
    pat: &Pat,
    matched: &Matched,
    out: &mut Vec<Ref>,
    wildcard: &mut bool,
) -> Result<()> {
    let path = match pat {
        Pat::Or(or) => {
            return or
                .cases
                .iter()
                .try_for_each(|case| variants(file, case, matched, out, wildcard));
        }
        Pat::Paren(p) => return variants(file, &p.pat, matched, out, wildcard),
        Pat::Reference(r) => return variants(file, &r.pat, matched, out, wildcard),
        Pat::Wild(_) => {
            *wildcard = true;
            return Ok(());
        }
        Pat::Path(p) => Some(&p.path),
        Pat::Struct(s) => Some(&s.path),
        Pat::TupleStruct(t) => Some(&t.path),
        _ => None,
    };
    let found = match matched {
        Matched::Enum { aggregate, kind } => path
            .and_then(|path| variant_of(path, &format!("{aggregate}{}", kind.suffix())))
            .map(|variant| out.push((aggregate.clone(), variant))),
        Matched::Wrapper { name, members } => {
            match (pat, path.and_then(|path| variant_of(path, name))) {
                (Pat::TupleStruct(t), Some(member))
                    if t.elems.len() == 1 && members.contains(&member) =>
                {
                    let inner = Matched::Enum {
                        aggregate: member
                            .strip_suffix("Event")
                            .unwrap_or(member.as_str())
                            .to_owned(),
                        kind: Kind::Event,
                    };
                    return variants(file, &t.elems[0], &inner, out, wildcard);
                }
                _ => None,
            }
        }
    };
    found.ok_or_else(|| {
        let expected = match matched {
            Matched::Enum { aggregate, kind } => {
                format!("`{aggregate}{}::<Variant>`", kind.suffix())
            }
            Matched::Wrapper { name, .. } => format!("`{name}::<Agg>Event(<Agg>Event::<Variant>)`"),
        };
        error(
            file,
            pat.span(),
            format!("a match arm pattern {expected}; found `{}`", snippet(pat)),
        )
    })
}

fn variant_of(path: &syn::Path, prefix: &str) -> Option<String> {
    let segments: Vec<_> = path.segments.iter().collect();
    match segments.as_slice() {
        // Qualified or not: `crate::bank::BankAccountEvent::Deposited` names the same variant.
        // The enum's generic arguments, `FileManagerEvent::<T>`, do not change the variant.
        [.., head, variant] if ident_name(&head.ident) == prefix => {
            Some(ident_name(&variant.ident))
        }
        _ => None,
    }
}

fn path_of(expr: &Expr) -> Option<Vec<String>> {
    match expr {
        Expr::Path(p) => Some(
            p.path
                .segments
                .iter()
                .map(|s| ident_name(&s.ident))
                .collect(),
        ),
        _ => None,
    }
}

/// The aggregate named by `Dispatch::to::<Agg>`.
fn dispatch_target(func: &Expr) -> Option<String> {
    let Expr::Path(p) = func else { return None };
    let segments: Vec<_> = p.path.segments.iter().collect();
    let [.., dispatch, to] = segments.as_slice() else {
        return None;
    };
    if dispatch.ident != "Dispatch" || to.ident != "to" {
        return None;
    }
    let PathArguments::AngleBracketed(generics) = &to.arguments else {
        return None;
    };
    match generics.args.iter().collect::<Vec<_>>().as_slice() {
        [GenericArgument::Type(ty)] => type_name(ty),
        _ => None,
    }
}

/// Reads what one arm builds, following every expression in result position down to its constructors.
struct Reader<'a> {
    file: &'a str,
    builds: Builds,
}

impl Reader<'_> {
    fn expr(&self, expr: &Expr, out: &mut Vec<Ref>) -> Result<()> {
        match expr {
            Expr::Paren(p) => self.expr(&p.expr, out),
            Expr::Group(g) => self.expr(&g.expr, out),
            // A labeled block can yield through `break 'label value`, which is not read.
            Expr::Block(b) if b.label.is_some() => Err(self.unreadable(expr)),
            Expr::Block(b) => self.block(&b.block, out),
            Expr::If(i) => {
                self.block(&i.then_branch, out)?;
                match &i.else_branch {
                    Some((_, otherwise)) => self.expr(otherwise, out),
                    None => Ok(()),
                }
            }
            Expr::Match(m) => m
                .arms
                .iter()
                .filter(|arm| !is_test(&arm.attrs))
                .try_for_each(|arm| self.expr(&arm.body, out)),
            Expr::Return(r) => self.returned(r, out),
            Expr::Call(call) => match (path_of(&call.func).as_deref(), &self.builds) {
                (Some([err]), _) if err == "Err" => Ok(()),
                (Some([wrap]), _) if (wrap == "Ok" || wrap == "Some") && call.args.len() == 1 => {
                    self.expr(&call.args[0], out)
                }
                (Some([vec, new]), _) if vec == "Vec" && new == "new" && call.args.is_empty() => {
                    Ok(())
                }
                (_, Builds::Dispatches) => match dispatch_target(&call.func) {
                    Some(target) if call.args.len() == 2 => {
                        self.constructor(&call.args[1], &target, Kind::Command, out)
                    }
                    _ => Err(self.unreadable(expr)),
                },
                (_, Builds::Events(aggregate)) => {
                    self.constructor(expr, aggregate, Kind::Event, out)
                }
                (_, Builds::Pairs(_)) => Err(self.unreadable(expr)),
            },
            Expr::Path(p) if p.path.is_ident("None") => Ok(()),
            Expr::Macro(m) if diverges(&m.mac) => Ok(()),
            Expr::Macro(m) if m.mac.path.is_ident("vec") => {
                let items = Punctuated::<Expr, Token![,]>::parse_terminated
                    .parse2(m.mac.tokens.clone())
                    .map_err(|_| self.unreadable(expr))?;
                items.iter().try_for_each(|item| self.expr(item, out))
            }
            Expr::MethodCall(call) => match call.method.to_string().as_str() {
                "collect" | "unwrap_or_default" if call.args.is_empty() => {
                    self.expr(&call.receiver, out)
                }
                "with_metadata" if matches!(self.builds, Builds::Dispatches) => {
                    self.expr(&call.receiver, out)
                }
                "map" | "flat_map" | "filter_map" | "and_then" => match call.args.first() {
                    Some(Expr::Closure(closure))
                        if call.args.len() == 1 && !self.mentioned_in(&call.receiver) =>
                    {
                        self.exits(&closure.body, out)?;
                        self.expr(&closure.body, out)
                    }
                    _ => Err(self.unreadable(expr)),
                },
                _ => Err(self.unreadable(expr)),
            },
            Expr::Tuple(t) if t.elems.len() == 2 => match &self.builds {
                Builds::Pairs(target) => self.constructor(&t.elems[1], target, Kind::Command, out),
                _ => Err(self.unreadable(expr)),
            },
            _ => match &self.builds {
                Builds::Events(aggregate) => self.constructor(expr, aggregate, Kind::Event, out),
                _ => Err(self.unreadable(expr)),
            },
        }
    }

    /// The block's tail. Its `return`s are read by `exits`, from the arm or closure it belongs to.
    fn block(&self, block: &Block, out: &mut Vec<Ref>) -> Result<()> {
        match block
            .stmts
            .iter()
            .rfind(|stmt| !is_test(&outer_attrs(stmt)))
        {
            Some(Stmt::Expr(tail, None)) => self.expr(tail, out),
            // `syn` reads a trailing `vec! { .. }` as a statement, though it is the block's value.
            Some(Stmt::Macro(m)) if m.semi_token.is_none() => self.expr(
                &Expr::Macro(syn::ExprMacro {
                    attrs: m.attrs.clone(),
                    mac: m.mac.clone(),
                }),
                out,
            ),
            // A block that ends in `m!(..);` is the arm's value only if the macro diverges, which a known one does by
            // panicking and an unknown one may do by returning what it was given.
            Some(Stmt::Macro(m)) => {
                if diverges(&m.mac) {
                    Ok(())
                } else {
                    Err(error(
                        self.file,
                        m.mac.path.span(),
                        self.expected(&format!("`{}` ending the arm", snippet(&m.mac))),
                    ))
                }
            }
            _ => Ok(()),
        }
    }

    /// Every `return` anywhere in `expr` that leaves the function or closure being read, in a guard, a condition or a
    /// scrutinee as much as in result position: what it returns is built as surely as the tail is.
    fn exits(&self, expr: &Expr, out: &mut Vec<Ref>) -> Result<()> {
        let mut returns = Returns::default();
        returns.visit_expr(expr);
        if let Some(&hidden) = returns.hidden.first() {
            return Err(error(
                self.file,
                hidden,
                self.expected("a `return` inside a macro whose input is not expressions"),
            ));
        }
        returns.found.iter().try_for_each(|r| self.returned(r, out))
    }

    fn returned(&self, r: &ExprReturn, out: &mut Vec<Ref>) -> Result<()> {
        match &r.expr {
            Some(value) => self.expr(value, out),
            None => Err(error(self.file, r.span(), self.expected("a bare `return`"))),
        }
    }

    fn constructor(
        &self,
        expr: &Expr,
        aggregate: &str,
        kind: Kind,
        out: &mut Vec<Ref>,
    ) -> Result<()> {
        let path = match expr {
            Expr::Path(p) => Some(&p.path),
            Expr::Struct(s) => Some(&s.path),
            Expr::Call(call) => match call.func.as_ref() {
                Expr::Path(p) => Some(&p.path),
                _ => None,
            },
            _ => None,
        };
        match path.and_then(|path| variant_of(path, &format!("{aggregate}{}", kind.suffix()))) {
            Some(variant) => {
                out.push((aggregate.to_owned(), variant));
                Ok(())
            }
            None => Err(self.unreadable(expr)),
        }
    }

    /// Whether `expr` builds anything itself, which a `.map` receiver must not: only the closure is read.
    fn mentioned_in(&self, expr: &Expr) -> bool {
        struct Mentions<'p>(&'p str, bool);
        impl Visit<'_> for Mentions<'_> {
            fn visit_path(&mut self, path: &syn::Path) {
                self.1 |= path
                    .segments
                    .first()
                    .is_some_and(|segment| ident_name(&segment.ident) == self.0);
                visit::visit_path(self, path);
            }
        }
        let marker = match &self.builds {
            Builds::Events(aggregate) => format!("{aggregate}Event"),
            Builds::Pairs(target) => format!("{target}Command"),
            Builds::Dispatches => "Dispatch".to_owned(),
        };
        let mut mentions = Mentions(&marker, false);
        mentions.visit_expr(expr);
        mentions.1
    }

    fn unreadable(&self, expr: &Expr) -> MapError {
        error(
            self.file,
            expr.span(),
            self.expected(&format!("`{}`", snippet(expr))),
        )
    }

    fn expected(&self, found: &str) -> String {
        match &self.builds {
            Builds::Events(aggregate) => format!(
                "each `handle` arm to build its events inline, as `Ok(vec![{aggregate}Event::<Variant>])`, \
                 `Vec::new()` or `Err(..)`; found {found}"
            ),
            Builds::Pairs(target) => format!(
                "each `react` arm to build its commands inline, as `vec![(urn, {target}Command::<Variant>)]`, a \
                 `.map` to such a pair, or `Vec::new()`; found {found}"
            ),
            Builds::Dispatches => format!(
                "each `react` arm to build its commands inline, as `vec![Dispatch::to::<Agg>(urn, \
                 <Agg>Command::<Variant>)]`, a `.map` to such a dispatch, or `Vec::new()`; found {found}"
            ),
        }
    }
}

/// The `return`s that leave the function being read: not those of a closure or a nested item. A macro's tokens are
/// opaque to the visitor, so one whose input reads as expressions, as `vec!`, `format!` and `assert!` do, is parsed and
/// searched; one whose input does not is `hidden` if a `return` appears anywhere in it.
#[derive(Default)]
struct Returns {
    found: Vec<ExprReturn>,
    hidden: Vec<Span>,
}

impl<'ast> Visit<'ast> for Returns {
    fn visit_expr_return(&mut self, r: &'ast ExprReturn) {
        self.found.push(r.clone());
        visit::visit_expr_return(self, r);
    }

    fn visit_macro(&mut self, mac: &'ast Macro) {
        // `vec![element; count]`.
        let repeated = |input: ParseStream| -> syn::Result<[Expr; 2]> {
            let element: Expr = input.parse()?;
            input.parse::<Token![;]>()?;
            Ok([element, input.parse()?])
        };
        if let Ok(items) =
            Punctuated::<Expr, Token![,]>::parse_terminated.parse2(mac.tokens.clone())
        {
            items.iter().for_each(|item| self.visit_expr(item));
        } else if let Ok(items) = repeated.parse2(mac.tokens.clone()) {
            items.iter().for_each(|item| self.visit_expr(item));
        } else if mentions_return(mac.tokens.clone()) {
            self.hidden.push(mac.path.span());
        }
    }

    fn visit_expr_closure(&mut self, _: &'ast ExprClosure) {}

    fn visit_expr_async(&mut self, _: &'ast syn::ExprAsync) {}

    fn visit_item(&mut self, _: &'ast Item) {}

    /// A `#[cfg(test)]` arm is not compiled outside test, so nothing it returns is built.
    fn visit_arm(&mut self, arm: &'ast syn::Arm) {
        if !is_test(&arm.attrs) {
            visit::visit_arm(self, arm);
        }
    }

    /// Nor is a `#[cfg(test)]` statement: a `let`, an expression statement or a macro.
    fn visit_stmt(&mut self, stmt: &'ast Stmt) {
        if !is_test(&outer_attrs(stmt)) {
            visit::visit_stmt(self, stmt);
        }
    }
}

/// A macro that never yields a value, so builds nothing: `panic!`, `unreachable!`, `todo!`, `unimplemented!`.
fn diverges(mac: &Macro) -> bool {
    ["panic", "unreachable", "todo", "unimplemented"]
        .iter()
        .any(|name| mac.path.is_ident(name))
}

/// The outer attributes of a statement, which `syn` keeps on whichever expression or item it holds.
fn outer_attrs(stmt: &Stmt) -> Vec<Attribute> {
    let leading = |input: ParseStream| -> syn::Result<Vec<Attribute>> {
        let attrs = Attribute::parse_outer(input)?;
        input.parse::<proc_macro2::TokenStream>()?;
        Ok(attrs)
    };
    leading.parse2(stmt.to_token_stream()).unwrap_or_default()
}

fn mentions_return(tokens: proc_macro2::TokenStream) -> bool {
    tokens.into_iter().any(|token| match token {
        TokenTree::Ident(ident) => ident == "return",
        TokenTree::Group(group) => mentions_return(group.stream()),
        _ => false,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const DRAWER: &str = r#"
define_aggregate! {
    Drawer {
        namespace: "drawer",
        state: { locked: bool },
        commands: { Fit { cabinet: CabinetUrn }, Lock, Unlock, Store(ItemUrn) },
        events: { Fitted { cabinet: CabinetUrn }, Locked, Unlocked, ItemStored(ItemUrn) }
    }
}

impl Aggregate for Drawer {
    async fn handle(&self, command: Self::Command, _: &()) -> Result<Vec<Self::Event>, AppError> {
        match command {
            DrawerCommand::Fit { cabinet } => {
                self.require(&cabinet)?;
                if self.cabinet.is_some() {
                    return Ok(Vec::new());
                }
                Ok(vec![DrawerEvent::Fitted { cabinet }])
            }
            DrawerCommand::Lock => Ok(if self.locked { Vec::new() } else { vec![DrawerEvent::Locked] }),
            DrawerCommand::Unlock => Err(AppError::conflict("never")),
            DrawerCommand::Store(item) => Ok(vec![DrawerEvent::ItemStored(item)]),
        }
    }
}
"#;

    const CABINET: &str = r#"
define_aggregate! {
    Cabinet {
        namespace: "cabinet",
        commands: { Seal, Install { drawer: DrawerUrn } },
        events: { Sealed { drawers: Vec<DrawerUrn> }, Installed { drawer: DrawerUrn }, Relabelled }
    }
}

impl Aggregate for Cabinet {
    async fn handle(&self, command: Self::Command, _: &()) -> Result<Vec<Self::Event>, AppError> {
        match command {
            CabinetCommand::Seal => Ok(vec![CabinetEvent::Sealed { drawers: self.drawers.clone() }]),
            CabinetCommand::Install { drawer } => Ok(vec![CabinetEvent::Installed { drawer }]),
        }
    }
}
"#;

    const POLICY: &str = r#"
pub struct DrawerPolicy;

impl AggregatePolicy for DrawerPolicy {
    type Event = CabinetEvent;
    type Target = Drawer;

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<(DrawerUrn, DrawerCommand)> {
        match &event.data {
            CabinetEvent::Installed { drawer } => CabinetUrn::try_from(event.stream_id.clone())
                .map(|cabinet| vec![(drawer.clone(), DrawerCommand::Fit { cabinet })])
                .unwrap_or_default(),
            CabinetEvent::Sealed { drawers } => drawers.iter().map(|d| (d.clone(), DrawerCommand::Lock)).collect(),
            CabinetEvent::Relabelled => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    impl AggregatePolicy for Unreadable {
        fn react(&self) -> Vec<()> { helper() }
    }
}
"#;

    /// A raw `Policy` reading two aggregates' events through a `query_events!` wrapper and commanding both.
    const HOUSEKEEPING: &str = r#"
query_events!(Workshop => [CabinetEvent, DrawerEvent]);

pub struct Housekeeping;

impl Policy for Housekeeping {
    type Event = Workshop;

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        match &event.data {
            Workshop::CabinetEvent(CabinetEvent::Sealed { drawers }) => drawers
                .iter()
                .map(|d| Dispatch::to::<Drawer>(d.clone(), DrawerCommand::Lock).with_metadata(trace()))
                .collect(),
            Workshop::DrawerEvent(DrawerEvent::Locked | DrawerEvent::Unlocked) => {
                vec![Dispatch::to::<Cabinet>(cabinet(), CabinetCommand::Seal)]
            }
            _ => Vec::new(),
        }
    }
}
"#;

    fn read_all(sources: &[(&str, &str)]) -> Result<DomainMap> {
        let sources: Vec<_> = sources
            .iter()
            .map(|(path, text)| Source {
                path: path.to_string(),
                text: text.to_string(),
            })
            .collect();
        DomainMap::read(sources.as_slice())
    }

    fn read(drawer: &str, policy: &str) -> Result<DomainMap> {
        read_all(&[
            ("cabinet.rs", CABINET),
            ("drawer.rs", drawer),
            ("policy.rs", policy),
        ])
    }

    fn line_of(text: &str, needle: &str) -> usize {
        text.lines().position(|line| line.contains(needle)).unwrap() + 1
    }

    fn refs(items: &[(&str, &str)]) -> Vec<Ref> {
        items
            .iter()
            .map(|(a, b)| (a.to_string(), b.to_string()))
            .collect()
    }

    #[test]
    fn every_aggregate_lists_its_commands_and_events_by_name() {
        let map = read(DRAWER, POLICY).unwrap();

        let names: Vec<_> = map.aggregates.iter().map(|a| a.name.as_str()).collect();
        assert_eq!(names, ["Cabinet", "Drawer"]);
        let drawer = &map.aggregates[1];
        assert_eq!(drawer.commands, ["Fit", "Lock", "Unlock", "Store"]);
        assert_eq!(
            drawer.events,
            ["Fitted", "Locked", "Unlocked", "ItemStored"]
        );
    }

    #[test]
    fn a_command_leads_to_every_event_its_arm_builds_and_to_nothing_else() {
        let map = read(DRAWER, POLICY).unwrap();

        assert_eq!(
            map.aggregates[1].decisions,
            refs(&[
                ("Fit", "Fitted"),
                ("Lock", "Locked"),
                ("Store", "ItemStored")
            ])
        );
    }

    #[test]
    fn a_reacting_arm_leads_from_its_event_to_the_command_it_issues() {
        let map = read(DRAWER, POLICY).unwrap();

        assert_eq!(
            map.policies[0].reactions,
            [
                (
                    refs(&[("Cabinet", "Installed")]),
                    refs(&[("Drawer", "Fit")])
                ),
                (refs(&[("Cabinet", "Sealed")]), refs(&[("Drawer", "Lock")])),
            ]
        );
    }

    #[test]
    fn a_raw_policy_leads_from_each_wrapped_event_to_the_commands_it_dispatches() {
        let map = read_all(&[
            ("cabinet.rs", CABINET),
            ("drawer.rs", DRAWER),
            ("housekeeping.rs", HOUSEKEEPING),
        ])
        .unwrap();

        let policy = &map.policies[0];
        assert_eq!(policy.name, "Housekeeping");
        assert_eq!(
            policy.reactions,
            [
                (refs(&[("Cabinet", "Sealed")]), refs(&[("Drawer", "Lock")])),
                (
                    refs(&[("Drawer", "Locked"), ("Drawer", "Unlocked")]),
                    refs(&[("Cabinet", "Seal")])
                ),
            ]
        );
    }

    #[test]
    fn a_wildcard_arm_that_dispatches_fails_at_its_line() {
        let policy = HOUSEKEEPING.replace(
            "_ => Vec::new(),",
            "_ => vec![Dispatch::to::<Cabinet>(cabinet(), CabinetCommand::Seal)],",
        );

        let error = read_all(&[
            ("cabinet.rs", CABINET),
            ("drawer.rs", DRAWER),
            ("p.rs", &policy),
        ])
        .unwrap_err();

        assert_eq!(error.line, line_of(&policy, "_ => vec!["));
    }

    #[test]
    fn a_handle_arm_that_builds_its_events_through_a_helper_fails_at_its_line() {
        let drawer = DRAWER.replace(
            "Ok(vec![DrawerEvent::ItemStored(item)])",
            "self.store(item)",
        );

        let error = read(&drawer, POLICY).unwrap_err();

        assert_eq!(
            (error.file.as_str(), error.line),
            ("drawer.rs", line_of(&drawer, "self.store(item)"))
        );
    }

    #[test]
    fn a_react_arm_that_builds_its_commands_through_a_helper_fails_at_its_line() {
        let policy = POLICY.replace(
            "drawers.iter().map(|d| (d.clone(), DrawerCommand::Lock)).collect()",
            "lock_all(drawers)",
        );

        let error = read(DRAWER, &policy).unwrap_err();

        assert_eq!(
            (error.file.as_str(), error.line),
            ("policy.rs", line_of(&policy, "lock_all(drawers)"))
        );
    }

    #[test]
    fn a_raw_policy_arm_that_dispatches_through_a_helper_fails_at_its_line() {
        let policy = HOUSEKEEPING.replace(
            "vec![Dispatch::to::<Cabinet>(cabinet(), CabinetCommand::Seal)]",
            "self.reseal()",
        );

        let error = read_all(&[
            ("cabinet.rs", CABINET),
            ("drawer.rs", DRAWER),
            ("p.rs", &policy),
        ])
        .unwrap_err();

        assert_eq!(
            (error.file.as_str(), error.line),
            ("p.rs", line_of(&policy, "self.reseal()"))
        );
    }

    #[test]
    fn an_arm_that_builds_an_undeclared_variant_fails_at_its_line() {
        let drawer = DRAWER.replace("vec![DrawerEvent::Locked]", "vec![DrawerEvent::Jammed]");

        let error = read(&drawer, POLICY).unwrap_err();

        assert_eq!(error.line, line_of(&drawer, "DrawerCommand::Lock =>"));
    }

    #[test]
    fn a_return_before_the_dispatch_fails_at_its_line() {
        let guard = "if self.sealed { return Ok(vec![DrawerEvent::Locked]); }";
        let drawer = DRAWER.replace(
            "        match command {",
            &format!("        {guard}\n        match command {{"),
        );

        let error = read(&drawer, POLICY).unwrap_err();

        assert_eq!(error.line, line_of(&drawer, guard));
    }

    #[test]
    fn two_policies_with_one_name_fail() {
        let policy = format!(
            "{POLICY}\nmod again {{\n{}\n}}",
            POLICY.split("#[cfg(test)]").next().unwrap()
        );

        assert!(read(DRAWER, &policy).is_err());
    }

    #[test]
    fn names_joined_by_an_underscore_still_get_distinct_nodes() {
        let source = |aggregate: &str, command: &str| {
            format!(
                "define_aggregate! {{ {aggregate} {{ commands: {{ {command} }}, events: {{ {command} }} }} }}
                 impl Aggregate for {aggregate} {{
                     async fn handle(&self, c: Self::Command, _: &()) -> R {{
                         match c {{ {aggregate}Command::{command} => Ok(vec![{aggregate}Event::{command}]) }}
                     }}
                 }}"
            )
        };
        let (a_b, a) = (source("A_B", "C"), source("A", "B_C"));

        let markdown = read_all(&[("a_b.rs", &a_b), ("a.rs", &a)])
            .unwrap()
            .to_markdown();

        assert!(markdown.contains("cmd-A_B-C("));
        assert!(markdown.contains("cmd-A-B_C("));
        assert!(markdown.contains("evt-A_B-C("));
        assert!(markdown.contains("evt-A-B_C("));
    }

    #[test]
    fn an_aggregate_with_generics_and_service_sections_is_read() {
        let generic = r#"
define_aggregate! {
    FileManager<T: PartialEq> {
        state: { processed: T },
        commands: { ProcessFile { data: T } },
        events: { FileProcessed { data: T } },
        service: FileService + LogService {
            fn validate(entry: &str) -> bool;
        }
    }
}

impl<T: PartialEq> Aggregate for FileManager<T> {
    async fn handle(&self, command: Self::Command, _: &Self::Services) -> R {
        match command {
            FileManagerCommand::ProcessFile { data } => Ok(vec![FileManagerEvent::FileProcessed { data }]),
        }
    }
}
"#;

        let map = read_all(&[("file_manager.rs", generic)]).unwrap();

        assert_eq!(
            map.aggregates[0].decisions,
            refs(&[("ProcessFile", "FileProcessed")])
        );
    }

    #[test]
    fn a_module_compiled_only_under_test_is_skipped_however_its_cfg_is_spelled() {
        let tests = r#"
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    impl Aggregate for OnlyInTests {
        async fn handle(&self) -> R { helper() }
    }
}
"#;

        assert!(read_all(&[("cabinet.rs", CABINET), ("tests.rs", tests)]).is_ok());
    }

    #[test]
    fn a_module_compiled_only_under_test_in_another_file_is_skipped() {
        let unreadable = r#"
impl Aggregate for OnlyInTests {
    async fn handle(&self) -> R { helper() }
}
"#;
        let root = r#"
mod cabinet;
mod drawer;
#[cfg(test)]
mod tests;
#[cfg(test)]
#[path = "../fixtures/support.rs"]
mod support;
mod inline {
    #[cfg(test)]
    mod nested;
}
"#;
        let drawer = format!("{DRAWER}\n#[cfg(all(test, unix))]\nmod fakes;\n");

        let map = read_all(&[
            ("src/lib.rs", root),
            ("src/cabinet.rs", CABINET),
            ("src/drawer.rs", &drawer),
            ("src/tests.rs", unreadable),
            ("src/tests/deeper.rs", unreadable),
            ("src/drawer/fakes/mod.rs", unreadable),
            ("src/drawer/fakes/more.rs", unreadable),
            ("src/inline/nested.rs", unreadable),
            ("fixtures/support.rs", unreadable),
        ])
        .unwrap();

        assert_eq!(map.aggregates.len(), 2);
    }

    #[test]
    fn a_module_compiled_outside_test_beside_a_test_one_is_still_read() {
        let root = "mod drawer;\n#[cfg(test)]\nmod drawer_tests;\n";
        let unreadable = "impl Aggregate for X { async fn handle(&self) -> R { helper() } }";

        let error = read_all(&[
            ("src/lib.rs", root),
            ("src/cabinet.rs", CABINET),
            ("src/drawer.rs", unreadable),
            ("src/drawer_tests.rs", unreadable),
        ])
        .unwrap_err();

        assert_eq!(error.file, "src/drawer.rs");
    }

    #[test]
    fn a_file_a_test_module_shares_with_production_is_still_read() {
        let root =
            "mod cabinet;\n#[cfg(test)]\n#[path = \"cabinet.rs\"]\nmod cabinet_under_test;\n";

        let map = read_all(&[("src/lib.rs", root), ("src/cabinet.rs", CABINET)]).unwrap();

        assert_eq!(map.aggregates.len(), 1);
    }

    #[test]
    fn a_return_in_the_closing_match_scrutinee_fails_at_its_line() {
        let early =
            "match { if self.locked { return Ok(vec![DrawerEvent::Unlocked]); } command } {";
        let drawer = DRAWER.replace("match command {", early);

        let error = read(&drawer, POLICY).unwrap_err();

        assert_eq!(error.line, line_of(&drawer, early));
    }

    #[test]
    fn a_return_in_a_guard_a_condition_or_a_closure_is_what_the_arm_issues() {
        let policy = POLICY
            .replace(
                "CabinetEvent::Relabelled => Vec::new(),",
                "CabinetEvent::Relabelled if { if self.on { return vec![(d(), DrawerCommand::Unlock)]; } false } => \
                 Vec::new(),",
            )
            .replace(
                "drawers.iter().map(|d| (d.clone(), DrawerCommand::Lock)).collect()",
                "drawers.iter().map(|d| { if d.open() { return (d.clone(), DrawerCommand::Unlock); } \
                 (d.clone(), DrawerCommand::Lock) }).collect()",
            )
            .replace(
                "CabinetEvent::Installed { drawer } => CabinetUrn::try_from(event.stream_id.clone())",
                "CabinetEvent::Installed { drawer } => if { if self.off { return vec![(d(), DrawerCommand::Lock)]; } \
                 true } { Vec::new() } else { CabinetUrn::try_from(event.stream_id.clone())",
            )
            .replace(".unwrap_or_default(),", ".unwrap_or_default() },");

        let map = read(DRAWER, &policy).unwrap();

        assert_eq!(
            map.policies[0].reactions,
            [
                (
                    refs(&[("Cabinet", "Installed")]),
                    refs(&[("Drawer", "Lock"), ("Drawer", "Fit")])
                ),
                (
                    refs(&[("Cabinet", "Sealed")]),
                    refs(&[("Drawer", "Unlock"), ("Drawer", "Lock")])
                ),
                (
                    refs(&[("Cabinet", "Relabelled")]),
                    refs(&[("Drawer", "Unlock")])
                ),
            ]
        );
    }

    #[test]
    fn a_guard_that_returns_through_a_helper_fails_at_its_line() {
        let guard = "CabinetEvent::Relabelled if { if self.on { return lock_all(); } false } => Vec::new(),";
        let policy = POLICY.replace("CabinetEvent::Relabelled => Vec::new(),", guard);

        let error = read(DRAWER, &policy).unwrap_err();

        assert_eq!(error.line, line_of(&policy, guard));
    }

    #[test]
    fn a_trailing_brace_delimited_vec_is_read_as_the_arms_value() {
        let policy = HOUSEKEEPING.replace(
            "_ => Vec::new(),",
            "_ => { vec! { Dispatch::to::<Cabinet>(cabinet(), CabinetCommand::Seal) } }",
        );

        let error = read_all(&[
            ("cabinet.rs", CABINET),
            ("drawer.rs", DRAWER),
            ("p.rs", &policy),
        ])
        .unwrap_err();

        assert_eq!(error.line, line_of(&policy, "_ => { vec! {"));
    }

    #[test]
    fn a_service_path_with_generic_arguments_is_read() {
        let source = r#"
define_aggregate! {
    Ledger {
        commands: { Post },
        events: { Posted }
        service: Storage<String, usize> + Callback<Box<dyn Fn(u8) -> u8>, Bound<Item: Clone>> {
            fn get(key: &str) -> usize;
        }
    }
}

impl Aggregate for Ledger {
    async fn handle(&self, command: Self::Command, _: &Self::Services) -> R {
        match command {
            LedgerCommand::Post => Ok(vec![LedgerEvent::Posted]),
        }
    }
}
"#;

        let map = read_all(&[("ledger.rs", source)]).unwrap();

        assert_eq!(map.aggregates[0].decisions, refs(&[("Post", "Posted")]));
    }

    #[test]
    fn a_module_declared_in_a_test_only_file_is_test_only_too() {
        let unreadable = "impl Aggregate for X { async fn handle(&self) -> R { helper() } }";

        // The child comes first: its gate is inherited however the sources are ordered.
        let map = read_all(&[
            ("src/tests/fixtures.rs", unreadable),
            ("src/tests/fixtures/deeper.rs", unreadable),
            ("src/lib.rs", "mod cabinet;\n#[cfg(test)]\nmod tests;\n"),
            ("src/cabinet.rs", CABINET),
            ("src/tests.rs", "mod fixtures;\n"),
        ])
        .unwrap();

        assert_eq!(map.aggregates.len(), 1);
    }

    #[test]
    fn a_labeled_block_is_rejected_at_its_line() {
        let arm = "_ => 'out: { if self.enabled { break 'out vec![Dispatch::to::<Drawer>(id(), DrawerCommand::Lock)]; } \
                   Vec::new() }";
        let policy = HOUSEKEEPING.replace("_ => Vec::new(),", arm);

        let error = read_all(&[
            ("cabinet.rs", CABINET),
            ("drawer.rs", DRAWER),
            ("p.rs", &policy),
        ])
        .unwrap_err();

        assert_eq!(error.line, line_of(&policy, "'out: {"));
    }

    #[test]
    fn a_return_inside_a_vec_element_is_what_the_arm_builds() {
        let drawer = DRAWER.replace(
            "Ok(if self.locked { Vec::new() } else { vec![DrawerEvent::Locked] })",
            "Ok(vec![{ if self.locked { return Ok(vec![DrawerEvent::Unlocked]); } DrawerEvent::Locked }])",
        );

        let map = read(&drawer, POLICY).unwrap();

        let drawer = map.aggregates.iter().find(|a| a.name == "Drawer").unwrap();
        assert!(drawer
            .decisions
            .contains(&("Lock".to_owned(), "Unlocked".to_owned())));
        assert!(drawer
            .decisions
            .contains(&("Lock".to_owned(), "Locked".to_owned())));
    }

    #[test]
    fn a_return_inside_a_vec_element_through_a_helper_fails_at_its_line() {
        let drawer = DRAWER.replace(
            "Ok(if self.locked { Vec::new() } else { vec![DrawerEvent::Locked] })",
            "Ok(vec![{ if self.locked { return self.unlock(); } DrawerEvent::Locked }])",
        );

        let error = read(&drawer, POLICY).unwrap_err();

        assert_eq!(error.line, line_of(&drawer, "return self.unlock()"));
    }

    #[test]
    fn a_file_gated_from_inside_is_skipped_with_the_modules_it_declares() {
        let unreadable = "impl Aggregate for X { async fn handle(&self) -> R { helper() } }";
        let tests = format!("#![cfg(test)]\nmod fixtures;\n{unreadable}\n");

        let map = read_all(&[
            ("src/lib.rs", "mod cabinet;\nmod tests;\n"),
            ("src/cabinet.rs", CABINET),
            ("src/tests.rs", &tests),
            ("src/tests/fixtures.rs", unreadable),
        ])
        .unwrap();

        assert_eq!(map.aggregates.len(), 1);
    }

    #[test]
    fn a_raw_identifier_names_the_same_aggregate_and_variant_as_its_plain_spelling() {
        let source = r#"
define_aggregate! {
    r#Light {
        commands: { r#Switch },
        events: { Switched }
    }
}

impl Aggregate for Light {
    async fn handle(&self, command: Self::Command, _: &()) -> R {
        match command {
            r#LightCommand::Switch => Ok(vec![LightEvent::r#Switched]),
        }
    }
}
"#;

        let map = read_all(&[("light.rs", source)]).unwrap();

        assert_eq!(map.aggregates[0].name, "Light");
        assert_eq!(map.aggregates[0].decisions, refs(&[("Switch", "Switched")]));
        assert!(map
            .to_markdown()
            .contains("cmd-Light-Switch --> evt-Light-Switched"));
    }

    #[test]
    fn an_omitted_section_is_an_empty_one() {
        let source = r#"
define_aggregate! {
    Gate {
        commands: { Refuse }
    }
}

impl Aggregate for Gate {
    async fn handle(&self, command: Self::Command, _: &()) -> R {
        match command {
            GateCommand::Refuse => Err(AppError::conflict("never")),
        }
    }
}
"#;

        let map = read_all(&[("gate.rs", source)]).unwrap();

        assert_eq!(map.aggregates[0].commands, ["Refuse"]);
        assert!(map.aggregates[0].events.is_empty());
    }

    #[test]
    fn a_return_inside_a_vec_outside_result_position_is_still_read() {
        let read_arm = |arm: &str| {
            let policy = HOUSEKEEPING.replace("_ => Vec::new(),", arm);
            let error = read_all(&[
                ("cabinet.rs", CABINET),
                ("drawer.rs", DRAWER),
                ("p.rs", &policy),
            ])
            .unwrap_err();
            (error.line, line_of(&policy, "let _ = vec!["))
        };

        // Through a helper: unreadable.
        let (line, expected) = read_arm(
            "_ => { let _ = vec![{ if self.enabled { return self.reseal(); } 0u8 }]; Vec::new() }",
        );
        assert_eq!(line, expected);
        // Inline: a command a `_` arm may not issue.
        let (line, expected) = read_arm(
            "_ => { let _ = vec![{ if self.enabled { return vec![Dispatch::to::<Cabinet>(c(), CabinetCommand::Seal)]; } \
             0u8 }]; Vec::new() }",
        );
        assert_eq!(line, expected);
    }

    #[test]
    fn a_return_inside_a_vec_before_the_dispatch_fails_at_its_line() {
        let early =
            "let _ = vec![{ if self.locked { return Ok(vec![DrawerEvent::Locked]); } 0u8 }];";
        let drawer = DRAWER.replace(
            "        match command {",
            &format!("        {early}\n        match command {{"),
        );

        let error = read(&drawer, POLICY).unwrap_err();

        assert_eq!(error.line, line_of(&drawer, early));
    }

    #[test]
    fn a_test_only_member_of_an_impl_is_passed_over_even_when_it_comes_first() {
        let drawer = DRAWER.replace(
            "impl Aggregate for Drawer {\n",
            "impl Aggregate for Drawer {\n    #[cfg(test)]\n    async fn handle(&self) -> R { helper() }\n\n",
        );
        let policy = POLICY.replace(
            "    type Event = CabinetEvent;\n",
            "    #[cfg(test)]\n    type Event = Bogus;\n    type Event = CabinetEvent;\n",
        );

        let map = read(&drawer, &policy).unwrap();
        let plain = read(DRAWER, POLICY).unwrap();

        assert_eq!(map.aggregates, plain.aggregates);
        assert_eq!(map.policies, plain.policies);
    }

    #[test]
    fn a_qualified_path_names_the_same_variant_and_dispatch_target() {
        let drawer = DRAWER
            .replace(
                "DrawerCommand::Store(item) => Ok(vec![DrawerEvent::ItemStored(item)])",
                "crate::drawer::DrawerCommand::Store(item) => Ok(vec![crate::drawer::DrawerEvent::ItemStored(item)])",
            );
        let policy = HOUSEKEEPING.replace(
            "vec![Dispatch::to::<Cabinet>(cabinet(), CabinetCommand::Seal)]",
            "vec![replay_persistence::Dispatch::to::<Cabinet>(cabinet(), crate::cabinet::CabinetCommand::Seal)]",
        );
        let sources = |drawer: &str, policy: &str| {
            read_all(&[
                ("cabinet.rs", CABINET),
                ("drawer.rs", drawer),
                ("housekeeping.rs", policy),
            ])
            .unwrap()
        };

        let map = sources(&drawer, &policy);
        let plain = sources(DRAWER, HOUSEKEEPING);

        assert_eq!(map.aggregates, plain.aggregates);
        assert_eq!(map.policies, plain.policies);
    }

    #[test]
    fn a_return_inside_any_macro_is_read_or_rejected_at_its_line() {
        let read_arm = |arm: &str| {
            let policy = HOUSEKEEPING.replace("_ => Vec::new(),", arm);
            let error = read_all(&[
                ("cabinet.rs", CABINET),
                ("drawer.rs", DRAWER),
                ("p.rs", &policy),
            ])
            .unwrap_err();
            (error.line, line_of(&policy, "_ => {"))
        };

        // Through a helper, inside `format!`'s arguments: unreadable.
        let (line, expected) =
            read_arm("_ => { let _ = format!(\"{}\", { if self.on { return self.reseal(); } 0u8 }); Vec::new() }");
        assert_eq!(line, expected);
        // Inline, in the same place: a command a `_` arm may not issue.
        let (line, expected) = read_arm(
            "_ => { let _ = format!(\"{}\", { if self.on { return vec![Dispatch::to::<Cabinet>(c(), \
             CabinetCommand::Seal)]; } 0u8 }); Vec::new() }",
        );
        assert_eq!(line, expected);
        // In a macro whose input is not expressions: rejected, since it cannot be read.
        let (line, expected) =
            read_arm("_ => { custom!(when on => { return self.reseal(); }); Vec::new() }");
        assert_eq!(line, expected);
    }

    #[test]
    fn a_test_module_at_the_declaring_files_own_mod_rs_gates_only_itself() {
        let unreadable = "impl Aggregate for X { async fn handle(&self) -> R { helper() } }";
        let tests = format!("mod fixtures;\n{unreadable}\n");

        let map = read_all(&[
            (
                "src/lib.rs",
                "mod cabinet;\n#[cfg(test)]\n#[path = \"mod.rs\"]\nmod tests;\n",
            ),
            ("src/cabinet.rs", CABINET),
            ("src/mod.rs", &tests),
            ("src/fixtures.rs", unreadable),
        ])
        .unwrap();

        assert_eq!(map.aggregates.len(), 1);
    }

    #[test]
    fn an_enums_generic_arguments_do_not_change_its_variant() {
        let source = r#"
define_aggregate! {
    FileManager<T: PartialEq> {
        commands: { ProcessFile { data: T } },
        events: { FileProcessed { data: T } }
    }
}

impl<T: PartialEq> Aggregate for FileManager<T> {
    async fn handle(&self, command: Self::Command, _: &Self::Services) -> R {
        match command {
            FileManagerCommand::<T>::ProcessFile { data } => Ok(vec![FileManagerEvent::<T>::FileProcessed { data }]),
        }
    }
}
"#;

        let map = read_all(&[("file_manager.rs", source)]).unwrap();

        assert_eq!(
            map.aggregates[0].decisions,
            refs(&[("ProcessFile", "FileProcessed")])
        );
    }

    #[test]
    fn a_test_only_arm_builds_nothing() {
        let drawer = DRAWER
            .replace(
                "            DrawerCommand::Lock =>",
                "            #[cfg(test)]\n            DrawerCommand::Lock => Ok(vec![DrawerEvent::Unlocked]),\n            \
                 #[cfg(test)]\n            DrawerCommand::Unlock => { return self.helper(); }\n            \
                 DrawerCommand::Lock =>",
            )
            .replace(
                "DrawerCommand::Store(item) => Ok(vec![DrawerEvent::ItemStored(item)]),",
                "DrawerCommand::Store(item) => match item {\n                #[cfg(test)]\n                _ => \
                 Ok(vec![DrawerEvent::Unlocked]),\n                _ => Ok(vec![DrawerEvent::ItemStored(item)]),\n            },",
            );

        let map = read(&drawer, POLICY).unwrap();
        let plain = read(DRAWER, POLICY).unwrap();

        assert_eq!(map.aggregates, plain.aggregates);
    }

    #[test]
    fn a_repeated_section_adds_to_the_earlier_one() {
        let source = r#"
define_aggregate! {
    Engine {
        commands: { Start },
        commands: { Stop },
        events: { Started }
    }
}

impl Aggregate for Engine {
    async fn handle(&self, command: Self::Command, _: &()) -> R {
        match command {
            EngineCommand::Start => Ok(vec![EngineEvent::Started]),
            EngineCommand::Stop => Err(AppError::conflict("never")),
        }
    }
}
"#;

        let map = read_all(&[("engine.rs", source)]).unwrap();

        assert_eq!(map.aggregates[0].commands, ["Start", "Stop"]);
    }

    #[test]
    fn an_arm_ending_in_a_macro_statement_must_be_one_that_diverges() {
        let read_arm = |arm: &str| {
            let policy = HOUSEKEEPING.replace("_ => Vec::new(),", arm);
            read_all(&[
                ("cabinet.rs", CABINET),
                ("drawer.rs", DRAWER),
                ("p.rs", &policy),
            ])
            .map_err(|error| (error.line, line_of(&policy, "issue!")))
        };

        let error = read_arm("_ => { issue!(DrawerCommand::Lock); }").unwrap_err();
        assert_eq!(error.0, error.1);
        assert!(read_arm("_ => { unreachable!(\"never\"); }").is_ok());
        assert!(read_arm("_ => unreachable!(),").is_ok());
    }

    #[test]
    fn a_test_only_statement_returns_nothing() {
        let drawer = DRAWER
            .replace(
                "            DrawerCommand::Fit { cabinet } => {\n",
                "            DrawerCommand::Fit { cabinet } => {\n                #[cfg(test)]\n                if self.on \
                 { return self.test_helper(); }\n                #[cfg(test)]\n                if self.on { return \
                 Ok(vec![DrawerEvent::Unlocked]); }\n",
            )
            .replace(
                "        match command {",
                "        #[cfg(test)]\n        if self.on { return Ok(vec![DrawerEvent::Locked]); }\n        match command {",
            );

        let map = read(&drawer, POLICY).unwrap();
        let plain = read(DRAWER, POLICY).unwrap();

        assert_eq!(map.aggregates, plain.aggregates);
    }

    #[test]
    fn a_test_only_tail_is_not_the_value_of_a_function_or_an_arm() {
        let drawer = DRAWER
            .replace(
                "            DrawerCommand::Store(item) => Ok(vec![DrawerEvent::ItemStored(item)]),",
                "            DrawerCommand::Store(item) => {\n                #[cfg(not(test))]\n                { \
                 Ok(vec![DrawerEvent::ItemStored(item)]) }\n                #[cfg(test)]\n                { \
                 self.test_helper() }\n            }",
            )
            .replace(
                "        match command {",
                "        #[cfg(not(test))]\n        match command {",
            )
            .replace(
                "        }\n    }\n}\n",
                "        }\n        #[cfg(test)]\n        match command { _ => self.test_helper() }\n    }\n}\n",
            );

        let map = read(&drawer, POLICY).unwrap();
        let plain = read(DRAWER, POLICY).unwrap();

        assert_eq!(map.aggregates, plain.aggregates);
    }

    #[test]
    fn a_wrapped_event_type_loses_one_event_suffix_only() {
        let source = r#"
define_aggregate! {
    DomainEvent {
        commands: { Record },
        events: { Recorded }
    }
}

impl Aggregate for DomainEvent {
    async fn handle(&self, command: Self::Command, _: &()) -> R {
        match command {
            DomainEventCommand::Record => Ok(vec![DomainEventEvent::Recorded]),
        }
    }
}

query_events!(Journal => [DomainEventEvent]);

pub struct Echo;

impl Policy for Echo {
    type Event = Journal;

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        match &event.data {
            Journal::DomainEventEvent(DomainEventEvent::Recorded) => {
                vec![Dispatch::to::<DomainEvent>(id(), DomainEventCommand::Record)]
            }
        }
    }
}
"#;

        let map = read_all(&[("domain_event.rs", source)]).unwrap();

        assert_eq!(
            map.policies[0].reactions,
            [(
                refs(&[("DomainEvent", "Recorded")]),
                refs(&[("DomainEvent", "Record")])
            )]
        );
    }

    #[test]
    fn an_aggregate_written_without_separating_commas_is_read() {
        let bare = r#"
define_aggregate! {
    Lamp {
        namespace: "lamp"
        state: { on: bool }
        commands: { TurnOn TurnOff { reason: String } }
        events: { TurnedOn TurnedOff { reason: String } }
    }
}

impl Aggregate for Lamp {
    async fn handle(&self, command: Self::Command, _: &()) -> R {
        match command {
            LampCommand::TurnOn => Ok(vec![LampEvent::TurnedOn]),
            LampCommand::TurnOff { reason } => Ok(vec![LampEvent::TurnedOff { reason }]),
        }
    }
}
"#;

        let map = read_all(&[("lamp.rs", bare)]).unwrap();

        assert_eq!(map.aggregates[0].commands, ["TurnOn", "TurnOff"]);
        assert_eq!(
            map.aggregates[0].decisions,
            refs(&[("TurnOn", "TurnedOn"), ("TurnOff", "TurnedOff")])
        );
    }

    #[test]
    fn the_map_draws_each_event_once_and_never_the_state() {
        let markdown = read(DRAWER, POLICY).unwrap().to_markdown();

        assert_eq!(markdown.matches("evt-Drawer-Locked(").count(), 1);
        assert!(markdown.contains("subgraph agg-Drawer [Drawer]"));
        assert!(markdown.contains("evt-Cabinet-Sealed --> pol-DrawerPolicy-1"));
        assert!(markdown.contains("pol-DrawerPolicy-1 --> cmd-Drawer-Lock"));
        assert!(!markdown.contains("bool"));
    }
}
