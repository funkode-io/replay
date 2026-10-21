//! `cargo replay-map`: writes a Replay domain's event-storming map, or with `--check` fails when the committed one is
//! stale.

use std::path::PathBuf;
use std::process::ExitCode;
use std::{env, fs};

use replay_map::{DomainMap, SourceDirs};

const USAGE: &str = "usage: cargo replay-map [--src <dir>]... [--out <file>] [--check]

  --src <dir>   a directory of domain source, read recursively (repeatable; default: src)
  --out <file>  where the Mermaid map is written (default: docs/domain-map.md)
  --check       write nothing; fail if <file> differs from what the source generates";

struct Options {
    sources: Vec<PathBuf>,
    out: PathBuf,
    check: bool,
}

fn options() -> Result<Options, String> {
    let mut args = env::args().skip(1).peekable();
    // Cargo runs `cargo-replay-map replay-map <args>`; run directly, the subcommand name is absent.
    if args.peek().map(String::as_str) == Some("replay-map") {
        args.next();
    }
    let mut options = Options {
        sources: Vec::new(),
        out: PathBuf::from("docs/domain-map.md"),
        check: false,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--src" => options
                .sources
                .push(value(&mut args, "--src", "a directory")?.into()),
            "--out" => options.out = value(&mut args, "--out", "a file")?.into(),
            "--check" => options.check = true,
            "-h" | "--help" => return Err(String::new()),
            other => return Err(format!("unexpected argument `{other}`")),
        }
    }
    if options.sources.is_empty() {
        options.sources.push(PathBuf::from("src"));
    }
    Ok(options)
}

/// An option's value. One that looks like an option is a forgotten value: `--out --check` must not write a file named
/// `--check`. A path that does start with `-` can be passed as `./-name`.
fn value(
    args: &mut impl Iterator<Item = String>,
    option: &str,
    what: &str,
) -> Result<String, String> {
    match args.next() {
        Some(value) if !value.starts_with('-') => Ok(value),
        Some(value) => Err(format!("{option} needs {what}; found `{value}`")),
        None => Err(format!("{option} needs {what}")),
    }
}

fn run(options: &Options) -> Result<(), String> {
    let markdown = DomainMap::read(&SourceDirs(&options.sources))
        .map_err(|e| e.to_string())?
        .to_markdown();
    let out = options.out.display();
    if options.check {
        // A missing map is as stale as a wrong one.
        // A checkout that turns the map's line endings into CRLF has not changed it.
        let committed = fs::read_to_string(&options.out).map(|text| text.replace("\r\n", "\n"));
        if committed.ok().as_deref() != Some(markdown.as_str()) {
            return Err(format!(
                "{out} does not match the domain's source; rerun this command without `--check` to update it"
            ));
        }
        return Ok(());
    }
    if let Some(parent) = options.out.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
    }
    fs::write(&options.out, markdown).map_err(|e| format!("cannot write {out}: {e}"))
}

fn main() -> ExitCode {
    let options = match options() {
        Ok(options) => options,
        Err(message) => {
            if !message.is_empty() {
                eprintln!("cargo replay-map: {message}\n");
            }
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(&options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("cargo replay-map: {message}");
            ExitCode::FAILURE
        }
    }
}
