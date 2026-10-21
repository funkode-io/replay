//! The README's example diagram is what the map draws from that example's source, in `tests/readme/`.

use std::fs;
use std::path::Path;

use replay_map::{DomainMap, SourceDirs};

#[test]
fn the_readme_diagram_is_what_the_example_domain_generates() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let markdown = DomainMap::read(&SourceDirs(&[manifest.join("tests/readme")]))
        .unwrap()
        .to_markdown();
    let diagram = &markdown[markdown.find("```mermaid").unwrap()..];

    let readme = fs::read_to_string(manifest.join("../README.md")).unwrap();

    assert!(
        readme.contains(diagram),
        "README.md's example diagram is stale; replace it with:\n\n{diagram}"
    );
}
