//! The binary end to end: write, re-run, check, and catch a stale map.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const DOMAIN: &str = r#"
define_aggregate! {
    Light {
        namespace: "light",
        state: { on: bool },
        commands: { Switch },
        events: { Switched }
    }
}

impl Aggregate for Light {
    async fn handle(&self, command: Self::Command, _: &()) -> Result<Vec<Self::Event>, Error> {
        match command {
            LightCommand::Switch => Ok(vec![LightEvent::Switched]),
        }
    }
}
"#;

/// A fresh directory per test, so tests running in parallel never share files.
fn workspace(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("replay-map-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join("src")).unwrap();
    fs::write(dir.join("src/light.rs"), DOMAIN).unwrap();
    dir
}

fn replay_map(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cargo-replay-map"))
        .arg("replay-map")
        .args(args)
        .current_dir(dir)
        .output()
        .unwrap()
}

#[test]
fn the_map_is_written_once_and_rerunning_changes_nothing() {
    let dir = workspace("rerun");

    assert!(replay_map(&dir, &[]).status.success());
    let first = fs::read_to_string(dir.join("docs/domain-map.md")).unwrap();
    assert!(replay_map(&dir, &[]).status.success());

    assert_eq!(
        fs::read_to_string(dir.join("docs/domain-map.md")).unwrap(),
        first
    );
    assert!(first.contains("cmd-Light-Switch --> evt-Light-Switched"));
}

#[test]
fn check_passes_on_a_current_map_and_fails_once_the_source_moves_on() {
    let dir = workspace("check");
    assert!(replay_map(&dir, &["--src", "src", "--out", "map.md"])
        .status
        .success());
    assert!(
        replay_map(&dir, &["--src", "src", "--out", "map.md", "--check"])
            .status
            .success()
    );

    let grown = DOMAIN.replace("events: { Switched }", "events: { Switched, Dimmed }");
    fs::write(dir.join("src/light.rs"), grown).unwrap();

    assert!(
        !replay_map(&dir, &["--src", "src", "--out", "map.md", "--check"])
            .status
            .success()
    );
}

#[test]
fn an_unreadable_arm_fails_the_run_with_its_file_and_line() {
    let dir = workspace("unreadable");
    let helper = DOMAIN.replace("Ok(vec![LightEvent::Switched])", "self.switch()");
    fs::write(dir.join("src/light.rs"), &helper).unwrap();

    let output = replay_map(&dir, &[]);

    assert!(!output.status.success());
    let line = helper
        .lines()
        .position(|l| l.contains("self.switch()"))
        .unwrap()
        + 1;
    assert!(String::from_utf8_lossy(&output.stderr).contains(&format!("light.rs:{line}:")));
}

#[test]
fn files_are_read_in_path_order_through_nested_directories() {
    let dir = workspace("order");
    let unreadable = DOMAIN.replace("Ok(vec![LightEvent::Switched])", "self.switch()");
    fs::create_dir_all(dir.join("src/a")).unwrap();
    fs::write(dir.join("src/a/z.rs"), &unreadable).unwrap();
    fs::write(dir.join("src/b.rs"), &unreadable).unwrap();

    let output = replay_map(&dir, &[]);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&format!("{}:", Path::new("src/a/z.rs").display())),
        "{stderr}"
    );
}

#[test]
fn a_source_directory_that_cannot_be_read_fails_the_run_naming_it() {
    let dir = workspace("missing");

    let output = replay_map(&dir, &["--src", "nowhere"]);

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("nowhere: cannot read"));
}

#[test]
fn check_passes_on_a_current_map_checked_out_with_crlf_line_endings() {
    let dir = workspace("crlf");
    assert!(replay_map(&dir, &["--out", "map.md"]).status.success());
    let map = fs::read_to_string(dir.join("map.md")).unwrap();
    fs::write(dir.join("map.md"), map.replace('\n', "\r\n")).unwrap();

    assert!(replay_map(&dir, &["--out", "map.md", "--check"])
        .status
        .success());
}

#[test]
fn an_option_given_where_a_path_belongs_is_refused_rather_than_written_to() {
    let dir = workspace("option-as-path");

    let output = replay_map(&dir, &["--out", "--check"]);

    assert!(!output.status.success());
    assert!(!dir.join("--check").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--out needs a file; found `--check`"));
}

#[cfg(unix)]
#[test]
fn a_link_back_to_a_directory_being_read_fails_the_run_naming_it() {
    let dir = workspace("link-cycle");
    std::os::unix::fs::symlink(".", dir.join("src/again")).unwrap();

    let output = replay_map(&dir, &[]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("src/again: links back to"), "{stderr}");
}

#[cfg(unix)]
#[test]
fn a_link_to_a_directory_outside_the_walk_is_followed() {
    let dir = workspace("link-out");
    fs::create_dir_all(dir.join("shared")).unwrap();
    let lamp = DOMAIN.replace("Light", "Lamp");
    fs::write(dir.join("shared/lamp.rs"), lamp).unwrap();
    std::os::unix::fs::symlink("../shared", dir.join("src/shared")).unwrap();

    assert!(replay_map(&dir, &[]).status.success());

    let map = fs::read_to_string(dir.join("docs/domain-map.md")).unwrap();
    assert!(map.contains("cmd-Lamp-Switch --> evt-Lamp-Switched"));
}
