//! Pins the empty default feature set: http-input crates stay inactive.
//!
//! `--edges normal` excludes `[dev-dependencies]` (this crate's test-only
//! `tokio`) so the denylist is the runtime tree, not the test harness.

const FORBIDDEN: &[&str] = &[
    "reqwest",
    "tokio",
    "quinn",
    "quinn-proto",
    "quinn-udp",
    "hyper-rustls",
];

#[test]
fn default_feature_tree_excludes_http_input_crates() {
    let output = std::process::Command::new(env!("CARGO"))
        .args([
            "tree",
            "--manifest-path",
            concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"),
            "--no-default-features",
            "--edges",
            "normal",
            "--prefix",
            "none",
            "--format",
            "{p}",
        ])
        .output()
        .expect("cargo tree");
    assert!(
        output.status.success(),
        "cargo tree --no-default-features failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tree = String::from_utf8_lossy(&output.stdout);
    let hits: Vec<&str> = tree
        .lines()
        .filter(|line| {
            line.split_whitespace()
                .next()
                .is_some_and(|name| FORBIDDEN.contains(&name))
        })
        .collect();
    assert!(
        hits.is_empty(),
        "default feature tree must not activate http-input crates: {hits:?}"
    );
}
