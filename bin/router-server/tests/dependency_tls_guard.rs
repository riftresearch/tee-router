use std::{path::PathBuf, process::Command};

const FORBIDDEN_PATTERNS: &[&str] = &[
    "default-tls",
    "native-tls",
    "hyper-tls",
    "rustls-native-certs",
    "rustls-tls-native-roots",
    "reqwest-default-tls",
    "reqwest-native-tls",
    "webpki-roots v0.25.4",
];

const REQUIRED_PATTERNS: &[&str] = &[
    r#"reqwest feature "rustls-tls-webpki-roots""#,
    r#"alloy feature "reqwest-rustls-tls""#,
    r#"sqlx-core feature "_tls-rustls-ring-webpki""#,
    r#"tracing-loki feature "rustls""#,
    r#"esplora-client feature "async-https-rustls""#,
    "webpki-roots v1.0.7",
];

#[test]
fn router_server_dependency_graph_uses_rustls_with_webpki_roots() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|dir| dir.parent())
        .expect("router-server manifest dir should live under the workspace root");

    let output = Command::new("cargo")
        .args([
            "tree",
            "-p",
            "router-server",
            "--edges",
            "normal",
            "-e",
            "features",
        ])
        .current_dir(workspace_root)
        .output()
        .expect("failed to run `cargo tree` for router-server TLS guard");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let tree_output = format!("{stdout}\n{stderr}");

    assert!(
        output.status.success(),
        "cargo tree failed for router-server TLS guard\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    for forbidden in FORBIDDEN_PATTERNS {
        assert!(
            !tree_output.contains(forbidden),
            "router-server dependency graph unexpectedly contains forbidden TLS pattern {forbidden:?}\n{tree_output}"
        );
    }

    for required in REQUIRED_PATTERNS {
        assert!(
            tree_output.contains(required),
            "router-server dependency graph no longer contains required TLS pattern {required:?}\n{tree_output}"
        );
    }
}
