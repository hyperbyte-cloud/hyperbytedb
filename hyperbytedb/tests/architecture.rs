//! The hexagonal layering rules, enforced over `src/`:
//!
//! - `domain/` imports none of `adapters`, `application` or `ports`;
//! - `ports/` imports neither `adapters` nor `application`;
//! - `application/` does not import `adapters`;
//! - `adapters/` imports `application` only from the driving adapters
//!   (`adapters/http/`) and the Raft state machine
//!   (`adapters/cluster/raft/{log_store,state_machine}.rs`).
//!
//! Test modules (`#[cfg(test)] mod … { … }`) and comments are not checked.

use std::path::{Path, PathBuf};

/// Known exceptions, as `"<file>::<target prefix>"`. Empty: add one only
/// with a comment naming the plan that removes it.
const ALLOWLIST: &[&str] = &[];

const LAYERS: [&str; 3] = ["adapters", "application", "ports"];

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Whether `file` (relative to `src/`, `/`-separated) in `layer` may import
/// `target` (one of [`LAYERS`]).
fn allowed(layer: &str, file: &str, target: &str) -> bool {
    match layer {
        "domain" => false,
        "ports" => target == "ports",
        "application" => target != "adapters",
        "adapters" => {
            target != "application"
                || file.starts_with("adapters/http/")
                || file == "adapters/cluster/raft/log_store.rs"
                || file == "adapters/cluster/raft/state_machine.rs"
        }
        _ => true,
    }
}

/// `crate::<layer>…` paths named on `line`, each as the full path.
fn crate_paths(line: &str) -> Vec<(&'static str, String)> {
    let mut found = Vec::new();
    for layer in LAYERS {
        let needle = format!("crate::{layer}");
        let mut rest = line;
        while let Some(i) = rest.find(&needle) {
            let tail = &rest[i..];
            let end = tail
                .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == ':'))
                .unwrap_or(tail.len());
            let path = tail[..end].trim_end_matches(':');
            // `crate::adapters` but not `crate::adaptersx`.
            if path.len() == needle.len() || path[needle.len()..].starts_with("::") {
                found.push((layer, path.to_string()));
            }
            rest = &tail[needle.len()..];
        }
    }
    found
}

/// Every production line under `src_root` that breaks a layering rule, as
/// `"<file>:<line>: <layer> imports <target>"`.
fn forbidden_imports(src_root: &Path) -> Vec<String> {
    let mut files = Vec::new();
    rs_files(src_root, &mut files);
    files.sort();
    let mut findings = Vec::new();
    for path in files {
        let rel = path
            .strip_prefix(src_root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let layer = rel.split('/').next().unwrap_or("");
        if !matches!(layer, "domain" | "ports" | "application" | "adapters") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        // Brace depth inside a `#[cfg(test)] mod`; `None` outside one.
        let mut test_depth: Option<i64> = None;
        let mut pending_cfg_test = false;
        for (n, raw) in lines.iter().enumerate() {
            let line = raw.trim();
            if let Some(depth) = test_depth.as_mut() {
                *depth += line.matches('{').count() as i64 - line.matches('}').count() as i64;
                if *depth <= 0 {
                    test_depth = None;
                }
                continue;
            }
            if line == "#[cfg(test)]" {
                pending_cfg_test = true;
                continue;
            }
            if pending_cfg_test {
                pending_cfg_test = false;
                if line.starts_with("mod ") || line.starts_with("pub mod ") {
                    if line.ends_with(';') {
                        continue;
                    }
                    let depth = line.matches('{').count() as i64 - line.matches('}').count() as i64;
                    if depth > 0 {
                        test_depth = Some(depth);
                    }
                    continue;
                }
            }
            if line.starts_with("//") {
                continue;
            }
            for (target, full) in crate_paths(line) {
                if allowed(layer, &rel, target) {
                    continue;
                }
                let allowlisted = ALLOWLIST.iter().any(|entry| {
                    entry
                        .split_once("::")
                        .is_some_and(|(file, prefix)| file == rel && full.starts_with(prefix))
                });
                if !allowlisted {
                    findings.push(format!("{rel}:{}: {layer} imports {full}", n + 1));
                }
            }
        }
    }
    findings
}

#[test]
fn scanner_flags_a_domain_adapter_import() {
    let dir = tempfile::tempdir().unwrap();
    let domain = dir.path().join("domain");
    std::fs::create_dir_all(&domain).unwrap();
    std::fs::write(
        domain.join("x.rs"),
        "use crate::adapters::wal;\n\
         // use crate::adapters::comment;\n\
         #[cfg(test)]\n\
         mod tests {\n    use crate::adapters::fine_in_tests;\n}\n",
    )
    .unwrap();
    let findings = forbidden_imports(dir.path());
    assert_eq!(
        findings,
        ["domain/x.rs:1: domain imports crate::adapters::wal"]
    );
}

#[test]
fn layering_rules_hold() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let findings = forbidden_imports(&src);
    assert!(
        findings.is_empty(),
        "layering violations ({}):\n{}",
        findings.len(),
        findings.join("\n")
    );
}
