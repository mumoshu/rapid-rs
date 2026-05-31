//! Build-time helpers for rapid-rs.
//!
//! Subcommands:
//!   `check-file-length` — fail if any non-test, non-generated source file
//!   exceeds 400 lines (per RULES.md).

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const HARD_LIMIT: usize = 400;

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("check-file-length") => check_file_length(),
        Some(other) => {
            eprintln!("xtask: unknown subcommand: {other}");
            ExitCode::from(2)
        }
        None => {
            eprintln!("xtask: missing subcommand (try: check-file-length)");
            ExitCode::from(2)
        }
    }
}

fn check_file_length() -> ExitCode {
    let workspace_root = match locate_workspace_root() {
        Ok(p) => p,
        Err(msg) => {
            eprintln!("xtask check-file-length: {msg}");
            return ExitCode::from(2);
        }
    };
    let crates_dir = workspace_root.join("crates");
    let mut violations: Vec<(PathBuf, usize)> = Vec::new();
    walk_rust_files(&crates_dir, &mut violations);

    if violations.is_empty() {
        println!("xtask check-file-length: OK (limit {HARD_LIMIT})");
        ExitCode::SUCCESS
    } else {
        eprintln!(
            "xtask check-file-length: {} violation(s) (>{HARD_LIMIT} lines):",
            violations.len()
        );
        for (p, n) in &violations {
            eprintln!("  {} : {} lines", p.display(), n);
        }
        ExitCode::FAILURE
    }
}

fn walk_rust_files(dir: &Path, violations: &mut Vec<(PathBuf, usize)>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            walk_rust_files(&path, violations);
        } else if path.extension().is_some_and(|e| e == "rs") {
            let count = count_logical_lines(&path);
            if count > HARD_LIMIT {
                violations.push((path, count));
            }
        }
    }
}

fn count_logical_lines(path: &Path) -> usize {
    let Ok(s) = fs::read_to_string(path) else {
        return 0;
    };
    // Generated proto modules are excluded (they may exceed by design).
    if s.contains("@generated") || s.contains("DO NOT EDIT") {
        return 0;
    }
    // RULES §File and function size: the 400-line cap excludes
    // `#[cfg(test)] mod tests { ... }` blocks. Find the first
    // `#[cfg(test)]` line and count only the lines above it.
    let mut total = 0usize;
    for line in s.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("#[cfg(test)]") {
            break;
        }
        total += 1;
    }
    total
}

fn locate_workspace_root() -> Result<PathBuf, String> {
    let here = env::current_dir().map_err(|e| e.to_string())?;
    let mut cur: Option<&Path> = Some(here.as_path());
    while let Some(p) = cur {
        if p.join("Cargo.toml").exists()
            && fs::read_to_string(p.join("Cargo.toml")).is_ok_and(|s| s.contains("[workspace]"))
        {
            return Ok(p.to_path_buf());
        }
        cur = p.parent();
    }
    Err("could not locate workspace root (no Cargo.toml with [workspace] above cwd)".into())
}
