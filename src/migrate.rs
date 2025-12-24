use std::path::Path;
use regex::Regex;

/// Scans a directory for migration files matching pattern NNNN_*.sql
/// Returns the next available migration number (highest + 1, or 1 if none exist)
pub fn find_next_migration_number(dir: &Path) -> std::io::Result<u32> {
    let pattern = Regex::new(r"^(\d{4})_.*\.sql$").unwrap();
    let mut max_number = 0;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let filename = entry.file_name();
        let filename_str = filename.to_string_lossy();

        if let Some(captures) = pattern.captures(&filename_str) {
            if let Some(number_str) = captures.get(1) {
                if let Ok(number) = number_str.as_str().parse::<u32>() {
                    if number > max_number {
                        max_number = number;
                    }
                }
            }
        }
    }

    Ok(max_number + 1)
}

/// Generates migration filename like "0003_add_users.sql"
/// Sanitizes name: lowercase, spaces to underscores, remove special chars
/// Collapses consecutive underscores and trims leading/trailing underscores
/// Panics if name contains no alphanumeric characters
pub fn generate_migration_filename(number: u32, name: &str) -> String {
    let sanitized: String = name
        .to_lowercase()
        .replace(' ', "_")
        .replace('-', "_")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();

    // Collapse consecutive underscores and trim
    let sanitized: String = sanitized
        .split('_')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("_");

    if sanitized.is_empty() {
        panic!("Migration name must contain at least one alphanumeric character");
    }

    format!("{:04}_{}.sql", number, sanitized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn finds_next_number_in_empty_dir() {
        let dir = TempDir::new().unwrap();
        let next = find_next_migration_number(dir.path()).unwrap();
        assert_eq!(next, 1);
    }

    #[test]
    fn finds_next_number_after_existing() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("0001_initial.sql"), "").unwrap();
        fs::write(dir.path().join("0002_users.sql"), "").unwrap();
        let next = find_next_migration_number(dir.path()).unwrap();
        assert_eq!(next, 3);
    }

    #[test]
    fn ignores_non_migration_files() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("README.md"), "").unwrap();
        fs::write(dir.path().join("0005_foo.sql"), "").unwrap();
        let next = find_next_migration_number(dir.path()).unwrap();
        assert_eq!(next, 6);
    }

    #[test]
    fn generates_filename_with_padding() {
        assert_eq!(generate_migration_filename(1, "initial"), "0001_initial.sql");
        assert_eq!(generate_migration_filename(42, "add users"), "0042_add_users.sql");
        assert_eq!(generate_migration_filename(999, "Test-Name"), "0999_test_name.sql");
    }

    #[test]
    fn handles_gaps_in_migration_numbers() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("0001_initial.sql"), "").unwrap();
        fs::write(dir.path().join("0005_skip.sql"), "").unwrap();
        let next = find_next_migration_number(dir.path()).unwrap();
        assert_eq!(next, 6);
    }

    #[test]
    fn sanitizes_special_characters() {
        assert_eq!(
            generate_migration_filename(1, "add@users!"),
            "0001_addusers.sql"
        );
        assert_eq!(
            generate_migration_filename(2, "   spaces   "),
            "0002_spaces.sql"
        );
        assert_eq!(
            generate_migration_filename(3, "multiple---dashes"),
            "0003_multiple_dashes.sql"
        );
    }

    #[test]
    #[should_panic(expected = "Migration name must contain at least one alphanumeric character")]
    fn panics_on_empty_name() {
        generate_migration_filename(1, "!!!");
    }
}
