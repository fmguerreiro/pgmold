use crate::model::Schema;
use crate::parser::parse_sql_string;
use crate::util::SchemaError;
use std::path::Path;
use std::process::Command;

type Result<T> = std::result::Result<T, SchemaError>;

pub fn load_drizzle_schema(config_path: &str) -> Result<Schema> {
    let path = Path::new(config_path);
    if !path.exists() {
        return Err(SchemaError::ParseError(format!(
            "Drizzle config file not found: {config_path}"
        )));
    }

    let working_dir = path.parent().unwrap_or(Path::new("."));

    let output = Command::new("npx")
        .arg("drizzle-kit")
        .arg("export")
        .arg("--config")
        .arg(path.file_name().unwrap_or(path.as_os_str()))
        .current_dir(working_dir)
        .output()
        .map_err(|e| {
            SchemaError::ParseError(format!(
                "Failed to run drizzle-kit export: {e}. \
                 Make sure drizzle-kit is installed (npm install drizzle-kit)"
            ))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Err(SchemaError::ParseError(format!(
            "drizzle-kit export failed with exit code {:?}:\nstderr: {stderr}\nstdout: {stdout}",
            output.status.code()
        )));
    }

    let sql = String::from_utf8(output.stdout).map_err(|e| {
        SchemaError::ParseError(format!(
            "drizzle-kit export produced invalid UTF-8 output: {e}"
        ))
    })?;

    if sql.trim().is_empty() {
        return Ok(Schema::new());
    }

    parse_sql_string(&sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_config_error() {
        let result = load_drizzle_schema("/nonexistent/drizzle.config.ts");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not found"));
    }
}
