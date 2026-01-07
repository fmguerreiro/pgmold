# Terraform Provider Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Rust-based Terraform provider for pgmold that enables declarative PostgreSQL schema management and migration file generation.

**Architecture:** Workspace member crate using tf-provider for Terraform plugin protocol. Embeds pgmold as a library for schema parsing, introspection, diffing, and migration generation. Two resources: `pgmold_schema` (declarative) and `pgmold_migration` (file generation).

**Tech Stack:** Rust, tf-provider crate, pgmold library, tokio async runtime, testcontainers for integration tests.

---

## Phase 1: Foundation

### Task 1: Initialize Workspace Member Crate

**Files:**
- Modify: `Cargo.toml` (root) - convert to workspace
- Create: `crates/terraform-provider/Cargo.toml`
- Create: `crates/terraform-provider/src/main.rs`
- Create: `crates/terraform-provider/src/lib.rs`

**Step 1: Convert root to workspace**

Edit `Cargo.toml` to add workspace configuration at the top:

```toml
[workspace]
members = [".", "crates/terraform-provider"]
resolver = "2"
```

**Step 2: Create provider crate directory**

```bash
mkdir -p crates/terraform-provider/src
```

**Step 3: Create provider Cargo.toml**

Create `crates/terraform-provider/Cargo.toml`:

```toml
[package]
name = "terraform-provider-pgmold"
version = "0.1.0"
edition = "2021"
description = "Terraform provider for pgmold PostgreSQL schema management"
license = "MIT"

[[bin]]
name = "terraform-provider-pgmold"
path = "src/main.rs"

[dependencies]
tf-provider = "0.2"
pgmold = { path = "../.." }
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
anyhow = "1"
sha2 = "0.10"
chrono = "0.4"

[dev-dependencies]
testcontainers = "0.23"
testcontainers-modules = { version = "0.11", features = ["postgres"] }
tempfile = "3"
```

**Step 4: Create minimal main.rs**

Create `crates/terraform-provider/src/main.rs`:

```rust
use terraform_provider_pgmold::PgmoldProvider;

#[tokio::main]
async fn main() {
    tf_provider::serve("pgmold", PgmoldProvider::default()).await;
}
```

**Step 5: Create lib.rs with provider stub**

Create `crates/terraform-provider/src/lib.rs`:

```rust
mod provider;

pub use provider::PgmoldProvider;
```

**Step 6: Verify workspace builds**

```bash
cargo build --workspace
```

Expected: Build succeeds (may have warnings about unused code)

**Step 7: Commit**

```bash
git add Cargo.toml crates/
git commit -m "Initialize terraform-provider-pgmold workspace member."
```

---

### Task 2: Implement Provider Configuration

**Files:**
- Create: `crates/terraform-provider/src/provider.rs`
- Modify: `crates/terraform-provider/src/lib.rs`

**Step 1: Write failing test for provider schema**

Create `crates/terraform-provider/src/provider.rs`:

```rust
use std::collections::HashMap;
use std::pin::Pin;
use std::future::Future;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tf_provider::{
    Attribute, AttributeConstraint, AttributeType, Block, Description, Diagnostics,
    Provider, Resource, Schema,
};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProviderConfig {
    pub database_url: Option<String>,
    pub target_schemas: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone)]
pub struct PgmoldProvider {
    pub config: Option<ProviderConfig>,
}

#[async_trait]
impl Provider for PgmoldProvider {
    type Config<'a> = ProviderConfig;
    type MetaState<'a> = ();

    fn schema(&self, _diags: &mut Diagnostics) -> Option<Schema> {
        Some(Schema {
            version: 1,
            block: Block {
                description: Some(Description::plain("pgmold PostgreSQL schema management provider")),
                attributes: vec![
                    Attribute {
                        name: "database_url".to_string(),
                        description: Some(Description::plain("PostgreSQL connection URL")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Optional,
                        sensitive: true,
                        ..Default::default()
                    },
                    Attribute {
                        name: "target_schemas".to_string(),
                        description: Some(Description::plain("PostgreSQL schemas to manage (default: public)")),
                        attr_type: AttributeType::List(Box::new(AttributeType::String)),
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        })
    }

    fn configure<'a>(
        &'a self,
        diags: &'a mut Diagnostics,
        _terraform_version: String,
        config: Self::Config<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<()>> + Send + 'a>> {
        Box::pin(async move {
            // Store config for use by resources
            // Note: In real impl, we'd use interior mutability or Arc<Mutex>
            Some(())
        })
    }

    fn get_resources<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
    ) -> Pin<Box<dyn Future<Output = Option<HashMap<String, Box<dyn Resource>>>> + Send + 'a>> {
        Box::pin(async move {
            let resources: HashMap<String, Box<dyn Resource>> = HashMap::new();
            // Resources will be added in later tasks
            Some(resources)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_schema_has_database_url() {
        let provider = PgmoldProvider::default();
        let mut diags = Diagnostics::default();
        let schema = provider.schema(&mut diags).expect("schema should exist");

        let attr = schema.block.attributes.iter()
            .find(|a| a.name == "database_url")
            .expect("database_url attribute should exist");

        assert!(attr.sensitive, "database_url should be sensitive");
    }

    #[test]
    fn provider_schema_has_target_schemas() {
        let provider = PgmoldProvider::default();
        let mut diags = Diagnostics::default();
        let schema = provider.schema(&mut diags).expect("schema should exist");

        let attr = schema.block.attributes.iter()
            .find(|a| a.name == "target_schemas")
            .expect("target_schemas attribute should exist");

        assert!(matches!(attr.attr_type, AttributeType::List(_)));
    }
}
```

**Step 2: Update lib.rs**

```rust
mod provider;

pub use provider::{PgmoldProvider, ProviderConfig};
```

**Step 3: Run tests**

```bash
cargo test -p terraform-provider-pgmold
```

Expected: Tests pass

**Step 4: Commit**

```bash
git add crates/terraform-provider/
git commit -m "Add provider configuration with database_url and target_schemas."
```

---

## Phase 2: pgmold_schema Resource

### Task 3: Define Schema Resource Types

**Files:**
- Create: `crates/terraform-provider/src/resources/mod.rs`
- Create: `crates/terraform-provider/src/resources/schema.rs`
- Modify: `crates/terraform-provider/src/lib.rs`

**Step 1: Create resources module**

Create `crates/terraform-provider/src/resources/mod.rs`:

```rust
mod schema;

pub use schema::SchemaResource;
```

**Step 2: Define schema resource state types**

Create `crates/terraform-provider/src/resources/schema.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResourceState {
    pub id: String,
    pub schema_file: String,
    pub database_url: Option<String>,
    pub target_schemas: Option<Vec<String>>,
    pub allow_destructive: bool,
    pub zero_downtime: bool,
    // Computed
    pub schema_hash: Option<String>,
    pub applied_at: Option<String>,
    pub migration_count: Option<u32>,
}

impl Default for SchemaResourceState {
    fn default() -> Self {
        Self {
            id: String::new(),
            schema_file: String::new(),
            database_url: None,
            target_schemas: None,
            allow_destructive: false,
            zero_downtime: false,
            schema_hash: None,
            applied_at: None,
            migration_count: None,
        }
    }
}

pub struct SchemaResource;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_state_defaults_allow_destructive_false() {
        let state = SchemaResourceState::default();
        assert!(!state.allow_destructive);
    }

    #[test]
    fn schema_state_defaults_zero_downtime_false() {
        let state = SchemaResourceState::default();
        assert!(!state.zero_downtime);
    }
}
```

**Step 3: Update lib.rs**

```rust
mod provider;
mod resources;

pub use provider::{PgmoldProvider, ProviderConfig};
pub use resources::SchemaResource;
```

**Step 4: Run tests**

```bash
cargo test -p terraform-provider-pgmold
```

Expected: Tests pass

**Step 5: Commit**

```bash
git add crates/terraform-provider/src/
git commit -m "Add SchemaResource state types."
```

---

### Task 4: Implement Schema Resource Schema Method

**Files:**
- Modify: `crates/terraform-provider/src/resources/schema.rs`

**Step 1: Add test for resource schema**

Add to `schema.rs` tests:

```rust
use tf_provider::{Diagnostics, Resource};

#[tokio::test]
async fn schema_resource_has_required_attributes() {
    let resource = SchemaResource;
    let mut diags = Diagnostics::default();
    let schema = resource.schema(&mut diags);

    let required_attrs = ["schema_file"];
    for name in required_attrs {
        assert!(
            schema.block.attributes.iter().any(|a| a.name == name),
            "missing required attribute: {name}"
        );
    }
}

#[tokio::test]
async fn schema_resource_has_optional_attributes() {
    let resource = SchemaResource;
    let mut diags = Diagnostics::default();
    let schema = resource.schema(&mut diags);

    let optional_attrs = ["database_url", "target_schemas", "allow_destructive", "zero_downtime"];
    for name in optional_attrs {
        assert!(
            schema.block.attributes.iter().any(|a| a.name == name),
            "missing optional attribute: {name}"
        );
    }
}
```

**Step 2: Run tests to verify they fail**

```bash
cargo test -p terraform-provider-pgmold schema_resource
```

Expected: FAIL (Resource trait not implemented)

**Step 3: Implement Resource trait schema method**

Add to `schema.rs`:

```rust
use std::pin::Pin;
use std::future::Future;
use async_trait::async_trait;
use tf_provider::{
    Attribute, AttributeConstraint, AttributeType, Block, Description,
    Diagnostics, Resource, Schema,
};

#[async_trait]
impl Resource for SchemaResource {
    type State<'a> = SchemaResourceState;
    type PrivateState<'a> = ();
    type ProviderMetaState<'a> = ();

    fn schema(&self, _diags: &mut Diagnostics) -> Schema {
        Schema {
            version: 1,
            block: Block {
                description: Some(Description::plain("Manages PostgreSQL schema declaratively")),
                attributes: vec![
                    Attribute {
                        name: "id".to_string(),
                        description: Some(Description::plain("Resource identifier")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                    Attribute {
                        name: "schema_file".to_string(),
                        description: Some(Description::plain("Path to SQL schema file")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Required,
                        ..Default::default()
                    },
                    Attribute {
                        name: "database_url".to_string(),
                        description: Some(Description::plain("PostgreSQL connection URL (overrides provider)")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Optional,
                        sensitive: true,
                        ..Default::default()
                    },
                    Attribute {
                        name: "target_schemas".to_string(),
                        description: Some(Description::plain("PostgreSQL schemas to manage")),
                        attr_type: AttributeType::List(Box::new(AttributeType::String)),
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    },
                    Attribute {
                        name: "allow_destructive".to_string(),
                        description: Some(Description::plain("Allow destructive operations (DROP TABLE, etc.)")),
                        attr_type: AttributeType::Bool,
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    },
                    Attribute {
                        name: "zero_downtime".to_string(),
                        description: Some(Description::plain("Use expand/contract pattern for zero-downtime")),
                        attr_type: AttributeType::Bool,
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    },
                    // Computed attributes
                    Attribute {
                        name: "schema_hash".to_string(),
                        description: Some(Description::plain("SHA256 hash of schema file")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                    Attribute {
                        name: "applied_at".to_string(),
                        description: Some(Description::plain("Timestamp of last migration")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                    Attribute {
                        name: "migration_count".to_string(),
                        description: Some(Description::plain("Number of operations applied")),
                        attr_type: AttributeType::Number,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        }
    }

    // Stub implementations - will be filled in next tasks
    fn read<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _state: Self::State<'a>,
        _private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>)>> + Send + 'a>> {
        Box::pin(async move { None })
    }

    fn plan_create<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _proposed_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>)>> + Send + 'a>> {
        Box::pin(async move { None })
    }

    fn plan_update<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _prior_state: Self::State<'a>,
        _proposed_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>, Vec<tf_provider::attribute_path::AttributePath>)>> + Send + 'a>> {
        Box::pin(async move { None })
    }

    fn plan_destroy<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _prior_state: Self::State<'a>,
        _prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<()>> + Send + 'a>> {
        Box::pin(async move { Some(()) })
    }

    fn create<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _planned_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _planned_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>)>> + Send + 'a>> {
        Box::pin(async move { None })
    }

    fn update<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _prior_state: Self::State<'a>,
        _planned_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _planned_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>)>> + Send + 'a>> {
        Box::pin(async move { None })
    }

    fn destroy<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _prior_state: Self::State<'a>,
        _prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<()>> + Send + 'a>> {
        Box::pin(async move { Some(()) })
    }
}
```

**Step 4: Run tests**

```bash
cargo test -p terraform-provider-pgmold schema_resource
```

Expected: Tests pass

**Step 5: Commit**

```bash
git add crates/terraform-provider/src/resources/schema.rs
git commit -m "Implement SchemaResource schema method."
```

---

### Task 5: Implement Schema Hash Computation

**Files:**
- Create: `crates/terraform-provider/src/util.rs`
- Modify: `crates/terraform-provider/src/lib.rs`

**Step 1: Write failing test**

Create `crates/terraform-provider/src/util.rs`:

```rust
use sha2::{Sha256, Digest};
use std::path::Path;

pub fn compute_schema_hash(path: &Path) -> anyhow::Result<String> {
    let content = std::fs::read_to_string(path)?;
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    let result = hasher.finalize();
    Ok(format!("{:x}", result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn compute_hash_returns_sha256() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "CREATE TABLE users (id INT);").unwrap();

        let hash = compute_schema_hash(file.path()).unwrap();

        assert_eq!(hash.len(), 64); // SHA256 hex is 64 chars
    }

    #[test]
    fn compute_hash_same_content_same_hash() {
        let mut file1 = NamedTempFile::new().unwrap();
        let mut file2 = NamedTempFile::new().unwrap();

        writeln!(file1, "CREATE TABLE users (id INT);").unwrap();
        writeln!(file2, "CREATE TABLE users (id INT);").unwrap();

        let hash1 = compute_schema_hash(file1.path()).unwrap();
        let hash2 = compute_schema_hash(file2.path()).unwrap();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn compute_hash_different_content_different_hash() {
        let mut file1 = NamedTempFile::new().unwrap();
        let mut file2 = NamedTempFile::new().unwrap();

        writeln!(file1, "CREATE TABLE users (id INT);").unwrap();
        writeln!(file2, "CREATE TABLE posts (id INT);").unwrap();

        let hash1 = compute_schema_hash(file1.path()).unwrap();
        let hash2 = compute_schema_hash(file2.path()).unwrap();

        assert_ne!(hash1, hash2);
    }
}
```

**Step 2: Update lib.rs**

```rust
mod provider;
mod resources;
mod util;

pub use provider::{PgmoldProvider, ProviderConfig};
pub use resources::SchemaResource;
pub use util::compute_schema_hash;
```

**Step 3: Run tests**

```bash
cargo test -p terraform-provider-pgmold compute_hash
```

Expected: Tests pass

**Step 4: Commit**

```bash
git add crates/terraform-provider/src/
git commit -m "Add schema hash computation utility."
```

---

### Task 6: Implement plan_create for Schema Resource

**Files:**
- Modify: `crates/terraform-provider/src/resources/schema.rs`

**Step 1: Add test for plan_create**

Add to schema.rs tests:

```rust
use tempfile::NamedTempFile;
use std::io::Write;

#[tokio::test]
async fn plan_create_computes_schema_hash() {
    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(schema_file, "CREATE TABLE users (id INT PRIMARY KEY);").unwrap();

    let resource = SchemaResource;
    let mut diags = Diagnostics::default();

    let proposed = SchemaResourceState {
        schema_file: schema_file.path().to_string_lossy().to_string(),
        database_url: Some("postgres://test".to_string()),
        ..Default::default()
    };

    let result = resource.plan_create(&mut diags, proposed.clone(), proposed, ()).await;

    assert!(result.is_some(), "plan_create should return Some");
    let (state, _) = result.unwrap();
    assert!(state.schema_hash.is_some(), "schema_hash should be computed");
    assert_eq!(state.schema_hash.unwrap().len(), 64);
}

#[tokio::test]
async fn plan_create_fails_without_database_url() {
    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(schema_file, "CREATE TABLE users (id INT);").unwrap();

    let resource = SchemaResource;
    let mut diags = Diagnostics::default();

    let proposed = SchemaResourceState {
        schema_file: schema_file.path().to_string_lossy().to_string(),
        database_url: None, // Missing!
        ..Default::default()
    };

    let result = resource.plan_create(&mut diags, proposed.clone(), proposed, ()).await;

    assert!(diags.errors().count() > 0, "should have error for missing database_url");
}
```

**Step 2: Run tests to verify they fail**

```bash
cargo test -p terraform-provider-pgmold plan_create
```

Expected: FAIL

**Step 3: Implement plan_create**

Replace the stub `plan_create` implementation:

```rust
fn plan_create<'a>(
    &'a self,
    diags: &'a mut Diagnostics,
    proposed_state: Self::State<'a>,
    _config_state: Self::State<'a>,
    _provider_meta_state: Self::ProviderMetaState<'a>,
) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>)>> + Send + 'a>> {
    Box::pin(async move {
        // Validate database_url is present
        if proposed_state.database_url.is_none() {
            diags.error_short("database_url is required");
            return None;
        }

        // Validate schema file exists and compute hash
        let schema_path = std::path::Path::new(&proposed_state.schema_file);
        if !schema_path.exists() {
            diags.error_short(format!("schema_file not found: {}", proposed_state.schema_file));
            return None;
        }

        let schema_hash = match crate::util::compute_schema_hash(schema_path) {
            Ok(h) => h,
            Err(e) => {
                diags.error_short(format!("Failed to read schema file: {e}"));
                return None;
            }
        };

        // Generate ID from hash
        let id = format!("pgmold-{}", &schema_hash[..8]);

        let mut state = proposed_state;
        state.id = id;
        state.schema_hash = Some(schema_hash);

        Some((state, ()))
    })
}
```

**Step 4: Run tests**

```bash
cargo test -p terraform-provider-pgmold plan_create
```

Expected: Tests pass

**Step 5: Commit**

```bash
git add crates/terraform-provider/src/resources/schema.rs
git commit -m "Implement plan_create for SchemaResource."
```

---

### Task 7: Implement create (Apply) for Schema Resource

**Files:**
- Modify: `crates/terraform-provider/src/resources/schema.rs`

**Step 1: Add integration test with testcontainers**

Create `crates/terraform-provider/tests/integration.rs`:

```rust
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tempfile::NamedTempFile;
use std::io::Write;

#[tokio::test]
async fn create_applies_schema_to_database() {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(schema_file, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);").unwrap();

    // Test that table doesn't exist before
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&db_url)
        .await
        .unwrap();

    let exists_before: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'users')"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!exists_before.0, "table should not exist before create");

    // Create resource and apply
    use terraform_provider_pgmold::{SchemaResource, resources::schema::SchemaResourceState};
    use tf_provider::{Diagnostics, Resource};

    let resource = SchemaResource;
    let mut diags = Diagnostics::default();

    let state = SchemaResourceState {
        schema_file: schema_file.path().to_string_lossy().to_string(),
        database_url: Some(db_url.clone()),
        ..Default::default()
    };

    let (planned_state, _) = resource.plan_create(&mut diags, state.clone(), state.clone(), ())
        .await
        .expect("plan should succeed");

    let result = resource.create(&mut diags, planned_state, state, (), ())
        .await;

    assert!(result.is_some(), "create should succeed");
    assert!(diags.errors().count() == 0, "should have no errors: {:?}", diags);

    // Verify table exists after
    let exists_after: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'users')"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(exists_after.0, "table should exist after create");
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test -p terraform-provider-pgmold --test integration create_applies
```

Expected: FAIL (create returns None)

**Step 3: Implement create**

Replace stub `create` implementation in `schema.rs`:

```rust
fn create<'a>(
    &'a self,
    diags: &'a mut Diagnostics,
    planned_state: Self::State<'a>,
    _config_state: Self::State<'a>,
    _planned_private_state: Self::PrivateState<'a>,
    _provider_meta_state: Self::ProviderMetaState<'a>,
) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>)>> + Send + 'a>> {
    Box::pin(async move {
        let db_url = planned_state.database_url.as_ref()?;
        let schema_path = std::path::Path::new(&planned_state.schema_file);

        // Parse schema file
        let schema_content = match std::fs::read_to_string(schema_path) {
            Ok(c) => c,
            Err(e) => {
                diags.error_short(format!("Failed to read schema file: {e}"));
                return None;
            }
        };

        let desired_schema = match pgmold::parser::parse(&schema_content) {
            Ok(s) => s,
            Err(e) => {
                diags.error_short(format!("Failed to parse schema: {e}"));
                return None;
            }
        };

        // Connect to database
        let pool = match sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(db_url)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                diags.error_short(format!("Failed to connect to database: {e}"));
                return None;
            }
        };

        // Introspect current schema
        let target_schemas = planned_state.target_schemas.clone()
            .unwrap_or_else(|| vec!["public".to_string()]);

        let current_schema = match pgmold::pg::introspect::introspect(&pool, &target_schemas).await {
            Ok(s) => s,
            Err(e) => {
                diags.error_short(format!("Failed to introspect database: {e}"));
                return None;
            }
        };

        // Compute diff
        let operations = pgmold::diff::compute(&current_schema, &desired_schema);

        // Check for destructive operations
        if !planned_state.allow_destructive {
            let destructive = operations.iter().any(|op| {
                matches!(op,
                    pgmold::diff::MigrationOp::DropTable { .. } |
                    pgmold::diff::MigrationOp::DropColumn { .. } |
                    pgmold::diff::MigrationOp::DropIndex { .. }
                )
            });
            if destructive {
                diags.error_short("Plan includes destructive operations. Set allow_destructive = true to proceed.");
                return None;
            }
        }

        // Apply migrations
        let migration_count = operations.len() as u32;
        if migration_count > 0 {
            if let Err(e) = pgmold::apply::execute(&pool, &operations).await {
                diags.error_short(format!("Migration failed: {e}"));
                return None;
            }
        }

        let mut state = planned_state;
        state.applied_at = Some(chrono::Utc::now().to_rfc3339());
        state.migration_count = Some(migration_count);

        Some((state, ()))
    })
}
```

Add to imports at top of schema.rs:

```rust
use chrono::Utc;
```

**Step 4: Run integration test**

```bash
cargo test -p terraform-provider-pgmold --test integration create_applies
```

Expected: Tests pass

**Step 5: Commit**

```bash
git add crates/terraform-provider/
git commit -m "Implement create for SchemaResource with migration execution."
```

---

### Task 8: Register Schema Resource with Provider

**Files:**
- Modify: `crates/terraform-provider/src/provider.rs`

**Step 1: Add test**

Add to provider.rs tests:

```rust
#[tokio::test]
async fn provider_returns_schema_resource() {
    let provider = PgmoldProvider::default();
    let mut diags = Diagnostics::default();

    let resources = provider.get_resources(&mut diags).await;

    assert!(resources.is_some());
    let resources = resources.unwrap();
    assert!(resources.contains_key("pgmold_schema"), "should have pgmold_schema resource");
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test -p terraform-provider-pgmold provider_returns_schema
```

Expected: FAIL

**Step 3: Implement get_resources**

Update `get_resources` in provider.rs:

```rust
use crate::resources::SchemaResource;

fn get_resources<'a>(
    &'a self,
    _diags: &'a mut Diagnostics,
) -> Pin<Box<dyn Future<Output = Option<HashMap<String, Box<dyn Resource>>>> + Send + 'a>> {
    Box::pin(async move {
        let mut resources: HashMap<String, Box<dyn Resource>> = HashMap::new();
        resources.insert("pgmold_schema".to_string(), Box::new(SchemaResource));
        Some(resources)
    })
}
```

**Step 4: Run test**

```bash
cargo test -p terraform-provider-pgmold provider_returns_schema
```

Expected: Tests pass

**Step 5: Commit**

```bash
git add crates/terraform-provider/src/provider.rs
git commit -m "Register pgmold_schema resource with provider."
```

---

## Phase 3: pgmold_migration Resource

### Task 9: Define Migration Resource Types

**Files:**
- Create: `crates/terraform-provider/src/resources/migration.rs`
- Modify: `crates/terraform-provider/src/resources/mod.rs`

**Step 1: Create migration resource state**

Create `crates/terraform-provider/src/resources/migration.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationResourceState {
    pub id: String,
    pub schema_file: String,
    pub database_url: Option<String>,
    pub output_dir: String,
    pub prefix: Option<String>,
    // Computed
    pub schema_hash: Option<String>,
    pub migration_file: Option<String>,
    pub migration_number: Option<u32>,
    pub operations: Option<Vec<String>>,
}

impl Default for MigrationResourceState {
    fn default() -> Self {
        Self {
            id: String::new(),
            schema_file: String::new(),
            database_url: None,
            output_dir: String::new(),
            prefix: None,
            schema_hash: None,
            migration_file: None,
            migration_number: None,
            operations: None,
        }
    }
}

pub struct MigrationResource;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_state_has_default_empty_prefix() {
        let state = MigrationResourceState::default();
        assert!(state.prefix.is_none());
    }
}
```

**Step 2: Update resources/mod.rs**

```rust
mod schema;
mod migration;

pub use schema::SchemaResource;
pub use migration::MigrationResource;
```

**Step 3: Run tests**

```bash
cargo test -p terraform-provider-pgmold migration_state
```

Expected: Tests pass

**Step 4: Commit**

```bash
git add crates/terraform-provider/src/resources/
git commit -m "Add MigrationResource state types."
```

---

### Task 10: Implement Migration Resource Schema and Methods

**Files:**
- Modify: `crates/terraform-provider/src/resources/migration.rs`

**Step 1: Add Resource trait implementation**

Add to migration.rs:

```rust
use std::pin::Pin;
use std::future::Future;
use async_trait::async_trait;
use tf_provider::{
    Attribute, AttributeConstraint, AttributeType, Block, Description,
    Diagnostics, Resource, Schema,
};

#[async_trait]
impl Resource for MigrationResource {
    type State<'a> = MigrationResourceState;
    type PrivateState<'a> = ();
    type ProviderMetaState<'a> = ();

    fn schema(&self, _diags: &mut Diagnostics) -> Schema {
        Schema {
            version: 1,
            block: Block {
                description: Some(Description::plain("Generates numbered migration files")),
                attributes: vec![
                    Attribute {
                        name: "id".to_string(),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                    Attribute {
                        name: "schema_file".to_string(),
                        description: Some(Description::plain("Path to SQL schema file")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Required,
                        ..Default::default()
                    },
                    Attribute {
                        name: "database_url".to_string(),
                        description: Some(Description::plain("PostgreSQL connection URL for introspection")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Required,
                        sensitive: true,
                        ..Default::default()
                    },
                    Attribute {
                        name: "output_dir".to_string(),
                        description: Some(Description::plain("Directory to write migration files")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Required,
                        ..Default::default()
                    },
                    Attribute {
                        name: "prefix".to_string(),
                        description: Some(Description::plain("Optional prefix for migration files (e.g., 'V' for Flyway)")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Optional,
                        ..Default::default()
                    },
                    // Computed
                    Attribute {
                        name: "schema_hash".to_string(),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                    Attribute {
                        name: "migration_file".to_string(),
                        description: Some(Description::plain("Path to generated migration file")),
                        attr_type: AttributeType::String,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                    Attribute {
                        name: "migration_number".to_string(),
                        attr_type: AttributeType::Number,
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                    Attribute {
                        name: "operations".to_string(),
                        description: Some(Description::plain("List of operations in migration")),
                        attr_type: AttributeType::List(Box::new(AttributeType::String)),
                        constraint: AttributeConstraint::Computed,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        }
    }

    fn read<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        state: Self::State<'a>,
        private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>)>> + Send + 'a>> {
        Box::pin(async move { Some((state, private_state)) })
    }

    fn plan_create<'a>(
        &'a self,
        diags: &'a mut Diagnostics,
        proposed_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>)>> + Send + 'a>> {
        Box::pin(async move {
            let schema_path = std::path::Path::new(&proposed_state.schema_file);
            if !schema_path.exists() {
                diags.error_short(format!("schema_file not found: {}", proposed_state.schema_file));
                return None;
            }

            let schema_hash = match crate::util::compute_schema_hash(schema_path) {
                Ok(h) => h,
                Err(e) => {
                    diags.error_short(format!("Failed to read schema file: {e}"));
                    return None;
                }
            };

            let id = format!("pgmold-migration-{}", &schema_hash[..8]);

            let mut state = proposed_state;
            state.id = id;
            state.schema_hash = Some(schema_hash);

            Some((state, ()))
        })
    }

    fn plan_update<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _prior_state: Self::State<'a>,
        proposed_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>, Vec<tf_provider::attribute_path::AttributePath>)>> + Send + 'a>> {
        Box::pin(async move { Some((proposed_state, (), vec![])) })
    }

    fn plan_destroy<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _prior_state: Self::State<'a>,
        _prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<()>> + Send + 'a>> {
        Box::pin(async move { Some(()) })
    }

    fn create<'a>(
        &'a self,
        diags: &'a mut Diagnostics,
        planned_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _planned_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>)>> + Send + 'a>> {
        Box::pin(async move {
            let db_url = planned_state.database_url.as_ref()?;
            let schema_path = std::path::Path::new(&planned_state.schema_file);
            let output_dir = std::path::Path::new(&planned_state.output_dir);

            // Parse schema
            let schema_content = match std::fs::read_to_string(schema_path) {
                Ok(c) => c,
                Err(e) => {
                    diags.error_short(format!("Failed to read schema: {e}"));
                    return None;
                }
            };

            let desired_schema = match pgmold::parser::parse(&schema_content) {
                Ok(s) => s,
                Err(e) => {
                    diags.error_short(format!("Failed to parse schema: {e}"));
                    return None;
                }
            };

            // Connect and introspect
            let pool = match sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(db_url)
                .await
            {
                Ok(p) => p,
                Err(e) => {
                    diags.error_short(format!("Failed to connect: {e}"));
                    return None;
                }
            };

            let current_schema = match pgmold::pg::introspect::introspect(&pool, &["public".to_string()]).await {
                Ok(s) => s,
                Err(e) => {
                    diags.error_short(format!("Failed to introspect: {e}"));
                    return None;
                }
            };

            // Compute diff
            let operations = pgmold::diff::compute(&current_schema, &desired_schema);

            if operations.is_empty() {
                // No changes - no migration file needed
                let mut state = planned_state;
                state.migration_file = None;
                state.migration_number = None;
                state.operations = Some(vec![]);
                return Some((state, ()));
            }

            // Find next migration number
            let migration_number = find_next_migration_number(output_dir, planned_state.prefix.as_deref());

            // Generate SQL
            let sql = pgmold::pg::sqlgen::generate_sql(&operations);
            let op_summaries: Vec<String> = operations.iter().map(|op| format!("{:?}", op)).collect();

            // Create output directory if needed
            if let Err(e) = std::fs::create_dir_all(output_dir) {
                diags.error_short(format!("Failed to create output directory: {e}"));
                return None;
            }

            // Write migration file
            let prefix = planned_state.prefix.as_deref().unwrap_or("");
            let timestamp = chrono::Utc::now().format("%Y%m%d%H%M%S");
            let filename = format!("{prefix}{migration_number:04}_{timestamp}.sql");
            let filepath = output_dir.join(&filename);

            if let Err(e) = std::fs::write(&filepath, sql) {
                diags.error_short(format!("Failed to write migration file: {e}"));
                return None;
            }

            let mut state = planned_state;
            state.migration_file = Some(filepath.to_string_lossy().to_string());
            state.migration_number = Some(migration_number);
            state.operations = Some(op_summaries);

            Some((state, ()))
        })
    }

    fn update<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _prior_state: Self::State<'a>,
        planned_state: Self::State<'a>,
        _config_state: Self::State<'a>,
        _planned_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<(Self::State<'a>, Self::PrivateState<'a>)>> + Send + 'a>> {
        // Migration resource is effectively immutable after creation
        Box::pin(async move { Some((planned_state, ())) })
    }

    fn destroy<'a>(
        &'a self,
        _diags: &'a mut Diagnostics,
        _prior_state: Self::State<'a>,
        _prior_private_state: Self::PrivateState<'a>,
        _provider_meta_state: Self::ProviderMetaState<'a>,
    ) -> Pin<Box<dyn Future<Output = Option<()>> + Send + 'a>> {
        // Don't delete migration files on destroy
        Box::pin(async move { Some(()) })
    }
}

fn find_next_migration_number(output_dir: &std::path::Path, prefix: Option<&str>) -> u32 {
    let prefix = prefix.unwrap_or("");
    let pattern = format!("{prefix}(\\d{{4}})_.*\\.sql$");
    let re = regex::Regex::new(&pattern).unwrap();

    let max_number = std::fs::read_dir(output_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let filename = entry.file_name().to_string_lossy().to_string();
            re.captures(&filename)
                .and_then(|caps| caps.get(1))
                .and_then(|m| m.as_str().parse::<u32>().ok())
        })
        .max()
        .unwrap_or(0);

    max_number + 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::io::Write;

    #[test]
    fn find_next_migration_number_empty_dir() {
        let dir = TempDir::new().unwrap();
        assert_eq!(find_next_migration_number(dir.path(), None), 1);
    }

    #[test]
    fn find_next_migration_number_with_existing() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("0001_20240101.sql"), "").unwrap();
        std::fs::write(dir.path().join("0002_20240102.sql"), "").unwrap();
        assert_eq!(find_next_migration_number(dir.path(), None), 3);
    }

    #[test]
    fn find_next_migration_number_with_prefix() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("V0001_20240101.sql"), "").unwrap();
        std::fs::write(dir.path().join("V0005_20240102.sql"), "").unwrap();
        assert_eq!(find_next_migration_number(dir.path(), Some("V")), 6);
    }
}
```

Add `regex` to dependencies in `Cargo.toml`:

```toml
regex = "1"
```

**Step 2: Run tests**

```bash
cargo test -p terraform-provider-pgmold migration
```

Expected: Tests pass

**Step 3: Commit**

```bash
git add crates/terraform-provider/
git commit -m "Implement MigrationResource with auto-increment numbering."
```

---

### Task 11: Register Migration Resource

**Files:**
- Modify: `crates/terraform-provider/src/provider.rs`

**Step 1: Update get_resources**

```rust
use crate::resources::{SchemaResource, MigrationResource};

fn get_resources<'a>(
    &'a self,
    _diags: &'a mut Diagnostics,
) -> Pin<Box<dyn Future<Output = Option<HashMap<String, Box<dyn Resource>>>> + Send + 'a>> {
    Box::pin(async move {
        let mut resources: HashMap<String, Box<dyn Resource>> = HashMap::new();
        resources.insert("pgmold_schema".to_string(), Box::new(SchemaResource));
        resources.insert("pgmold_migration".to_string(), Box::new(MigrationResource));
        Some(resources)
    })
}
```

**Step 2: Run all tests**

```bash
cargo test -p terraform-provider-pgmold
```

Expected: All tests pass

**Step 3: Commit**

```bash
git add crates/terraform-provider/src/provider.rs
git commit -m "Register pgmold_migration resource with provider."
```

---

## Phase 4: Polish

### Task 12: Add Example Terraform Configuration

**Files:**
- Create: `crates/terraform-provider/examples/basic/main.tf`
- Create: `crates/terraform-provider/examples/basic/schema.sql`

**Step 1: Create example files**

Create `crates/terraform-provider/examples/basic/main.tf`:

```hcl
terraform {
  required_providers {
    pgmold = {
      source = "fmguerreiro/pgmold"
    }
  }
}

provider "pgmold" {
  database_url = "postgres://postgres:postgres@localhost:5432/mydb"
}

resource "pgmold_schema" "main" {
  schema_file = "${path.module}/schema.sql"
}
```

Create `crates/terraform-provider/examples/basic/schema.sql`:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    title TEXT NOT NULL,
    body TEXT,
    published_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX posts_user_id_idx ON posts(user_id);
```

**Step 2: Commit**

```bash
git add crates/terraform-provider/examples/
git commit -m "Add basic Terraform configuration example."
```

---

### Task 13: Add README

**Files:**
- Create: `crates/terraform-provider/README.md`

**Step 1: Create README**

Create `crates/terraform-provider/README.md`:

```markdown
# terraform-provider-pgmold

Terraform provider for [pgmold](https://github.com/fmguerreiro/pgmold) PostgreSQL schema management.

## Installation

Build from source:

```bash
cargo build --release -p terraform-provider-pgmold
```

Copy binary to Terraform plugins directory:

```bash
mkdir -p ~/.terraform.d/plugins/fmguerreiro/pgmold/0.1.0/darwin_arm64/
cp target/release/terraform-provider-pgmold ~/.terraform.d/plugins/fmguerreiro/pgmold/0.1.0/darwin_arm64/
```

## Usage

### Provider Configuration

```hcl
provider "pgmold" {
  database_url   = "postgres://user:pass@localhost:5432/mydb"
  target_schemas = ["public"]  # Optional
}
```

### pgmold_schema Resource

Manages PostgreSQL schema declaratively:

```hcl
resource "pgmold_schema" "main" {
  schema_file       = "./schema.sql"
  allow_destructive = false  # Set true to allow DROP operations
  zero_downtime     = false  # Set true for expand/contract pattern
}
```

### pgmold_migration Resource

Generates numbered migration files:

```hcl
resource "pgmold_migration" "current" {
  schema_file  = "./schema.sql"
  database_url = "postgres://..."
  output_dir   = "./migrations/"
  prefix       = "V"  # Optional, for Flyway-style naming
}
```

## Development

Run tests:

```bash
cargo test -p terraform-provider-pgmold
```

Integration tests require Docker:

```bash
cargo test -p terraform-provider-pgmold --test integration
```
```

**Step 2: Commit**

```bash
git add crates/terraform-provider/README.md
git commit -m "Add terraform-provider-pgmold README."
```

---

### Task 14: Final Integration Test

**Files:**
- Modify: `crates/terraform-provider/tests/integration.rs`

**Step 1: Add full workflow test**

Add to integration.rs:

```rust
#[tokio::test]
async fn migration_resource_generates_file() {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let schema_file = NamedTempFile::new().unwrap();
    writeln!(&schema_file, "CREATE TABLE products (id SERIAL PRIMARY KEY);").unwrap();

    let output_dir = tempfile::tempdir().unwrap();

    use terraform_provider_pgmold::resources::migration::{MigrationResource, MigrationResourceState};
    use tf_provider::{Diagnostics, Resource};

    let resource = MigrationResource;
    let mut diags = Diagnostics::default();

    let state = MigrationResourceState {
        schema_file: schema_file.path().to_string_lossy().to_string(),
        database_url: Some(db_url),
        output_dir: output_dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };

    let (planned, _) = resource.plan_create(&mut diags, state.clone(), state.clone(), ())
        .await
        .expect("plan should succeed");

    let (final_state, _) = resource.create(&mut diags, planned, state, (), ())
        .await
        .expect("create should succeed");

    assert!(final_state.migration_file.is_some());
    assert!(final_state.migration_number == Some(1));

    let migration_path = std::path::Path::new(final_state.migration_file.as_ref().unwrap());
    assert!(migration_path.exists(), "migration file should exist");

    let content = std::fs::read_to_string(migration_path).unwrap();
    assert!(content.contains("CREATE TABLE"), "should contain CREATE TABLE");
}
```

**Step 2: Run integration tests**

```bash
cargo test -p terraform-provider-pgmold --test integration
```

Expected: All tests pass

**Step 3: Commit**

```bash
git add crates/terraform-provider/tests/
git commit -m "Add migration resource integration test."
```

---

### Task 15: Build and Verify

**Step 1: Build release binary**

```bash
cargo build --release -p terraform-provider-pgmold
```

**Step 2: Verify binary exists**

```bash
ls -la target/release/terraform-provider-pgmold
```

**Step 3: Run all tests one final time**

```bash
cargo test --workspace
```

Expected: All tests pass

**Step 4: Final commit**

```bash
git add -A
git commit -m "Complete terraform-provider-pgmold v0.1.0."
```

---

## Summary

After completing all tasks, you will have:

1. A workspace member crate `terraform-provider-pgmold`
2. Provider with `database_url` and `target_schemas` configuration
3. `pgmold_schema` resource for declarative schema management
4. `pgmold_migration` resource for migration file generation
5. Unit tests, integration tests with testcontainers
6. Example Terraform configuration
7. README documentation

Total: 15 tasks, ~45-60 commits following TDD pattern.
