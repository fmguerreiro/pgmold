# Function Configuration Parameters and Ownership Support

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Full round-trip support for PostgreSQL function `SET configuration_parameter` clauses and `OWNER TO` statements.

**Architecture:** Extend `model::Function` with `config_params` and `owner` fields, update introspection to query `proconfig` and `proowner` from pg_proc, modify SQL generation to emit SET clauses and ALTER FUNCTION OWNER TO statements, and update diff to detect changes.

**Tech Stack:** Rust, sqlparser-rs (forked), pgmold, PostgreSQL pg_catalog

---

## Prerequisites

- sqlparser-rs fork: `https://github.com/fmguerreiro/datafusion-sqlparser-rs` branch `partition-of-support`
- Docker running (for integration tests)

---

## Phase 1: SET Configuration Parameters

### Task 1: Extend sqlparser Fork to Parse SET Clauses

**Files:**
- Modify: `~/projects/sqlparser-fork/src/parser/mod.rs`
- Modify: `~/projects/sqlparser-fork/tests/sqlparser_postgres.rs`

**Step 1: Clone/checkout the fork**

```bash
cd ~/projects
git clone git@github.com:fmguerreiro/datafusion-sqlparser-rs.git sqlparser-fork 2>/dev/null || true
cd sqlparser-fork
git checkout partition-of-support
git pull origin partition-of-support
```

**Step 2: Write failing test for SET in CREATE FUNCTION**

Add to `tests/sqlparser_postgres.rs`:

```rust
#[test]
fn parse_create_function_with_set_config() {
    let sql = r#"CREATE FUNCTION auth.hook(event jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = auth, pg_temp, public
AS $$ BEGIN RETURN event; END; $$"#;

    let stmt = pg().verified_stmt(sql);
    match stmt {
        Statement::CreateFunction(CreateFunction { name, options, .. }) => {
            assert_eq!(name.to_string(), "auth.hook");
            let opts = options.expect("should have options");
            assert!(!opts.is_empty(), "SET should be parsed as option");
        }
        _ => panic!("Expected CreateFunction"),
    }
}
```

**Step 3: Run test to verify it fails**

```bash
cargo test parse_create_function_with_set_config --test sqlparser_postgres -- --nocapture
```

Expected: FAIL with parse error on SET

**Step 4: Add options field to Body struct**

In `src/parser/mod.rs`, find `parse_postgres_create_function` and add to the Body struct:

```rust
#[derive(Default)]
struct Body {
    language: Option<Ident>,
    behavior: Option<FunctionBehavior>,
    function_body: Option<CreateFunctionBody>,
    called_on_null: Option<FunctionCalledOnNull>,
    parallel: Option<FunctionParallel>,
    options: Vec<SqlOption>,  // ADD THIS LINE
}
```

**Step 5: Add SET clause parsing in the loop**

After the `PARALLEL` handling and before `RETURN`, add:

```rust
} else if self.parse_keyword(Keyword::SET) {
    let name = self.parse_identifier()?;
    self.expect_token(&Token::Eq)?;
    let value = self.parse_comma_separated(|p| {
        p.parse_identifier().map(|id| id.to_string())
    })?.join(", ");
    body.options.push(SqlOption::KeyValue {
        key: name,
        value: Expr::Value(Value::SingleQuotedString(value).with_empty_span()),
    });
```

**Step 6: Update return statement to include options**

Change `options: None,` to:

```rust
options: if body.options.is_empty() { None } else { Some(body.options) },
```

**Step 7: Run test to verify it passes**

```bash
cargo test parse_create_function_with_set_config --test sqlparser_postgres -- --nocapture
```

Expected: PASS

**Step 8: Run full test suite**

```bash
cargo test
```

**Step 9: Commit and push**

```bash
git add src/parser/mod.rs tests/sqlparser_postgres.rs
git commit --message "Add SET configuration_parameter parsing for PostgreSQL functions."
git push origin partition-of-support
```

---

### Task 2: Add config_params Field to Function Model

**Files:**
- Modify: `src/model/mod.rs`

**Step 1: Write failing test for config_params field**

Add to the tests module in `src/model/mod.rs`:

```rust
#[test]
fn function_with_config_params() {
    let func = Function {
        name: "test".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "void".to_string(),
        language: "sql".to_string(),
        body: "SELECT 1".to_string(),
        volatility: Volatility::Volatile,
        security: SecurityType::Invoker,
        config_params: vec![("search_path".to_string(), "public".to_string())],
        owner: None,
    };
    assert_eq!(func.config_params.len(), 1);
    assert_eq!(func.config_params[0].0, "search_path");
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test function_with_config_params -- --nocapture
```

Expected: FAIL - `config_params` field doesn't exist

**Step 3: Add config_params and owner fields to Function struct**

In `src/model/mod.rs`, update the Function struct (lines 204-214):

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Function {
    pub name: String,
    pub schema: String,
    pub arguments: Vec<FunctionArg>,
    pub return_type: String,
    pub language: String,
    pub body: String,
    pub volatility: Volatility,
    pub security: SecurityType,
    pub config_params: Vec<(String, String)>,
    pub owner: Option<String>,
}
```

**Step 4: Update semantically_equals to include config_params**

Update the `semantically_equals` method (around line 219):

```rust
pub fn semantically_equals(&self, other: &Function) -> bool {
    self.name == other.name
        && self.schema == other.schema
        && self.arguments == other.arguments
        && self.return_type == other.return_type
        && self.language == other.language
        && self.volatility == other.volatility
        && self.security == other.security
        && self.config_params == other.config_params
        && normalize_sql_body(&self.body) == normalize_sql_body(&other.body)
}
```

**Step 5: Run test to verify it passes**

```bash
cargo test function_with_config_params -- --nocapture
```

Expected: PASS

**Step 6: Fix compilation errors in other files**

The compiler will complain about missing fields. Update all Function instantiations with:

```rust
config_params: vec![],
owner: None,
```

Files to update:
- `src/parser/mod.rs` (parse_create_function)
- `src/pg/introspect.rs` (introspect_functions)
- `src/model/mod.rs` (test functions)
- `src/pg/sqlgen.rs` (test functions)

**Step 7: Run full test suite**

```bash
cargo test
```

**Step 8: Commit**

```bash
git add src/model/mod.rs src/parser/mod.rs src/pg/introspect.rs src/pg/sqlgen.rs
git commit --message "Add config_params and owner fields to Function model."
```

---

### Task 3: Update pgmold to Use New sqlparser Version

**Files:**
- Modify: `Cargo.toml`

**Step 1: Update sqlparser dependency**

After pushing the fork, update Cargo.toml:

```toml
sqlparser = { git = "https://github.com/fmguerreiro/datafusion-sqlparser-rs", branch = "partition-of-support", features = ["visitor"] }
```

**Step 2: Run cargo update**

```bash
cargo update -p sqlparser
```

**Step 3: Run tests to verify**

```bash
cargo test
```

**Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit --message "Update sqlparser fork with SET clause parsing."
```

---

### Task 4: Extract config_params from Parsed AST

**Files:**
- Modify: `src/parser/mod.rs`

**Step 1: Write failing test for config_params extraction**

Add to parser tests:

```rust
#[test]
fn parses_function_with_set_search_path() {
    let sql = r#"
        CREATE FUNCTION auth.hook(event jsonb) RETURNS jsonb
        LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = auth, pg_temp, public
        AS $$ BEGIN RETURN event; END; $$;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema.functions.get("auth.hook(jsonb)").unwrap();
    assert_eq!(func.config_params.len(), 1);
    assert_eq!(func.config_params[0].0, "search_path");
    assert_eq!(func.config_params[0].1, "auth, pg_temp, public");
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test parses_function_with_set_search_path -- --nocapture
```

Expected: FAIL - config_params is empty

**Step 3: Remove the ALTER FUNCTION regex stripping (partially)**

In `src/parser/mod.rs` around line 89, we need to be more selective. Keep the regex but don't strip `SET` inside CREATE FUNCTION.

Actually, the issue is that `SET search_path` inside CREATE FUNCTION is handled by sqlparser. The regex strips standalone `ALTER FUNCTION` statements. We need to extract options from the parsed CreateFunction AST.

**Step 4: Update parse_create_function to accept and extract options**

First, update the function signature to accept options. In `src/parser/mod.rs`, find the `parse_create_function` function and add an `options` parameter:

```rust
fn parse_create_function(
    schema: &str,
    name: &str,
    args: Option<&[OperateFunctionArg]>,
    return_type: Option<&DataType>,
    body: Option<&CreateFunctionBody>,
    language: Option<&Ident>,
    behavior: Option<&FunctionBehavior>,
    security: Option<&FunctionDefinitionSecurity>,
    options: Option<&[SqlOption]>,  // ADD THIS PARAMETER
) -> Result<Function, SchemaError> {
```

**Step 5: Extract config_params from options**

Inside `parse_create_function`, add before the return:

```rust
let config_params = options
    .map(|opts| {
        opts.iter()
            .filter_map(|opt| match opt {
                SqlOption::KeyValue { key, value } => {
                    let val = match value {
                        Expr::Value(v) => v.to_string().trim_matches('\'').to_string(),
                        _ => value.to_string(),
                    };
                    Some((key.to_string().to_lowercase(), val))
                }
                _ => None,
            })
            .collect()
    })
    .unwrap_or_default();
```

**Step 6: Update the return statement**

```rust
Ok(Function {
    name: name.to_string(),
    schema: schema.to_string(),
    arguments,
    return_type: ret_type,
    language: lang,
    body: func_body,
    volatility: vol,
    security: sec,
    config_params,
    owner: None,
})
```

**Step 7: Update the call site to pass options**

In the `Statement::CreateFunction` match arm (around line 314), update to pass options:

```rust
Statement::CreateFunction(sqlparser::ast::CreateFunction {
    name,
    args,
    return_type,
    function_body,
    language,
    behavior,
    security,
    options,  // ADD THIS
    ..
}) => {
    let (func_schema, func_name) = extract_qualified_name(&name);
    let func = parse_create_function(
        &func_schema,
        &func_name,
        args.as_deref(),
        return_type.as_ref(),
        function_body.as_ref(),
        language.as_ref(),
        behavior.as_ref(),
        security.as_ref(),
        options.as_deref(),  // ADD THIS
    )?;
```

**Step 8: Run test to verify it passes**

```bash
cargo test parses_function_with_set_search_path -- --nocapture
```

Expected: PASS

**Step 9: Commit**

```bash
git add src/parser/mod.rs
git commit --message "Extract config_params from parsed function AST."
```

---

### Task 5: Introspect config_params from Database

**Files:**
- Modify: `src/pg/introspect.rs`

**Step 1: Write failing integration test**

Add to `tests/integration.rs`:

```rust
#[tokio::test]
async fn introspects_function_config_params() {
    let container = start_postgres_container().await;
    let connection_string = container.connection_string();

    let setup_sql = r#"
        CREATE FUNCTION test_func() RETURNS void
        LANGUAGE sql SECURITY DEFINER
        SET search_path = public
        AS $$ SELECT 1 $$;
    "#;

    let conn = PgConnection::connect(&connection_string).await.unwrap();
    conn.execute(setup_sql).await.unwrap();

    let schema = introspect_schema(&conn, &["public".to_string()], false).await.unwrap();
    let func = schema.functions.get("public.test_func()").unwrap();

    assert_eq!(func.config_params.len(), 1);
    assert_eq!(func.config_params[0].0, "search_path");
    assert_eq!(func.config_params[0].1, "public");
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test introspects_function_config_params --test integration -- --nocapture
```

Expected: FAIL - config_params is empty

**Step 3: Update introspect_functions query to include proconfig**

In `src/pg/introspect.rs`, update the SQL query (around line 927):

```sql
SELECT
    p.proname as name,
    n.nspname as schema,
    pg_get_function_arguments(p.oid) as arguments,
    pg_get_function_result(p.oid) as return_type,
    l.lanname as language,
    p.prosrc as body,
    p.provolatile as volatility,
    p.prosecdef as security_definer,
    p.proconfig as config_params
FROM pg_proc p
```

**Step 4: Parse proconfig array**

In the row processing loop, add:

```rust
let config_params_raw: Option<Vec<String>> = row.get("config_params");
let config_params = config_params_raw
    .unwrap_or_default()
    .into_iter()
    .filter_map(|param| {
        let parts: Vec<&str> = param.splitn(2, '=').collect();
        if parts.len() == 2 {
            Some((parts[0].to_string(), parts[1].to_string()))
        } else {
            None
        }
    })
    .collect();
```

**Step 5: Update Function construction**

```rust
let func = Function {
    name: name.clone(),
    schema: schema.clone(),
    arguments,
    return_type,
    language,
    body,
    volatility,
    security,
    config_params,
    owner: None,
};
```

**Step 6: Run test to verify it passes**

```bash
cargo test introspects_function_config_params --test integration -- --nocapture
```

Expected: PASS

**Step 7: Commit**

```bash
git add src/pg/introspect.rs tests/integration.rs
git commit --message "Introspect function config_params from proconfig."
```

---

### Task 6: Generate SET Clauses in SQL Output

**Files:**
- Modify: `src/pg/sqlgen.rs`

**Step 1: Write failing test for SET clause generation**

Add to sqlgen tests:

```rust
#[test]
fn generate_function_ddl_with_config_params() {
    use crate::model::{Function, FunctionArg, SecurityType, Volatility};

    let func = Function {
        name: "test_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "void".to_string(),
        language: "sql".to_string(),
        body: "SELECT 1".to_string(),
        volatility: Volatility::Volatile,
        security: SecurityType::Definer,
        config_params: vec![("search_path".to_string(), "public".to_string())],
        owner: None,
    };

    let ddl = generate_create_function(&func);

    assert!(
        ddl.contains("SET search_path = public"),
        "Expected SET clause in: {ddl}"
    );
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test generate_function_ddl_with_config_params -- --nocapture
```

Expected: FAIL - no SET clause in output

**Step 3: Update generate_function_ddl to emit SET clauses**

In `src/pg/sqlgen.rs`, update `generate_function_ddl` (around line 727):

```rust
let config = func
    .config_params
    .iter()
    .map(|(k, v)| format!("SET {} = {}", k, v))
    .collect::<Vec<_>>()
    .join(" ");

format!(
    "{} {}({}) RETURNS {} LANGUAGE {} {} {} {} AS $${}$$;",
    create_stmt,
    quote_qualified(&func.schema, &func.name),
    args,
    func.return_type,
    func.language,
    volatility,
    security,
    config,
    func.body
)
```

**Step 4: Run test to verify it passes**

```bash
cargo test generate_function_ddl_with_config_params -- --nocapture
```

Expected: PASS

**Step 5: Run full test suite**

```bash
cargo test
```

**Step 6: Commit**

```bash
git add src/pg/sqlgen.rs
git commit --message "Generate SET clauses in function DDL output."
```

---

### Task 7: Add Integration Test for config_params Round-Trip

**Files:**
- Modify: `tests/integration.rs`

**Step 1: Write round-trip integration test**

```rust
#[tokio::test]
async fn function_config_params_round_trip() {
    let container = start_postgres_container().await;
    let connection_string = container.connection_string();

    let schema_sql = r#"
        CREATE FUNCTION auth.hook(event jsonb) RETURNS jsonb
        LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = auth, pg_temp, public
        AS $$ BEGIN RETURN event; END; $$;
    "#;

    // Parse schema file
    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let parsed_func = parsed_schema.functions.get("auth.hook(jsonb)").unwrap();
    assert!(!parsed_func.config_params.is_empty(), "Parsed function should have config_params");

    // Apply to database
    let conn = PgConnection::connect(&connection_string).await.unwrap();
    conn.execute("CREATE SCHEMA auth").await.unwrap();

    let ops = diff::compute(&Schema::default(), &parsed_schema);
    let sql = sqlgen::generate_sql(&ops);
    for stmt in &sql {
        conn.execute(stmt).await.unwrap();
    }

    // Introspect back
    let introspected = introspect_schema(&conn, &["auth".to_string()], false).await.unwrap();
    let introspected_func = introspected.functions.get("auth.hook(jsonb)").unwrap();

    // Verify config_params match
    assert_eq!(
        parsed_func.config_params, introspected_func.config_params,
        "config_params should round-trip"
    );

    // Verify no diff
    let diff_ops = diff::compute(&introspected, &parsed_schema);
    let func_ops: Vec<_> = diff_ops.iter().filter(|op| {
        matches!(op, MigrationOp::CreateFunction(_) | MigrationOp::AlterFunction { .. })
    }).collect();
    assert!(func_ops.is_empty(), "Should have no function diff after round-trip");
}
```

**Step 2: Run integration test**

```bash
cargo test function_config_params_round_trip --test integration -- --nocapture
```

Expected: PASS

**Step 3: Commit**

```bash
git add tests/integration.rs
git commit --message "Add integration test for function config_params round-trip."
```

---

## Phase 2: OWNER TO Support

### Task 8: Introspect Function Owner from Database

**Files:**
- Modify: `src/pg/introspect.rs`

**Step 1: Write failing integration test**

Add to `tests/integration.rs`:

```rust
#[tokio::test]
async fn introspects_function_owner() {
    let container = start_postgres_container().await;
    let connection_string = container.connection_string();

    let conn = PgConnection::connect(&connection_string).await.unwrap();
    conn.execute("CREATE ROLE test_owner").await.unwrap();
    conn.execute(r#"
        CREATE FUNCTION test_func() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$;
        ALTER FUNCTION test_func() OWNER TO test_owner;
    "#).await.unwrap();

    let schema = introspect_schema(&conn, &["public".to_string()], false).await.unwrap();
    let func = schema.functions.get("public.test_func()").unwrap();

    assert_eq!(func.owner, Some("test_owner".to_string()));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test introspects_function_owner --test integration -- --nocapture
```

Expected: FAIL - owner is None

**Step 3: Update introspect_functions query to include owner**

In `src/pg/introspect.rs`, update the SQL query:

```sql
SELECT
    p.proname as name,
    n.nspname as schema,
    pg_get_function_arguments(p.oid) as arguments,
    pg_get_function_result(p.oid) as return_type,
    l.lanname as language,
    p.prosrc as body,
    p.provolatile as volatility,
    p.prosecdef as security_definer,
    p.proconfig as config_params,
    r.rolname as owner
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
JOIN pg_language l ON p.prolang = l.oid
JOIN pg_roles r ON p.proowner = r.oid
```

**Step 4: Extract owner from row**

```rust
let owner: String = row.get("owner");
```

**Step 5: Update Function construction**

```rust
let func = Function {
    // ... existing fields ...
    owner: Some(owner),
};
```

**Step 6: Run test to verify it passes**

```bash
cargo test introspects_function_owner --test integration -- --nocapture
```

Expected: PASS

**Step 7: Commit**

```bash
git add src/pg/introspect.rs tests/integration.rs
git commit --message "Introspect function owner from proowner."
```

---

### Task 9: Parse ALTER FUNCTION OWNER TO Statements

**Files:**
- Modify: `src/parser/mod.rs`

**Step 1: Write failing test**

```rust
#[test]
fn parses_alter_function_owner_to() {
    let sql = r#"
        CREATE FUNCTION auth.hook() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$;
        ALTER FUNCTION auth.hook() OWNER TO supabase_auth_admin;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema.functions.get("auth.hook()").unwrap();
    assert_eq!(func.owner, Some("supabase_auth_admin".to_string()));
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test parses_alter_function_owner_to -- --nocapture
```

Expected: FAIL - owner is None (ALTER FUNCTION is stripped)

**Step 3: Remove the overly broad ALTER FUNCTION stripping**

In `src/parser/mod.rs`, remove or modify the regex at line 89. Instead of stripping all ALTER FUNCTION, we need to parse them.

Replace:

```rust
let alter_function_re = Regex::new(r"(?i)ALTER\s+FUNCTION\s+[^;]+;").unwrap();
let processed = alter_function_re.replace_all(&processed, "");
```

With: Nothing (remove these lines). We'll handle ALTER FUNCTION in the parser.

**Step 4: Add ALTER FUNCTION handling in the statement loop**

sqlparser may not parse ALTER FUNCTION OWNER TO. Check if it does. If not, we need to handle it manually with regex after parsing.

Add a post-processing step:

```rust
// After the main parsing loop, extract ALTER FUNCTION OWNER TO statements
let alter_owner_re = Regex::new(
    r#"(?i)ALTER\s+FUNCTION\s+([^\s(]+)\s*\([^)]*\)\s+OWNER\s+TO\s+([^\s;]+)"#
).unwrap();

for cap in alter_owner_re.captures_iter(sql) {
    let func_name = cap.get(1).unwrap().as_str();
    let owner = cap.get(2).unwrap().as_str().trim_matches('"');

    // Find matching function and set owner
    for (_, func) in schema.functions.iter_mut() {
        let qualified = format!("{}.{}", func.schema, func.name);
        if qualified == func_name || func.name == func_name {
            func.owner = Some(owner.to_string());
            break;
        }
    }
}
```

**Step 5: Run test to verify it passes**

```bash
cargo test parses_alter_function_owner_to -- --nocapture
```

Expected: PASS

**Step 6: Commit**

```bash
git add src/parser/mod.rs
git commit --message "Parse ALTER FUNCTION OWNER TO statements."
```

---

### Task 10: Generate ALTER FUNCTION OWNER TO in SQL Output

**Files:**
- Modify: `src/pg/sqlgen.rs`
- Modify: `src/diff/mod.rs`

**Step 1: Write failing test**

```rust
#[test]
fn generate_function_owner_change() {
    use crate::model::{Function, SecurityType, Volatility};

    let func = Function {
        name: "test_func".to_string(),
        schema: "public".to_string(),
        arguments: vec![],
        return_type: "void".to_string(),
        language: "sql".to_string(),
        body: "SELECT 1".to_string(),
        volatility: Volatility::Volatile,
        security: SecurityType::Invoker,
        config_params: vec![],
        owner: Some("custom_role".to_string()),
    };

    let ddl = generate_create_function(&func);
    let owner_stmt = generate_function_owner(&func);

    assert!(
        !ddl.contains("OWNER"),
        "CREATE FUNCTION should not include OWNER"
    );
    assert_eq!(
        owner_stmt,
        Some(r#"ALTER FUNCTION "public"."test_func"() OWNER TO "custom_role";"#.to_string())
    );
}
```

**Step 2: Run test to verify it fails**

```bash
cargo test generate_function_owner_change -- --nocapture
```

Expected: FAIL - generate_function_owner doesn't exist

**Step 3: Add generate_function_owner function**

```rust
pub fn generate_function_owner(func: &Function) -> Option<String> {
    func.owner.as_ref().map(|owner| {
        let args = func
            .arguments
            .iter()
            .map(|a| a.data_type.clone())
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "ALTER FUNCTION {}({}) OWNER TO {};",
            quote_qualified(&func.schema, &func.name),
            args,
            quote_ident(owner)
        )
    })
}
```

**Step 4: Add MigrationOp::SetFunctionOwner**

In `src/diff/mod.rs`, add to the MigrationOp enum:

```rust
SetFunctionOwner {
    name: String,
    args: String,
    owner: String,
},
```

**Step 5: Update diff to detect owner changes**

In the function diff logic, add after checking semantically_equals:

```rust
// Check for owner changes
if from_func.owner != func.owner {
    if let Some(ref owner) = func.owner {
        ops.push(MigrationOp::SetFunctionOwner {
            name: qualified_name(&func.schema, &func.name),
            args: func.arguments.iter().map(|a| a.data_type.clone()).collect::<Vec<_>>().join(", "),
            owner: owner.clone(),
        });
    }
}
```

**Step 6: Add SQL generation for SetFunctionOwner**

In `src/pg/sqlgen.rs`, add to the match:

```rust
MigrationOp::SetFunctionOwner { name, args, owner } => {
    format!("ALTER FUNCTION {}({}) OWNER TO {};", name, args, quote_ident(owner))
}
```

**Step 7: Run test to verify it passes**

```bash
cargo test generate_function_owner_change -- --nocapture
```

Expected: PASS

**Step 8: Commit**

```bash
git add src/pg/sqlgen.rs src/diff/mod.rs
git commit --message "Generate ALTER FUNCTION OWNER TO statements."
```

---

### Task 11: Add Integration Test for Owner Round-Trip

**Files:**
- Modify: `tests/integration.rs`

**Step 1: Write round-trip integration test**

```rust
#[tokio::test]
async fn function_owner_round_trip() {
    let container = start_postgres_container().await;
    let connection_string = container.connection_string();

    let conn = PgConnection::connect(&connection_string).await.unwrap();
    conn.execute("CREATE ROLE custom_owner").await.unwrap();

    let schema_sql = r#"
        CREATE FUNCTION test_func() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$;
        ALTER FUNCTION test_func() OWNER TO custom_owner;
    "#;

    // Parse schema file
    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let parsed_func = parsed_schema.functions.get("public.test_func()").unwrap();
    assert_eq!(parsed_func.owner, Some("custom_owner".to_string()));

    // Apply to database
    let ops = diff::compute(&Schema::default(), &parsed_schema);
    let sql = sqlgen::generate_sql(&ops);
    for stmt in &sql {
        conn.execute(stmt).await.unwrap();
    }

    // Introspect back
    let introspected = introspect_schema(&conn, &["public".to_string()], false).await.unwrap();
    let introspected_func = introspected.functions.get("public.test_func()").unwrap();

    // Verify owner matches
    assert_eq!(parsed_func.owner, introspected_func.owner);

    // Verify no diff
    let diff_ops = diff::compute(&introspected, &parsed_schema);
    let owner_ops: Vec<_> = diff_ops.iter().filter(|op| {
        matches!(op, MigrationOp::SetFunctionOwner { .. })
    }).collect();
    assert!(owner_ops.is_empty(), "Should have no owner diff after round-trip");
}
```

**Step 2: Run integration test**

```bash
cargo test function_owner_round_trip --test integration -- --nocapture
```

Expected: PASS

**Step 3: Commit**

```bash
git add tests/integration.rs
git commit --message "Add integration test for function owner round-trip."
```

---

## Phase 3: Final Verification

### Task 12: Run Full Test Suite and Verify

**Step 1: Run all tests**

```bash
cargo test
```

Expected: All tests pass

**Step 2: Run integration tests**

```bash
cargo test --test integration
```

Expected: All integration tests pass

**Step 3: Build release**

```bash
cargo build --release
```

**Step 4: Manual verification**

Create a test schema file with both features:

```sql
-- test_schema.sql
CREATE FUNCTION auth.on_auth_user_created() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = public
AS $$ BEGIN RETURN NEW; END; $$;

ALTER FUNCTION auth.on_auth_user_created() OWNER TO supabase_auth_admin;
```

Run pgmold plan against a test database and verify:
1. SET search_path appears in the generated SQL
2. ALTER FUNCTION OWNER TO appears in the generated SQL

**Step 5: Final commit**

```bash
git add -A
git commit --message "Complete function config_params and owner support."
```

---

## Summary

| Task | Description | Repo |
|------|-------------|------|
| 1 | Add SET clause parsing to sqlparser fork | sqlparser-fork |
| 2 | Add config_params and owner fields to Function model | pgmold |
| 3 | Update pgmold to use new sqlparser version | pgmold |
| 4 | Extract config_params from parsed AST | pgmold |
| 5 | Introspect config_params from proconfig | pgmold |
| 6 | Generate SET clauses in SQL output | pgmold |
| 7 | Integration test for config_params round-trip | pgmold |
| 8 | Introspect function owner from proowner | pgmold |
| 9 | Parse ALTER FUNCTION OWNER TO statements | pgmold |
| 10 | Generate ALTER FUNCTION OWNER TO statements | pgmold |
| 11 | Integration test for owner round-trip | pgmold |
| 12 | Final verification | pgmold |
