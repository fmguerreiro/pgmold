# Trigger Support Design

## Data Model

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum TriggerTiming { Before, After, InsteadOf }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum TriggerEvent { Insert, Update, Delete, Truncate }

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trigger {
    pub name: String,
    pub table_schema: String,
    pub table: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub update_columns: Vec<String>,  // For UPDATE OF col1, col2
    pub for_each_row: bool,           // false = FOR EACH STATEMENT
    pub when_clause: Option<String>,
    pub function_schema: String,
    pub function_name: String,
    pub function_args: Vec<String>,
}
```

Add `triggers: BTreeMap<String, Trigger>` to `Schema`.

## Migration Operations

```rust
CreateTrigger(Trigger),
DropTrigger {
    table_schema: String,
    table: String,
    name: String,
},
AlterTrigger {
    table_schema: String,
    table: String,
    name: String,
    new_trigger: Trigger,
}
```

## SQL Generation

```sql
CREATE TRIGGER "name"
  BEFORE|AFTER|INSTEAD OF INSERT OR UPDATE OF col1, col2 OR DELETE
  ON "schema"."table"
  FOR EACH ROW|STATEMENT
  WHEN (condition)
  EXECUTE FUNCTION "schema"."func"(args);

DROP TRIGGER "name" ON "schema"."table";
```

## Implementation Order (TDD)

1. model - Trigger struct + enums
2. parser - Parse CREATE TRIGGER
3. sqlgen - Generate DDL
4. diff - diff_triggers
5. planner - Dependency ordering
6. introspect - pg_trigger query
7. lint - Destructive drop warning
