# Shared Task Notes

## In Progress: pgmold-60 - ORM Schema Loading

Drizzle support is implemented. Prisma support is still pending.

**Files created/modified:**
- `src/provider/mod.rs` - Schema provider routing by prefix (sql:, drizzle:)
- `src/provider/drizzle.rs` - Drizzle provider (runs drizzle-kit export)
- `src/cli/mod.rs` - Updated to use provider module

**Usage:**
```bash
pgmold plan -s drizzle:drizzle.config.ts -d postgres://localhost/db
pgmold plan -s sql:schema.sql -s drizzle:drizzle.config.ts -d postgres://localhost/db
```

## Next Steps for pgmold-60

1. Add Prisma provider (run `prisma db diff` or similar)
2. Consider other ORMs: SQLAlchemy, TypeORM, etc.

## Other Ready Tasks

- pgmold-58: Create Kubernetes operator
- pgmold-56: Create Terraform provider
