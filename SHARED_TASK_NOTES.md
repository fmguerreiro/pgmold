# Shared Task Notes

## Last Session (2026-01-17)

### Completed
- **pgmold-52**: Added partition bound change support
  - Added `AttachPartition` and `DetachPartition` migration operations
  - `diff_partitions` now detects bound/parent changes and generates detach+attach ops
  - SQL generation for `ALTER TABLE ATTACH/DETACH PARTITION`
  - Lock hazard detection for attach/detach operations
  - Integration test `partition_bound_change` validates the full workflow

### Note on PG17 MERGE/SPLIT PARTITIONS
The task mentioned PG17 MERGE/SPLIT PARTITIONS but **this feature was reverted from PostgreSQL 17** due to security concerns. It may appear in PostgreSQL 18.

### Commit
- `26426b6`: Add partition bound change support with ATTACH/DETACH operations.

### Pending Unstaged Changes
There are linting fixes from the precommit runner in:
- `src/util/mod.rs` (collapsible if, inline format args)
- `src/model/mod.rs`, `src/parser/mod.rs`, `src/pg/introspect.rs` (minor clippy fixes)

These can be committed as a cleanup or discarded.

## Next Steps

### Priority 2 Tasks Ready
- **pgmold-62**: Add data loss warnings to lint system
- **pgmold-55**: Implement SERIAL column support (plan at `docs/plans/2025-12-03-serial-support.md`)
- **pgmold-59**: Add hazard detection system with temp DB validation

## Notes
- Partition support now includes: create, drop, attach, detach, and bound change detection
- Phase 4 (sub-partitioning, partition-level indexes) not yet implemented
