# Shared Task Notes

## Completed This Iteration

1. **pgmold-55: SERIAL column support** - Already implemented and tested. Closed issue.
2. **pgmold-62: Add data loss warnings** - Implemented and committed (596b64c).
   - Added `warn_data_loss_drop_table` and `warn_data_loss_drop_column` warnings
   - These warnings appear even when `--allow-destructive` is passed
   - 4 new tests added

## Next Priority Tasks

From `bd ready --json`:
1. **pgmold-59**: Add hazard detection system with temp DB validation (priority 2)
   - Spin up temp DB, apply migration, validate success
   - Reference: https://github.com/stripe/pg-schema-diff
2. **pgmold-54**: Enhance CI/CD documentation with drift detection examples (priority 2)
3. **pgmold-61**: Document minimum PostgreSQL versions (priority 3)

## Notes

- TRUNCATE is not in MigrationOp (pgmold is schema diff, not DML)
- Column type narrowing already has `warn_type_narrowing`
- Consider adding more lint rules from Atlas (50+ rules total)
