# Shared Task Notes

## Last Completed Work

- **pgmold-57**: Added migration dry-run with timing estimates
  - New `--estimate-time` flag on `plan` command
  - Estimates based on table row counts and sizes
  - Per-operation timing with confidence levels
  - JSON output support for CI integration
  - Commit: 58d653b

## Ready Tasks (Priority 3)

From `bd ready --json`:
1. **pgmold-60**: Add ORM schema loading support (Drizzle, Prisma) - significant effort
2. **pgmold-58**: Create Kubernetes operator - significant effort
3. **pgmold-56**: Create Terraform provider - significant effort

All remaining tasks are priority 3 with significant scope. Consider:
- Picking the one with clearest ROI
- Breaking into smaller sub-tasks first
- Or identifying smaller improvements not yet tracked

## Implementation Notes for Next Developer

The timing estimate feature (`src/estimate/mod.rs`) uses:
- `ROWS_PER_SECOND_*` constants for throughput estimates
- Table stats from `pg_class` and `pg_stat_user_tables`
- Confidence levels based on whether table stats are available

Could be enhanced:
- Add `--estimate-time` to `apply --dry-run`
- Add hardware-specific calibration
- Track historical execution times for better estimates
