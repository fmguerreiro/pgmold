use crate::diff::MigrationOp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase {
    Expand,
    Backfill,
    Contract,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhasedOp {
    pub phase: Phase,
    pub op: MigrationOp,
    pub rationale: String,
}

#[derive(Debug, Clone, Default)]
pub struct ExpandContractPlan {
    pub expand_ops: Vec<PhasedOp>,
    pub backfill_ops: Vec<PhasedOp>,
    pub contract_ops: Vec<PhasedOp>,
}

impl ExpandContractPlan {
    pub fn new() -> Self {
        Self::default()
    }
}

pub fn expand_operations(ops: Vec<MigrationOp>) -> ExpandContractPlan {
    let mut plan = ExpandContractPlan::new();

    for op in ops {
        match op {
            MigrationOp::AddColumn { table, column } => {
                if !column.nullable {
                    let mut nullable_column = column.clone();
                    nullable_column.nullable = true;

                    plan.expand_ops.push(PhasedOp {
                        phase: Phase::Expand,
                        op: MigrationOp::AddColumn {
                            table: table.clone(),
                            column: nullable_column,
                        },
                        rationale: format!(
                            "Add column '{}' as nullable to allow existing rows to have NULL values",
                            column.name
                        ),
                    });

                    plan.backfill_ops.push(PhasedOp {
                        phase: Phase::Backfill,
                        op: MigrationOp::BackfillHint {
                            table: table.clone(),
                            column: column.name.clone(),
                            hint: format!(
                                "UPDATE {} SET {} = <value> WHERE {} IS NULL;",
                                table, column.name, column.name
                            ),
                        },
                        rationale: format!(
                            "Backfill values for column '{}' before adding NOT NULL constraint",
                            column.name
                        ),
                    });

                    plan.contract_ops.push(PhasedOp {
                        phase: Phase::Contract,
                        op: MigrationOp::SetColumnNotNull {
                            table: table.clone(),
                            column: column.name.clone(),
                        },
                        rationale: format!(
                            "Add NOT NULL constraint to column '{}' after backfill is complete",
                            column.name
                        ),
                    });
                } else {
                    plan.expand_ops.push(PhasedOp {
                        phase: Phase::Expand,
                        op: MigrationOp::AddColumn { table, column },
                        rationale: "Add nullable column directly".to_string(),
                    });
                }
            }
            _ => {
                plan.expand_ops.push(PhasedOp {
                    phase: Phase::Expand,
                    op,
                    rationale: "Direct operation".to_string(),
                });
            }
        }
    }

    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Column, PgType};

    #[test]
    fn empty_operations_produce_empty_plan() {
        let plan = expand_operations(vec![]);
        assert!(plan.expand_ops.is_empty());
        assert!(plan.backfill_ops.is_empty());
        assert!(plan.contract_ops.is_empty());
    }

    #[test]
    fn add_not_null_column_expands_to_three_phases() {
        let column = Column {
            name: "email".to_string(),
            data_type: PgType::Text,
            nullable: false,
            default: None,
            comment: None,
        };

        let ops = vec![MigrationOp::AddColumn {
            table: "users".to_string(),
            column,
        }];

        let plan = expand_operations(ops);

        assert_eq!(plan.expand_ops.len(), 1);
        assert_eq!(plan.backfill_ops.len(), 1);
        assert_eq!(plan.contract_ops.len(), 1);

        match &plan.expand_ops[0].op {
            MigrationOp::AddColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column.name, "email");
                assert!(column.nullable);
            }
            _ => panic!("Expected AddColumn in expand phase"),
        }

        match &plan.backfill_ops[0].op {
            MigrationOp::BackfillHint { table, column, .. } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
            }
            _ => panic!("Expected BackfillHint in backfill phase"),
        }

        match &plan.contract_ops[0].op {
            MigrationOp::SetColumnNotNull { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
            }
            _ => panic!("Expected SetColumnNotNull in contract phase"),
        }
    }

    #[test]
    fn add_nullable_column_stays_in_expand_only() {
        let column = Column {
            name: "bio".to_string(),
            data_type: PgType::Text,
            nullable: true,
            default: None,
            comment: None,
        };

        let ops = vec![MigrationOp::AddColumn {
            table: "users".to_string(),
            column,
        }];

        let plan = expand_operations(ops);

        assert_eq!(plan.expand_ops.len(), 1);
        assert_eq!(plan.backfill_ops.len(), 0);
        assert_eq!(plan.contract_ops.len(), 0);

        match &plan.expand_ops[0].op {
            MigrationOp::AddColumn { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column.name, "bio");
                assert!(column.nullable);
            }
            _ => panic!("Expected AddColumn in expand phase"),
        }
    }
}
