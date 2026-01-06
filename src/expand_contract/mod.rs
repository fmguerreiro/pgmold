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

#[derive(Debug, Clone)]
pub struct ExpandContractPlan {
    pub expand_ops: Vec<PhasedOp>,
    pub backfill_ops: Vec<PhasedOp>,
    pub contract_ops: Vec<PhasedOp>,
}

impl ExpandContractPlan {
    pub fn new() -> Self {
        Self {
            expand_ops: Vec::new(),
            backfill_ops: Vec::new(),
            contract_ops: Vec::new(),
        }
    }
}

pub fn expand_operations(ops: Vec<MigrationOp>) -> ExpandContractPlan {
    let mut plan = ExpandContractPlan::new();

    for op in ops {
        plan.expand_ops.push(PhasedOp {
            phase: Phase::Expand,
            op,
            rationale: "Direct operation".to_string(),
        });
    }

    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_operations_produce_empty_plan() {
        let plan = expand_operations(vec![]);
        assert!(plan.expand_ops.is_empty());
        assert!(plan.backfill_ops.is_empty());
        assert!(plan.contract_ops.is_empty());
    }
}
