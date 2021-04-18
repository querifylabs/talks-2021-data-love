package com.querifylabs.datalove2021.sql;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.Arrays;

/**
 * A custom optimization rule to push the filter to the {@link BackendTableScan}.
 */
public class BackendFilterTableScanRule extends RelRule<BackendFilterTableScanRule.Config> {

	public static final RelOptRule RULE = Config.DEFAULT.toRule();

	private BackendFilterTableScanRule(Config config) { super(config); }

	@Override
	public void onMatch(RelOptRuleCall call) {
		LogicalFilter filter = call.rel(0);
		BackendTableScan scan = call.rel(1);

		RexNode condition = filter.getCondition();

		RexNode oldCondition = scan.getFilter();
		RexNode newCondition;

		if (oldCondition == null) {
			newCondition = condition;
		} else {
			newCondition = RexUtil.composeConjunction(
				filter.getCluster().getRexBuilder(),
				Arrays.asList(oldCondition, condition)
			);
		}

		call.transformTo(new BackendTableScan(scan.getCluster(), scan.getTraitSet(), scan.getTable(), newCondition));
	}

	public interface Config extends RelRule.Config {
		Config DEFAULT =
			EMPTY
				.withOperandSupplier(
					b0 -> b0.operand(LogicalFilter.class).oneInput(b1 -> b1.operand(BackendTableScan.class).noInputs()))
				.as(Config.class);

		@Override
		default BackendFilterTableScanRule toRule() {
			return new BackendFilterTableScanRule(this);
		}
	}
}
