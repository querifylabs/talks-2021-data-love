package com.querifylabs.datalove2021.sql;

import com.querifylabs.datalove2021.database.DatabaseTable;
import com.querifylabs.datalove2021.sql.enumerable.BackendPredicates;
import org.apache.calcite.adapter.enumerable.EnumerableBindable;
import org.apache.calcite.adapter.enumerable.EnumerableBindable.EnumerableToBindableConverterRule;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

import java.util.Collections;
import java.util.List;

import static com.querifylabs.datalove2021.sql.enumerable.BackendTableQueryable.METHOD_CAST;
import static com.querifylabs.datalove2021.sql.enumerable.BackendTableQueryable.METHOD_WITH_PREDICATE;

/**
 * A custom physical {@link TableScan} operator that could be integrated with the
 * {@code Enumerable} backend.
 * <p>
 * In this demo, our backend supports only scanning for simplicity. Therefore,
 * we expose that operator as {@link EnumerableRel} in the {@link EnumerableConvention}.
 * <p>
 * In the real implementations, you may support multiple relational operators.
 * In this case, you may want to define your own {@link Convention}
 * for your operators, and also define a dedicated converter operator that would
 * translate your convention to the {@code Enumerable} convention. Please see
 * {@link BindableConvention}, {@link EnumerableBindable}, and {@link EnumerableToBindableConverterRule}
 * as an example of such translation between conventions.
 */
public class BackendTableScan extends TableScan implements EnumerableRel {

    /** The filter that would be pushed down to {@link DatabaseTable} */
    private final RexNode filter;

    protected BackendTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        this(cluster, traitSet, table, null);
    }

    protected BackendTableScan(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        RexNode filter
    ) {
        super(cluster, traitSet, Collections.emptyList(), table);

        this.filter = filter;
    }

    public RexNode getFilter() {
        return filter;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .itemIf("filter", filter, filter != null);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new BackendTableScan(getCluster(), traitSet, table, filter);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = table.getRowCount();

        if (filter != null) {
            // Adjust the extimated row count taking into account the filter.
            rowCount = RelMdUtil.guessSelectivity(filter) * rowCount;
        }

        return rowCount;
    }

    /**
     * Wire up the operator with the {@code Enumerable} convention.
     */
    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        boolean scalar = getRowType().getFieldCount() == 1;

        PhysType physType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            scalar ? JavaRowFormat.SCALAR : JavaRowFormat.ARRAY
        );

        Expression expression = table.getExpression(Queryable.class);

        expression = Expressions.call(METHOD_CAST, expression);

        if (filter != null) {
            var predicateDescriptor = BackendPredicates.filterToPredicate(filter);

            expression = Expressions.call(expression, METHOD_WITH_PREDICATE, Expressions.constant(predicateDescriptor));
        }

        expression = Expressions.call(expression, BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);

        if (scalar) {
            expression = Expressions.call(BuiltInMethod.SLICE0.method, expression);
        }

        return implementor.result(physType, Blocks.toBlock(expression));
    }

    /**
     * A callback to register custom optimization rules in the default Apache Calcite optimization flow.
     */
    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(BackendFilterTableScanRule.RULE);
    }
}
