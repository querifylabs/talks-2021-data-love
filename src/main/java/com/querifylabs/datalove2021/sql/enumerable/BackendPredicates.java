package com.querifylabs.datalove2021.sql.enumerable;

import com.querifylabs.datalove2021.database.DatabaseDataType;
import com.querifylabs.datalove2021.database.DatabaseTuple;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.time.LocalDate;
import java.util.function.Predicate;

public class BackendPredicates {
    /**
     * Convert Apache Calcite's expression into a descriptor that could be serialized into a source code.
     * <p>
     * Apache Calcite allows you to emit constants in the generated {@code Enumerable} source code.
     * But the constant should be either of one of the predefined Java types, or it should have a proper
     * constructor. In this method, we emit a descriptor that has a constructor with all the public fields,
     * so that it could be converted into {@code new DescriptorClass(args)} in the source code.
     * <p>
     * In this demo, we support only "column <= date_literal" expressions. In the normal implementation,
     * you may want to create a fully-fledged translation instead.
     *
     * @see org.apache.calcite.linq4j.tree.Expressions#constant(Object)
     * @see org.apache.calcite.linq4j.tree.ConstantExpression
     */
    public static BackendPredicateDescriptor filterToPredicate(RexNode rexNode) {
        if (rexNode.getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
            var operands = ((RexCall) rexNode).getOperands();

            var left = operands.get(0);
            var right = operands.get(1);

            if (left.getKind() != SqlKind.INPUT_REF) {
                throw new UnsupportedOperationException("The left operand must be column reference: " + left);
            }
            if (right.getKind() != SqlKind.LITERAL && right.getType().getSqlTypeName() != SqlTypeName.DATE) {
                throw new UnsupportedOperationException("The right operand must be DATE literal: " + right);
            }

            return new LessThanEqualsDatePredicateDescriptor(
                ((RexInputRef) left).getIndex(),
                DatabaseDataType.DATE,
                LocalDate.ofEpochDay(((RexLiteral) right).getValueAs(Integer.class)).toString()
            );
        } else {
            throw new UnsupportedOperationException("Only <= is supported in this example.");
        }
    }

    public static class LessThanEqualsDatePredicateDescriptor implements BackendPredicateDescriptor {

        public final int leftIndex;
        public final DatabaseDataType rightType;
        public final String rightValueString;

        public LessThanEqualsDatePredicateDescriptor(int leftIndex, DatabaseDataType rightType, String rightValueString) {
            this.leftIndex = leftIndex;
            this.rightType = rightType;
            this.rightValueString = rightValueString;
        }

        @Override
        public Predicate<DatabaseTuple> toPredicate() {
            if (rightValueString == null) {
                throw new UnsupportedOperationException("NULL values are not supported in this example.");
            }

            if (rightType == DatabaseDataType.DATE) {
                LocalDate rightValue = LocalDate.parse(rightValueString);
                return t -> ((LocalDate) t.getColumn(leftIndex)).compareTo(rightValue) <= 0;
            } else {
                throw new UnsupportedOperationException("Unsupported type: " + rightType);
            }
        }
    }
}
