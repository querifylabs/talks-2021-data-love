package com.querifylabs.datalove2021.sql;

import com.querifylabs.datalove2021.database.DatabaseColumn;
import com.querifylabs.datalove2021.database.DatabaseDataType;
import com.querifylabs.datalove2021.database.DatabaseTable;
import com.querifylabs.datalove2021.sql.enumerable.BackendTableQueryable;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class BackendTable extends AbstractTable implements TranslatableTable, QueryableTable {

    private final DatabaseTable databaseTable;

    private BackendTableStatistic statistic;
    private RelDataType rowType;

    public BackendTable(DatabaseTable databaseTable) {
        this.databaseTable = databaseTable;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType != null) {
            return rowType;
        }

        List<DatabaseColumn> backendFields = databaseTable.getSchema().getColumns();
        List<RelDataTypeField> calciteFields = new ArrayList<>(backendFields.size());

        for (DatabaseColumn backendField : backendFields) {
            RelDataType fieldType = typeFactory.createSqlType(mapColumnType(backendField.getType()));

            calciteFields.add(new RelDataTypeFieldImpl(backendField.getName(), calciteFields.size(), fieldType));
        }

        rowType = new RelRecordType(StructKind.PEEK_FIELDS, calciteFields, false);

        return rowType;
    }

    /**
     * Expose the row count to the optimizer.
     */
    @Override
    public Statistic getStatistic() {
        if (statistic == null) {
            statistic = new BackendTableStatistic(databaseTable.getRowCount());
        }

        return statistic;
    }

    /**
     * Define how to convert the table to the {@link TableScan} during the sql-to-rel
     * translation.
     */
    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new BackendTableScan(
            context.getCluster(),
            context.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
            relOptTable);
    }

    /**
     * Required for the linking with the {@code Enumerable} backend.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return (Queryable<T>) new BackendTableQueryable(queryProvider, schema, this, tableName, databaseTable);
    }

    /**
     * Required for the linking with the {@code Enumerable} backend.
     */
    @Override
    public Type getElementType() {
        return Object[].class;
    }

    /**
     * Required for the linking with the {@code Enumerable} backend.
     */
    @Override
    public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
        return Schemas.tableExpression(schema, Object[].class, tableName, clazz);
    }

    private static SqlTypeName mapColumnType(DatabaseDataType backendType) {
        switch (backendType) {
            case STRING:
                return SqlTypeName.VARCHAR;

            case DATE:
                return SqlTypeName.DATE;

            default:
                throw new UnsupportedOperationException("Unsupported backend type: " + backendType);
        }
    }
}
