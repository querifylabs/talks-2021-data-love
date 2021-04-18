package com.querifylabs.datalove2021.sql.enumerable;

import com.querifylabs.datalove2021.database.DatabaseTable;
import com.querifylabs.datalove2021.database.DatabaseTuple;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Predicate;

public class BackendTableQueryable extends AbstractTableQueryable<Object[]> {

    public static final Method METHOD_CAST;
    public static final Method METHOD_WITH_PREDICATE;

    private final DatabaseTable databaseTable;
    private Predicate<DatabaseTuple> predicate;

    static {
        try {
            METHOD_CAST = BackendTableQueryable.class.getMethod("cast", Queryable.class);
            METHOD_WITH_PREDICATE = BackendTableQueryable.class.getMethod("withPredicate", BackendPredicateDescriptor.class);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public BackendTableQueryable(
        QueryProvider queryProvider,
        SchemaPlus schema,
        QueryableTable table,
        String tableName,
        DatabaseTable databaseTable
    ) {
        super(queryProvider, schema, table, tableName);

        this.databaseTable = databaseTable;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        List<DatabaseTuple> rows = predicate == null ? databaseTable.query() : databaseTable.query(predicate);

        BackendTableIterable iterable = new BackendTableIterable(
            databaseTable.getSchema(),
            rows
        );

        return Linq4j.iterableEnumerator(iterable);
    }

    public BackendTableQueryable withPredicate(BackendPredicateDescriptor predicateDescriptor) {
        predicate = predicateDescriptor.toPredicate();

        return this;
    }

    public static BackendTableQueryable cast(Queryable<?> queryable) {
        assert queryable instanceof BackendTableQueryable;

        return (BackendTableQueryable) queryable;
    }
}
