package com.querifylabs.datalove2021.sql.enumerable;

import com.querifylabs.datalove2021.database.DatabaseSchema;
import com.querifylabs.datalove2021.database.DatabaseTuple;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BackendTableIterable implements Iterable<Object[]> {

    private final DatabaseSchema schema;
    private final List<DatabaseTuple> tuples;

    public BackendTableIterable(DatabaseSchema schema, List<DatabaseTuple> tuples) {
        this.schema = schema;
        this.tuples = tuples;
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<Object[]> iterator() {
        return new DataIterator();
    }

    private Object[] convert(DatabaseTuple tuple) {
        Object[] row = new Object[schema.getColumns().size()];

        for (int i = 0; i < schema.getColumns().size(); i++) {
            var type = schema.getColumns().get(i).getType();
            var value = tuple.getColumn(i);

            switch (schema.getColumns().get(i).getType()) {
                case STRING:
                    row[i] = value;

                    break;

                case DATE:
                    row[i] = ((LocalDate) value).toEpochDay();

                    break;

                default:
                    throw new UnsupportedOperationException("Unsupported type: " + type);
            }
        }

        return row;
    }

    private class DataIterator implements Iterator<Object[]> {

        private int index;

        @Override
        public boolean hasNext() {
            return index < tuples.size();
        }

        @Override
        public Object[] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            var tuple = tuples.get(index++);

            return convert(tuple);
        }
    }
}
