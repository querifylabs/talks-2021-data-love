package com.querifylabs.datalove2021.database;

import java.util.ArrayList;
import java.util.List;

public class DatabaseSchema {

    private final String tableName;
    private final List<DatabaseColumn> columns;

    public static Builder builder(String tableName) {
        return new Builder(tableName);
    }

    private DatabaseSchema(String tableName, List<DatabaseColumn> columns) {
        this.tableName = tableName;
        this.columns = List.copyOf(columns);
    }

    public String getTableName() {
        return tableName;
    }

    public List<DatabaseColumn> getColumns() {
        return columns;
    }

    public static final class Builder {

        private final String tableName;
        private final List<DatabaseColumn> columns = new ArrayList<>();

        private Builder(String tableName) {
            this.tableName = tableName.toUpperCase();
        }

        public Builder addColumn(String name, DatabaseDataType type) {
            columns.add(DatabaseColumn.of(name.toUpperCase(), type));

            return this;
        }

        public DatabaseSchema build() {
            return new DatabaseSchema(tableName, columns);
        }
    }
}
