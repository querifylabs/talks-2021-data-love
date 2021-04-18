package com.querifylabs.datalove2021.sql;

import com.querifylabs.datalove2021.database.DatabaseSchema;
import com.querifylabs.datalove2021.database.DatabaseTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collections;
import java.util.Map;

public class BackendSchema extends AbstractSchema {

    private final Map<String, Table> tableMap;

    public static BackendSchema fromDatabase(DatabaseTable database) {
        DatabaseSchema databaseSchema = database.getSchema();

        return new BackendSchema(Collections.singletonMap(databaseSchema.getTableName(), new BackendTable(database)));
    }

    private BackendSchema(Map<String, Table> tableMap) {
        this.tableMap = tableMap;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tableMap;
    }
}
