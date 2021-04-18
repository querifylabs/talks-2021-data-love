package com.querifylabs.datalove2021.database;

public class DatabaseColumn {

    private final String name;
    private final DatabaseDataType type;

    public static DatabaseColumn of(String name, DatabaseDataType type) {
        return new DatabaseColumn(name, type);
    }

    public DatabaseColumn(String name, DatabaseDataType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public DatabaseDataType getType() {
        return type;
    }
}
