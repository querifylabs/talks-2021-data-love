package com.querifylabs.datalove2021.database;

import java.util.List;

public class DatabaseTuple {

    private final List<Object> values;

    public DatabaseTuple(List<Object> values) {
        this.values = values;
    }

    public Object getColumn(int index) {
        return values.get(index);
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
