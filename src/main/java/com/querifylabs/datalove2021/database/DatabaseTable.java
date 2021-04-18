package com.querifylabs.datalove2021.database;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class DatabaseTable {

    private final Path path;
    private final DatabaseSchema schema;

    public static DatabaseTable create(Path path, DatabaseSchema schema) {
        return new DatabaseTable(path, schema);
    }

    private DatabaseTable(Path path, DatabaseSchema schema) {
        this.path = path;
        this.schema = schema;
    }

    public DatabaseSchema getSchema() {
        return schema;
    }

    public List<DatabaseTuple> query() {
        return query((tuple) -> true);
    }

    public List<DatabaseTuple> query(Predicate<DatabaseTuple> filter) {
        List<DatabaseTuple> res = new ArrayList<>();

        for (String line : readFile()) {
            var tuple = convert(line);

            if (filter.test(tuple)) {
                res.add(convert(line));
            }
        }

        return res;
    }

    public long getRowCount() {
        return query().size();
    }

    private List<String> readFile() {
        try {
            return Files.readAllLines(path);
        } catch (IOException e) {
            throw new RuntimeException("Cannot read file " + path, e);
        }
    }

    private DatabaseTuple convert(String line) {
        String[] parts = line.split("\\|");

        var convertedValues = new ArrayList<>(schema.getColumns().size());

        for (int i = 0; i < schema.getColumns().size(); i++) {
            var column = schema.getColumns().get(i);
            var value = parts[i];

            var convertedValue = convert(column, value);
            convertedValues.add(convertedValue);
        }

        return new DatabaseTuple(convertedValues);
    }

    private Object convert(DatabaseColumn column, String value) {
        switch (column.getType()) {
            case STRING:
                return value;

            case DATE:
                return LocalDate.parse(value);

            default:
                throw new UnsupportedOperationException("Unsupported type: " + value);
        }
    }
}
