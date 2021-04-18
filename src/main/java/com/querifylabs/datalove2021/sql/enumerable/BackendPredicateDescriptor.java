package com.querifylabs.datalove2021.sql.enumerable;

import com.querifylabs.datalove2021.database.DatabaseTuple;

import java.util.function.Predicate;

public interface BackendPredicateDescriptor {
    Predicate<DatabaseTuple> toPredicate();
}
