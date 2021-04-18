package com.querifylabs.datalove2021.sql;

import com.querifylabs.datalove2021.database.DatabaseDataType;
import com.querifylabs.datalove2021.database.DatabaseSchema;
import com.querifylabs.datalove2021.database.DatabaseTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings({"SqlDialectInspection", "SqlNoDataSourceInspection"})
public class JdbcTest {

    private static final String TPCH_Q1 =
        "select\n" +
        "    l_returnflag,\n" +
        "    l_linestatus,\n" +
        "    count(*) as count_order\n" +
        "from\n" +
        "    lineitem\n" +
        "where\n" +
        "    l_shipdate <= '1996-09-02'\n" +
        "group by\n" +
        "    l_returnflag,\n" +
        "    l_linestatus\n" +
        "order by\n" +
        "    l_returnflag,\n" +
        "    l_linestatus";

    @Test
    public void testJdbc() throws Exception {
        Properties properties = new Properties();
        properties.put("model", model());

        AtomicReference<String> planInitial = new AtomicReference<>();
        AtomicReference<String> planOptimized = new AtomicReference<>();

        Hook.TRIMMED.addThread(p -> {
            planInitial.set(RelOptUtil.toString((RelNode) p));
        });
        Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(p -> {
            planOptimized.set(RelOptUtil.toString(((RelRoot) p).rel));
        });

        try (Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
            PreparedStatement statement = connection.prepareStatement(TPCH_Q1);

            System.out.println(">>> INITIAL PLAN:");
            System.out.println(planInitial);

            System.out.println(">>> OPTIMIZED PLAN:");
            System.out.println(planOptimized);

            try (ResultSet rs = statement.executeQuery()) {
                printResultSet(rs);
            }
        }
    }

    private static String model() {
        return String.join("\n", new String[]{
            "inline:{",
            "  version: '1.0',",
            "  defaultSchema: 'tpch',",
            "  schemas: [",
            "    {",
            "      type: 'custom',",
            "      name: 'tpch',",
            "      factory: '" + TestSchemaFactory.class.getName() + "'",
            "    }",
            "  ]",
            "}",
        });
    }

    private static void printResultSet(ResultSet rs) throws Exception {
        System.out.println(">>> RESULT:");

        int columnCount = rs.getMetaData().getColumnCount();

        while (rs.next()) {
            List<Object> values = new ArrayList<>(columnCount);

            for (int i = 0; i < columnCount; i++) {
                Object value = rs.getObject(i + 1);
                if (value instanceof Long) {
                    value = String.format("%5d", value);
                }
                values.add(value);
            }

            System.out.println(values);
        }
    }

    public static class TestSchemaFactory implements SchemaFactory {
        @Override
        public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
            return BackendSchema.fromDatabase(createDatabase());
        }

        private static DatabaseTable createDatabase() {
            Path path = Paths.get("src/test/resources/lineitem.txt");

            DatabaseSchema schema = DatabaseSchema.builder("lineitem")
                .addColumn("l_returnflag", DatabaseDataType.STRING)
                .addColumn("l_linestatus", DatabaseDataType.STRING)
                .addColumn("l_shipdate", DatabaseDataType.DATE)
                .build();

            return DatabaseTable.create(path, schema);
        }
    }
}
