package org.apache.flink.connector.pulsar.table.catalog;

import org.apache.flink.connector.pulsar.table.PulsarTableTestBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.types.Row;

import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * IT cases for the Read Only Pulsar Catalog. It aims to verify runtime behaviour and certain use
 * cases are correct and can produce/consume the desired records as user specifies.
 */
public class ReadOnlyPulsarCatalogITCase extends PulsarTableTestBase {
    @Test
    public void pulsarCatalog() throws Exception {
        final String test = this.getClass().getSimpleName();
        final String tenant = "test_tenant_" + test + "_" + randomAlphanumeric(3);
        final String namespace = "test_namespace_" + test + "_" + randomAlphanumeric(3);
        final String topic = "test_topic_" + test + "_" + randomAlphanumeric(3);
        final String createCatalog =
                String.format(
                        "CREATE CATALOG %s "
                                + "WITH ( "
                                + "    'type' = '%s', "
                                + "    'catalog-tenant' = '%s', "
                                + "    'catalog-default-database' = '%s', "
                                + "    'catalog-admin-url' = '%s', "
                                + "    'catalog-service-url' = '%s' "
                                + ");",
                        test,
                        ReadOnlyPulsarCatalogFactory.IDENTIFIER,
                        tenant,
                        namespace,
                        getTestAdminUrl(),
                        getTestServiceUrl());

        preparePulsar(tenant, namespace, topic);
        prepareCatalog(createCatalog, test);
        showDatabases(namespace);
        showTables(namespace, topic);
        describeTable(topic);
        insertToAndSelectFromTable(topic);
    }

    private void preparePulsar(String tenant, String namespace, String topic) throws Exception {
        createTestTenant(tenant);
        createTestNamespace(tenant + "/" + namespace);
        createTestTopic(tenant + "/" + namespace + "/" + topic, 1);
        createTestSchema(tenant + "/" + namespace + "/" + topic, Schema.JSON(Event.class));
    }

    private void prepareCatalog(String createCatalog, String catalog) {
        tableEnv.executeSql(createCatalog);
        tableEnv.executeSql("USE CATALOG " + catalog);
    }

    private void showDatabases(String database) {
        tableEnv.executeSql("SHOW DATABASES")
                .collect()
                .forEachRemaining(row -> assertThat(row.getField(0)).isEqualTo(database));
    }

    private void showTables(String database, String table) {
        tableEnv.executeSql(String.format("SHOW TABLES IN `%s`", database))
                .collect()
                .forEachRemaining(row -> assertThat(row.getField(0)).isEqualTo(table));
    }

    private void describeTable(String table) {
        tableEnv.executeSql(String.format("DESCRIBE %s", table))
                .collect()
                .forEachRemaining(
                        row -> {
                            assertThat(row.getField(0)).isEqualTo("name");
                            assertThat(row.getField(1)).isEqualTo(DataTypes.STRING().toString());
                            assertThat(row.getField(2)).isEqualTo(true);
                        });
    }

    private void insertToAndSelectFromTable(String table) throws Exception {
        final String name = randomAlphabetic(5);
        insertToTable(table, name);
        selectFromTable(table);
        assertThat(TestingSinkFunction.rows)
                .isEqualTo(Collections.singletonList(String.format("+I[%s]", name)));
    }

    private void insertToTable(String table, String name) throws Exception {
        tableEnv.executeSql(String.format("INSERT INTO %s (name) VALUES ('%s')", table, name))
                .await();
    }

    private void selectFromTable(String table) throws Exception {
        DataStream<Row> result =
                tableEnv.toDataStream(tableEnv.sqlQuery(String.format("SELECT * FROM %s", table)));
        TestingSinkFunction sink = new TestingSinkFunction(1);
        result.addSink(sink).setParallelism(1);
        execute(table);
    }

    private void execute(String jobName) throws Exception {
        try {
            env.execute(jobName);
        } catch (Throwable e) {
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }
    }

    private static final class TestingSinkFunction implements SinkFunction<Row> {

        private static final long serialVersionUID = -2627251419283922580L;
        private static final List<String> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        @Override
        public void invoke(Row value, Context context) {
            rows.add(value.toString());
            if (rows.size() >= expectedSize) {
                // job finish
                throw new SuccessException();
            }
        }
    }

    private static boolean isCausedByJobFinished(Throwable e) {
        if (e instanceof SuccessException) {
            return true;
        } else if (e.getCause() != null) {
            return isCausedByJobFinished(e.getCause());
        } else {
            return false;
        }
    }

    private static class Event {
        public String name;
    }
}
