package org.apache.flink.connector.pulsar.table.catalog;

import org.apache.flink.connector.pulsar.table.PulsarTableOptions;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Catalog implementation that enables interaction with Apache Pulsar in a read-only manner. It maps
 * the concepts of tenants into catalog, namespaces into database and topics into table from Apache
 * Pulsar to Apache Flink concepts.
 */
public class ReadOnlyPulsarCatalog extends AbstractCatalog {
    private PulsarAdmin pulsarAdmin;

    private final ReadOnlyPulsarCatalogConfiguration readOnlyPulsarCatalogConfiguration;

    public ReadOnlyPulsarCatalog(
            String catalogName,
            ReadOnlyPulsarCatalogConfiguration readOnlyPulsarCatalogConfiguration) {
        super(catalogName, readOnlyPulsarCatalogConfiguration.defaultDatabase);
        this.readOnlyPulsarCatalogConfiguration = readOnlyPulsarCatalogConfiguration;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new ReadOnlyPulsarCatalogFactory());
    }

    @Override
    public void open() throws CatalogException {
        if (pulsarAdmin == null) {
            try {
                final PulsarAdminBuilderImpl pulsarAdminBuilder = new PulsarAdminBuilderImpl();
                pulsarAdmin =
                        pulsarAdminBuilder
                                .serviceHttpUrl(readOnlyPulsarCatalogConfiguration.adminURL)
                                .build();
            } catch (PulsarClientException pulsarClientException) {
                throw new CatalogException(
                        "Failed to create Pulsar admin with configuration:"
                                + readOnlyPulsarCatalogConfiguration,
                        pulsarClientException);
            }
        }
    }

    @Override
    public void close() throws CatalogException {
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return pulsarAdmin.namespaces().getNamespaces(readOnlyPulsarCatalogConfiguration.tenant)
                    .stream()
                    .map(
                            namespaceNameWithTenant ->
                                    namespaceNameWithTenant.substring(
                                            namespaceNameWithTenant.lastIndexOf("/") + 1))
                    .collect(Collectors.toList());
        } catch (PulsarAdminException pulsarAdminException) {
            throw new CatalogException(
                    String.format(
                            "Failed to list all namespaces for tenant: %s",
                            readOnlyPulsarCatalogConfiguration.tenant),
                    pulsarAdminException);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws CatalogException {
        return new CatalogDatabaseImpl(new HashMap<>(), null);
    }

    @Override
    public boolean tableExists(ObjectPath objectPath) throws CatalogException {
        return listTables(objectPath.getDatabaseName()).contains(objectPath.getObjectName());
    }

    @Override
    public List<String> listTables(String databaseName) throws CatalogException {
        try {
            return pulsarAdmin.topics()
                    .getPartitionedTopicList(
                            NamespaceName.get(
                                            readOnlyPulsarCatalogConfiguration.tenant, databaseName)
                                    .toString())
                    .stream()
                    .map(
                            topicNameWithNamespaceAndTenant ->
                                    topicNameWithNamespaceAndTenant.substring(
                                            topicNameWithNamespaceAndTenant.lastIndexOf("/") + 1))
                    .collect(Collectors.toList());
        } catch (PulsarAdminException pulsarAdminException) {
            throw new CatalogException(
                    String.format("Failed to list all tables in database: %s", databaseName),
                    pulsarAdminException);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath objectPath) throws CatalogException {
        try {
            final String topicName =
                    getTopicName(objectPath.getDatabaseName(), objectPath.getObjectName());
            final SchemaInfo schemaInfo = getSchemaInfo(topicName);
            final Schema schema = getSchema(schemaInfo);
            Map<String, String> options = new HashMap<>();
            options.put(FactoryUtil.CONNECTOR.key(), "pulsar");
            options.put(FactoryUtil.FORMAT.key(), JsonFormatFactory.IDENTIFIER);
            options.put(
                    PulsarTableOptions.SERVICE_URL.key(),
                    readOnlyPulsarCatalogConfiguration.serviceURL);
            options.put(PulsarTableOptions.TOPICS.key(), topicName);
            options.put(PulsarTableOptions.EXPLICIT.key(), "false");
            options.put(PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID.key(), "earliest");
            options.put(PulsarTableOptions.SOURCE_SUBSCRIPTION_TYPE.key(), "Exclusive");
            options.put(PulsarTableOptions.SOURCE_SUBSCRIPTION_NAME.key(), getName());
            return CatalogTable.of(schema, "", Collections.emptyList(), options);
        } catch (PulsarAdminException pulsarAdminException) {
            throw new CatalogException(
                    String.format(
                            "Failed to get table %s in database: %s",
                            objectPath.getObjectName(), objectPath.getDatabaseName()),
                    pulsarAdminException);
        }
    }

    private Schema getSchema(SchemaInfo schemaInfo) {
        return Schema.newBuilder().fromRowDataType(getDataType(schemaInfo)).build();
    }

    private DataType getDataType(SchemaInfo schemaInfo) {
        return AvroSchemaConverter.convertToDataType(getSchemaString(schemaInfo.getSchema()));
    }

    private String getSchemaString(byte[] schema) {
        return new String(schema, StandardCharsets.UTF_8);
    }

    private SchemaInfo getSchemaInfo(String topicName) throws PulsarAdminException {
        return pulsarAdmin.schemas().getSchemaInfo(topicName);
    }

    private String getTopicName(String namespace, String topic) {
        return String.join(
                "/", Arrays.asList(readOnlyPulsarCatalogConfiguration.tenant, namespace, topic));
    }

    // ------------------------------------------------------------------------
    // Unsupported catalog operations for Pulsar
    // There should not be such permission in the connector, it is very dangerous
    // ------------------------------------------------------------------------

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> expressions)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }
}
