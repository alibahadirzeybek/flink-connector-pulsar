package org.apache.flink.connector.pulsar.table.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Options for passing from the FlinkSQL/Table API catalog options and holding the key/value pairs
 * so that they can be later passed into the actual Read Only Pulsar Catalog implementation.
 */
public class ReadOnlyPulsarCatalogFactoryOptions {
    public static final ConfigOption<String> CATALOG_TENANT =
            ConfigOptions.key("catalog-tenant")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("(Required) The tenant of the Pulsar cluster.");

    public static final ConfigOption<String> CATALOG_DEFAULT_DATABASE =
            ConfigOptions.key("catalog-default-database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("(Required) The default database.");

    public static final ConfigOption<String> CATALOG_ADMIN_URL =
            ConfigOptions.key("catalog-admin-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("(Required) The admin URL of the Pulsar cluster.");

    public static final ConfigOption<String> CATALOG_SERVICE_URL =
            ConfigOptions.key("catalog-service-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("(Required) The service URL of the Pulsar cluster.");
}
