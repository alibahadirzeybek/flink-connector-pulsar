package org.apache.flink.connector.pulsar.table.catalog;

/**
 * Data class for holding the configuration related values for passing the arguments to the catalog.
 */
public class ReadOnlyPulsarCatalogConfiguration {
    final String tenant;
    final String defaultDatabase;
    final String adminURL;
    final String serviceURL;

    public ReadOnlyPulsarCatalogConfiguration(
            String tenant, String defaultDatabase, String adminURL, String serviceURL) {
        this.tenant = tenant;
        this.defaultDatabase = defaultDatabase;
        this.adminURL = adminURL;
        this.serviceURL = serviceURL;
    }
}
