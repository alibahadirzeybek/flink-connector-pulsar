package org.apache.flink.connector.pulsar.table.catalog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.pulsar.table.catalog.ReadOnlyPulsarCatalogFactoryOptions.CATALOG_ADMIN_URL;
import static org.apache.flink.connector.pulsar.table.catalog.ReadOnlyPulsarCatalogFactoryOptions.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.connector.pulsar.table.catalog.ReadOnlyPulsarCatalogFactoryOptions.CATALOG_SERVICE_URL;
import static org.apache.flink.connector.pulsar.table.catalog.ReadOnlyPulsarCatalogFactoryOptions.CATALOG_TENANT;

/** Factory method for the creation of ReadOnlyPulsarCatalog. */
public class ReadOnlyPulsarCatalogFactory implements CatalogFactory {

    public static final String IDENTIFIER = "pulsar";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(
                        CATALOG_TENANT,
                        CATALOG_DEFAULT_DATABASE,
                        CATALOG_SERVICE_URL,
                        CATALOG_TENANT)
                .collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public Catalog createCatalog(Context context) {
        return new ReadOnlyPulsarCatalog(
                context.getName(),
                getReadOnlyPulsarCatalogConfiguration(getReadableConfig(context)));
    }

    private ReadOnlyPulsarCatalogConfiguration getReadOnlyPulsarCatalogConfiguration(
            ReadableConfig readableConfig) {
        return new ReadOnlyPulsarCatalogConfiguration(
                readableConfig.get(CATALOG_TENANT),
                readableConfig.get(CATALOG_DEFAULT_DATABASE),
                readableConfig.get(CATALOG_ADMIN_URL),
                readableConfig.get(CATALOG_SERVICE_URL));
    }

    private ReadableConfig getReadableConfig(Context context) {
        return getCatalogFactoryHelper(context).getOptions();
    }

    private FactoryUtil.CatalogFactoryHelper getCatalogFactoryHelper(Context context) {
        return FactoryUtil.createCatalogFactoryHelper(this, context);
    }
}
