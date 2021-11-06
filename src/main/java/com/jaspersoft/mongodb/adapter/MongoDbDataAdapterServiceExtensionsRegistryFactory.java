package com.jaspersoft.mongodb.adapter;

import net.sf.jasperreports.data.DataAdapterContributorFactory;
import net.sf.jasperreports.engine.JRPropertiesMap;
import net.sf.jasperreports.extensions.ExtensionsRegistry;
import net.sf.jasperreports.extensions.ExtensionsRegistryFactory;
import net.sf.jasperreports.extensions.SingletonExtensionRegistry;

public class MongoDbDataAdapterServiceExtensionsRegistryFactory implements ExtensionsRegistryFactory {
    private static final ExtensionsRegistry extensionsRegistry =
            (ExtensionsRegistry) new SingletonExtensionRegistry(DataAdapterContributorFactory.class, MongoDbDataAdapterServiceFactory.getInstance());

    public ExtensionsRegistry createRegistry(String registryId, JRPropertiesMap properties) {
        return extensionsRegistry;
    }
}
