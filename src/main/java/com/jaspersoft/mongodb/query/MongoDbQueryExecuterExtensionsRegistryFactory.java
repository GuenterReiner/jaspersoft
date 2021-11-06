package com.jaspersoft.mongodb.query;

import java.util.Collections;
import java.util.List;

import net.sf.jasperreports.engine.JRPropertiesMap;
import net.sf.jasperreports.engine.query.JRQueryExecuterFactoryBundle;
import net.sf.jasperreports.extensions.ExtensionsRegistry;
import net.sf.jasperreports.extensions.ExtensionsRegistryFactory;

public class MongoDbQueryExecuterExtensionsRegistryFactory implements ExtensionsRegistryFactory {
    private static final ExtensionsRegistry defaultExtensionsRegistry = new ExtensionsRegistry() {
        public <T> List<T> getExtensions(Class<T> extensionType) {
            if (JRQueryExecuterFactoryBundle.class.equals(extensionType)) {
                return Collections.singletonList((T) MongoDbQueryExecuterFactoryBundle.getInstance());
            }
            return null;
        }
    };

    public ExtensionsRegistry createRegistry(String registryId, JRPropertiesMap properties) {
        return defaultExtensionsRegistry;
    }
}
