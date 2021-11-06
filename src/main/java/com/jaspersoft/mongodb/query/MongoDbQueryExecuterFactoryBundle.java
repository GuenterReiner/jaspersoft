package com.jaspersoft.mongodb.query;

import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.query.JRQueryExecuterFactoryBundle;
import net.sf.jasperreports.engine.query.QueryExecuterFactory;
import net.sf.jasperreports.engine.util.JRSingletonCache;

public class MongoDbQueryExecuterFactoryBundle implements JRQueryExecuterFactoryBundle {
    private static final JRSingletonCache<QueryExecuterFactory> cache = new JRSingletonCache(QueryExecuterFactory.class);
    private static final MongoDbQueryExecuterFactoryBundle instance = new MongoDbQueryExecuterFactoryBundle();
    private static final String[] languages = new String[]{"MongoDbQuery"};

    public static MongoDbQueryExecuterFactoryBundle getInstance() {
        return instance;
    }

    public String[] getLanguages() {
        return languages;
    }

    public QueryExecuterFactory getQueryExecuterFactory(String language) throws JRException {
        if ("MongoDbQuery".equals(language)) {
            return (QueryExecuterFactory) cache.getCachedInstance(MongoDbQueryExecuterFactory.class.getName());
        }
        return null;
    }
}
