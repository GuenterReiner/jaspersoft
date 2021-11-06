package com.jaspersoft.mongodb.connection;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class MongoDbConnectionPool extends GenericObjectPool<MongoDbConnection> {
    public MongoDbConnectionPool(PooledObjectFactory<MongoDbConnection> factory) {
        super(factory);
    }

    public MongoDbConnectionPool(PooledObjectFactory<MongoDbConnection> factory, GenericObjectPoolConfig<MongoDbConnection> config) {
        super(factory, config);
    }
}
