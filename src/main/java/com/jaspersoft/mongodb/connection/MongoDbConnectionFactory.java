package com.jaspersoft.mongodb.connection;

import lombok.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

@Data
public class MongoDbConnectionFactory implements PooledObjectFactory<MongoDbConnection> {
    private static final Log logger = LogFactory.getLog(MongoDbConnectionFactory.class);
    private String mongoURI;
    private String username;
    private String password;

    public PooledObject<MongoDbConnection> makeObject() throws Exception {
        logger.info("Factory make object");
        return new DefaultPooledObject<>(new MongoDbConnection(this.mongoURI, this.username, this.password));
    }

    @Override
    public void destroyObject(PooledObject<MongoDbConnection> pooledObject) throws Exception {
    }

    @Override
    public boolean validateObject(PooledObject<MongoDbConnection> pooledObject) {
        return false;
    }

    @Override
    public void activateObject(PooledObject<MongoDbConnection> pooledObject) throws Exception {
    }

    @Override
    public void passivateObject(PooledObject<MongoDbConnection> pooledObject) throws Exception {
    }
}
