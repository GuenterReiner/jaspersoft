package com.jaspersoft.mongodb.connection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class MongoDbConnectionManager {
    private MongoDbConnectionPool connectionsPool;
    private GenericObjectPoolConfig<MongoDbConnection> poolConfiguration;
    private MongoDbConnectionFactory connectionFactory;
    private final Log logger = LogFactory.getLog(MongoDbConnectionManager.class);

    public MongoDbConnectionManager() {
        this.connectionFactory = new MongoDbConnectionFactory();
        this.poolConfiguration = new GenericObjectPoolConfig<>();
        this.poolConfiguration.setTestOnBorrow(true);
        this.poolConfiguration.setTestWhileIdle(true);
        this.poolConfiguration.setMaxIdle(2);
        this.poolConfiguration.setMinIdle(1);
        this.poolConfiguration.setMaxTotal(-1);
    }

    private GenericObjectPool<MongoDbConnection> startConnectionsPool() {
        if (this.connectionsPool == null) {
            this.connectionsPool = new MongoDbConnectionPool(this.connectionFactory, this.poolConfiguration);
        }
        return this.connectionsPool;
    }

    public MongoDbConnection borrowConnection() throws Exception {
        if (this.connectionsPool == null) {
            startConnectionsPool();
        }
        if (this.connectionsPool == null) {
            this.logger.error("No connection pool created");
            return null;
        }
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Current active connections before borrow: " + this.connectionsPool.getNumActive());
        }
        return (MongoDbConnection) this.connectionsPool.borrowObject();
    }

    public void returnConnection(MongoDbConnection connection) {
        if (this.connectionsPool == null) {
            this.logger.error("No connection pool created");
            return;
        }
        try {
            this.connectionsPool.returnObject(connection);
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("Current active connections on return: " + this.connectionsPool.getNumActive());
            }
        } catch (Exception e) {
            this.logger.error(e);
        }
    }

    public void shutdown() {
        if (this.connectionsPool != null) {
            try {
                this.connectionsPool.clear();
                this.connectionsPool.close();
            } catch (Exception e) {
                this.logger.error(e);
            }
        }
    }

    public void setMaxActive(int maxActive) {
    }

    public void setMaxIdle(int maxIdle) {
        this.poolConfiguration.setMaxIdle(maxIdle);
    }

    public void setMinIdle(int minIdle) {
        this.poolConfiguration.setMinIdle(minIdle);
    }

    public void setMongoURI(String mongoURI) {
        this.connectionFactory.setMongoURI(mongoURI);
    }

    public void setUsername(String username) {
        this.connectionFactory.setUsername(username);
    }

    public void setPassword(String password) {
        this.connectionFactory.setPassword(password);
    }
}
