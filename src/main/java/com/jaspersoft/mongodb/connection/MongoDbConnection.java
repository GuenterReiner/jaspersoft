
package com.jaspersoft.mongodb.connection;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import net.sf.jasperreports.engine.JRException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

@Data
public class MongoDbConnection implements Connection {
    private static final String URL_PREFIX = "mongodb://";
    private MongoClient client;
    private static final Log logger = LogFactory.getLog(MongoDbConnection.class);

    public MongoDbConnection(MongoClient mongoClient) throws JRException {
        try {
            this.client = mongoClient;
        } catch (Exception ex) {
            String msg = "Error creating MongoDBConnection; Caused by: " + ex.getMessage();
            logger.error(msg, ex);
            throw new JRException(msg, ex);
        }
    }

    public MongoDbConnection(String mongoURI, String username, String password) throws JRException {
        try {
            this.client = MongoClients.create(mongoURI);
        } catch (Exception ex) {
            String msg = "Error creating MongoDBConnection; Caused by: " + ex.getMessage();
            logger.error(msg, ex);
            throw new JRException(msg, ex);
        }
    }

    public MongoDatabase getMongoDatabase() {
        if (this.client == null) {
            logger.error("No client");
            return null;
        } else {
            return this.client.getDatabase("boxoffice");
        }
    }

    public void test() {
        
    }

    public void close() {
    }

    public boolean isClosed() throws SQLException {
        return this.client == null;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public Statement createStatement() throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return null;
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    public String nativeSQL(String sql) throws SQLException {
        return null;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
    }

    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    public void commit() throws SQLException {
    }

    public void rollback() throws SQLException {
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public void setCatalog(String catalog) throws SQLException {
    }

    public String getCatalog() throws SQLException {
        return null;
    }

    public void setTransactionIsolation(int level) throws SQLException {
    }

    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public void clearWarnings() throws SQLException {
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    }

    public void setHoldability(int holdability) throws SQLException {
    }

    public int getHoldability() throws SQLException {
        return 0;
    }

    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    public void rollback(Savepoint savepoint) throws SQLException {
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return null;
    }

    public Clob createClob() throws SQLException {
        return null;
    }

    public Blob createBlob() throws SQLException {
        return null;
    }

    public NClob createNClob() throws SQLException {
        return null;
    }

    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    public boolean isValid(int timeout) throws SQLException {
        return true;
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
    }

    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    public Properties getClientInfo() throws SQLException {
        return null;
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    public void setSchema(String schema) throws SQLException {
    }

    public String getSchema() throws SQLException {
        return null;
    }

    public void abort(Executor executor) throws SQLException {
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    }

    public int getNetworkTimeout() throws SQLException {
        return 0;
    }
}
