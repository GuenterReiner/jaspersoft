package com.jaspersoft.mongodb.adapter;

import com.jaspersoft.mongodb.connection.MongoDbConnection;
import net.sf.jasperreports.data.AbstractDataAdapterService;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JasperReportsContext;
import net.sf.jasperreports.util.SecretsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

public class MongoDbDataAdapterService extends AbstractDataAdapterService {
    private static final Log log = LogFactory.getLog(MongoDbDataAdapterService.class);
    private MongoDbConnection connection;
    private MongoDbDataAdapter dataAdapter;

    public MongoDbDataAdapterService(JasperReportsContext jasperReportsContext, MongoDbDataAdapter dataAdapter) {
        super(jasperReportsContext, dataAdapter);
        this.dataAdapter = dataAdapter;
    }

    public void contributeParameters(Map<String, Object> parameters) throws JRException {
        if (this.connection != null) {
            dispose();
        }
        if (this.dataAdapter != null) {
            try {
                createConnection();
                parameters.put("REPORT_CONNECTION", this.connection);
            } catch (Exception e) {
                throw new JRException(e);
            }
        }
    }

    private void createConnection() throws JRException {
        String password = this.dataAdapter.getPassword();
        SecretsUtil secretService = SecretsUtil.getInstance(getJasperReportsContext());
        if (secretService != null) {
            password = secretService.getSecret("net.sf.jasperreports.data.adapter", password);
        }
        this.connection = new MongoDbConnection(this.dataAdapter.getMongoURI(), this.dataAdapter.getUsername(), password);
    }

    public void dispose() {
        try {
            if (this.connection != null)
                this.connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            if (log.isErrorEnabled()) {
                log.error("Error while closing the connection.", e);
            }
        }
    }

    public void test() throws JRException {
        try {
            if (this.connection == null) {
                createConnection();
            }
            this.connection.test();
        } finally {
            dispose();
        }
    }
}
