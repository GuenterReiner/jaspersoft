package com.jaspersoft.mongodb.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaspersoft.mongodb.MongoDbDataSource;
import com.jaspersoft.mongodb.connection.MongoDbConnection;
import lombok.SneakyThrows;
import net.sf.jasperreports.engine.*;
import net.sf.jasperreports.engine.query.JRAbstractQueryExecuter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

public class MongoDbQueryExecuter extends JRAbstractQueryExecuter {
    private static String MONGO_NULL_OBJECT = "null";
    private ObjectMapper mapper = new ObjectMapper();
    private static final Log logger = LogFactory.getLog(MongoDbQueryExecuter.class);
    private Map<String, ? extends JRValueParameter> reportParameters;
    private Map<String, Object> parameters;
    private MongoDbQueryWrapper wrapper;
    private boolean directParameters;

    public MongoDbQueryExecuter(JasperReportsContext jasperReportsContext, JRDataset dataset, Map<String, ? extends JRValueParameter> parameters) throws JRException {
        this(jasperReportsContext, dataset, parameters, false);
    }

    public MongoDbQueryExecuter(JasperReportsContext jasperReportsContext, JRDataset dataset, Map<String, ? extends JRValueParameter> parameters, boolean directParameters) {
        super(jasperReportsContext, dataset, parameters);
        this.directParameters = directParameters;
        this.reportParameters = parameters;
        this.parameters = new HashMap<>();
        parseQuery();
    }

    public boolean cancelQuery() throws JRException {
        logger.warn("Cancel not implemented");
        return false;
    }

    public void close() {
        this.wrapper = null;
    }

    private MongoDbConnection processConnection(JRValueParameter valueParameter) throws JRException {
        if (valueParameter == null) {
            throw new JRException("No MongoDB connection");
        }
        return (MongoDbConnection) valueParameter.getValue();
    }

    public JRDataSource createDatasource() throws JRException {
        MongoDbConnection connection = (MongoDbConnection) ((Map) getParameterValue("REPORT_PARAMETERS_MAP")).get("REPORT_CONNECTION");
        if (connection == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("REPORT_PARAMETERS_MAP: " + ((Map) getParameterValue("REPORT_PARAMETERS_MAP")).keySet());
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Direct parameters: " + this.reportParameters.keySet());
            }
            connection = processConnection(this.reportParameters.get("REPORT_CONNECTION"));
            if (connection == null) {
                throw new JRException("No MongoDB connection");
            }
        }
        this.wrapper = new MongoDbQueryWrapper(getQueryString(), connection, this.parameters);
        return (JRDataSource) new MongoDbDataSource(this.wrapper);
    }

    protected String getParameterReplacement(String parameterName) {
        Object parameterValue = this.reportParameters.get(parameterName);
        if (parameterValue == null) {
            throw new JRRuntimeException("Parameter \"" + parameterName + "\" does not exist.");
        }
        if (parameterValue instanceof JRValueParameter) {
            parameterValue = ((JRValueParameter) parameterValue).getValue();
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Geting parameter replacement, parameterName: " + parameterName + "; replacement:" + parameterValue);
        }
        return processParameter(parameterName, parameterValue);
    }

    private String processParameter(String parameterName, Object parameterValue) {
        if (parameterValue instanceof java.util.Collection) {
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            for (Object value : (java.util.Collection) parameterValue) {
                if (value != null) {
                    builder.append(generateParameterObject(parameterName, value));
                } else {
                    builder.append(MONGO_NULL_OBJECT);
                }
                builder.append(", ");
            }
            if (builder.length() > 2) {
                builder.delete(builder.length() - 2, builder.length());
            }
            builder.append("]");
            return builder.toString();
        }
        this.parameters.put(parameterName, parameterValue);
        return generateParameterObject(parameterName, parameterValue);
    }

    @SneakyThrows
    private String generateParameterObject(String parameterName, Object parameterValue) {
        if (logger.isDebugEnabled()) {
            if (parameterValue != null) {
                logger.debug("Generating parameter object, parameterName: " + parameterName + "; value:" + parameterValue.toString() + "; class:" + parameterValue.getClass().toString());
            } else {
                logger.debug("Generating parameter object, parameterName: " + parameterName + "; value: null");
            }
        }
        if (parameterValue != null) {
            //return JSONSerializers.getStrict().serialize(parameterValue);
            return mapper.writer().writeValueAsString(parameterValue);
        }
        return "{ " + parameterName + " : null }";
    }

    public String getProcessedQueryString() {
        return getQueryString();
    }

    protected Object getParameterValue(String parameterName, boolean ignoreMissing) {
        try {
            return super.getParameterValue(parameterName, ignoreMissing);
        } catch (Exception e) {
            if (e.getMessage().endsWith("cannot be cast to net.sf.jasperreports.engine.JRValueParameter") && this.directParameters) {
                return this.reportParameters.get(parameterName);
            }

            return null;
        }
    }

    public Map<String, Object> getParameters() {
        return this.parameters;
    }
}
