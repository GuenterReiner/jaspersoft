package com.jaspersoft.mongodb;

import com.jaspersoft.mongodb.connection.MongoDbConnection;
import com.jaspersoft.mongodb.query.MongoDbParameter;
import com.jaspersoft.mongodb.query.MongoDbQueryExecuter;
import com.jaspersoft.mongodb.query.MongoDbQueryWrapper;
import com.mongodb.BasicDBObject;
import net.sf.jasperreports.engine.JRDataset;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JRValueParameter;
import net.sf.jasperreports.engine.JasperReportsContext;
import net.sf.jasperreports.engine.design.JRDesignField;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MongoDbFieldsProvider {
    private static MongoDbFieldsProvider instance;
    private static final Lock lock = new ReentrantLock();
    private static final Log logger = LogFactory.getLog(MongoDbFieldsProvider.class);
    public static final String FIELD_NAME_SEPARATOR = ".";

    public static MongoDbFieldsProvider getInstance() {
        lock.lock();
        try {
            if (instance == null) {
                instance = new MongoDbFieldsProvider();
            }
            return instance;
        } finally {
            lock.unlock();
        }
    }

    public List<JRDesignField> getFields(JasperReportsContext context, JRDataset dataset, Map<String, Object> parameters, MongoDbConnection connection) throws JRException {
        MongoDbQueryExecuter queryExecuter = null;
        MongoDbQueryWrapper wrapper = null;
        try {
            Map<String, JRValueParameter> newValueParameters = new HashMap<>();
            for (String parameterName : parameters.keySet()) {
                Object parameterValue = parameters.get(parameterName);
                MongoDbParameter newParameter = new MongoDbParameter(parameterName, parameterValue);
                newValueParameters.put(parameterName, newParameter);
            }
            parameters.clear();
            newValueParameters.put("REPORT_PARAMETERS_MAP", new MongoDbParameter("REPORT_PARAMETERS_MAP", parameters));

            queryExecuter = new MongoDbQueryExecuter(context, dataset, newValueParameters, true);

            wrapper = new MongoDbQueryWrapper(queryExecuter.getProcessedQueryString(), connection, queryExecuter.getParameters());
            logger.info("FieldsProvider will find fields from the first " + wrapper.rowsToProcess + " records.");
            Map<String, Class<?>> fieldNames = new TreeMap<>();
            if (wrapper.iterator != null) {
                processCursorFields(wrapper, fieldNames);
            } else if (wrapper.commandResults != null) {
                processCommandResultFields(wrapper, fieldNames);
            }
            List<JRDesignField> fields = new ArrayList<>();
            JRDesignField field = null;
            logger.info("Found " + fieldNames.keySet().size() + " fields");
            for (String fieldName : fieldNames.keySet()) {
                field = new JRDesignField();
                field.setName(fieldName);
                field.setValueClass(fieldNames.get(fieldName));
                field.setDescription(null);
                fields.add(field);
            }
            return fields;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void processCommandResultFields(MongoDbQueryWrapper wrapper, Map<String, Class<?>> fieldNames) {
        Iterator<?> resultsIterator = wrapper.commandResults.iterator();
        while (resultsIterator.hasNext() && wrapper.rowsToProcess >= 0) {
            Map<?, ?> currentResult = (Map<?, ?>) resultsIterator.next();
            processMapResult(null, currentResult, fieldNames);
            wrapper.rowsToProcess--;
        }
    }

    private void processCursorFields(MongoDbQueryWrapper wrapper, Map<String, Class<?>> fieldNames) {
        try {
            while (wrapper.iterator.hasNext() && wrapper.rowsToProcess >= 0) {
                BasicDBObject currentDbObject = (BasicDBObject) wrapper.iterator.next();
                processDBObject(null, currentDbObject, fieldNames);
                wrapper.rowsToProcess--;
            }
        } finally {
            if (wrapper.iterator != null) {
                wrapper.iterator.close();
            }
        }
    }

    private void processMapResult(String parentFieldName, Map<?, ?> currentResult, Map<String, Class<?>> fieldNames) {
        if (logger.isDebugEnabled()) {
            logger.debug("processDBObject parentFieldName: " + parentFieldName);
            logger.debug("processDBObject currentDbObject: " + currentResult.toString());
        }
        for (Object fieldName : currentResult.keySet()) {
            Object value = currentResult.get(fieldName);
            if (value == null) {
                continue;
            }
            if (value instanceof Map) {
                processMapResult(((parentFieldName == null) ? "" : (parentFieldName + ".")) + fieldName, (Map<?, ?>) value, fieldNames);
                continue;
            }
            fieldNames.put(((parentFieldName == null) ? "" : (parentFieldName + ".")) + fieldName, value
                    .getClass());
        }
    }

    private void processDBObject(String parentFieldName, BasicDBObject currentDbObject, Map<String, Class<?>> fieldNames) {
        if (logger.isDebugEnabled()) {
            logger.debug("processDBObject parentFieldName: " + parentFieldName);
            logger.debug("processDBObject currentDbObject: " + currentDbObject.toString());
        }
        for (String fieldName : currentDbObject.keySet()) {
            Object value = currentDbObject.get(fieldName);
            if (value == null) {
                continue;
            }
            if (value instanceof com.mongodb.BasicDBList) {
                fieldNames.put(((parentFieldName == null) ? "" : (parentFieldName + ".")) + fieldName, List.class);
                continue;
            }
            if (value instanceof BasicDBObject) {
                processDBObject(((parentFieldName == null) ? "" : (parentFieldName + ".")) + fieldName, (BasicDBObject) value, fieldNames);
                continue;
            }
            fieldNames.put(((parentFieldName == null) ? "" : (parentFieldName + ".")) + fieldName, value.getClass());
        }
    }
}
