package com.jaspersoft.mongodb;

import com.jaspersoft.mongodb.query.MongoDbQueryWrapper;
import com.mongodb.BasicDBObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import net.sf.jasperreports.engine.JRDataSource;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JRField;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.beanutils.Converter;
import org.apache.commons.beanutils.converters.DateConverter;
import org.apache.commons.beanutils.converters.DoubleConverter;
import org.apache.commons.beanutils.converters.FloatConverter;
import org.apache.commons.beanutils.converters.IntegerConverter;
import org.apache.commons.beanutils.converters.LongConverter;
import org.apache.commons.beanutils.converters.ShortConverter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MongoDbDataSource implements JRDataSource {
    private MongoDbQueryWrapper wrapper;
    private BasicDBObject currentDbObject;
    public static final String QUERY_LANGUAGE = "MongoDbQuery";
    private static final Log logger = LogFactory.getLog(MongoDbDataSource.class);

    private boolean hasIterator = false;

    private boolean hasCommandResult = false;

    private Iterator<?> resultsIterator;

    private Map<?, ?> currentResult;
    ConvertUtilsBean convertUtilsBean;

    public MongoDbDataSource(MongoDbQueryWrapper wrapper) {
        logger.info("New MongoDB Data Source");
        this.wrapper = wrapper;
        this.hasIterator = (wrapper.iterator != null);
        if (!this.hasIterator) {
            this.hasCommandResult = (wrapper.commandResults != null);
            this.resultsIterator = wrapper.commandResults.iterator();
        }
        initConverter();
    }

    public void initConverter() {
        this.convertUtilsBean = new ConvertUtilsBean();
        DoubleConverter doubleConverter = new DoubleConverter();
        FloatConverter floatConverter = new FloatConverter();
        IntegerConverter integerConverter = new IntegerConverter();
        LongConverter longConverter = new LongConverter();
        ShortConverter shortConverter = new ShortConverter();
        DateConverter dateConverter = new DateConverter();
        dateConverter.setLocale(Locale.getDefault());
        DateFormat formatter = DateFormat.getDateTimeInstance(3, 3, Locale.getDefault());
        String pattern = ((SimpleDateFormat) formatter).toPattern();
        dateConverter.setPattern(pattern);

        this.convertUtilsBean.register((Converter) doubleConverter, double.class);
        this.convertUtilsBean.register((Converter) doubleConverter, Double.class);
        this.convertUtilsBean.register((Converter) floatConverter, float.class);
        this.convertUtilsBean.register((Converter) floatConverter, Float.class);
        this.convertUtilsBean.register((Converter) integerConverter, int.class);
        this.convertUtilsBean.register((Converter) integerConverter, Integer.class);
        this.convertUtilsBean.register((Converter) longConverter, long.class);
        this.convertUtilsBean.register((Converter) longConverter, Long.class);
        this.convertUtilsBean.register((Converter) shortConverter, short.class);
        this.convertUtilsBean.register((Converter) shortConverter, Short.class);
        this.convertUtilsBean.register((Converter) dateConverter, Date.class);
    }

    public Object getFieldValue(JRField field) throws JRException {
        try {
            String name = field.getDescription();
            if (name == null || name.isEmpty()) name = field.getName();
            if (name == null) return null;
            String[] ids = name.split("\\.");
            if (this.hasIterator) {
                Object result = getCursorValue(ids);
                return converter(field, result, ids[ids.length - 1]);
            }
            if (this.hasCommandResult) {
                Object result = getCommandResult(ids);
                return converter(field, result, ids[ids.length - 1]);
            }
            return null;
        } catch (Exception e) {
            logger.error(e);
            throw new JRException(e.getMessage());
        }
    }

    public Object converter(JRField field, Object value, String fieldName) {
        if (value == null)
            return null;
        Class<?> requiredClass = field.getValueClass();

        if (requiredClass.equals(value.getClass())) {
            return value;
        }
        if (requiredClass == Object.class) {
            return value;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Converting value " + value.toString() + " with type " + value.getClass().getName() + " to " + requiredClass.getName() + " type");
        }

        try {
            if (requiredClass == String.class) {
                return value.toString();
            }
            return this.convertUtilsBean.convert(value, requiredClass);
        } catch (Exception e) {
            String message = "Conversion error, field name: \"" + field.getName() + "\" requested type: \"" + field.getValueClassName() + "\" received type: \"" + value.getClass().getName() + "\" value: \"" + value.toString() + "\"";
            logger.error(message);
            message.concat("\n");
            message.concat(e.getMessage());
            throw new ClassCastException(message);
        }
    }

    private Object getCommandResult(String[] ids) {
        Map<?, ?> currentMap = this.currentResult;
        for (int index = 0; index < ids.length; index++) {
            boolean isLast = (index == ids.length - 1);
            String id = ids[index];
            Object currentFieldObject = currentMap.get(id);
            if (currentFieldObject == null) {
                return null;
            }
            if (currentFieldObject instanceof Map) {
                if (isLast) {
                    return currentFieldObject;
                }
                currentMap = (Map<?, ?>) currentFieldObject;
            } else {
                if (isLast) {
                    return currentFieldObject;
                }
                return null;
            }
        }
        return null;
    }

    private Object getCursorValue(String[] ids) {
        BasicDBObject fieldObject = this.currentDbObject;
        for (int index = 0; index < ids.length; index++) {
            boolean isLast = (index == ids.length - 1);
            String id = ids[index];
            Object currentFieldObject = fieldObject.get(id);
            if (currentFieldObject == null) {
                return null;
            }
            if (currentFieldObject instanceof BasicDBObject) {
                if (isLast) {
                    return currentFieldObject;
                }
                fieldObject = (BasicDBObject) currentFieldObject;
            } else {
                if (isLast) {
                    return currentFieldObject;
                }
                return null;
            }
        }
        return null;
    }
    
    public boolean next() throws JRException {
        boolean next = false;
        if (this.hasIterator && (next = this.wrapper.iterator.hasNext())) {
            this.currentDbObject = (BasicDBObject) this.wrapper.iterator.next();
        } else if (this.hasCommandResult) {
            next = this.resultsIterator.hasNext();
            this.currentResult = null;
            if (next) {
                this.currentResult = (Map<?, ?>) this.resultsIterator.next();
            }
        }
        return next;
    }
}
