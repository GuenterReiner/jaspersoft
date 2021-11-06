package com.jaspersoft.mongodb.query;

import net.sf.jasperreports.engine.*;
import net.sf.jasperreports.engine.query.JRQueryExecuter;
import net.sf.jasperreports.engine.query.QueryExecuterFactory;

import java.util.Map;

public class MongoDbQueryExecuterFactory implements QueryExecuterFactory {
    public JRQueryExecuter createQueryExecuter(JRDataset dataset, Map<String, ? extends JRValueParameter> parameters) throws JRException {
        return (JRQueryExecuter) new MongoDbQueryExecuter(
                (JasperReportsContext) DefaultJasperReportsContext.getInstance(), dataset, parameters);
    }

    public Object[] getBuiltinParameters() {
        return null;
    }

    public boolean supportsQueryParameterType(String queryParameterType) {
        return true;
    }


    public JRQueryExecuter createQueryExecuter(JasperReportsContext jasperReportsContext, JRDataset dataset, Map<String, ? extends JRValueParameter> parameters) throws JRException {
        return (JRQueryExecuter) new MongoDbQueryExecuter(jasperReportsContext, dataset, parameters);
    }
}
