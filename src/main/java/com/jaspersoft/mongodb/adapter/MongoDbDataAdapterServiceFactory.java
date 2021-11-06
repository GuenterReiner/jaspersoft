package com.jaspersoft.mongodb.adapter;

import net.sf.jasperreports.data.DataAdapter;
import net.sf.jasperreports.data.DataAdapterContributorFactory;
import net.sf.jasperreports.data.DataAdapterService;
import net.sf.jasperreports.engine.JasperReportsContext;
import net.sf.jasperreports.engine.ParameterContributorContext;

public class MongoDbDataAdapterServiceFactory implements DataAdapterContributorFactory {
    private static final MongoDbDataAdapterServiceFactory INSTANCE = new MongoDbDataAdapterServiceFactory();

    public static MongoDbDataAdapterServiceFactory getInstance() {
        return INSTANCE;
    }

    public DataAdapterService getDataAdapterService(ParameterContributorContext context, DataAdapter dataAdapter) {
        MongoDbDataAdapterService mongoDbDataAdapterService = null;
        JasperReportsContext jasperReportsContext = context.getJasperReportsContext();
        DataAdapterService dataAdapterService = null;

        if (dataAdapter instanceof MongoDbDataAdapter) {
            mongoDbDataAdapterService = new MongoDbDataAdapterService(jasperReportsContext, (MongoDbDataAdapter) dataAdapter);
        }
        return (DataAdapterService) mongoDbDataAdapterService;
    }
}
