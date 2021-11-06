package com.jaspersoft.mongodb.adapter;

import net.sf.jasperreports.data.DataAdapter;

public interface MongoDbDataAdapter extends DataAdapter {
    void setMongoURI(String paramString);

    String getMongoURI();

    void setUsername(String paramString);

    String getUsername();

    void setPassword(String paramString);

    String getPassword();
}
