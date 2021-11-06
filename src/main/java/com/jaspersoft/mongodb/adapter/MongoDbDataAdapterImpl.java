package com.jaspersoft.mongodb.adapter;

import lombok.Data;
import lombok.EqualsAndHashCode;
import net.sf.jasperreports.data.AbstractDataAdapter;

@Data
@EqualsAndHashCode(callSuper = true)
public class MongoDbDataAdapterImpl extends AbstractDataAdapter implements MongoDbDataAdapter {
    private String mongoURI;
    private String username;
    private String password;
}
