package com.jaspersoft.mongodb.query;

import lombok.Data;
import net.sf.jasperreports.engine.JRExpression;
import net.sf.jasperreports.engine.JRPropertiesHolder;
import net.sf.jasperreports.engine.JRPropertiesMap;
import net.sf.jasperreports.engine.JRValueParameter;
import net.sf.jasperreports.engine.type.ParameterEvaluationTimeEnum;

@Data
public class MongoDbParameter implements JRValueParameter {
    private String name;
    private Object value;

    public MongoDbParameter(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    public Object clone() {
        return null;
    }

    public boolean hasProperties() {
        return false;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return null;
    }

    public void setDescription(String description) {
    }

    public Class<?> getValueClass() {
        if (this.value != null) {
            return this.value.getClass();
        }
        return null;
    }
    
    public String getValueClassName() {
        if (this.value != null) {
            return this.value.getClass().getName();
        }
        return null;
    }

    public boolean isSystemDefined() {
        return false;
    }

    public boolean isForPrompting() {
        return false;
    }

    public JRExpression getDefaultValueExpression() {
        return null;
    }

    public Class<?> getNestedType() {
        return null;
    }

    public String getNestedTypeName() {
        return null;
    }

    public JRPropertiesMap getPropertiesMap() {
        return null;
    }

    public JRPropertiesHolder getParentProperties() {
        return null;
    }

    public Object getValue() {
        return this.value;
    }

    public void setValue(Object value) {
    }

    public ParameterEvaluationTimeEnum getEvaluationTime() {
        return null;
    }
}
