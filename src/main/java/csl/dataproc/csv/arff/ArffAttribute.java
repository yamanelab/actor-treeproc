package csl.dataproc.csv.arff;

import csl.dataproc.csv.ArffReader;

import java.io.Serializable;

public class ArffAttribute implements Serializable {
    protected String name;
    protected ArffAttributeType type;

    static final long serialVersionUID = 1L;

    public ArffAttribute() {
        this("", ArffAttributeType.SimpleAttributeType.Real);
    }

    public ArffAttribute(String name, ArffAttributeType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public ArffAttributeType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "@attribute "  + ArffReader.quoteString(name) + " " + type;
    }
}
