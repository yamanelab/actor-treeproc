package csl.dataproc.csv.arff;

import csl.dataproc.csv.ArffReader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ArffHeader implements Serializable {
    protected String name;
    protected List<ArffAttribute> attributes;
    protected long dataIndex;

    static final long serialVersionUID = 1L;

    public ArffHeader() {
        this("", new ArrayList<>(), 0);
    }

    public ArffHeader(String name, List<ArffAttribute> attributes, long dataIndex) {
        this.name = name;
        this.attributes = attributes;
        this.dataIndex = dataIndex;
    }

    public String getName() {
        return name;
    }

    public List<ArffAttribute> getAttributes() {
        return attributes;
    }

    public long getDataIndex() {
        return dataIndex;
    }

    @Override
    public String toString() {
        return (name != null ? "@relation " + ArffReader.quoteString(name) + "\n" : "") +
                    attributes.stream()
                            .map(ArffAttribute::toString)
                            .collect(Collectors.joining("\n")) +
                (dataIndex != -1 ? "\n@data\n" : "\n");
    }

}
