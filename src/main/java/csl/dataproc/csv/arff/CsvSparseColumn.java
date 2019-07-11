package csl.dataproc.csv.arff;

import csl.dataproc.csv.ArffReader;

import java.io.Serializable;

public class CsvSparseColumn implements Comparable<CsvSparseColumn>, Serializable {
    protected int index;
    protected String value;

    static final long serialVersionUID = 1L;

    public static CsvSparseColumn column(int index, String value) {
        return new CsvSparseColumn(index, value);
    }

    public CsvSparseColumn() {
        this(0, "");
    }

    public CsvSparseColumn(int index, String value) {
        this.index = index;
        this.value = value;
    }

    public int getIndex() {
        return index;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return index + " " + ArffReader.quoteString(value);
    }

    /** compare indexes */
    @Override
    public int compareTo(CsvSparseColumn o) {
        return Integer.compare(index, o.index);
    }
}
