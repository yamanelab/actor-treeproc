package csl.dataproc.csv.arff;

import csl.dataproc.csv.CsvLine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** CsvLine with an optional weight value */
public class ArffLine extends CsvLine {
    protected String weight;

    static final long serialVersionUID = 1L;

    public ArffLine() {
        this(new ArrayList<>(), "");
    }

    public ArffLine(long start, long end, List<String> data, boolean emptyLine) {
        super(start, end, data, emptyLine);
    }

    public ArffLine(Collection<String> data, String weight) {
        super(data);
        this.weight = weight;
    }

    public ArffLine(String weight, String... data) {
        super(data);
        this.weight = weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }


    @Override
    public ArffLine copy() {
        return (ArffLine) super.copy();
    }

    @Override
    public ArffLine addColumns(String... columns) {
        return (ArffLine) super.addColumns(columns);
    }

    @Override
    public ArffLine setColumn(int i, String column) {
        return (ArffLine) super.setColumn(i, column);
    }

    @Override
    public ArffLine setColumn(String... columns) {
        return (ArffLine) super.setColumn(columns);
    }

    @Override
    public ArffLine addColumns(int i, String... columns) {
        return (ArffLine) super.addColumns(i, columns);
    }
}
