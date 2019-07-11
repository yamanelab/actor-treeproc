package csl.dataproc.birch;

import csl.dataproc.csv.CsvLine;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class BirchNodeInput implements Serializable, BirchNode {
    protected ClusteringFeature clusteringFeature;
    protected long id;

    static final long serialVersionUID = 1L;

    public BirchNodeInput() {
    }

    public BirchNodeInput(long id, double... data) {
        this(id, new ClusteringFeature(data));
    }
    public BirchNodeInput(long id, ClusteringFeature clusteringFeature) {
        this.clusteringFeature = clusteringFeature;
        this.id = id;
    }

    @Override
    public BirchNode getParent() {
        throw new UnsupportedOperationException(); //TODO
    }

    @Override
    public LongList getIndexes() {
        LongList l = new LongList.ArrayLongList(10);
        l.add(id);
        return l;
    }

    @Override
    public void addEntry(BirchNodeInput e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClusteringFeature getClusteringFeature() {
        return clusteringFeature;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean accept(BirchVisitor visitor) {
        return visitor.visitInput(this);
    }

    public static class BirchNodeInputLabeled extends BirchNodeInput {
        protected int label;
        protected BirchNode parent;

        public BirchNodeInputLabeled(int label) {
            this.label = label;
        }

        public BirchNodeInputLabeled(long id, int label, double... data) {
            super(id, data);
            this.label = label;
        }

        public BirchNodeInputLabeled(long id, ClusteringFeature clusteringFeature, int label) {
            super(id, clusteringFeature);
            this.label = label;
        }

        @Override
        public BirchNode getParent() {
            return parent;
        }

        @Override
        public void setParent(BirchNode parent) {
            this.parent = parent;
        }

        public int getLabel() {
            return label;
        }

        @Override
        public CsvLine toLine() {
            double[] ds = clusteringFeature.getLinearSum();
            List<String> cols = new ArrayList<>(ds.length + 1);
            for (double d : ds) {
                cols.add(Double.toString(d));
            }
            cols.add(Integer.toString(label));
            return new CsvLine(cols);
        }

        @Override
        public String toString() {
            return "BirchInput(id=" + id + ",ss=" + clusteringFeature.squareSum + ",k=" + label + ")";
        }
    }

    @Override
    public String toString() {
        return "BirchInput(id=" + id + ",ss=" + clusteringFeature.squareSum + ")";
    }

    public CsvLine toLine() {
        return new CsvLine(Arrays.stream(clusteringFeature.getLinearSum())
                .mapToObj(Double::toString)
                .collect(Collectors.toList()));
    }
}
