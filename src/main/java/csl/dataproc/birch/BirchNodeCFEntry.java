package csl.dataproc.birch;

import java.io.Serializable;

public class BirchNodeCFEntry implements BirchNode, Serializable {
    protected ClusteringFeature clusteringFeature;
    protected LongList indexes;
    protected BirchNode parent;

    static final long serialVersionUID = 1L;

    public BirchNodeCFEntry() {}

    public BirchNodeCFEntry(ClusteringFeature clusteringFeature, LongList indexes) {
        this.clusteringFeature = clusteringFeature;
        this.indexes = indexes;
    }

    public BirchNodeCFEntry(BirchNodeInput input) {
        this.clusteringFeature = input.getClusteringFeature();
        this.indexes = input.getIndexes();
    }

    @Override
    public void setParent(BirchNode parent) {
        this.parent = parent;
    }

    @Override
    public BirchNode getParent() {
        return parent;
    }

    @Override
    public ClusteringFeature getClusteringFeature() {
        return clusteringFeature;
    }

    public LongList getIndexes() {
        return indexes;
    }

    @Override
    public void addEntry(BirchNodeInput e) {
        clusteringFeature.add(e.getClusteringFeature());
        indexes.addAll(e.getIndexes());
    }

    public void addEntryWithoutUpdatingCf(BirchNodeInput e) {
        indexes.addAll(e.getIndexes());
    }

    @Override
    public BirchNode findClosestChild(BirchNodeInput input, ClusteringFeature cf, BirchNodeCFTree.DistanceFunction distanceFunction) {
        return null;
    }

    @Override
    public boolean accept(BirchVisitor visitor) {
        return visitor.visitEntry(this);
    }
}
