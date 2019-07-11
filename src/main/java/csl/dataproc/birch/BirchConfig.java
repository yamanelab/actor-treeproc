package csl.dataproc.birch;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class BirchConfig implements Serializable {
    public int maxNodeEntries = 10;
    public double maxDistance = 1.0;
    public BirchNode.DistanceFunction distanceFunction = BirchNode.d0CentroidEuclidDistance;

    public boolean debug;

    static final long serialVersionUID = 1L;

    public BirchConfig() {}

    public BirchConfig(int maxNodeEntries, double maxDistance, BirchNodeCFEntry.DistanceFunction distanceFunction) {
        this.maxNodeEntries = maxNodeEntries;
        this.maxDistance = maxDistance;
        this.distanceFunction = distanceFunction;
    }

    public BirchConfig(BirchConfig conf) {
        this.maxNodeEntries = conf.maxNodeEntries;
        this.maxDistance = conf.maxDistance;
        this.distanceFunction = conf.distanceFunction;
        this.debug = conf.debug;
    }

    /** true if the node is a tree node as it has no children */
    public boolean isTreeNode(BirchNode node) {
        return node instanceof BirchNodeCFTree;
    }

    public boolean canBeInsertedNewEntry(BirchNode target) {
        return target.getChildSize() < maxNodeEntries;
    }

    public boolean canBeMerged(BirchNode target, BirchNode newEntry) {
        BirchNode.DistanceFunction distanceFunction = this.distanceFunction;
        return distanceFunction.distance(target.getClusteringFeature(), newEntry.getClusteringFeature()) < maxDistance;
    }

    public BirchNodeInput newPoint(long id, double... data) {
        return new BirchNodeInput(id, data);
    }

    public BirchNode newEntry(BirchNodeInput entry) {
        return new BirchNodeCFEntry(entry);
    }

    public BirchNode newRoot(int dim) {
        return new BirchNodeCFTree(this, dim);
    }

    public BirchNode newNode(List<BirchNode> children) {
        return new BirchNodeCFTree(this, children);
    }

    public BirchConfig toTest() {
        return new BirchConfigTest(this);
    }

    public boolean isTest() {
        return false;
    }

    public static class BirchConfigTest extends BirchConfig {
        public long nodeCount;

        public BirchConfigTest(BirchConfig conf) {
            super(conf);
        }

        /** use the last column of the data as the class */
        @Override
        public BirchNodeInput newPoint(long id, double... data) {
            int label = (int) data[data.length - 1];
            double[] attrs = Arrays.copyOf(data, data.length - 1);
            return new BirchNodeInput.BirchNodeInputLabeled(id, label, attrs);
        }

        @Override
        public BirchNode newEntry(BirchNodeInput entry) {
            ++nodeCount;
            return super.newEntry(entry);
        }

        @Override
        public BirchNode newRoot(int dim) {
            ++nodeCount;
            return super.newRoot(dim);
        }

        @Override
        public BirchNode newNode(List<BirchNode> children) {
            ++nodeCount;
            return super.newNode(children);
        }

        @Override
        public boolean isTest() {
            return true;
        }
    }
}


