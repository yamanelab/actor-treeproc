package csl.dataproc.birch;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

public interface BirchNode {
    BirchNode getParent();
    default void setParent(BirchNode parent) {
        throw new UnsupportedOperationException();
    }

    ClusteringFeature getClusteringFeature();

    interface DistanceFunction {
        double distance(ClusteringFeature cf1, ClusteringFeature cf2);
    }


    /** a leaf returns null indicating no further lower trees */
    default BirchNode findClosestChild(BirchNodeInput input, ClusteringFeature cf, BirchNodeCFTree.DistanceFunction distanceFunction) {
        return null;
    }

    void addEntry(BirchNodeInput e);

    /** entry interface */
    default LongList getIndexes() {
        throw new UnsupportedOperationException();
    }

    /** node interface */
    default BirchNode put(BirchNodeInput e) {
        throw new UnsupportedOperationException();
    }
    /** node interface */
    default int getChildSize() {
        throw new UnsupportedOperationException();
    }
    /** node interface */
    default List<BirchNode> getChildren() {
        throw new UnsupportedOperationException();
    }
    /** node interface */
    default void setChildren(List<BirchNode> children) {
        throw new UnsupportedOperationException();
    }
    /** node interface */
    default void insertAfterWithoutAddingCf(BirchNode child, BirchNode newChildNext) {
        throw new UnsupportedOperationException();
    }

    boolean accept(BirchVisitor visitor);

    class BirchVisitor {
        public boolean visit(BirchNode node) {
            return true;
        }
        public boolean visitEntry(BirchNodeCFEntry e) {
            return visit(e);
        }
        public boolean visitTree(BirchNodeCFTree t) {
            return visit(t);
        }
        public boolean visitInput(BirchNodeInput i) {
            return visit(i);
        }
        public void visitTreeChild(BirchNodeCFTree tree, int i, BirchNodeCFTree.NodeHolder nodeHolder) {
        }
        public void visitAfterTree(BirchNodeCFTree t) {}
    }

    DefaultDistanceFunction d0CentroidEuclidDistance = new DefaultDistanceFunction(0);
    DefaultDistanceFunction d1CentroidManhattanDistance = new DefaultDistanceFunction(1);
    DefaultDistanceFunction d2AverageInterClusterDistance = new DefaultDistanceFunction(2);
    DefaultDistanceFunction d3AverageIntraClusterDistance = new DefaultDistanceFunction(3);
    DefaultDistanceFunction d4VarianceIncreaseDistance = new DefaultDistanceFunction(4);

    class DefaultDistanceFunction implements DistanceFunction, Serializable {
        private int num;
        private transient DistanceFunction function;

        static final long serialVersionUID = 1L;

        public DefaultDistanceFunction() {
            this(0);
        }

        public DefaultDistanceFunction(int num) {
            set(num);
        }

        public DefaultDistanceFunction(int num, DistanceFunction function) {
            this.num = num;
            this.function = function;
        }

        private void set(int num) {
            this.num = num;
            switch (num) {
                case 0:
                    this.function = ClusteringFeature::centroidEuclidDistance;
                    break;
                case 1:
                    this.function = ClusteringFeature::centroidManhattanDistance;
                    break;
                case 2:
                    this.function = ClusteringFeature::averageInterClusterDistance;
                    break;
                case 3:
                    this.function = ClusteringFeature::averageIntraClusterDistance;
                    break;
                case 4:
                    this.function = ClusteringFeature::varianceIncreaseDistance;
                    break;
            }
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            set(in.readInt());
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeInt(num);
        }

        @Override
        public double distance(ClusteringFeature cf1, ClusteringFeature cf2) {
            return function.distance(cf1, cf2);
        }

        @Override
        public String toString() {
            return "d" + num;
        }

        public DistanceFunction getFunction() {
            return function;
        }

        public int getNum() {
            return num;
        }
    }
}
