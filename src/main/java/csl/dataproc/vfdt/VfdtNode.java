package csl.dataproc.vfdt;

import csl.dataproc.csv.CsvLine;
import csl.dataproc.csv.arff.ArffAttributeType;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public interface VfdtNode {
    long getInstanceCount();
    int getDepth();

    VfdtNode put(DataLine line);
    VfdtNode putArrayLine(ArrayDataLine line);
    VfdtNode putSparseLine(SparseDataLine line);

    /** downs to the next child node or returns this if it is a leaf (VfdtNodeLeaf or VfdtNodeGrowing) */
    default VfdtNode stepClassify(DataLine line) {
        return this;
    }

    /** a leaf (VfdtNodeLeaf or VfdtNodeGrowing) can return a predicted class */
    default long getClassValue() {
        return -1;
    }

    default VfdtNode downToLeaf(DataLine line) {
        VfdtNode n = this;
        while (true) {
            VfdtNode next = n.stepClassify(line);
            if (next == n || next == null) {
                break;
            }
            n = next;
        }
        return n;
    }

    interface DataLine {
        long getLong(int idx);
        double getDouble(int idx);
    }

    class ArrayDataLine implements DataLine, Serializable {
        public Object[] data;

        static final long serialVersionUID = 1L;

        public ArrayDataLine() {
            this(new Object[0]);
        }

        public ArrayDataLine(Object... data) {
            this.data = data;
        }

        @Override
        public long getLong(int idx) {
            return (long) data[idx];
        }

        @Override
        public double getDouble(int idx) {
            return (double) data[idx];
        }

        @Override
        public String toString() {
            return Arrays.stream(data)
                    .map(o -> o == null ? "?" : o)
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
        }
    }

    boolean accept(NodeVisitor visitor);

    class NodeVisitor {
        public boolean visit(VfdtNode node) {
            return true;
        }
        public boolean visitSplit(VfdtNodeSplit node) {
            return visit(node);
        }
        public boolean visitLeaf(VfdtNodeLeaf node) {
            return visit(node);
        }
        public boolean visitGrowing(VfdtNodeGrowing node) {
            return visit(node);
        }


        public boolean visitSplitDiscrete(VfdtNodeSplitDiscrete node) {
            return visitSplit(node);
        }

        public boolean visitSplitContinuous(VfdtNodeSplitContinuous node) {
            return visitSplit(node);
        }

        public void visitSplitChild(VfdtNodeSplit node, int index, VfdtNode child) {}

        public void visitAfterSplit(VfdtNodeSplit node) {}

        public void visitAfterSplitContinuous(VfdtNodeSplitContinuous node) {
            visitAfterSplit(node);
        }

        public void visitAfterSplitDiscrete(VfdtNodeSplitDiscrete node) {
            visitAfterSplit(node);
        }
    }

    static DataLine parse(List<ArffAttributeType> types, CsvLine line) {
        Object[] array = new Object[types.size()];
        for (int i = 0, l = array.length; i < l; ++i) {
            ArffAttributeType type = types.get(i);
            switch (type.getKind()) {
                case ArffAttributeType.ATTRIBUTE_KIND_INTEGER:
                    array[i] = line.getLong(i, -1);
                    break;
                case ArffAttributeType.ATTRIBUTE_KIND_ENUM:
                    //-1 if not found
                    array[i] = (long) ((ArffAttributeType.EnumAttributeType) type).getValues().indexOf(line.get(i));
                    break;
                case ArffAttributeType.ATTRIBUTE_KIND_REAL:
                    array[i] = line.getDouble(i, -1);
                    break;
                case ArffAttributeType.ATTRIBUTE_KIND_STRING:
                    array[i] = line.get(i);
                    break;
                case ArffAttributeType.ATTRIBUTE_KIND_DATE:
                    String data = line.get(i);
                    Instant time = ((ArffAttributeType.DateAttributeType) type).parse(data);
                    array[i] = ((double) time.getEpochSecond()) + (time.getNano() / (double) 1_000_000_000);
            }
        }
        return new ArrayDataLine(array);
    }
}
