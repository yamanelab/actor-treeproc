package csl.dataproc.vfdt;

import csl.dataproc.tgls.util.ArraysAsList;

import java.io.Serializable;
import java.util.List;

public class VfdtNodeSplitContinuous extends VfdtNodeSplit implements Serializable {
    protected double threshold;
    protected VfdtNode left;
    protected VfdtNode right;

    static final long serialVersionUID = 1L;

    public VfdtNodeSplitContinuous() {
        super(0, 0, 0);
    }

    public VfdtNodeSplitContinuous(long instanceCount, int splitAttributeIndex, int depth,
                                   double threshold, VfdtNode left, VfdtNode right) {
        super(instanceCount, splitAttributeIndex, depth);
        this.threshold = threshold;
        this.left = left;
        this.right = right;
    }

    public List<VfdtNode> getChildNodes() {
        return ArraysAsList.get(left, right);
    }

    @Override
    public boolean accept(NodeVisitor visitor) {
        if (visitor.visitSplitContinuous(this)) {
            int i = 0;
            for (VfdtNode child : new VfdtNode[] {left, right}) {
                visitor.visitSplitChild(this, i, child);
                if (!child.accept(visitor)) {
                    visitor.visitAfterSplitContinuous(this);
                    return false;
                }
                ++i;
            }
            visitor.visitAfterSplitContinuous(this);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void replaceChild(VfdtNode oldNode, VfdtNode newNode) {
        if (oldNode == left) {
            left = newNode;
        } else if (oldNode == right) {
            right = newNode;
        }
    }

    /** "learners/vfdt/vfdt-engine.c:_ProcessExampleBatch" */
    @Override
    public VfdtNode put(DataLine line) {
        double v = line.getDouble(splitAttributeIndex);
        if (v < threshold) {
            VfdtNode nextLeft = left.put(line);
            if (nextLeft != left) {
                left = nextLeft;
            }
        } else {
            VfdtNode nextRight = right.put(line);
            if (nextRight != right){
                right = nextRight;
            }
        }
        return this;
    }

    @Override
    public VfdtNode putArrayLine(ArrayDataLine line) {
        double v = line.getDouble(splitAttributeIndex);
        if (v < threshold) {
            VfdtNode nextLeft = left.putArrayLine(line);
            if (nextLeft != left) {
                left = nextLeft;
            }
        } else {
            VfdtNode nextRight = right.putArrayLine(line);
            if (nextRight != right){
                right = nextRight;
            }
        }
        return this;
    }

    @Override
    public VfdtNode putSparseLine(SparseDataLine line) {
        double v = line.getDouble(splitAttributeIndex);
        if (v < threshold) {
            VfdtNode nextLeft = left.putSparseLine(line);
            if (nextLeft != left) {
                left = nextLeft;
            }
        } else {
            VfdtNode nextRight = right.putSparseLine(line);
            if (nextRight != right){
                right = nextRight;
            }
        }
        return this;
    }

    /** "core/DecisionTree.c:DecisionTreeOneStepClassify" */
    @Override
    public VfdtNode stepClassify(DataLine line) {
        double v = line.getDouble(splitAttributeIndex);
        if (v < threshold) {
            return left;
        } else {
            return right;
        }
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public VfdtNode getLeft() {
        return left;
    }

    public void setLeft(VfdtNode left) {
        this.left = left;
    }

    public VfdtNode getRight() {
        return right;
    }

    public void setRight(VfdtNode right) {
        this.right = right;
    }
}
