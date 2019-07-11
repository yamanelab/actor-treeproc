package csl.dataproc.vfdt;

import java.util.List;

public abstract class VfdtNodeSplit implements VfdtNode {
    protected long instanceCount;
    protected int splitAttributeIndex;
    protected int depth;


    public VfdtNodeSplit(long instanceCount, int splitAttributeIndex, int depth) {
        this.instanceCount = instanceCount;
        this.splitAttributeIndex = splitAttributeIndex;
        this.depth = depth;
    }

    @Override
    public long getInstanceCount() {
        return instanceCount;
    }

    public int getSplitAttributeIndex() {
        return splitAttributeIndex;
    }

    public int getDepth() {
        return depth;
    }

    public abstract void replaceChild(VfdtNode oldNode, VfdtNode newNode);

    public abstract List<VfdtNode> getChildNodes();

    public void setInstanceCount(long instanceCount) {
        this.instanceCount = instanceCount;
    }

    public void setSplitAttributeIndex(int splitAttributeIndex) {
        this.splitAttributeIndex = splitAttributeIndex;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }
}
