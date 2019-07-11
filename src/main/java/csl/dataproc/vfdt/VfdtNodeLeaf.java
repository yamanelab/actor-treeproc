package csl.dataproc.vfdt;

import java.io.Serializable;

public class VfdtNodeLeaf implements VfdtNode, Serializable {
    protected long classValue;
    protected int classIndexInAttributes;
    protected long instanceCount;
    protected long errorCount;
    protected int depth;

    static final long serialVersionUID = 1L;

    public VfdtNodeLeaf() {}

    public VfdtNodeLeaf(long classValue, int classIndexInAttributes, int depth) {
        this.classValue = classValue;
        this.classIndexInAttributes = classIndexInAttributes;
        this.depth = depth;
    }

    /** "learners/vfdt/vfdt-engine.c:_AddExampleToDeactivatedNode" */
    public VfdtNode put(VfdtNode.DataLine line) {
        instanceCount++;
        long cls = line.getLong(classIndexInAttributes);
        if (cls != classValue) {
            errorCount++;
        }
        return this;
    }

    @Override
    public VfdtNode putArrayLine(ArrayDataLine line) {
        return put(line);
    }

    @Override
    public VfdtNode putSparseLine(SparseDataLine line) {
        return put(line);
    }


    @Override
    public boolean accept(NodeVisitor visitor) {
        return visitor.visitLeaf(this);
    }

    @Override
    public long getInstanceCount() {
        return instanceCount;
    }


    public long getClassValue() {
        return classValue;
    }

    public double getErrorRate() {
        if (instanceCount == 0) {
            return 0;
        } else {
            return ((double) errorCount) / instanceCount;
        }
    }

    public int getDepth() {
        return depth;
    }

    public void setClassValue(long classValue) {
        this.classValue = classValue;
    }

    public int getClassIndexInAttributes() {
        return classIndexInAttributes;
    }

    public void setClassIndexInAttributes(int classIndexInAttributes) {
        this.classIndexInAttributes = classIndexInAttributes;
    }

    public void setInstanceCount(long instanceCount) {
        this.instanceCount = instanceCount;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }
}
