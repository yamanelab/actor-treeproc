package csl.dataproc.vfdt;

import csl.dataproc.vfdt.count.AttributeDiscreteClassCount;
import csl.dataproc.vfdt.count.LongCount;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class VfdtNodeSplitDiscrete extends VfdtNodeSplit implements Serializable {
    protected List<NodeHolder> children;
    protected AttrAndTotalToNode generator;

    static final long serialVersionUID = 1L;

    public VfdtNodeSplitDiscrete() {
        super(0, 0, 0);
    }

    public VfdtNodeSplitDiscrete(long instanceCount, int splitAttributeIndex, int depth, AttrAndTotalToNode generator,
                                 List<NodeHolder> children) {
        super(instanceCount, splitAttributeIndex, depth);
        this.children = children;
        this.generator = generator;
    }

    public List<NodeHolder> getChildren() {
        return children;
    }

    public List<VfdtNode> getChildNodes() {
        return children.stream()
                .map(e -> e.node)
                .collect(Collectors.toList());

    }

    public static class NodeHolder implements Comparable<NodeHolder> {
        public long attrValue;
        public VfdtNode node;

        public NodeHolder(long attrValue, VfdtNode node) {
            this.attrValue = attrValue;
            this.node = node;
        }

        @Override
        public int compareTo(NodeHolder o) {
            return Long.compare(attrValue, o.attrValue);
        }
    }

    public static List<NodeHolder> create(AttributeDiscreteClassCount attrCount, AttrAndTotalToNode toNode) {
        ArrayList<NodeHolder> children = new ArrayList<>();
        for (LongCount.LongKeyValueIterator iter = attrCount.attributeToClassesTotal(); iter.hasNext(); ) {
            long attr = iter.getKey();
            long total = iter.nextValue();

            VfdtNode n = toNode.create(attr, total);
            if (n != null){
                children.add(new NodeHolder(attr, n));
            }
        }
        Collections.sort(children);
        children.trimToSize();
        return children;
    }

    public interface AttrAndTotalToNode {
        VfdtNode create(long attr, long total);
    }

    @Override
    public boolean accept(NodeVisitor visitor) {
        if (visitor.visitSplitDiscrete(this)) {
            int i = 0;
            for (NodeHolder child : children) {
                visitor.visitSplitChild(this, i, child.node);
                if (!child.node.accept(visitor)) {
                    visitor.visitAfterSplitDiscrete(this);
                    return false;
                }
                ++i;
            }
            visitor.visitAfterSplitDiscrete(this);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public VfdtNode put(DataLine line) {
        long attrVal = line.getLong(splitAttributeIndex);
        NodeHolder child = getChild(attrVal);
        if (child != null) {
            child.node = child.node.put(line);
        }
        return this;
    }

    @Override
    public VfdtNode putArrayLine(ArrayDataLine line) {
        long attrVal = line.getLong(splitAttributeIndex);
        NodeHolder child = getChild(attrVal);
        if (child != null) {
            child.node = child.node.putArrayLine(line);
        }
        return this;
    }

    @Override
    public VfdtNode putSparseLine(SparseDataLine line) {
        long attrVal = line.getLong(splitAttributeIndex);
        NodeHolder child = getChild(attrVal);
        if (child != null) {
            child.node = child.node.putSparseLine(line);
        }
        return this;
    }

    public NodeHolder getChild(long attrVal) {
        if (children.size() < 100) {
            int insertionPoint = 0;
            for (NodeHolder child : children) {
                if (child.attrValue == attrVal) {
                    return child;
                } else if (child.attrValue > attrVal) {
                    break;
                }
                ++insertionPoint;
            }
            return newNode(insertionPoint, attrVal);
        } else {
            int i = Collections.binarySearch(children, new NodeHolder(attrVal, null));
            if (i < 0) {
                int insertionPoint = -i - 1;
                return newNode(insertionPoint, attrVal);
            } else {
                return children.get(i);
            }
        }
    }
    private NodeHolder newNode(int insertionPoint, long attrVal) {
        NodeHolder child = new NodeHolder(attrVal, generator.create(attrVal, 0));
        children.add(insertionPoint, child);
        return child;
    }


    /** "core/DecisionTree.c:DecisionTreeOneStepClassify" */
    @Override
    public VfdtNode stepClassify(DataLine line) {
        long attrVal = line.getLong(splitAttributeIndex);
        NodeHolder child = getChild(attrVal);
        if (child != null) {
            return child.node;
        }
        return null;
    }

    @Override
    public void replaceChild(VfdtNode oldNode, VfdtNode newNode) {
        for (NodeHolder child : children) {
            if (child.node == oldNode) {
                child.node = newNode;
                return;
            }
        }
    }

    public void setChildren(List<NodeHolder> children) {
        this.children = children;
    }

    public AttrAndTotalToNode getGenerator() {
        return generator;
    }

    public void setGenerator(AttrAndTotalToNode generator) {
        this.generator = generator;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }
}
