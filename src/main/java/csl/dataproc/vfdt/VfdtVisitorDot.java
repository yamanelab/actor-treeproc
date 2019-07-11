package csl.dataproc.vfdt;

import csl.dataproc.dot.DotAttribute;
import csl.dataproc.dot.DotGraph;
import csl.dataproc.dot.DotNode;
import csl.dataproc.vfdt.count.AttributeClassCount;

import java.util.LinkedList;

public class VfdtVisitorDot extends VfdtNode.NodeVisitor {
    protected DotGraph graph;
    protected LinkedList<DotNode> stack = new LinkedList<>();
    protected int childIndex;

    public DotGraph run(VfdtNode n) {
        initGraph();
        n.accept(this);
        return graph;
    }

    protected void initGraph() {
        graph = new DotGraph();
        graph.getGraphAttrs().add(DotAttribute.graphRankDirectionLR());
    }


    @Override
    public void visitSplitChild(VfdtNodeSplit node, int index, VfdtNode child) {
        this.childIndex = index;
    }

    @Override
    public boolean visitGrowing(VfdtNodeGrowing node) {
        long count = node.getInstanceCount();
        long countCT = node.getInstanceCountClassTotal();
        long cls = node.getClassValue();
        double err = node.getErrorRate();
        VfdtNodeGrowing.AttributesGiniIndex g = node.giniIndices(!node.config.useGini);
        AttributeClassCount.GiniIndex i = g.first;
        double gi = 0;
        double th = 0;
        int attrIdx = 0;
        if (i != null) {
            gi = i.index;
            th = i.threshold;
            attrIdx = node.getAttributeCountIndex(i.sourceAttribute);
        }

        double hb = node.hoeffdingBound(!node.config.useGini);
        double giDiff = 0;
        if (g.second != null) {
            giDiff = gi - g.second.index;
        }

        add(String.format("grow[count=%d,countCT=%d,mostCommonCls=%d,err=%.3f,\n" +
                        "gini=%.3f,thrsh=%.3f,idxAttr=%d,giniDiff=%.3f,hoeffB=%.3f]", count, countCT, cls, err,
                gi, th, attrIdx, giDiff, hb), false, true);
        return true;
    }

    @Override
    public boolean visitLeaf(VfdtNodeLeaf node) {
        long count = node.getInstanceCount();
        long cls = node.getClassValue();
        double err = node.getErrorRate();

        add(String.format("leaf[count=%d,cls=%d,err=%.3f]", count, cls, err), false);
        return true;
    }

    protected void add(String label, boolean push) {
        add(label, push, false);
    }

    protected void add(String label, boolean push, boolean box) {
        DotNode dn = graph.add(label);
        if (!stack.isEmpty()) {
            graph.addEdge(stack.peek(), dn).getAttrs().add(DotAttribute.labelQuote("" + childIndex));
        }
        if (!box) {
            dn.getAttrs().add(DotAttribute.styleRounded());
        }
        dn.getAttrs().add(DotAttribute.shapeBox());
        if (push) {
            stack.push(dn);
        }
    }


    @Override
    public boolean visitSplitContinuous(VfdtNodeSplitContinuous node) {
        int splitAttr = node.getSplitAttributeIndex();
        long count = node.getSplitAttributeIndex();

        double th = node.getThreshold();

        add(String.format("splitReal[count=%d,splitAttr=%d,thrsh=%.3f]", count, splitAttr,
                th), true);
        return true;
    }

    @Override
    public boolean visitSplitDiscrete(VfdtNodeSplitDiscrete node) {
        int splitAttr = node.getSplitAttributeIndex();
        long count = node.getSplitAttributeIndex();

        int cs = node.getChildren().size();

        add(String.format("splitInt[count=%d,splitAttr=%d,children=%d]", count, splitAttr,
                cs), true);
        return true;
    }

    @Override
    public void visitAfterSplit(VfdtNodeSplit node) {
        stack.pop();
    }
}
