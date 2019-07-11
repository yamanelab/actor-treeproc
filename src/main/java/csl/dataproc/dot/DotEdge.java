package csl.dataproc.dot;

public class DotEdge {
    private DotNode from;
    private DotNode to;
    private DotAttributeList attrs = new DotAttributeList();

    public DotEdge(DotNode from, DotNode to) {
        this.from = from;
        this.to = to;
    }

    public DotNode getFrom() {
        return from;
    }

    public DotNode getTo() {
        return to;
    }

    public DotAttributeList getAttrs() {
        return attrs;
    }
}