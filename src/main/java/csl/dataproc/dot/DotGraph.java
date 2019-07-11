package csl.dataproc.dot;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * a GraphViz dot file writer.
 * 
 * <pre>
 * DotGraph g = new DotGraph(); 
 * DotNode n1 = g.add("node1"); 
 * DotNode n2 = g.add("node2"); 
 * g.addEdge(n1, n2); 
 * g.save(file);
 * </pre>
 * @author KoheiSakurai
 * 
 */
public class DotGraph {
    private List<DotNode> nodes = new ArrayList<DotNode>();
    private List<DotEdge> edges = new ArrayList<DotEdge>();
    private DotAttributeList graphAttrs = new DotAttributeList();
    private long nodeId;
    
    public DotAttributeList getGraphAttrs() {
        return graphAttrs;
    }

    public List<DotNode> getNodes() {
        return nodes;
    }

    public List<DotEdge> getEdges() {
        return edges;
    }

    public DotNode add(String data) {
        return add(new DotNode(data));
    }

    public DotNode add(DotNode node) {
        if (node.getName() == null) {
            node.setName("n" + nextNodeId());
        }
        nodes.add(node);
        return node;
    }

    public long nextNodeId() {
        long i = nodeId;
        ++nodeId;
        return i;
    }

    public DotEdge add(DotEdge edge) {
        edges.add(edge);
        return edge;
    }

    public DotEdge addEdge(DotNode from, DotNode to) {
        return add(new DotEdge(from, to));
    }

    private PrintWriter out;

    public void setOut(File file) {
        try {
            setOut(new FileOutputStream(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void setOut(OutputStream out) {
        this.out = new PrintWriter(out);
    }

    public void close() {
        out.close();
        out = null;
    }
    
    public void writeGraphAttribute(DotAttribute attr) {
        out.print("  ");
        out.print(attr.toString());
        out.println(";");
    }

    public void writeNode(DotNode n) {
        out.print("  ");
        out.print(n.getName());
        out.print(" ");
        out.print(n.getAttrs());
        out.println(";");
    }

    public void writeEdge(DotEdge e) {
        out.print("  ");
        out.print(e.getFrom().getName());
        out.print(" -> ");
        out.print(e.getTo().getName());
        out.print(" ");
        out.print(e.getAttrs());
        out.println(";");
    }

    public void writeGraphBegin() {
        out.print("digraph ");
        out.print("g");
        out.println("{");
    }

    public void writeGraphEnd() {
        out.println("}");
    }

    public void write() {
        writeGraphBegin();
        for (DotAttribute gAttr : graphAttrs.getList()) {
            writeGraphAttribute(gAttr);
        }
        for (DotNode node : nodes) {
            writeNode(node);
        }
        for (DotEdge edge : edges) {
            writeEdge(edge);
        }
        writeGraphEnd();
        close();
    }

    public void save(File file) {
        setOut(file);
        write();
    }

    public void save(OutputStream out) {
        setOut(out);
        write();
    }
}
