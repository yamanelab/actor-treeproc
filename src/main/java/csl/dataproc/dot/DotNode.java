package csl.dataproc.dot;

public class DotNode {
    private String name;
    private DotAttributeList attrs = new DotAttributeList();

    public DotNode(String data) {
        if (data != null) {
            attrs.addQuote("label", data);
        }
    }

    public String getData() {
        return attrs.get("label").value;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public DotAttributeList getAttrs() {
        return attrs;
    }
}