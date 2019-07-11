package csl.dataproc.vfdt.count;

import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.vfdt.VfdtNodeGrowing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AttributeStringClassCount implements AttributeClassCount, Serializable {
    protected int classTableSize;
    protected Map<String,LongCount> count;

    static final long serialVersionUID = 1L;

    public AttributeStringClassCount() {}

    public AttributeStringClassCount(int classTableSize) {
        this.classTableSize = classTableSize;
        this.count = new HashMap<>();
    }

    public long increment(String attr, long cls) {
        LongCount s = count.get(attr);
        if (s == null) {
            s = new LongCount(classTableSize);
            count.put(attr, s);
        }
        return s.increment(cls);
    }

    public long get(String value, long cls) {
        LongCount s = count.get(value);
        if (s == null) {
            return 0;
        } else {
            return s.get(cls);
        }
    }

    public int getClassTableSize() {
        return classTableSize;
    }

    public Map<String, LongCount> getCount() {
        return count;
    }

    public char getKind() {
        return ArffAttributeType.ATTRIBUTE_KIND_STRING;
    }

    @Override
    public GiniIndex[] giniIndex(long nodeInstanceCount, int classes, boolean entropy) {
        return new GiniIndex[] {
                new GiniIndex(computeGiniIndex(nodeInstanceCount, classes, entropy), 0, this)};
    }

    public double computeGiniIndex(long nodeInstanceCount, int classes, boolean entropy) {
        double out = 0;
        boolean hasAttr = false;
        for (LongCount e : count.values()) {
            hasAttr = true;
            double attrClsTotal = (double) e.total();

            double gini = entropy ? 0 : 1;

            for (LongCount.LongKeyValueIterator clsIter = e.iterator(); clsIter.hasNext(); ) {
                double attrClsCount = clsIter.nextValue();

                double v = attrClsCount / attrClsTotal;
                if (entropy) {
                    if (v > 0) {
                        gini -= v * VfdtNodeGrowing.lg(v);
                    }
                } else {
                    gini -= v * v;
                }
            }
            out += (attrClsTotal / (double) nodeInstanceCount) * gini;
        }
        if (!hasAttr) {
            return entropy ? VfdtNodeGrowing.lg(classes) : 1.0;
        } else {
            return out;
        }
    }

    public void setClassTableSize(int classTableSize) {
        this.classTableSize = classTableSize;
    }

    public void setCount(Map<String, LongCount> count) {
        this.count = count;
    }
}
