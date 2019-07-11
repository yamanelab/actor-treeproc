package csl.dataproc.vfdt.count;

import csl.dataproc.vfdt.VfdtConfig;

import java.io.Serializable;

public class AttributeDiscreteClassCountHash implements AttributeDiscreteClassCount, Serializable {
    /**
     * <pre>
     * layout:
     *    {attr1, attr2, ... attrN},
     *    {cls1, cls2,  ...  clsM},
     *    {attr1cls1Count, attr1cls2Count, ... attr1clsMCount,
     *     attr2cls1Count, attr2cls2Count, ... attr2clsMCount,
     *     ...
     *     attrNcls1Count, attrNcls2Count, ... attrNclsMCount}
     *  </pre>
     * */
    protected long[] data;
    protected AttributeDiscreteClassCountHash left;
    protected AttributeDiscreteClassCountHash right;

    protected VfdtConfig config;

    static final long serialVersionUID = 1L;

    public AttributeDiscreteClassCountHash() {}

    public AttributeDiscreteClassCountHash(VfdtConfig config) {
        this.config = config;
        data = new long[config.discreteClassCountHashAttrs + config.discreteClassCountHashClasses +
                        config.discreteClassCountHashAttrs * config.discreteClassCountHashClasses];
    }

    public int attrIndex(int attrHash) {
        return attrHash;
    }
    public int clsIndex(int clsHash) {
        return config.discreteClassCountHashAttrs + clsHash;
    }

    public int countIndex(int attrHash, int clsHash) {
        return config.discreteClassCountHashAttrs + config.discreteClassCountHashClasses +
               attrHash * config.discreteClassCountHashClasses + clsHash;
    }

    public int attrHashCode(long attr) {
        return Long.hashCode(attr) % config.discreteClassCountHashAttrs;
    }
    public int clsHashCode(long cls) {
        return Long.hashCode(cls) % config.discreteClassCountHashClasses;
    }

    public int attrSize() {
        return config.discreteClassCountHashAttrs;
    }
    public int clsSize() {
        return config.discreteClassCountHashClasses;
    }

    public long increment(long attr, long cls) {
        int attrHash = attrHashCode(attr);
        int clsHash = clsHashCode(cls);
        int attrIndex = attrIndex(attrHash);
        int clsIndex = clsIndex(clsHash);
        int countIndex = countIndex(attrHash, clsHash);

        AttributeDiscreteClassCountHash c = this;

        while (true) {
            long exAttr = c.data[attrIndex];
            long exCls = c.data[clsIndex];
            if (exAttr == 0 && c.data[countIndex] == 0) { //free
                if (exCls == 0) {
                    c.data[attrIndex] = attr;
                    c.data[clsIndex] = cls;
                    c.data[countIndex]++;
                    return c.data[countIndex];
                } else if (exCls == cls) {
                    c.data[attrIndex] = attr;
                    c.data[countIndex]++;
                    return c.data[countIndex];
                }
            } else if (exAttr == attr) {
                if (exCls == 0 && c.data[countIndex] == 0) {
                    c.data[clsIndex] = cls;
                    c.data[countIndex]++;
                    return c.data[countIndex];
                } else if (exCls == cls) {
                    c.data[countIndex]++;
                    return c.data[countIndex];
                }
            }

            if (attr < exAttr) {
                c = c.getLeft();
            } else if (attr > exAttr) {
                c =  c.getRight();
            } else {
                if (cls < exCls) {
                    c = c.getLeft();
                } else {
                    c = c.getRight();
                }
            }
        }
    }
    public AttributeDiscreteClassCountHash getLeft() {
        if (left == null) {
            left = newSubNode();
        }
        return left;
    }
    public AttributeDiscreteClassCountHash getRight() {
        if (right == null) {
            right = newSubNode();
        }
        return right;
    }

    public AttributeDiscreteClassCountHash getLeftWithoutCreate() {
        return left;
    }
    public AttributeDiscreteClassCountHash getRightWithoutCreate() {
        return right;
    }

    public long getArray(int i) {
        return data[i];
    }

    protected AttributeDiscreteClassCountHash newSubNode() {
        return new AttributeDiscreteClassCountHash(config);
    }

    public long get(long attr, long cls) {
        int attrHash = attrHashCode(attr);
        int clsHash = clsHashCode(cls);
        int attrIndex = attrIndex(attrHash);
        int clsIndex = clsIndex(clsHash);
        int countIndex = countIndex(attrHash, clsHash);

        AttributeDiscreteClassCountHash c = this;

        while (c != null) {
            long exAttr = c.data[attrIndex];
            long exCls = c.data[clsIndex];

            if (exAttr == 0 || exCls == 0) { //for (x,0) or (0,x), or free
                return c.data[countIndex];
            }

            if (attr < exAttr) {
                c = c.left;
            } else if (attr > exAttr) {
                c = c.right;
            } else {
                if (cls < exCls) {
                    c = c.left;
                } else if (cls > exCls) {
                    c = c.right;
                } else {
                    return c.data[countIndex];
                }
            }
        }
        return 0;
    }

    /** () -&gt; (key:attrValue -&gt; value:instanceCount) */
    public LongCount.LongKeyValueIterator attributeToClassesTotal() {
        return getAttributeToClassesTotal().iterator();
    }

    /** (attrValue) -&gt; (key:clsValue -&gt; value:instanceCount) */
    public LongCount.LongKeyValueIterator classToCount(long attr) {
        return getClassToCount(attr).iterator();
    }

    public LongCount getAttributeToClassesTotal() {
        LongCount count = new LongCount(config.discreteAttributeTable);
        collectAttributeToClassesTotal(count);
        return count;
    }

    private void collectAttributeToClassesTotal(LongCount count) {
        for (int attrHash = 0; attrHash < config.discreteClassCountHashAttrs; ++attrHash) {
            for (int clsHash = 0; clsHash < config.discreteClassCountHashClasses; ++clsHash) {
                long attr = data[attrIndex(attrHash)];
                long clsCount = data[countIndex(attrHash, clsHash)];
                if (clsCount != 0) {
                    count.add(attr, clsCount);
                }
            }
        }
        if (left != null) {
            left.collectAttributeToClassesTotal(count);
        }
        if (right != null) {
            right.collectAttributeToClassesTotal(count);
        }
    }

    public LongCount getClassToCount(long attr) {
        LongCount classes = new LongCount(config.maxClassCountTable);
        collectClassToCount(classes, attr, attrHashCode(attr));
        return classes;
    }
    private void collectClassToCount(LongCount classes, long attr, int attrHash) {
        int attrIdx = attrIndex(attrHash);
        for (int clsHash = 0; clsHash < config.discreteClassCountHashClasses; ++clsHash) {
            if (data[attrIdx] == attr) {
                long cls = data[clsIndex(clsHash)];
                long count = data[countIndex(attrHash, clsHash)];
                if (count > 0) {
                    classes.add(cls, count);
                }
            }
        }

        if (left != null) {
            left.collectClassToCount(classes, attr, attrHash);
        }
        if (right != null) {
            right.collectClassToCount(classes, attr, attrHash);
        }
    }

    ///////

    public long[] getData() {
        return data;
    }

    public void setData(long[] data) {
        this.data = data;
    }

    public void setLeft(AttributeDiscreteClassCountHash left) {
        this.left = left;
    }

    public void setRight(AttributeDiscreteClassCountHash right) {
        this.right = right;
    }

    public VfdtConfig getConfig() {
        return config;
    }

    public void setConfig(VfdtConfig config) {
        this.config = config;
    }
}
