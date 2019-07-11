package csl.dataproc.vfdt;

import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.vfdt.count.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class VfdtNodeGrowing implements VfdtNode, Serializable {
    protected List<ArffAttributeType> types;
    protected long instanceCount;
    protected long instanceCountClassTotal;
    protected LongCount classTotals;
    protected AttributeClassCount[] attributeCounts;
    protected int depth;
    protected int classes;

    protected VfdtConfig config;
    protected VfdtNodeGrowingSplitter splitter;

    static final long serialVersionUID = 1L;

    public static boolean DEBUG = false;

    public VfdtNodeGrowing() {}

    /** "learners/vfdt/vfdt-engine.c:VFDTNew"
     *
     *   type: types of attributes and a class
     * */
    public VfdtNodeGrowing(VfdtConfig config, List<ArffAttributeType> types) {
        this(config, types, 0, -1, rootPathActiveAttrs(types), 0, 0);

    }
    private static BitSet rootPathActiveAttrs(List<ArffAttributeType> types) {
        BitSet pathActiveAttrs = new BitSet(types.size() - 1);
        pathActiveAttrs.set(0, types.size() - 1);
        return pathActiveAttrs;
    }

    /** types: types of attributes and a class */
    public VfdtNodeGrowing(VfdtConfig config, List<ArffAttributeType> types,
                           long instanceCountClassTotal, long parentClass, BitSet pathActiveAttributes, double parentErrorRate,
                           int depth) {
        this.config = config;
        this.types = types;
        this.instanceCountClassTotal = instanceCountClassTotal;
        this.instanceCount = 0;
        this.depth = depth;
        int attrs = types.size() - 1;
        ArffAttributeType classType = types.get(attrs);

        this.classes = classes(classType);
        int classTableSize = classTableSize(classType);

        attributeCounts = new AttributeClassCount[attrs];
        for (int i = 0; i < attrs; ++i) {
            ArffAttributeType attrType = types.get(i);

            if (pathActiveAttributes.get(i)) {
                setAttributeCount(i, newCount(attrType, classType, classTableSize));
            } else {
                setAttributeCount(i, newIgnore());
            }
        }

        classTotals = new LongCount(classTableSize);

        splitter = newSplitter(parentClass, pathActiveAttributes, parentErrorRate);
    }

    protected VfdtNodeGrowingSplitter newSplitter(long parentClass, BitSet pathActiveAttributes, double parentErrorRate) {
        return new VfdtNodeGrowingSplitter(config, parentClass, pathActiveAttributes, parentErrorRate);
    }

    public VfdtNodeGrowingSplitter getSplitter() {
        return splitter;
    }

    public LongCount getClassTotals() {
        return classTotals;
    }

    public AttributeClassCount[] getAttributeCounts() {
        return attributeCounts;
    }

    public void setAttributeCount(int i, AttributeClassCount count) {
        attributeCounts[i] = count;
    }

    public AttributeClassCount getAttributeCount(int i) {
        return attributeCounts[i];
    }

    public List<ArffAttributeType> getTypes() {
        return types;
    }

    public long getInstanceCount() {
        return instanceCount;
    }

    public long getInstanceCountClassTotal() {
        return instanceCountClassTotal;
    }

    public int getDepth() {
        return depth;
    }

    /** [attr1,attr2,...attrN,cls] =&gt; [attrCount1,attrCount2,...attrCountN] */
    public int getClassIndexInAttributeCounts() {
        return attributeCounts.length;
    }

    public int getAttributeCountIndex(AttributeClassCount count) {
        int i = 0;
        for (AttributeClassCount c : attributeCounts) {
            if (c == count) {
                return i;
            } else {
                ++i;
            }
        }
        return -1;
    }

    private int classTableSize(ArffAttributeType classType) {
        if (classType instanceof ArffAttributeType.EnumAttributeType) {
            return  Math.min(config.maxClassCountTable,
                    Math.max(config.minClassCountTable,
                            ((ArffAttributeType.EnumAttributeType) classType).getValues().size()));
        } else {
            return config.minClassCountTable;
        }
    }

    private int classes(ArffAttributeType classType) {
        if (classType instanceof ArffAttributeType.EnumAttributeType) {
            return ((ArffAttributeType.EnumAttributeType) classType).getValues().size();
        } else {
            return 1;
        }
    }

    public AttributeClassCount newCount(ArffAttributeType attrType, ArffAttributeType clsType, int clsTableSize) {
        if (attrType instanceof ArffAttributeType.SimpleAttributeType) {
            switch ((ArffAttributeType.SimpleAttributeType) attrType) {
                case Real:
                case Numeric:
                    return new AttributeContinuousClassCount(config, clsTableSize);

                case Integer:
                    return new AttributeDiscreteClassCountHash(config);

                case String:
                    return new AttributeStringClassCount(clsTableSize);
                default:
                    return newIgnore();
            }
        } else if (attrType instanceof ArffAttributeType.EnumAttributeType) {
            if (clsType instanceof ArffAttributeType.EnumAttributeType) {
                return AttributeDiscreteClassCount.getFixed(config,
                        ((ArffAttributeType.EnumAttributeType) attrType).getValues().size(),
                        ((ArffAttributeType.EnumAttributeType) clsType).getValues().size());
            } else {
                return new AttributeDiscreteClassCountHash(config);
            }
        } else {
            return newIgnore();
        }
    }

    public AttributeClassCount.AttributeIgnoreClassCount newIgnore() {
        return new AttributeClassCount.AttributeIgnoreClassCount(config);
    }


    public VfdtNode put(DataLine line) {
        if (line instanceof ArrayDataLine) {
            return putArrayLine((ArrayDataLine) line);
        } else if (line instanceof SparseDataLine) {
            return putSparseLine((SparseDataLine) line);
        }
        return this;
    }

    /** "learners/vfdt/vfdt-engine.c:_AddExampleToGrowingNode" */
    public VfdtNode putArrayLine(ArrayDataLine line) {
        putArrayLineWithoutSplit(line);
        return splitter.split(this);
    }

    public VfdtNode putSparseLine(SparseDataLine line) {
        putSparseLineWithoutSplit(line);
        return splitter.split(this);
    }

    /** "learners/vfdt/vfdt-engine.c:_AddExampleToGrowingNode",
     *  "core/ExampleGroupStats.c:ExampleGroupStatsAddExample" */
    public void putArrayLineWithoutSplit(ArrayDataLine line) {
        long cls = incrementAndGetClass(line);
        Object[] data = line.data;

        for (int i = 0, l = data.length - 1; i < l; ++i) {
            Object o = data[i];
            AttributeClassCount a = attributeCounts[i];
            switch (a.getKind()) {
                case ArffAttributeType.ATTRIBUTE_KIND_INTEGER:
                    ((AttributeDiscreteClassCount) a).increment((long) o, cls);
                    break;
                case ArffAttributeType.ATTRIBUTE_KIND_REAL:
                    ((AttributeContinuousClassCount) a).increment((double) o, cls);
                    break;
                case ArffAttributeType.ATTRIBUTE_KIND_STRING:
                    ((AttributeStringClassCount) a).increment((String) o, cls);
                    break;
            }
        }
    }

    private long incrementAndGetClass(DataLine line) {
        instanceCount++;
        instanceCountClassTotal++;
        int clsIdx = getClassIndexInAttributeCounts();
        long cls = line.getLong(clsIdx);
        classTotals.increment(cls);
        return cls;
    }

    public void putSparseLineWithoutSplit(SparseDataLine line) {
        SparseDataLine.SparseDataColumn[] cols = line.data;
        long cls = incrementAndGetClass(line);
        int clsIdx = getClassIndexInAttributeCounts();

        for (SparseDataLine.SparseDataColumn col : cols) {
            if (col.index == clsIdx) {
                continue;
            }

            AttributeClassCount a = attributeCounts[col.index];
            switch (a.getKind()) {
                case ArffAttributeType.ATTRIBUTE_KIND_INTEGER:
                    ((AttributeDiscreteClassCount) a).increment(((SparseDataLine.SparseDataColumnInteger) col).value, cls);
                    break;
                case ArffAttributeType.ATTRIBUTE_KIND_REAL:
                    ((AttributeContinuousClassCount) a).increment(((SparseDataLine.SparseDataColumnReal) col).value, cls);
                    break;
                case ArffAttributeType.ATTRIBUTE_KIND_STRING:
                    ((AttributeStringClassCount) a).increment(((SparseDataLine.SparseDataColumnString) col).value, cls);
                    break;
            }
        }
    }

    public static class AttributesGiniIndex implements Serializable {
        public List<AttributeClassCount.GiniIndex> attributes;
        public AttributeClassCount.GiniIndex first;
        public AttributeClassCount.GiniIndex second;

        static final long serialVersionUID = 1L;

        public AttributeClassCount.GiniIndex get(int i) {
            if (i == 0) {
                return first;
            } else if (i == 1) {
                return second;
            } else {
                throw new IndexOutOfBoundsException("" + i + " >= 2");
            }
        }
        public int size() {
            return 2;
        }
        public void set(int i, AttributeClassCount.GiniIndex g) {
            if (i == 0) {
                first = g;
            } else if (i == 1) {
                second = g;
            } else {
                throw new IndexOutOfBoundsException("" + i + " >= 2");
            }
        }

        public void insert(int i, AttributeClassCount.GiniIndex newIndex) {
            for (int n = size() - 1; n > i; --n) {
                set(n, get(n - 1));
            }
            set(i, newIndex);
        }
    }

    /** "learners/vfdt/vfdt-engine.c:_CheckSplit" */
    public AttributesGiniIndex giniIndices(boolean entropy) {
        AttributesGiniIndex result = new AttributesGiniIndex();
        result.attributes = new ArrayList<>(attributeCounts.length);
        result.first = new AttributeClassCount.GiniIndex(config.initGiniIndex, 0, null);
        result.second = new AttributeClassCount.GiniIndex(config.initGiniIndex, 0, null);

        if (DEBUG) {
            System.err.println("indices: " + instanceCount);
        }
        for (AttributeClassCount a : attributeCounts) {
            if (DEBUG) {
                System.err.println("  attr[" + getAttributeCountIndex(a) + "] ");
            }
            AttributeClassCount.GiniIndex[] indices = a.giniIndex(instanceCount, classes, entropy);
            result.attributes.add(indices[0]);
            updateIndices(result, indices);
        }
        return result;
    }

    /** "learners/vfdt/vfdt-engine.c:_CheckSplit" */
    protected void updateIndices(AttributesGiniIndex selected, AttributeClassCount.GiniIndex[] indices) {
        int prevIns = -1;
        for (AttributeClassCount.GiniIndex next : indices) {
            boolean inserted = false;
            for (int j = prevIns + 1; j < selected.size(); ++j) {
                if (next.index < selected.get(j).index) {
                    selected.insert(j, next);
                    inserted = true;
                    prevIns = j;
                    break;
                }
            }

            if (!inserted) { //no more smaller indices in selected
                break;
            }
        }
    }

    public double hoeffdingBound(boolean entropy) {
        double range;
        if (!entropy) {
            range = config.giniRange;
        } else {
            range = lg(classes);
        }
        return hoeffdingBound(range, splitter.getSplitConfidence());
    }

    public double hoeffdingBound(double range, double splitConfidence) {
        return Math.sqrt(
                (range * range) * Math.log(1.0 / splitConfidence) /
                (2.0 * instanceCount));
    }

    public double totalGini(boolean entropy) {
        if (entropy) {
            return totalEntropy();
        } else {
            return totalGini();
        }
    }

    public double totalGini() {
        if (instanceCount == 0) {
            return 1;
        } else {
            double out = 1;
            for (LongCount.LongKeyValueIterator iter = classTotals.iterator(); iter.hasNext(); ) {
                double clsCount = iter.nextValue() / (double) instanceCount;
                out -= clsCount * clsCount;
            }
            return out;
        }
    }

    public double totalEntropy() {
        if (instanceCount == 0) {
            return lg(classes);
        } else {
            double out = 0;
            for (LongCount.LongKeyValueIterator iter = classTotals.iterator(); iter.hasNext(); ) {
                double clsCount = iter.nextValue() / (double) instanceCount;
                if (clsCount > 0) {
                    out += -(clsCount * lg(clsCount));
                }
            }
            return out;
        }
    }

    public static double lg(double n) {
        return Math.log(n) / Math.log(2.0);
    }

    public boolean isPure() {
        int classes = 0;
        for (LongCount.LongKeyValueIterator iter = classTotals.iterator(); iter.hasNext(); ) {
            if (iter.nextValue() > 0) {
                classes++;
                if (classes > 1) {
                    return false;
                }
            }
        }
        return classes <= 1;
    }

    public long getClassValue() {
        return splitter.getClassValueAsLeaf(this);
    }

    public long getMostCommonClass() {
        return getMostCommonClassLaplace(0, 0);
    }

    public long getMostCommonClassLaplace(long addedCls, long addedClsCount) {
        long max = 0;
        long maxCls = -1;
        for (LongCount.LongKeyValueIterator iter = classTotals.iterator(); iter.hasNext(); ) {
            long cls = iter.getKey();
            long count = iter.nextValue();

            if (addedCls == cls) {
                count += addedClsCount;
            }

            if (count > max) {
                max = count;
                maxCls = cls;
            }
        }
        return maxCls;
    }

    public double getErrorRate() {
        if (instanceCount == 0) {
            return 0;
        } else {
            return 1.0 - ((double) getMostCommonClass()) / instanceCount;
        }
    }

    public double getLeafifyIndex(long totalInstanceCount) {
        return splitter.getLeafifyIndex(this, totalInstanceCount);
    }

    public VfdtNode makeLeaf() {
        return splitter.makeLeaf(this);
    }

    @Override
    public boolean accept(NodeVisitor visitor) {
        return visitor.visitGrowing(this);
    }


    ///////


    public void setTypes(List<ArffAttributeType> types) {
        this.types = types;
    }

    public void setInstanceCount(long instanceCount) {
        this.instanceCount = instanceCount;
    }

    public void setInstanceCountClassTotal(long instanceCountClassTotal) {
        this.instanceCountClassTotal = instanceCountClassTotal;
    }

    public void setClassTotals(LongCount classTotals) {
        this.classTotals = classTotals;
    }

    public void setAttributeCounts(AttributeClassCount[] attributeCounts) {
        this.attributeCounts = attributeCounts;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public VfdtConfig getConfig() {
        return config;
    }

    public void setConfig(VfdtConfig config) {
        this.config = config;
    }

    public void setSplitter(VfdtNodeGrowingSplitter splitter) {
        this.splitter = splitter;
    }

    public void setClasses(int classes) {
        this.classes = classes;
    }

    public int getClasses() {
        return classes;
    }
}
