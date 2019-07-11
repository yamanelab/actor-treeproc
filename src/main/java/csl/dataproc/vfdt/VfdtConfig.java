package csl.dataproc.vfdt;

import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.vfdt.count.AttributeDiscreteClassCount;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/** "learners/vfdt/vfdt.c:_printUsage"
 *
 *  this class is also a factory for VfdtNodes.
 * */
public class VfdtConfig implements Serializable {
    public double splitConfidence = 0.0000001;
    public double tieConfidence = 0.05;
    public double growingNodeProcessChunkSize = 300;
    public long laplace = 5;
    public double prePruneTau = 0;

    public double initGiniIndex = 10000;
    public double giniRange = 1;
    public long forceSplitMinInstanceCount = 5;

    public boolean useBonferroni = true;
    public boolean useGini = false;
    
    public int continuousClassCountSplitsInitialCapacity = 10;
    public int continuousClassCountMaxSplits = 1000;

    public int discreteClassCountArrayMax = 16 * 2 + 16 * 16;

    public int discreteClassCountHashAttrs = 16;
    public int discreteClassCountHashClasses = 16;

    public int minClassCountTable = 64;
    public int maxClassCountTable = 256;

    public int discreteAttributeTable = 128;

    static final long serialVersionUID = 1L;

    public Class<?> getType(String name) {
        try {
            return getClass().getField(name).getType();
        } catch (Exception ex) {
            return null;
        }
    }

    public void setValue(String name, Object value) {
        try {
            getClass().getField(name).set(this, value);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<String> parseArgs(List<String> args) {
        List<String> rest = new ArrayList<>(args.size());
        for (int i = 0, l = args.size(); i < l; ++i) {
            String arg = args.get(i);
            if (arg.startsWith("--")) { //--<configField> <val>
                String name = arg.substring(2);
                Class<?> valueType = getType(name);
                if (valueType == null) {
                    rest.add(arg);
                } else {
                    ++i;
                    arg = args.get(i);
                    if (valueType.equals(int.class)) {
                        setValue(name, Integer.valueOf(arg));
                    } else if (valueType.equals(long.class)) {
                        setValue(name, Long.valueOf(arg));
                    } else if (valueType.equals(double.class)) {
                        setValue(name, Double.valueOf(arg));
                    } else if (valueType.equals(boolean.class)) {
                        setValue(name, Boolean.valueOf(arg));
                    }
                }
            } else {
                rest.add(arg);
            }
        }
        return rest;
    }

    public List<String> getHelp() {
        List<String> list = new ArrayList<>();
        for (Field fld : getClass ().getFields()) {
            String name = fld.getName();
            Class<?> type = fld.getType();
            try {
                list.add("    --" + name + " <" + type.getName() + "> : default is " + fld.get(this));
            } catch (Exception ex) {
                //nothing
            }
        }
        return list;
    }

    public VfdtNode newRoot(List<ArffAttributeType> types) {
        return new VfdtNodeGrowing(this, types);
    }

    public VfdtNode newGrowing(GrowingParameters p) {
        return new VfdtNodeGrowing(this, p.getTypes(), p.getInstanceCountClassTotal(),
                p.getParentClass(), p.getPathActiveAttributes(), p.getParentErrorRate(), p.getDepth());
    }

    public VfdtNode newLeaf(LeafParameters p) {
        return new VfdtNodeLeaf(p.getClassValue(), p.getClassIndexInAttributes(), p.getDepth());
    }

    public VfdtNode newSplitContinuous(SplitContinuousParameters p) {
        return new VfdtNodeSplitContinuous(p.getInstanceCount(), p.getSplitAttributeIndex(), p.getDepth(), p.getThreshold(),
                newGrowing(p.newGrowingParameters(true)),
                newGrowing(p.newGrowingParameters(false)));
    }


    public VfdtNode newSplitDiscrete(SplitDiscreteParameters p) {
        return new VfdtNodeSplitDiscrete(p.getInstanceCount(), p.getSplitAttributeIndex(), p.getDepth(), p.getGenerator(),
                VfdtNodeSplitDiscrete.create(p.getDiscreteClassCount(), p.getGenerator()));
    }

    ///below classes can provide opportunity to provide additional information from parent nodes

    public static class GrowingParameters implements Serializable {
        protected List<ArffAttributeType> types;
        protected long instanceCountClassTotal;
        protected long parentClass;
        protected BitSet pathActiveAttributes;
        protected double parentErrorRate;
        protected int depth;

        public GrowingParameters() {}

        public GrowingParameters(List<ArffAttributeType> types, long instanceCountClassTotal, long parentClass,
                                 BitSet pathActiveAttributes, double parentErrorRate, int depth) {
            this.types = types;
            this.instanceCountClassTotal = instanceCountClassTotal;
            this.parentClass = parentClass;
            this.pathActiveAttributes = pathActiveAttributes;
            this.parentErrorRate = parentErrorRate;
            this.depth = depth;
        }

        public List<ArffAttributeType> getTypes() {
            return types;
        }

        public long getInstanceCountClassTotal() {
            return instanceCountClassTotal;
        }

        public long getParentClass() {
            return parentClass;
        }

        public BitSet getPathActiveAttributes() {
            return pathActiveAttributes;
        }

        public double getParentErrorRate() {
            return parentErrorRate;
        }

        public int getDepth() {
            return depth;
        }
    }

    public static class LeafParameters implements Serializable {
        protected long classValue;
        protected int classIndexInAttributes;
        protected int depth;

        public LeafParameters() {}

        public LeafParameters(long classValue, int classIndexInAttributes, int depth) {
            this.classValue = classValue;
            this.classIndexInAttributes = classIndexInAttributes;
            this.depth = depth;
        }

        public long getClassValue() {
            return classValue;
        }

        public int getClassIndexInAttributes() {
            return classIndexInAttributes;
        }

        public int getDepth() {
            return depth;
        }
    }

    public static class SplitContinuousParameters implements Serializable {
        protected long instanceCount;
        protected int splitAttributeIndex;
        protected double threshold;
        protected List<ArffAttributeType> types;
        protected long leftCount;
        protected long rightCount;
        protected long parentClass;
        protected BitSet pathActiveAttributes;
        protected double parentErrorRate;
        protected int depth;

        public SplitContinuousParameters() {}

        public SplitContinuousParameters(long instanceCount, int splitAttributeIndex, double threshold, List<ArffAttributeType> types,
                                         long leftCount, long rightCount, long parentClass, BitSet pathActiveAttributes, double parentErrorRate,
                                         int depth) {
            this.instanceCount = instanceCount;
            this.splitAttributeIndex = splitAttributeIndex;
            this.threshold = threshold;
            this.types = types;
            this.leftCount = leftCount;
            this.rightCount = rightCount;
            this.parentClass = parentClass;
            this.pathActiveAttributes = pathActiveAttributes;
            this.parentErrorRate = parentErrorRate;
            this.depth = depth;
        }

        public long getInstanceCount() {
            return instanceCount;
        }

        public int getSplitAttributeIndex() {
            return splitAttributeIndex;
        }

        public double getThreshold() {
            return threshold;
        }

        public List<ArffAttributeType> getTypes() {
            return types;
        }

        public long getLeftCount() {
            return leftCount;
        }

        public long getRightCount() {
            return rightCount;
        }

        public long getParentClass() {
            return parentClass;
        }

        public BitSet getPathActiveAttributes() {
            return pathActiveAttributes;
        }

        public double getParentErrorRate() {
            return parentErrorRate;
        }

        public int getDepth() {
            return depth;
        }

        public GrowingParameters newGrowingParameters(boolean left) {
            return new GrowingParameters(getTypes(), left ? getLeftCount() : getRightCount(),
                    getParentClass(), getPathActiveAttributes(), getParentErrorRate(), getDepth() + 1);
        }
    }

    public static class SplitDiscreteParameters implements Serializable {
        protected long instanceCount;
        protected int splitAttributeIndex;
        protected VfdtNodeSplitDiscrete.AttrAndTotalToNode generator;
        protected AttributeDiscreteClassCount discreteClassCount;
        protected int depth;

        public SplitDiscreteParameters() {}

        public SplitDiscreteParameters(long instanceCount, int splitAttributeIndex, VfdtNodeSplitDiscrete.AttrAndTotalToNode generator,
                                       AttributeDiscreteClassCount discreteClassCount, int depth) {
            this.instanceCount = instanceCount;
            this.splitAttributeIndex = splitAttributeIndex;
            this.generator = generator;
            this.discreteClassCount = discreteClassCount;
            this.depth = depth;
        }

        public long getInstanceCount() {
            return instanceCount;
        }

        public int getSplitAttributeIndex() {
            return splitAttributeIndex;
        }

        public VfdtNodeSplitDiscrete.AttrAndTotalToNode getGenerator() {
            return generator;
        }

        public AttributeDiscreteClassCount getDiscreteClassCount() {
            return discreteClassCount;
        }

        public int getDepth() {
            return depth;
        }
    }
}
