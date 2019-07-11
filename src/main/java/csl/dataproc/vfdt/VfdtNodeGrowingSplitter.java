package csl.dataproc.vfdt;

import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.vfdt.count.AttributeClassCount;
import csl.dataproc.vfdt.count.AttributeContinuousClassCount;
import csl.dataproc.vfdt.count.AttributeDiscreteClassCount;

import java.io.Serializable;
import java.util.*;

public class VfdtNodeGrowingSplitter implements Serializable {
    protected long parentClass;
    protected double parentErrorRate;

    protected long sinceLastProcess;
    protected BitSet pathActiveAttributes;
    protected double splitConfidence;

    protected VfdtConfig config;

    static final long serialVersionUID = 1L;

    public static boolean DEBUG = false;

    public VfdtNodeGrowingSplitter() {}

    /** types: types of attributes and a class */
    public VfdtNodeGrowingSplitter(VfdtConfig config,
                                   long parentClass, BitSet pathActiveAttributes, double parentErrorRate) {
        this.config = config;

        this.parentClass = parentClass;
        this.parentErrorRate = parentErrorRate;
        this.pathActiveAttributes = (BitSet) pathActiveAttributes.clone();

        splitConfidence = config.splitConfidence;
        if (config.useBonferroni) {
            splitConfidence /= (double) pathActiveAttributes.cardinality();
        }
    }

    public double getParentErrorRate() {
        return parentErrorRate;
    }

    public long getParentClass() {
        return parentClass;
    }


    public BitSet getPathActiveAttributes() {
        return pathActiveAttributes;
    }

    public long getSinceLastProcess() {
        return sinceLastProcess;
    }

    public double getSplitConfidence() {
        return splitConfidence;
    }

    /** "learners/vfdt/vfdt-engine.c:_CheckSplit" */
    public VfdtNode split(VfdtNodeGrowing node) {
        if (splitIncrement(config.growingNodeProcessChunkSize)) {
            sinceLastProcess = 0;
            if (!node.isPure()) {
                return split(node, checkSplit(node, false));
            }
        }
        return node;
    }

    public boolean splitIncrement(double chunkSize) {
        ++sinceLastProcess;
        return sinceLastProcess >= chunkSize;
    }

    /** "learners/vfdt/vfdt-engine.c:_CheckSplit" */
    public VfdtNode split(VfdtNodeGrowing node, CheckSplitResult result) {
        if (result.equals(CheckSplitResultSimple.Nothing)) {
            return node;

        } else if (result.equals(CheckSplitResultSimple.MakeLeaf)) {
            return makeLeaf(node);

        } else if (result instanceof CheckSplitResultSplit) {
            AttributeClassCount.GiniIndex selected = ((CheckSplitResultSplit) result).selected;
            AttributeClassCount count = selected.sourceAttribute;
            if (count.isContinuous()) {
                return splitContinuous(node, (AttributeContinuousClassCount) count, selected.threshold);
            } else {
                return splitDiscrete(node, (AttributeDiscreteClassCount) count);
            }
        } else if (result instanceof CheckSplitResultIgnores) {
            CheckSplitResultIgnores ignores = (CheckSplitResultIgnores) result;
            ignores.ignoreAttributes.forEach(i -> ignore(node, i));
            ignores.ignoreContinuousSplit.forEach(i ->
                    ((AttributeContinuousClassCount) node.getAttributeCount(i))
                            .removeSplitsWorseThanGini(ignores.firstAndHoeffdingBound, !config.useGini));
        }
        return node;
    }

    /** "learners/vfdt/vfdt-engine.c:_DoMakeLeaf" */
    public VfdtNode makeLeaf(VfdtNodeGrowing node) {
        long mostCommonClass = getClassValueAsLeaf(node);
        return config.newLeaf(newLeafParameters(mostCommonClass, node.getClassIndexInAttributeCounts(), node.getDepth()));
    }

    protected VfdtConfig.LeafParameters newLeafParameters(long mostCommonClass, int classIdx, int depth) {
        return new VfdtConfig.LeafParameters(mostCommonClass, classIdx, depth);
    }

    public long getClassValueAsLeaf(VfdtNodeGrowing node) {
        return node.getMostCommonClassLaplace(parentClass, config.laplace);
    }

    /** "learners/vfdt/vfdt-engine.c:_DoContinuousSplit" */
    public VfdtNode splitContinuous(VfdtNodeGrowing node, AttributeContinuousClassCount continuousClassCount, double threshold) {
        int attributeIndex = node.getAttributeCountIndex(continuousClassCount);
        long mostCommonClass = node.getMostCommonClass();
        //it seems that a continuous split does not inactivate it's attribute
        double percent = continuousClassCount.percentBelowThreshold(threshold);

        long leftCount = (long) (percent * node.getInstanceCountClassTotal());
        long rightCount = (long) ((1.0 - percent) * node.getInstanceCountClassTotal());

        double errorRate = node.getErrorRate();

        int depth = node.getDepth();

        return config.newSplitContinuous(newSplitContinuousParameters(node.getInstanceCount(), attributeIndex, threshold,
                    node.getTypes(), leftCount, rightCount, mostCommonClass,
                            pathActiveAttributes, errorRate, depth));
    }

    protected VfdtConfig.SplitContinuousParameters newSplitContinuousParameters(long instanceCount, int splitAttributeIndex, double threshold,
                                                                                List<ArffAttributeType> types, long leftCount, long rightCount,
                                                                                long parentClass, BitSet pathActiveAttributes, double parentErrorRate,
                                                                                int depth) {
        return new VfdtConfig.SplitContinuousParameters(instanceCount, splitAttributeIndex, threshold, types, leftCount, rightCount, parentClass, pathActiveAttributes, parentErrorRate, depth);
    }

    /** "learners/vfdt/vfdt-engine.c:_DoDiscreteSplit" */
    public VfdtNode splitDiscrete(VfdtNodeGrowing node, AttributeDiscreteClassCount discreteClassCount) {
        int attributeIndex = node.getAttributeCountIndex(discreteClassCount);
        long mostCommonClass = node.getMostCommonClass();
        int depth = node.getDepth();
        if (pathActiveAttributes.isEmpty()) {
            return config.newLeaf(newLeafParameters(mostCommonClass, node.getClassIndexInAttributeCounts(), depth));
        } else {
            pathActiveAttributes.clear(attributeIndex);

            VfdtConfig config = this.config;
            double errorRate = node.getErrorRate();
            List<ArffAttributeType> types = node.getTypes();
            BitSet pathActiveAttributes = this.pathActiveAttributes;

            double classTotal = node.getInstanceCountClassTotal();
            double count = node.getInstanceCount();

            VfdtNodeSplitDiscrete.AttrAndTotalToNode generator = newGenerator(config, classTotal,
                    count, mostCommonClass, errorRate, pathActiveAttributes, types, depth + 1);

            return config.newSplitDiscrete(
                    newSplitDiscreteParameters(node.getInstanceCount(), attributeIndex, depth, generator, discreteClassCount));
        }
    }

    protected VfdtConfig.SplitDiscreteParameters newSplitDiscreteParameters(long instanceCount, int splitAttributeIndex, int depth,
                                                                            VfdtNodeSplitDiscrete.AttrAndTotalToNode generator, AttributeDiscreteClassCount discreteClassCount) {
        return new VfdtConfig.SplitDiscreteParameters(instanceCount, splitAttributeIndex, generator, discreteClassCount, depth);
    }

    protected VfdtNodeSplitDiscrete.AttrAndTotalToNode newGenerator(VfdtConfig config, double classTotal, double count,
                                                                    long mostCommonClass, double errorRate, BitSet pathActiveAttributes, List<ArffAttributeType> types, int depth) {
        return new SplitterAttrAndTotalToNode(config, classTotal,
                count, mostCommonClass, errorRate, pathActiveAttributes, types, depth);
    }

    public static class SplitterAttrAndTotalToNode implements VfdtNodeSplitDiscrete.AttrAndTotalToNode, Serializable {
        protected VfdtConfig config;
        protected double classTotal;
        protected double count;
        protected long mostCommonClass;
        protected double errorRate;
        protected BitSet pathActiveAttributes;
        protected List<ArffAttributeType> types;
        protected int depth;

        static final long serialVersionUID = 1L;

        public SplitterAttrAndTotalToNode() {}

        public SplitterAttrAndTotalToNode(VfdtConfig config, double classTotal, double count,
                                          long mostCommonClass, double errorRate, BitSet pathActiveAttributes, List<ArffAttributeType> types, int depth) {
            this.config = config;
            this.classTotal = classTotal;
            this.count = count;
            this.mostCommonClass = mostCommonClass;
            this.errorRate = errorRate;
            this.pathActiveAttributes = pathActiveAttributes;
            this.types = types;
            this.depth = depth;
        }

        @Override
        public VfdtNode create(long attr, long attrClsTotal) {
            long subClassTotal = (long) ((attrClsTotal / count) * classTotal);
            return config.newGrowing(newGrowingParameters(subClassTotal));

        }

        protected VfdtConfig.GrowingParameters newGrowingParameters(long instanceCountClassTotal) {
            return new VfdtConfig.GrowingParameters(types, instanceCountClassTotal, mostCommonClass, pathActiveAttributes, errorRate, depth);
        }

        public VfdtConfig getConfig() {
            return config;
        }

        public double getClassTotal() {
            return classTotal;
        }

        public double getCount() {
            return count;
        }

        public long getMostCommonClass() {
            return mostCommonClass;
        }

        public double getErrorRate() {
            return errorRate;
        }

        public BitSet getPathActiveAttributes() {
            return pathActiveAttributes;
        }

        public List<ArffAttributeType> getTypes() {
            return types;
        }
    }

    /** "core/ExampleGroupStats.c:ExampleGroupStatsIgnoreAttribute" */
    public void ignore(VfdtNodeGrowing node, int attrIndex) {
        node.setAttributeCount(attrIndex, node.newIgnore());
    }


    /** "learners/vfdt/vfdt-engine.c:_CheckSplit" */
    public CheckSplitResult checkSplit(VfdtNodeGrowing node, boolean forceSplit) {
        boolean useGini = config.useGini;
        VfdtNodeGrowing.AttributesGiniIndex indices = node.giniIndices(!useGini);

        double hoeffdingBound = node.hoeffdingBound(!useGini);
        double nullIndex = node.totalGini(!useGini);

        boolean pruned = pruned(indices.first.index, hoeffdingBound, nullIndex);
        if (pruned) {
            return CheckSplitResultSimple.MakeLeaf;
        } else {
            if ((indices.second.index - indices.first.index > hoeffdingBound) ||
                    (forceSplit && node.getInstanceCount() > config.forceSplitMinInstanceCount && node.isPure())) {
                if (DEBUG &&
                        node.getDepth() == 0) {
                    System.err.println(String.format("HOEFF root split : 1st attr[%d]  %.6f, 2nd attr[%d] %.6f, diff=%.6f count=%,d hoeff=%.6f",
                            node.getAttributeCountIndex(indices.first.sourceAttribute), indices.first.index,
                            node.getAttributeCountIndex(indices.second.sourceAttribute), indices.second.index,
                            (indices.second.index - indices.first.index),
                            node.getInstanceCount(),
                            hoeffdingBound));
                }

                return new CheckSplitResultSplit(indices.first);
            } else if (hoeffdingBound < config.tieConfidence &&
                    nullIndex - indices.first.index > config.prePruneTau) {
                if (DEBUG &&
                         node.getDepth() == 0) {
                    System.err.println(String.format("TIE   root split : 1st attr[%d]  %.6f, 2nd attr[%d] %.6f, diff=%.6f count=%,d hoeff=%.6f",
                            node.getAttributeCountIndex(indices.first.sourceAttribute), indices.first.index,
                            node.getAttributeCountIndex(indices.second.sourceAttribute), indices.second.index,
                            (indices.second.index - indices.first.index),
                            node.getInstanceCount(),
                            hoeffdingBound));
                }
                return new CheckSplitResultSplit(indices.first);
            } else {
                CheckSplitResultIgnores ignores = new CheckSplitResultIgnores();
                double firstAndHoeffdingBound = indices.first.index + hoeffdingBound;
                ignores.firstAndHoeffdingBound = firstAndHoeffdingBound;

                if (DEBUG &&
                        node.getDepth() == 0) {
                    System.err.println(String.format("NO    root split : 1st attr[%d]  %.6f, 2nd attr[%d] %.6f, diff=%.6f count=%,d hoeff=%.6f",
                            node.getAttributeCountIndex(indices.first.sourceAttribute), indices.first.index,
                            node.getAttributeCountIndex(indices.second.sourceAttribute), indices.second.index,
                            (indices.second.index - indices.first.index),
                            node.getInstanceCount(),
                            hoeffdingBound));
                }

                for (int i = 0, as = indices.attributes.size(); i < as; ++i) {
                    AttributeClassCount.GiniIndex attrGini = indices.attributes.get(i);
                    int aIdx = node.getAttributeCountIndex(attrGini.sourceAttribute);
                    if (attrGini.index > firstAndHoeffdingBound &&
                            attrGini.sourceAttribute != indices.first.sourceAttribute &&
                            attrGini.sourceAttribute != indices.second.sourceAttribute) {
                        ignores.ignoreAttributes.add(aIdx);
                    } else if (attrGini.sourceAttribute.isContinuous()) {
                        ignores.ignoreContinuousSplit.add(aIdx);
                    }
                }
                return ignores;
            }
        }
    }

    public interface CheckSplitResult { }

    public static class CheckSplitResultSplit implements CheckSplitResult, Serializable {
        public AttributeClassCount.GiniIndex selected;

        static final long serialVersionUID = 1L;

        public CheckSplitResultSplit() {}

        public CheckSplitResultSplit(AttributeClassCount.GiniIndex selected) {
            this.selected = selected;
        }
    }

    public enum CheckSplitResultSimple implements CheckSplitResult, Serializable {
        Nothing,
        MakeLeaf;

        static final long serialVersionUID = 1L;
    }

    public static class CheckSplitResultIgnores implements CheckSplitResult, Serializable {
        public List<Integer> ignoreAttributes = new ArrayList<>();
        public double firstAndHoeffdingBound;
        public List<Integer> ignoreContinuousSplit = new ArrayList<>();

        static final long serialVersionUID = 1L;
    }

    private boolean pruned(double firstIndex, double hoeffdingBound, double nullIndex) {
        if (firstIndex - nullIndex > hoeffdingBound) {
            return true;
        } else if (nullIndex - firstIndex < hoeffdingBound &&
                hoeffdingBound < config.prePruneTau) {
            return true;
        } else {
            return false;
        }
    }


    public double getLeafifyIndex(VfdtNodeGrowing node, long totalInstanceCount) {
        long instanceCount = node.getInstanceCount();
        double percent = node.getInstanceCountClassTotal() / (double) totalInstanceCount;

        double estimatedError =
                ((instanceCount - node.getMostCommonClass()) +
                 10.0 * parentErrorRate) /
                (10.0 + instanceCount);

        return percent * estimatedError;
    }

    /////


    public void setParentClass(long parentClass) {
        this.parentClass = parentClass;
    }

    public void setParentErrorRate(double parentErrorRate) {
        this.parentErrorRate = parentErrorRate;
    }

    public void setSinceLastProcess(long sinceLastProcess) {
        this.sinceLastProcess = sinceLastProcess;
    }

    public void setPathActiveAttributes(BitSet pathActiveAttributes) {
        this.pathActiveAttributes = pathActiveAttributes;
    }

    public void setSplitConfidence(double splitConfidence) {
        this.splitConfidence = splitConfidence;
    }

    public VfdtConfig getConfig() {
        return config;
    }

    public void setConfig(VfdtConfig config) {
        this.config = config;
    }

    public static class LeafifyVisitor extends VfdtNode.NodeVisitor {
        protected int limit;
        protected List<LeafifyIndex> indices;
        protected LinkedList<VfdtNodeSplit> parents;
        protected long totalInstanceCount; //total count

        public LeafifyVisitor(int limit) {
            this.limit = limit;
            this.indices = new ArrayList<>(limit);
            parents = new LinkedList<>();
        }

        public void run(VfdtNode root) {
            totalInstanceCount = root.getInstanceCount();
            root.accept(this);

            for (LeafifyIndex index : indices) {
                VfdtNode leaf = index.node.makeLeaf();
                index.parent.replaceChild(index.node, leaf);
            }
        }

        @Override
        public boolean visitSplit(VfdtNodeSplit node) {
            parents.push(node);
            return super.visitSplit(node);
        }

        @Override
        public void visitAfterSplit(VfdtNodeSplit node) {
            parents.pop();
        }

        @Override
        public boolean visitGrowing(VfdtNodeGrowing node) {
            LeafifyIndex index = new LeafifyIndex(node.getLeafifyIndex(totalInstanceCount), parents.peek(), node);

            int n = Collections.binarySearch(indices, index);
            while (indices.size() >= limit) {
                indices.remove(indices.size() - 1);
            }
            indices.add(n, index);

            return true;
        }
    }

    public static class LeafifyIndex implements Comparable<LeafifyIndex>, Serializable {
        public double index;
        public VfdtNodeSplit parent;
        public VfdtNodeGrowing node;

        public LeafifyIndex() {}

        public LeafifyIndex(double index, VfdtNodeSplit parent, VfdtNodeGrowing node) {
            this.index = index;
            this.parent = parent;
            this.node = node;
        }

        @Override
        public int compareTo(LeafifyIndex o) {
            return Double.compare(index, o.index);
        }
    }
}
