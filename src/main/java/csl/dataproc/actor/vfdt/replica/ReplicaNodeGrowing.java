package csl.dataproc.actor.vfdt.replica;

import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.vfdt.VfdtConfig;
import csl.dataproc.vfdt.VfdtNodeGrowing;
import csl.dataproc.vfdt.VfdtNodeGrowingSplitter;
import csl.dataproc.vfdt.count.*;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ReplicaNodeGrowing extends VfdtNodeGrowing {
    public ReplicaNodeGrowing() {
    }

    public ReplicaNodeGrowing(VfdtConfig config, List<ArffAttributeType> types) {
        super(config, types);
    }

    public ReplicaNodeGrowing(VfdtConfig config, List<ArffAttributeType> types, long instanceCountClassTotal, long parentClass, BitSet pathActiveAttributes, double parentErrorRate, int depth) {
        super(config, types, instanceCountClassTotal, parentClass, pathActiveAttributes, parentErrorRate, depth);
    }

    public VfdtConfig getConfig() {
        return config;
    }

    public void merge(VfdtNodeGrowing g) {
        instanceCount += g.getInstanceCount();
        instanceCountClassTotal += g.getInstanceCountClassTotal();

        mergeClassTotal(g.getClassTotals());
        mergeAttributeCounts(g.getAttributeCounts());
        mergeSplitter(g.getSplitter());
    }

    public void mergeClassTotal(LongCount count) {
        merge(classTotals, count);
    }

    public static void merge(LongCount left, LongCount right) {
        LongCount.LongKeyValueIterator iter = right.iterator();
        while (iter.hasNext()) {
            long k = iter.getKey();
            long v = iter.nextValue();
            left.add(k, v);
        }
    }

    public static void mergeDouble(LongCount.DoubleCount left, LongCount.DoubleCount right) {
        LongCount.LongKeyDoubleValueIterator iter = right.iterator();
        while (iter.hasNext()) {
            long k = iter.getKey();
            double d = iter.nextValueDouble();
            left.addDouble(k, d);
        }
    }

    public void mergeAttributeCounts(AttributeClassCount[] counts) {
        for (int i = 0, l = counts.length; i < l; ++i) {
            merge(this.attributeCounts[i], counts[i]);
        }
    }

    public void mergeSplitter(VfdtNodeGrowingSplitter splitter) {
        this.splitter.setSinceLastProcess(
                this.splitter.getSinceLastProcess() + splitter.getSinceLastProcess());
    }

    public void merge(AttributeClassCount left, AttributeClassCount right) {
        if (left instanceof AttributeContinuousClassCount) {
            mergeContinuous((AttributeContinuousClassCount) left, (AttributeContinuousClassCount) right);
        } else if (left instanceof AttributeDiscreteClassCountHash) {
            mergeDiscreteHash((AttributeDiscreteClassCountHash) left, (AttributeDiscreteClassCountHash) right);
        } else if (left instanceof AttributeDiscreteClassCountArray) {
            mergeDiscreteArray((AttributeDiscreteClassCountArray) left, (AttributeDiscreteClassCountArray) right);
        } else if (left instanceof AttributeStringClassCount) {
            mergeString((AttributeStringClassCount) left, (AttributeStringClassCount) right);
        }
    }

    public void mergeContinuous(AttributeContinuousClassCount left, AttributeContinuousClassCount right) {
        mergeTotal(left.getTotal(), right.getTotal());
        left.setAddingNewSplits(left.isAddingNewSplits() && right.isAddingNewSplits());
        left.setClassTableSize(Math.max(left.getClassTableSize(), right.getClassTableSize()));

        int leftSplitSize = left.getSplits().size();
        int rightSplitSize = right.getSplits().size();

        List<AttributeContinuousSplit> leftSplits = shred(new ArrayList<>(left.getSplits()), new ArrayList<>(right.getSplits()), false);
        List<AttributeContinuousSplit> rightSplits = shred(new ArrayList<>(right.getSplits()), new ArrayList<>(left.getSplits()), true);

        leftSplits.addAll(rightSplits);
        leftSplits.sort(this::compare);

        List<AttributeContinuousSplit> result = new ArrayList<>();
        AttributeContinuousSplit prev = null;
        for (AttributeContinuousSplit next : leftSplits) {
            if (prev == null || prev.getUpper() <= next.getLower()) {
                result.add(next);
                if (prev != null) {
                    prev.setUpper(next.getLower());
                }
            } else if (next.getLower() <= prev.getUpper()) { //shredded: leftSplits and rightSplits have same bounds
                mergeCount(prev, next);
            }
            prev = next;
        }

        shrink(leftSplitSize, rightSplitSize, result);

        left.setSplits(result);
    }

    public void shrink(int leftSplitSize, int rightSplitSize, List<AttributeContinuousSplit> result) {
        VfdtConfig conf = getConfig();
        double f = (conf instanceof ReplicaVfdtConfig? ((ReplicaVfdtConfig) conf).splitMergeSizeFactor : 1);

        int splitSizeLimit = Math.min(
                getConfig().continuousClassCountMaxSplits + 1, //fix if it overs the max. so it can be the max+1
                Math.max(2, (int) ((leftSplitSize + rightSplitSize)  * f)));
        List<AttributeContinuousSplit> ranking = new ArrayList<>(result);
        ranking.sort(Comparator.comparingDouble(AttributeContinuousSplit::getCount));
        while (splitSizeLimit < result.size() && !ranking.isEmpty()) {
            AttributeContinuousSplit mergeLeft = ranking.remove(0);
            int n = result.indexOf(mergeLeft);
            if (n != -1) {
                AttributeContinuousSplit mergeRight;
                if (n + 1 < result.size()) {
                    mergeRight = result.get(n + 1);
                } else {
                    mergeLeft = result.get(n - 1);
                    mergeRight = result.get(n);
                }
                mergeTotal(mergeLeft, mergeRight);
                result.remove(mergeRight);
            }
        }

    }

    public List<AttributeContinuousSplit> shred(List<AttributeContinuousSplit> existing, List<AttributeContinuousSplit> newBounds, boolean allCopy) {
        List<Double> bounds = new ArrayList<>();
        for (AttributeContinuousSplit nextBound: newBounds) {
            if (bounds.isEmpty() || bounds.get(bounds.size() - 1) != nextBound.getLower()) {
                bounds.add(nextBound.getLower());
            }
            if (nextBound.getLower() != nextBound.getUpper()) {
                bounds.add(nextBound.getUpper());
            }
        }

        List<AttributeContinuousSplit> result = new ArrayList<>(
                allCopy ? existing.stream().map(this::copy).collect(Collectors.toList()) :
                        existing);
        int startIndex = 0;
        for (double b : bounds) {
            boolean over = false;
            for (int i = startIndex; i < result.size(); ++i) {
                AttributeContinuousSplit split = result.get(i);
                double lower = split.getLower();
                double upper = split.getUpper();
                if (b < lower) {
                    over = false;
                    break;
                } else if (lower == b) {
                    startIndex = 0;
                    over = false;
                    break;
                } else if (lower <= b && b < upper) {
                    result.add(i + 1, split.newSplit(b, -1L, i));
                    startIndex = i;
                    over = false;
                    break;
                } else if (upper <= b) {
                    startIndex = i;
                    over = true;
                }
            }
            if (over) {
                break;
            }
        }
        return result;
    }


    public void takeOutCount(AttributeContinuousSplit prev1, AttributeContinuousSplit prev2, double p2) {
        prev1.setCount((long) (prev1.getCount() * (1 - p2)));
        prev2.setCount((long) (prev1.getCount() * p2));
    }

    public AttributeContinuousSplit copy(AttributeContinuousSplit split) {
        AttributeContinuousSplit s = new AttributeContinuousSplit();
        s.setBoundaryClassCount(split.getBoundaryClassCount());
        s.setBoundaryClass(split.getBoundaryClass());
        s.setUpper(split.getUpper());
        s.setLower(split.getLower());
        s.setCount(split.getCount());
        s.setClassCount(split.getClassCount().copy());
        return s;
    }

    public int compare(AttributeContinuousSplit left, AttributeContinuousSplit right) {
        int n = Double.compare(left.getLower(), right.getLower());
        if (n == 0) {
            return Double.compare(right.getUpper(), left.getUpper());
        } else {
            return n;
        }
    }

    public void mergeTotal(AttributeContinuousSplit left, AttributeContinuousSplit right) {
        if (left.getLower() > right.getLower()) {
            left.setLower(right.getLower());
            left.setBoundaryClass(right.getBoundaryClass());
            left.setBoundaryClassCount(0);
        }
        if (left.getUpper() < right.getUpper()) {
            left.setUpper(right.getUpper());
        }
        mergeCount(left, right);
    }

    public void mergeCount(AttributeContinuousSplit left, AttributeContinuousSplit right) {
        mergeDouble(left.getClassCount(), right.getClassCount());
        left.setCount(left.getCount() + right.getCount());
        if (left.getBoundaryClass() == -1) {
            left.setBoundaryClass(right.getBoundaryClass());
        }
        if (left.getBoundaryClass() == right.getBoundaryClass() && left.getLower() == right.getLower()) {
            left.setBoundaryClassCount(left.getBoundaryClassCount() + right.getBoundaryClassCount());
        }
    }

    public void mergeDiscreteHash(AttributeDiscreteClassCountHash left, AttributeDiscreteClassCountHash right) {
        //TODO define add(attr, cls, count)
    }

    public void mergeDiscreteArray(AttributeDiscreteClassCountArray left, AttributeDiscreteClassCountArray right) {
        //TODO define add(attr, cls, count)
    }

    public void mergeString(AttributeStringClassCount left, AttributeStringClassCount right) {
        right.getCount().forEach((k,v) ->
                merge(left.getCount().computeIfAbsent(k, (k2) -> new LongCount(left.getClassTableSize())),
                    v));
    }
}
