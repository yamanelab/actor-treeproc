package csl.dataproc.vfdt.count;

import csl.dataproc.csv.arff.ArffAttributeType;
import csl.dataproc.vfdt.VfdtConfig;
import csl.dataproc.vfdt.VfdtNodeGrowing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/** attribute count for continuous value i.e. double -&gt; long -&gt; long.
 *
 *  this class manages values with {@link AttributeContinuousSplit}.
 * */
public class AttributeContinuousClassCount implements AttributeClassCount, Serializable {
    protected AttributeContinuousSplit total;
    protected List<AttributeContinuousSplit> splits;
    protected boolean addingNewSplits = true;

    protected VfdtConfig config;
    protected int classTableSize;

    static final long serialVersionUID = 1L;

    public AttributeContinuousClassCount() {}

    public AttributeContinuousClassCount(VfdtConfig config, int classTableSize) {
        this.config = config;
        this.classTableSize = classTableSize;
        total = new AttributeContinuousSplit(Long.MAX_VALUE, Long.MIN_VALUE , new LongCount.DoubleCount(classTableSize));
        splits = new ArrayList<>(config.continuousClassCountSplitsInitialCapacity);
    }

    public char getKind() {
        return ArffAttributeType.ATTRIBUTE_KIND_REAL;
    }

    public List<AttributeContinuousSplit> getSplits() {
        return splits;
    }

    public AttributeContinuousSplit getTotal() {
        return total;
    }

    public boolean isAddingNewSplits() {
        return addingNewSplits;
    }

    public int getClassTableSize() {
        return classTableSize;
    }

    /** "core/ExampleGroupStats.c:ContinuousTrackerAddExample" */
    public void increment(double attr, long cls) {
        total.incrementAsTotal(attr, cls);
        incrementOrSplit(attr, cls);
    }

    public double get(double attr, long cls) {
        int rangeIndex = searchSplit(attr);
        if (rangeIndex < 0 || rangeIndex >= splits.size()) {
            return 0;
        } else {
            return splits.get(rangeIndex).get(cls);
        }
    }


    /** "core/ExampleGroupStats.c:ContinuousTrackerAddExample" */
    public void incrementOrSplit(double attr, long cls) {
        int splitIndex = searchSplit(attr);
        AttributeContinuousSplit split = getSplit(splitIndex);

        if (split == null) {
            splits.add(new AttributeContinuousSplit(attr, attr, cls, new LongCount.DoubleCount(classTableSize)));
        } else {
            //special case
            if (!addingNewSplits || split.isEqualToLower(attr)) {
                split.increment(attr, cls);
            } else {
                AttributeContinuousSplit newSplit = split.newSplit(attr, cls, splitIndex);
                insertSplitAfter(splitIndex, newSplit);

                //"learners/vfdt/vfdt-engine.c:_AddExampleToGrowingNode"
                if (needsToSplit()) {
                    //"core/ExampleGroupStats.c:ExampleGroupStatsStopAddingSplits"
                    addingNewSplits = false;
                }
            }
        }
    }

    private AttributeContinuousSplit getSplit(int splitIndex) {
        if (splitIndex == SPLIT_INDEX_LOWER) {
            return splits.get(0);
        } else if (splitIndex == SPLIT_INDEX_UPPER) {
            return splits.get(splits.size() - 1);
        } else if (splitIndex == SPLIT_INDEX_NONE) {
            return null;
        } else {
            return splits.get(splitIndex);
        }
    }
    private void insertSplitAfter(int splitIndex, AttributeContinuousSplit newSplit) {
        if (splitIndex == SPLIT_INDEX_LOWER) {
            splits.add(0, newSplit);
        } else if (splitIndex == SPLIT_INDEX_UPPER || splitIndex == SPLIT_INDEX_NONE) {
            splits.add(newSplit);
        } else {
            splits.add(splitIndex + 1, newSplit);
        }
    }

    public static final int SPLIT_INDEX_LOWER = -1;
    public static final int SPLIT_INDEX_UPPER = -2;
    public static final int SPLIT_INDEX_NONE = -3;

    /** @return an index of ranges,
     *   {@link #SPLIT_INDEX_NONE}(empty splits), {@link #SPLIT_INDEX_LOWER} (lower), or {@link #SPLIT_INDEX_UPPER} (upper)  */
    public int searchSplit(double value) {
        int i = searchSplitBinary(value);
        if (i != -1) {
            return i;
        } else if (splits.isEmpty()) {
            return SPLIT_INDEX_NONE;
        } else if (splits.get(0).isLower(value)) {
            return SPLIT_INDEX_LOWER;
        } else {
            return SPLIT_INDEX_UPPER;
        }
    }

    //binary search: -1=not-found
    private int searchSplitBinary(double value) {
        int min = 0;
        int max = splits.size() - 1;
        while (min <= max) {
            int i = (min + max) / 2;

            AttributeContinuousSplit split = splits.get(i);
            if (split.inBounds(value, i == splits.size() - 1)) {
                return i;
            } else if (split.isLower(value)) {
                max = i - 1;
            } else {
                min = i + 1;
            }
        }
        return -1;
    }

    protected boolean needsToSplit() {
        return splits.size() > config.continuousClassCountMaxSplits;
    }

    /** "core/ExampleGroupStats.c:ExampleGroupStatsGiniContinuousAttributeSplit",
     *  "core/ExampleGroupStats.c:ContinuousTrackerGiniAttributeSplit"
     *  "core/ExampleGroupStats.c:ContinuousTrackerEntropyAttributeSplit" */
    @Override
    public GiniIndex[] giniIndex(long nodeInstanceCount, int classes, boolean entropy) {
        return entropy ? entropyIndex(nodeInstanceCount, classes) :
                         giniIndex(nodeInstanceCount);
    }

    /** "core/ExampleGroupStats.c:ExampleGroupStatsGiniContinuousAttributeSplit",
     *  "core/ExampleGroupStats.c:ContinuousTrackerGiniAttributeSplit" */
    public GiniIndex[] giniIndex(long nodeInstanceCount) {
        GiniIndex first = new GiniIndex(config.initGiniIndex, 0, this);
        GiniIndex second = new GiniIndex(config.initGiniIndex, 0, this);
        SplitGiniIndexFunction function = (split, totalGini) -> {
            if (totalGini < first.index) {
                second.set(first.index, first.threshold);
                first.set(totalGini, split.getLower());
            } else if (totalGini < second.index) {
                second.set(totalGini, split.getLower());
            }
        };
        if (nodeInstanceCount > 0) {
            giniIndex(nodeInstanceCount, function, false, this::giniIndexUpdate);
        }
        return new GiniIndex[] {first, second};
    }

    /** "core/ExampleGroupStats.c:ContinuousTrackerEntropyAttributeSplit" */
    public GiniIndex[] entropyIndex(long nodeInstanceCount, int classes) {
        double mdlCost;
        if (splits.size() - 1 < 1) {
            mdlCost = 0;
        } else {
            mdlCost = Math.max(0, VfdtNodeGrowing.lg(splits.size() - 1) / (double) nodeInstanceCount);
        }

        if (VfdtNodeGrowing.DEBUG) {
            System.err.println(" splits: " + splits.size());
        }

        GiniIndex first = new GiniIndex(config.initGiniIndex, 0, this);
        GiniIndex second = new GiniIndex(config.initGiniIndex, 0, this);
        SplitGiniIndexFunctionBase function = (split, totalGini, splitIter,
                                           belowCount, aboveCount, belowGini, aboveGini,
                                           belowClassTotals, aboveClassTotals) -> {
            totalGini += mdlCost;

            double thresh;
            if (nodeInstanceCount == 1) {
                thresh = (split.getLower() + split.getUpper()) / 2.0;
            } else {
                thresh = split.getUpper();
            }

            if (totalGini < first.index) {
                second.set(first.index, first.threshold);
                first.set(totalGini, thresh);
            } else if (totalGini < second.index) {
                second.set(totalGini, thresh);
            }

            if (VfdtNodeGrowing.DEBUG) {
                StringBuilder buf = new StringBuilder();
                for (LongCount.LongKeyDoubleValueIterator iter = split.getClassCount().iterator(); iter.hasNext(); ) {
                    buf.append("(").append(iter.getKey()).append(":");
                    buf.append(String.format("%.6f", iter.nextValueDouble())).append(")");
                }
                System.err.println(String.format("  split[%d] entropy=%.7f belowCount=%.7f aboveCount=%.7f belowEntropy=%.7f aboveEntropy=%.7f 1st %.7f 2nd %.7f upper=%.5f, lower=%.5f, count=%.6f %s",
                         splits.indexOf(split), totalGini,
                        belowCount, aboveCount, belowGini, aboveGini,
                        first.index, second.index,
                        split.getLower(), split.getUpper(), split.getCount(), buf.toString()));
            }
        };

        if (nodeInstanceCount > 0) {
            giniIndex(nodeInstanceCount, function, true, this::entropyIndexUpdate);
        }
        return new GiniIndex[] {first, second};
    }

    /** "core/ExampleGroupStats.c:ExampleGroupStatsIgnoreSplitsWorseThanGini"
     *  "core/ExampleGroupStats.c:ContinuousTrackerDisableWorseThanGini"
     *  "core/ExampleGroupStats.c:ExampleGroupStatsIgnoreSplitsWorseThanEntropy"
     *  "core/ExampleGroupStats.c:ContinuousTrackerDisableWorseThanEntropy"
     *  */
    public void removeSplitsWorseThanGini(double threshold, boolean entropy) {
        SplitGiniIndexFunctionBase function = (split, totalGini, splitIter,
                                               belowCount, aboveCount, belowGini, aboveGini,
                                               belowClassTotals, aboveClassTotals) -> {
            if (totalGini > threshold &&
                    split != splits.get(splits.size() - 1)) { //exclude last split

                AttributeContinuousSplit nextSplit = splits.get(splits.indexOf(split) + 1);
                split.mergeLeft(nextSplit);
                splitIter.remove();

                for (LongCount.LongKeyDoubleValueIterator iter = split.getClassCount().iterator(); iter.hasNext(); ) {
                    long cls = iter.getKey();
                    double clsCount = iter.nextValueDouble();
                    aboveClassTotals.addDouble(cls, clsCount);
                    belowClassTotals.addDouble(cls, -clsCount);
                }
            }
        };

        giniIndex(total.count, function, entropy, this::giniIndexUpdate); //call giniIndexUpdate even if entropy
    }


    public interface SplitGiniIndexFunctionBase {
        void next(AttributeContinuousSplit split, double totalGini,
                  ListIterator<AttributeContinuousSplit> splitIter,
                  double belowCount, double aboveCount, double belowGini, double aboveGini,
                  LongCount.DoubleCount belowClassTotals, LongCount.DoubleCount aboveClassTotals);
    }

    public interface SplitGiniIndexFunction extends SplitGiniIndexFunctionBase {
        @Override
        default void next(AttributeContinuousSplit split, double totalGini,
                          ListIterator<AttributeContinuousSplit> splitIter,
                          double belowCount, double aboveCount, double belowGini, double aboveGini,
                          LongCount.DoubleCount belowClassTotals, LongCount.DoubleCount aboveClassTotals) {
            next(split, totalGini);
        }

        void next(AttributeContinuousSplit split, double totalGini);
    }

    /** "core/ExampleGroupStats.c:ContinuousTrackerGiniAttributeSplit"
     *  "core/ExampleGroupStats.c:ContinuousTrackerDisableWorseThanGini"
     *  "core/ExampleGroupStats.c:ContinuousTrackerDisableWorseThanEntropy"
     *  "core/ExampleGroupStats.c:ContinuousTrackerEntropyAttributeSplit" */
    public void giniIndex(double nodeInstanceCount, SplitGiniIndexFunctionBase function, boolean entropy, GiniUpdate update) {
        double belowCount = 0;
        double aboveCount = nodeInstanceCount;

        LongCount.DoubleCount belowClassTotals = new LongCount.DoubleCount(classTableSize);
        LongCount.DoubleCount aboveClassTotals = total.getClassCount().copy();

        for (ListIterator<AttributeContinuousSplit> splitIter = splits.listIterator(); splitIter.hasNext(); ) {
            AttributeContinuousSplit split = splitIter.next();

            if (!splitIter.hasNext()) { //skip last item
                break;
            }

            double count = split.getCount();
            belowCount += count;
            aboveCount -= count;

            for (LongCount.LongKeyDoubleValueIterator iter = split.getClassCount().iterator(); iter.hasNext(); ) {
                long cls = iter.getKey();
                double clsCount = iter.nextValueDouble();
                belowClassTotals.addDouble(cls, clsCount);
                aboveClassTotals.addDouble(cls, -clsCount);
            }

            update.apply(nodeInstanceCount, split, splitIter, function, entropy,
                    belowCount, aboveCount,
                    belowClassTotals, aboveClassTotals);
        }
    }

    protected interface GiniUpdate {
        void apply(double nodeInstanceCount, AttributeContinuousSplit split,
                   ListIterator<AttributeContinuousSplit> splitIter,
                   SplitGiniIndexFunctionBase function, boolean entropy,
                   double belowCount, double aboveCount,
                   LongCount.DoubleCount belowClassTotals, LongCount.DoubleCount aboveClassTotals);
    }

    static boolean DEBUG_logging;
    static String DEBUG_header = "";
    public static final boolean DEBUG_ABOVE_BELOW=true;

    /** "core/ExampleGroupStats.c:ContinuousTrackerEntropyAttributeSplit"
     *  */
    protected void entropyIndexUpdate(double nodeInstanceCount, AttributeContinuousSplit split,
                                      ListIterator<AttributeContinuousSplit> splitIter,
                                      SplitGiniIndexFunctionBase function, boolean entropy,
                                      double belowCount, double aboveCount,
                                      LongCount.DoubleCount belowClassTotals, LongCount.DoubleCount aboveClassTotals) {
        double belowEntropy = 0;
        if (belowCount > 0) {
            belowEntropy = belowOrAboveGiniUpdate(true, belowEntropy, belowCount, belowClassTotals, true);
        }
        double aboveEntropy = 0;
        if (aboveCount > 0) {
            DEBUG_logging = true;
            DEBUG_header = "above";
            aboveEntropy = belowOrAboveGiniUpdate(true, aboveEntropy, aboveCount, aboveClassTotals, true);
            DEBUG_logging = false;
        }

        double totalEntropy =
                (belowCount / nodeInstanceCount) * belowEntropy +
                (aboveCount / nodeInstanceCount) * aboveEntropy;

        function.next(split, totalEntropy, splitIter,
                belowCount, aboveCount, belowEntropy, aboveEntropy,
                belowClassTotals, aboveClassTotals);
    }

    /** "core/ExampleGroupStats.c:ContinuousTrackerGiniAttributeSplit"
     *  "core/ExampleGroupStats.c:ContinuousTrackerDisableWorseThanGini"
     *  "core/ExampleGroupStats.c:ContinuousTrackerDisableWorseThanEntropy" */
    protected void giniIndexUpdate(double nodeInstanceCount, AttributeContinuousSplit split,
                                   ListIterator<AttributeContinuousSplit> splitIter,
                                   SplitGiniIndexFunctionBase function, boolean entropy,
                                   double belowCount, double aboveCount,
                                   LongCount.DoubleCount belowClassTotals, LongCount.DoubleCount aboveClassTotals) {
        if (belowCount > 0 && aboveCount > 0) {
            double belowGini = belowOrAboveGiniUpdate(entropy, entropy ? 0 : 1, belowCount, belowClassTotals, entropy);
            double aboveGini = belowOrAboveGiniUpdate(true, 0, aboveCount, aboveClassTotals, entropy);

            double totalGini =
                    (belowCount / nodeInstanceCount) * belowGini +
                    (aboveCount / nodeInstanceCount) * aboveGini;
            function.next(split, totalGini, splitIter,
                    belowCount, aboveCount, belowGini, aboveGini,
                    belowClassTotals, aboveClassTotals);
        }
    }

    protected double belowOrAboveGiniUpdate(boolean countGreaterThanZeo, double gini, double count, LongCount.DoubleCount classTotal, boolean entropy) {
        int i = 0;
        for (LongCount.LongKeyDoubleValueIterator iter = classTotal.iterator(); iter.hasNext(); ) {
            double d = iter.nextValueDouble();
            if (VfdtNodeGrowing.DEBUG && DEBUG_logging && DEBUG_ABOVE_BELOW) {
                System.err.print(String.format("  index: %.7f ", gini));
            }
            if (entropy) {
                if (!countGreaterThanZeo || d > 0) {
                    double v = d / count;
                    gini += -(v * VfdtNodeGrowing.lg(v));
                    if (VfdtNodeGrowing.DEBUG && DEBUG_logging && DEBUG_ABOVE_BELOW) {
                        System.err.println(String.format("  %s %d: total=%.7f prob=%.7f ent=%.7f", DEBUG_header, i, d, v, gini));
                    }
                }
            } else {
                if (!countGreaterThanZeo || d > 0) {
                    double v = d / count;
                    gini -= v * v;
                }
            }
            ++i;
        }
        return gini;
    }

    /** "core/ExampleGroupStats.c:ContinuousTrackerGetPercentBelowThreshold" */
    public double percentBelowThreshold(double threshold) {
        int splitIndex = searchSplit(threshold);
        if (splitIndex == SPLIT_INDEX_LOWER) {
            return 0;
        } else if (splitIndex == SPLIT_INDEX_UPPER || splitIndex == SPLIT_INDEX_NONE) {
            return 1.0;
        } else {
            long leftCount = 0;
            for (int i = 0; i < splitIndex; ++i) {
                leftCount += splits.get(i).getCount();
            }
            //it currently does not consider to extend bounds of end splits with total.lower and total.upper
            return (leftCount + splits.get(splitIndex).percentCountWithBoundary(threshold)) / total.count;
        }
    }

    ////


    public VfdtConfig getConfig() {
        return config;
    }

    public void setTotal(AttributeContinuousSplit total) {
        this.total = total;
    }

    public void setSplits(List<AttributeContinuousSplit> splits) {
        this.splits = splits;
    }

    public void setAddingNewSplits(boolean addingNewSplits) {
        this.addingNewSplits = addingNewSplits;
    }

    public void setConfig(VfdtConfig config) {
        this.config = config;
    }

    public void setClassTableSize(int classTableSize) {
        this.classTableSize = classTableSize;
    }
}
