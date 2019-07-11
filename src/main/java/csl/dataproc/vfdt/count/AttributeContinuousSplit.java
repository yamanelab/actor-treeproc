package csl.dataproc.vfdt.count;

import java.io.Serializable;

/** describes bounds of continuous values */
public class AttributeContinuousSplit implements Serializable {
    protected double lower;
    protected double upper;
    protected double count;
    protected LongCount.DoubleCount classCount;
    protected long boundaryClass;
    protected long boundaryClassCount;

    static final long serialVersionUID = 1L;

    public AttributeContinuousSplit() {}

    public AttributeContinuousSplit(double lower, double upper, LongCount.DoubleCount classCount) {
        this.lower = lower;
        this.upper = upper;
        this.classCount = classCount;
        boundaryClass = -1;
    }

    public AttributeContinuousSplit(double lower, double upper, long cls, LongCount.DoubleCount classCount) {
        this(lower, upper, classCount);
        count = 1;
        boundaryClass = cls;
        if (cls != -1) {
            boundaryClassCount = 1;
            this.classCount.increment(cls);
        }
    }

    public double getLower() {
        return lower;
    }

    public double getUpper() {
        return upper;
    }

    public long getBoundaryClassCount() {
        return boundaryClassCount;
    }


    public boolean inBounds(double value, boolean isEnd) {
        return lower <= value &&
                (isEnd ? value <= upper : value < upper);
    }

    /** follows the original implementation as dealing with lower precision values */
    public boolean isEqualToLower(double value) {
        return ((float) value) == (float) lower;
    }

    public boolean isLower(double value) {
        return value < lower;
    }

    public boolean noBounds() {
        return lower == upper;
    }


    /** "core/ExampleGroupStats.c:ContinuousTrackerAddExample" */
    public long increment(double value, long cls) {
        count++;
        if (value == this.lower && cls == boundaryClass) {
            boundaryClassCount++;
        }
        return classCount.increment(cls);
    }

    public void incrementAsTotal(double value, long cls) {
        count++;
        classCount.increment(cls);
        if (value < lower) {
            lower = value;
        }
        if (value > upper) {
            upper = value;
        }
    }

    public double get(long cls) {
        return classCount.getDouble(cls);
    }

    public long getBoundaryClass() {
        return boundaryClass;
    }

    public AttributeContinuousSplit newSplit(double value, long cls, int splitIndex) {
        AttributeContinuousSplit newSplit = newSplit(value, cls, outOfBounds(splitIndex));

        if (splitIndex == AttributeContinuousClassCount.SPLIT_INDEX_UPPER) {
            upper = value;
            newSplit.upper = value;
        } else if (splitIndex == AttributeContinuousClassCount.SPLIT_INDEX_LOWER) {
            newSplit.upper = lower;
        } else {
            newSplit.upper = upper;
            upper = value;
        }

        return newSplit;
    }

    private boolean outOfBounds(int splitIndex) {
        return  splitIndex == AttributeContinuousClassCount.SPLIT_INDEX_LOWER ||
                splitIndex == AttributeContinuousClassCount.SPLIT_INDEX_UPPER ||
                splitIndex == AttributeContinuousClassCount.SPLIT_INDEX_NONE;
    }

    private AttributeContinuousSplit newSplit(double value, long cls, boolean outOfBounds) {
        //temporarily take out
        count -= boundaryClassCount;
        classCount.add(boundaryClass, -boundaryClassCount);

        AttributeContinuousSplit newSplit;
        {
            double percent = (noBounds() || outOfBounds) ? 0 : (1.0 - percent(value));

            newSplit = new AttributeContinuousSplit(value, upper, cls,
                    classCount.takeOut(percent));

            takeOut(percent, newSplit);
        }

        //put back in
        count += boundaryClassCount;
        classCount.add(boundaryClass, boundaryClassCount);
        return newSplit;
    }

    private void takeOut(double percent, AttributeContinuousSplit newSplit) {
        double percentCount = (count * percent);
        newSplit.count += percentCount;
        count -= percentCount;
    }

    public double percent(double value) {
        if (noBounds()) {
            return 0;
        } else {
            return ((value - lower) / (upper - lower));
        }
    }

    public double percentCountWithBoundary(double value) {
        return (count - boundaryClassCount) * percent(value) + boundaryClassCount;
    }

    public LongCount.DoubleCount getClassCount() {
        return classCount;
    }

    public double getCount() {
        return count;
    }

    public void mergeLeft(AttributeContinuousSplit left) {
        lower = left.lower;
        count += left.count;
        for (LongCount.LongKeyDoubleValueIterator iter = left.classCount.iterator(); iter.hasNext(); ) {
            long cls = iter.getKey();
            double clsCount = iter.nextValueDouble();
            classCount.addDouble(cls, clsCount);
        }
        //boundary class is not merged?
    }

    /// setters

    public void setLower(double lower) {
        this.lower = lower;
    }

    public void setUpper(double upper) {
        this.upper = upper;
    }

    public void setCount(double count) {
        this.count = count;
    }

    public void setClassCount(LongCount.DoubleCount classCount) {
        this.classCount = classCount;
    }

    public void setBoundaryClass(long boundaryClass) {
        this.boundaryClass = boundaryClass;
    }

    public void setBoundaryClassCount(long boundaryClassCount) {
        this.boundaryClassCount = boundaryClassCount;
    }
}
