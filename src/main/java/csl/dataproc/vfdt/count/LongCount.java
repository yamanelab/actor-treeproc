package csl.dataproc.vfdt.count;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * (key:long) -&gt; count:long
 *
 * <pre>
 *   layout: e.g. rowSize = 64
 *     data[0..63]:   keys
 *     data[64..127]: counts
 *
 *   increment(key):
 *       keyIndex: key.hashCode() &amp; 0x3F
 *     countIndex: keyIndex + 64
 *     if ((data[keyIndex] == 0 data[countIndex] == 0) or
 *          data[keyIndex] == key)
 *          data[keyIndex] = key, and  data[countIndex]++
 *     else if (key &lt; data[keyIndex])  left.increment(key)
 *     else if (key &gt; data[keyIndex]) right.increment(key)
 * </pre>
 */
public class LongCount implements Serializable {
    protected long[] data;
    protected LongCount left;
    protected LongCount right;

    static final long serialVersionUID = 1L;

    /** the data will be null, useless as LongCount */
    public LongCount() {}

    public LongCount(long[] data) {
        this.data = data;
    }

    public LongCount(int rowSize) {
         this(new long[rowSize * 2]);
    }

    public int hashCode(long value) {
        return Math.abs(Long.hashCode(value) % rowSize());
    }

    public int getCountIndex(int hash) {
        return hash + rowSize();
    }

    protected boolean isZero(int countIndex) {
        return data[countIndex] == 0;
    }

    public int rowSize() {
        return data.length / 2;
    }

    public long increment(long value) {
        int hash = hashCode(value);
        int countIndex = getCountIndex(hash);
        LongCount c = getOrCreate(value, hash, countIndex);
        c.data[countIndex]++;
        return c.data[countIndex];
    }

    public long add(long value, long n) {
        int hash = hashCode(value);
        int countIndex = getCountIndex(hash);
        LongCount c = getOrCreate(value, hash, countIndex);
        c.data[countIndex] += n;
        return c.data[countIndex];
    }

    protected LongCount getOrCreate(long value, int hash, int countIndex) {
        LongCount c = this;
        while (true) {
            long exValue = c.data[hash];
            if (exValue == 0 && c.isZero(countIndex)) {
                c.data[hash] = value;
                return c;
            } else if (exValue == value) {
                return c;
            }

            if (value < exValue) {
                c = c.getOrCreateLeft();
            } else {
                c = c.getOrCreateRight();
            }
        }
    }


    protected LongCount getOrCreateLeft() {
        if (left == null) {
            left = newSubCount();
        }
        return left;
    }
    protected LongCount getOrCreateRight() {
        if (right == null) {
            right = newSubCount();
        }
        return right;
    }

    protected LongCount newSubCount() {
        return new LongCount(rowSize());
    }

    public long get(long value) {
        int hash = hashCode(value);
        int countIndex = getCountIndex(hash);
        LongCount c = this;

        while (c != null) {
            long exValue = c.data[hash];
            if (exValue == value) {
                return c.data[countIndex];
            } else if (exValue == 0) {
                return 0; //stop
            }
            if (value < exValue) {
                c = c.left;
            } else {
                c = c.right;
            }
        }
        return 0;
    }

    public LongCount takeOut(double percent) {
        LongCount src = this;
        LongCount dst = newSubCount();

        int n = src.getCountIndex(0);
        System.arraycopy(src.data, 0, dst.data, 0, n); //key part

        for (int i = n, l = src.data.length; i < l; ++i) {
            long v = (long) (src.data[i] * percent);
            src.data[i] -= v;
            dst.data[i] = v;
        }

        if (src.left != null) {
            dst.left = src.left.takeOut(percent);
        }
        if (src.right != null) {
            dst.right = src.right.takeOut(percent);
        }

        return dst;
    }


    public long getArray(int i) {
        return data[i];
    }
    public long getArrayWithCountIndex(int hash) {
        return data[hash + rowSize()];
    }

    public LongKeyValueIterator iterator() {
        return new CountIteratorImpl(this);
    }

    public interface LongKeyValueIterator {
        boolean hasNext();
        /** obtains key but does not proceed */
        long getKey();
        /** obtains value and proceed to next */
        long nextValue();
    }

    public static class CountIteratorImpl implements LongKeyValueIterator {
        protected List<LongCount> stack;
        protected int hash;

        public CountIteratorImpl(LongCount count) {
            stack = new ArrayList<>();
            push(count);
        }

        public boolean hasNext() {
            if (stack.isEmpty()) {
                return false;
            } else {
                LongCount count = stack.get(stack.size() - 1);
                while (!stack.isEmpty()) {
                    if (hasNextWithinCount(count)) {
                        return true;
                    } else {
                        count = stepCount(count);
                    }
                }
                return false;
            }
        }

        private boolean hasNextWithinCount(LongCount count) {
            for (; hash < count.rowSize(); ++hash) {
                long key = count.data[hash];
                if (key != 0 || !count.isZero(count.getCountIndex(hash))) {
                    return true;
                }
            }
            return false;
        }

        private LongCount stepCount(LongCount count) {
            if (count.left != null) {
                return push(count.left);
            } else if (count.right != null) {
                return push(count.right);
            } else {
                while (stack.size() >= 2) {
                    stack.remove(stack.size() - 1);
                    LongCount parent = stack.get(stack.size() - 1);

                    if (parent.left == count && parent.right != null) {
                        return push(parent.right);
                    } else {
                        count = parent;
                    }
                }
                stack.remove(stack.size() - 1);
                return null;
            }
        }
        private LongCount push(LongCount count) {
            hash = 0;
            stack.add(count);
            return count;
        }

        public long getKey() {
            if (hasNext()) {
                return stack.get(stack.size() - 1).data[hash];
            } else {
                return -1;
            }
        }

        public long nextValue() {
            if (hasNext()) {
                LongCount c = stack.get(stack.size() - 1);
                long v = c.getArrayWithCountIndex(hash);
                ++hash;
                return v;
            } else {
                return -1;
            }
        }
    }

    public LongCount copy() {
        LongCount c = new LongCount(Arrays.copyOf(data, data.length));
        c.left = (left == null ? null : left.copy());
        c.right = (right == null ? null : right.copy());
        return c;
    }

    public long total() {
        long s = 0;
        for (int i = 0; i < rowSize(); ++i) {
            s += getArrayWithCountIndex(i);
        }
        return s +
                (left == null ? 0 : left.total()) +
                (right == null ? 0 : right.total());
    }

    public long size() {
        long s = 0;
        for (int i = 0; i < rowSize(); ++i) {
            if (!isZero(getCountIndex(i))) {
                ++s;
            }
        }
        return s +
                (left == null ? 0 : left.size()) +
                (right == null ? 0 : right.size());
    }

    public LongCount getLeft() {
        return left;
    }

    public LongCount getRight() {
        return right;
    }

    /////

    public long[] getData() {
        return data;
    }

    public void setData(long[] data) {
        this.data = data;
    }

    public void setLeft(LongCount left) {
        this.left = left;
    }

    public void setRight(LongCount right) {
        this.right = right;
    }

    public static class DoubleCount extends LongCount {
        protected double[] counts;

        public DoubleCount() {
        }

        public DoubleCount(long[] data, double[] counts) {
            super(data);
            this.counts = counts;
        }

        public DoubleCount(int rowSize) {
            this(new long[rowSize], new double[rowSize]);
        }

        @Override
        public int rowSize() {
            return data.length;
        }

        @Override
        public int getCountIndex(int hash) {
            return hash;
        }

        @Override
        protected boolean isZero(int countIndex) {
            return counts[countIndex] == 0;
        }

        @Override
        public long increment(long value) {
            return (long) incrementDouble(value);
        }

        @Override
        public long add(long value, long n) {
            return (long) addDouble(value, n);
        }

        @Override
        public long get(long value) {
            return (long) getDouble(value);
        }

        public double incrementDouble(long value) {
            return addDouble(value, 1.0);
        }

        public double addDouble(long value, double n) {
            int hash = hashCode(value);
            int countIndex = getCountIndex(hash);
            DoubleCount c = (DoubleCount) getOrCreate(value, hash, countIndex);

            c.counts[countIndex] += n;
            return c.counts[countIndex];
        }

        public double getDouble(long value) {
            int hash = hashCode(value);
            int countIndex = getCountIndex(hash);
            DoubleCount c = this;

            while (c != null) {
                long exValue = c.data[hash];
                if (exValue == value) {
                    return c.counts[countIndex];
                } else if (exValue == 0) {
                    return 0; //stop
                }
                if (value < exValue) {
                    c = c.getLeft();
                } else {
                    c = c.getRight();
                }
            }
            return 0;
        }

        @Override
        public DoubleCount getLeft() {
            return (DoubleCount) super.getLeft();
        }

        @Override
        public DoubleCount getRight() {
            return (DoubleCount) super.getRight();
        }

        @Override
        protected DoubleCount newSubCount() {
            return new DoubleCount(rowSize());
        }

        @Override
        public DoubleCount takeOut(double percent) {
            DoubleCount src = this;
            DoubleCount dst = newSubCount();

            System.arraycopy(src.data, 0, dst.data, 0, src.data.length); //key part

            for (int i = 0, l = src.counts.length; i < l; ++i) {
                double v = (src.counts[i] * percent);
                src.counts[i] -= v;
                dst.counts[i] += v;
            }

            if (src.left != null) {
                dst.left = src.left.takeOut(percent);
            }
            if (src.right != null) {
                dst.right = src.right.takeOut(percent);
            }

            return dst;
        }

        @Override
        public long getArrayWithCountIndex(int hash) {
            return (long) getArrayWithCountIndexDouble(hash);
        }

        public double getArrayWithCountIndexDouble(int i) {
            return counts[i];
        }

        @Override
        public LongKeyDoubleValueIterator iterator() {
            return new DoubleCountIterator(this);
        }
        @Override
        public DoubleCount copy() {
            DoubleCount c = new DoubleCount(
                    Arrays.copyOf(data, data.length),
                    Arrays.copyOf(counts, counts.length));
            c.left = (left == null ? null : left.copy());
            c.right = (right == null ? null : right.copy());
            return c;
        }

        @Override
        public long total() {
            return (long) totalDouble();
        }

        public double totalDouble() {
            double s = 0;
            for (int i = 0; i < rowSize(); ++i) {
                s += counts[getCountIndex(i)];
            }
            return s +
                    (left == null ? 0 : getLeft().totalDouble()) +
                    (right == null ? 0 : getRight().totalDouble());
        }

    }

    public interface LongKeyDoubleValueIterator extends LongKeyValueIterator {
        double nextValueDouble();
    }

    public static class DoubleCountIterator extends CountIteratorImpl implements LongKeyDoubleValueIterator {
        public DoubleCountIterator(DoubleCount count) {
            super(count);
        }

        @Override
        public double nextValueDouble() {
            if (hasNext()) {
                double v = ((DoubleCount) stack.get(stack.size() - 1)).getArrayWithCountIndexDouble(hash);
                ++hash;
                return v;
            } else {
                return -1;
            }
        }
    }
}
