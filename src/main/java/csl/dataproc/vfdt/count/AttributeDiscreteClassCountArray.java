package csl.dataproc.vfdt.count;

import java.io.Serializable;

public class AttributeDiscreteClassCountArray implements AttributeDiscreteClassCount, Serializable {
    protected long[] data; //{attr0: {cls0Count, cls1Count, ..., clsNCount, sum}, attr1: ...}
    protected int attributes;
    protected int classes;

    static final long serialVersionUID = 1L;

    public AttributeDiscreteClassCountArray() {}

    public AttributeDiscreteClassCountArray(int attributes, int classes) {
        data = new long[attributes * (classes + 1)];
        this.attributes = attributes;
        this.classes = classes;
    }

    public long increment(long attr, long cls) {
        if (attr < 0 || cls < 0) {
            return 0;
        }
        int i = getIndex((int) attr, (int) cls);
        data[i]++;
        int s = getIndex((int) attr, classes);
        data[s]++;
        return data[i];
    }
    int getIndex(int attr, int cls) {
        return (classes + 1) * attr + cls;
    }

    public long get(long attr, long cls) {
        if (attr < 0 || cls < 0) {
            return 0;
        }
        return data[getIndex((int) attr, (int) cls)];
    }


    public LongCount.LongKeyValueIterator attributeToClassesTotal() {
        return new ArrayClassesTotalIterator(this);
    }

    @Override
    public LongCount.LongKeyValueIterator classToCount(long attr) {
        return new ArrayClassToCountIterator(this, (int) attr);
    }

    public int getAttributes() {
        return attributes;
    }

    public int getClasses() {
        return classes;
    }

    public long[] getData() {
        return data;
    }

    public void setData(long[] data) {
        this.data = data;
    }

    public void setAttributes(int attributes) {
        this.attributes = attributes;
    }

    public void setClasses(int classes) {
        this.classes = classes;
    }

    public static class ArrayClassToCountIterator implements LongCount.LongKeyValueIterator {
        protected AttributeDiscreteClassCountArray array;
        protected int attrIndex;
        protected int clsIndex;

        public ArrayClassToCountIterator(AttributeDiscreteClassCountArray array, int attr) {
            this.array = array;
            this.attrIndex = attr;
            this.clsIndex = 0;
        }

        @Override
        public boolean hasNext() {
            while (hasMoveNext()) {
                long v = array.data[array.getIndex(attrIndex, clsIndex)];
                if (v != 0) {
                    return true;
                }
                moveNext();
            }
            return false;
        }
        protected boolean hasMoveNext() {
            return clsIndex < array.getClasses();
        }

        protected void moveNext() {
            ++clsIndex;
        }
        protected long nextKeyIfHashNext() {
            return clsIndex;
        }

        @Override
        public long getKey() {
            if (hasNext()) {
                return nextKeyIfHashNext();
            } else {
                return -1;
            }
        }

        @Override
        public long nextValue() {
            if (hasNext()) {
                long v = array.data[array.getIndex(attrIndex, clsIndex)];
                moveNext();
                return v;
            } else {
                return -1;
            }
        }
    }


    public static class ArrayClassesTotalIterator extends ArrayClassToCountIterator {
        public ArrayClassesTotalIterator(AttributeDiscreteClassCountArray array) {
            super(array, 0);
            this.clsIndex = array.getClasses();
        }

        @Override
        protected void moveNext() {
            ++attrIndex;
        }

        @Override
        protected long nextKeyIfHashNext() {
            return attrIndex;
        }

        @Override
        protected boolean hasMoveNext() {
            return attrIndex < array.getAttributes();
        }
    }
}
