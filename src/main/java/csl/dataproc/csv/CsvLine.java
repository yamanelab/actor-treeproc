package csl.dataproc.csv;

import csl.dataproc.tgls.util.ArraysAsList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CsvLine implements Serializable, Iterable<Object> {
    protected long start;
    /** exclusive */
    protected long end;
    protected List<String> data;

    public boolean emptyLine;

    static final long serialVersionUID = 1L;

    public CsvLine() {
        data = new ArrayList<>(3);
    }

    public CsvLine(long start, long end, List<String> data, boolean emptyLine) {
        this.start = start;
        this.end = end;
        this.data = data;
        this.emptyLine = emptyLine;
    }

    /** data is copied as a new ArrayList. start and end are 0. never emptyLine */
    public CsvLine(Collection<String> data) {
        this(0, 0, new ArrayList<>(data), false);
    }

    public CsvLine(String... data) {
        this(ArraysAsList.get(data));
    }

    /**
     * @return true if the line has valid data-position. The property does not care about the list returned by {@link #getData()}
     */
    public boolean hasData() {
        return start < end;
    }

    /**
     *
     * @return true if the line has no valid data
     */
    public boolean isEnd() {
        return start >= end;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public List<String> getData() {
        return data;
    }

    /**
     * @return true if the line is 0 length (<code>\n\n</code>),
     *    or single whitespace field without any commas and quotations (<code>\n     \n</code>) if trimNonQuoted. <br>
     *    (<code>\n""\n</code> and <code>\n,\n</code> are empty lines.) <br>
     *   Also defaultData does not affect to the property:
     *    i.e. even if isEmptyLine(), getData().get(0).isEmpty() might be false. <br>
     *
     *
     * @see CsvReader#setTrimNonEscape(boolean)
     * @see CsvReader#setDefaultData(List)
     */
    public boolean isEmptyLine() {
        return emptyLine;
    }

    /**
     * @return column i or if i exceeds the size of the data, it returns "".
     */
    public String get(int i) {
        return get(i, "");
    }

    /**
     * @return column i or if (and only if) i exceeds the size of the data, it returns defVal.
     */
    public String get(int i, String defVal) {
        if (i < data.size()) {
            return data.get(i);
        } else {
            return defVal;
        }
    }

    /**
     * @return column i or if i exceeds the size of the data, it returns 0.
     */
    public int getInt(int i) {
        return getInt(i, 0);
    }
    /**
     * @return column i or if i exceeds the size of the data, it returns 0.
     */
    public long getLong(int i) {
        return getLong(i, 0);
    }
    /**
     * @return column i or if i exceeds the size of the data, it returns 0.
     */
    public float getFloat(int i) {
        return getFloat(i, 0);
    }
    /**
     * @return column i or if i exceeds the size of the data, it returns 0.
     */
    public double getDouble(int i) {
        return getDouble(i, 0);
    }

    /**
     * @return column i or if get(i, "") returns an empty string, it returns defVal.
     */
    public int getInt(int i, int defVal) {
        String s = get(i, "");
        if (!s.isEmpty()) {
            try {
                return Integer.valueOf(s);
            } catch (NumberFormatException ne) {
                return defVal;
            }
        } else {
            return defVal;
        }
    }

    /**
     * @return column i or if get(i, "") returns an empty string, it returns defVal.
     */
    public long getLong(int i, long defVal) {
        String s = get(i, "");
        if (!s.isEmpty()) {
            try {
                return Long.valueOf(s);
            } catch (NumberFormatException ne) {
                return defVal;
            }
        } else {
            return defVal;
        }
    }

    /**
     * @return column i or if get(i, "") returns an empty string, it returns defVal.
     */
    public float getFloat(int i, float defVal) {
        String s = get(i, "");
        if (!s.isEmpty()) {
            try {
                return Float.valueOf(s);
            } catch (NumberFormatException ne) {
                return defVal;
            }
        } else {
            return defVal;
        }
    }

    /**
     * @return column i or if get(i, "") returns an empty string, it returns defVal.
     */
    public double getDouble(int i, double defVal) {
        String s = get(i, "");
        if (!s.isEmpty()) {
            try {
                return Double.valueOf(s);
            } catch (NumberFormatException ne) {
                return defVal;
            }
        } else {
            return defVal;
        }
    }

    /** @return the number of columns */
    public int size() {
        return data.size();
    }

    @Override
    public String toString() {
        return String.format("%d-%d:%s%s%s", start, end,
                emptyLine ? "(Empty)" : "",
                isEnd() ? "(End)" : "", data);
    }

    ////operations for writing

    public CsvLine copy() {
        try {
            CsvLine line = (CsvLine) super.clone();
            line.data = new ArrayList<>(data);
            return line;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** @return this */
    public CsvLine addColumns(String... columns) {
        data.addAll(ArraysAsList.get(columns));
        return this;
    }

    /** if i over the size of data, extends with empty strings.
     * @return this */
    public CsvLine setColumn(int i, String column) {
        for (int n = 0, len = i - data.size(); n < len + 1; ++n) {
            data.add("");
        }
        data.set(i, column);
        return this;
    }

    /** overwrites existing data and appends if exceeds
     * @return this */
    public CsvLine setColumn(String... columns) {
        int preSize = data.size();
        for (int i = 0, len =  Math.min(columns.length, preSize); i < len; ++i) {
            data.set(i, columns[i]);
        }
        for (int i = preSize, len = columns.length; i < len; ++i) {
            data.add(columns[i]);
        }
        return this;
    }

    /** inserts columns or appends columns if exceeds (with appending empty strings)
     *  @return this */
    public CsvLine addColumns(int i, String... columns) {
        for (int n = 0, len = i - data.size(); n < len; ++n) {
            data.add("");
        }
        data.addAll(i, ArraysAsList.get(columns));
        return this;
    }

    /**
     * @param columnValue the searched value
     * @return index of searched column, or -1, useful for the header column
     */
    public int indexOf(String columnValue) {
        return data.indexOf(columnValue);
    }

    public int[] toArrayInt(int defValue) {
        return toArrayIntRange(defValue, 0, size());
    }

    public long[] toArrayLong(long defValue) {
        return toArrayLongRange(defValue, 0, size());
    }

    public float[] toArrayFloat(float defValue) {
        return toArrayFloatRange(defValue, 0, size());
    }

    public double[] toArrayDouble(double defValue) {
        return toArrayDoubleRange(defValue, 0, size());
    }


    public int[] toArrayIntRange(int defValue, int fromCol, int toColExclusive) {
        int size = toColExclusive - fromCol;
        int[] a = new int[size];
        for (int i = 0; i < size; ++i) {
            a[i] = getInt(i + fromCol, defValue);
        }
        return a;
    }

    public long[] toArrayLongRange(long defValue, int fromCol, int toColExclusive) {
        int size = toColExclusive - fromCol;
        long[] a = new long[size];
        for (int i = 0; i < size; ++i) {
            a[i] = getLong(i + fromCol, defValue);
        }
        return a;
    }

    public float[] toArrayFloatRange(float defValue, int fromCol, int toColExclusive) {
        int size = toColExclusive - fromCol;
        float[] a = new float[size];
        for (int i = 0; i < size; ++i) {
            a[i] = getFloat(i + fromCol, defValue);
        }
        return a;
    }

    public double[] toArrayDoubleRange(double defValue, int fromCol, int toColExclusive) {
        int size = toColExclusive - fromCol;
        double[] a = new double[size];
        for (int i = 0; i < size; ++i) {
            a[i] = getDouble(i + fromCol, defValue);
        }
        return a;
    }


    public int[] toArrayIntSelected(int defValue, int... indices) {
        int size = indices.length;
        int[] a = new int[size];
        for (int i = 0; i < size; ++i) {
            a[i] = getInt(indices[i], defValue);
        }
        return a;
    }

    public long[] toArrayLongSelected(long defValue, int... indices) {
        int size = indices.length;
        long[] a = new long[size];
        for (int i = 0; i < size; ++i) {
            a[i] = getLong(indices[i], defValue);
        }
        return a;
    }

    public float[] toArrayFloatSelected(float defValue, int... indices) {
        int size = indices.length;
        float[] a = new float[size];
        for (int i = 0; i < size; ++i) {
            a[i] = getFloat(indices[i], defValue);
        }
        return a;
    }

    public double[] toArrayDoubleSelected(double defValue, int... indices) {
        int size = indices.length;
        double[] a = new double[size];
        for (int i = 0; i < size; ++i) {
            a[i] = getDouble(indices[i], defValue);
        }
        return a;
    }



    public List<Integer> toListInt(int defValue) {
        return toListIntRange(defValue, 0, size());
    }

    public List<Long> toListLong(long defValue) {
        return toListLongRange(defValue, 0, size());
    }

    public List<Float> toListFloat(float defValue) {
        return toListFloatRange(defValue, 0, size());
    }

    public List<Double> toListDouble(double defValue) {
        return toListDoubleRange(defValue, 0, size());
    }


    public List<Integer> toListIntRange(int defValue, int fromCol, int toColExclusive) {
        int size = toColExclusive - fromCol;
        List<Integer> a = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            a.add(getInt(i + fromCol, defValue));
        }
        return a;
    }

    public List<Long> toListLongRange(long defValue, int fromCol, int toColExclusive) {
        int size = toColExclusive - fromCol;
        List<Long> a = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            a.add(getLong(i + fromCol, defValue));
        }
        return a;
    }

    public List<Float> toListFloatRange(float defValue, int fromCol, int toColExclusive) {
        int size = toColExclusive - fromCol;
        List<Float> a = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            a.add(getFloat(i + fromCol, defValue));
        }
        return a;
    }

    public List<Double> toListDoubleRange(double defValue, int fromCol, int toColExclusive) {
        int size = toColExclusive - fromCol;
        List<Double> a = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            a.add(getDouble(i + fromCol, defValue));
        }
        return a;
    }


    public List<Integer> toListIntSelected(int defValue, int... indices) {
        int size = indices.length;
        List<Integer> a = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            a.add(getInt(indices[i], defValue));
        }
        return a;
    }

    public List<Long> toListLongSelected(long defValue, int... indices) {
        int size = indices.length;
        List<Long> a = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            a.add(getLong(indices[i], defValue));
        }
        return a;
    }

    public List<Float> toListFloatSelected(float defValue, int... indices) {
        int size = indices.length;
        List<Float> a = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            a.add(getFloat(indices[i], defValue));
        }
        return a;
    }

    public List<Double> toListDoubleSelected(double defValue, int... indices) {
        int size = indices.length;
        List<Double> a = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            a.add(getDouble(indices[i], defValue));
        }
        return a;
    }

    public List<Object> toListObject() {
        return toListObject("");
    }

    public List<Object> toListObject(Object defValue) {
        int size = size();
        List<Object> a = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            a.add(getObject(i, defValue));
        }
        return a;
    }

    public Object getObject(int i, Object defValue) {
        String str = get(i, null); //considering subclasses
        if (str != null) {
            try {
                String noCommaStr = str.replace(",", "");
                if (noCommaStr.indexOf('.') != -1) {
                    return Double.valueOf(noCommaStr);
                } else {
                    return Long.valueOf(noCommaStr);
                }
            } catch (NumberFormatException ex) {
                return str;
            }
        } else {
            return defValue;
        }
    }

    @Override
    public Iterator<Object> iterator() {
        return new Iterator<Object>() {
            int index = 0;
            int size = size();
            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public Object next() {
                Object v = getObject(index, "");
                ++index;
                return v;
            }
        };
    }
}
