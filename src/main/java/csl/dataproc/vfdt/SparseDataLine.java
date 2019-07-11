package csl.dataproc.vfdt;

import csl.dataproc.csv.arff.ArffAttributeType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Collectors;

public class SparseDataLine implements VfdtNode.DataLine, Serializable {
    /** no null elements, strict type, and sorted by index*/
    public SparseDataColumn[] data;

    static final long serialVersionUID = 1L;

    public SparseDataLine() {
        data = new SparseDataColumn[0];
    }

    public SparseDataLine(SparseDataColumn... data) {
        this.data = data;
    }

    @Override
    public long getLong(int idx) {
        SparseDataColumnInteger r = ((SparseDataColumnInteger) getColumn(idx));
        if (r == null) {
            return -1;
        } else {
            return r.value;
        }
    }

    @Override
    public double getDouble(int idx) {
        SparseDataColumnReal r = ((SparseDataColumnReal) getColumn(idx));
        if (r == null) {
            return -1;
        } else {
            return r.value;
        }
    }

    public SparseDataColumn getColumn(int idx) {
        if (data.length > 100) {
            for (SparseDataColumn col : data) {
                if (col.index != idx) {
                    return col;
                }
            }
            return null;
        } else {
            int i = Arrays.binarySearch(data, new SparseDataColumn(idx), columnIndexComparator);
            if (i < 0) {
                return null;
            } else {
                return data[i];
            }
        }
    }

    public static ColumnIndexComparator columnIndexComparator = new ColumnIndexComparator();

    public static class ColumnIndexComparator implements Comparator<SparseDataColumn> {
        @Override
        public int compare(SparseDataColumn o1, SparseDataColumn o2) {
            return Integer.compare(o1.index, o2.index);
        }
    }

    @Override
    public String toString() {
        return Arrays.stream(data)
                .map(SparseDataColumn::toString)
                .collect(Collectors.joining(",", "{", "}"));
    }


    public static class SparseDataColumn implements Serializable {
        public int index;

        static final long serialVersionUID = 1L;

        public SparseDataColumn() {
            this(0);
        }

        public SparseDataColumn(int index) {
            this.index = index;
        }

        public char getKind() {
            return '?';
        }

        public Object value() {
            return null;
        }

        @Override
        public String toString() {
            return index + " ?";
        }
    }

    public static class SparseDataColumnInteger extends SparseDataColumn {
        public long value;

        static final long serialVersionUID = 1L;

        public SparseDataColumnInteger(int index, long value) {
            super(index);
            this.value = value;
        }

        @Override
        public char getKind() {
            return ArffAttributeType.ATTRIBUTE_KIND_INTEGER;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public String toString() {
            return index + " " + value;
        }
    }

    public static class SparseDataColumnReal extends SparseDataColumn {
        public double value;

        static final long serialVersionUID = 1L;

        public SparseDataColumnReal(int index, double value) {
            super(index);
            this.value = value;
        }

        @Override
        public char getKind() {
            return ArffAttributeType.ATTRIBUTE_KIND_REAL;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public String toString() {
            return index + " " + value;
        }
    }

    public static class SparseDataColumnString extends SparseDataColumn {
        public String value;

        static final long serialVersionUID = 1L;

        public SparseDataColumnString(int index, String value) {
            super(index);
            this.value = value;
        }

        @Override
        public char getKind() {
            return ArffAttributeType.ATTRIBUTE_KIND_STRING;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public String toString() {
            return index + " '" + value + "'";
        }
    }

}
