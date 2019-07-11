package csl.dataproc.csv.arff;

import csl.dataproc.tgls.util.ArraysAsList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ArffSparseLine extends ArffLine {
    protected List<CsvSparseColumn> columns;

    static final long serialVersionUID = 1L;

    public ArffSparseLine() {
        this(Collections.emptyList());
    }

    public ArffSparseLine(long start, long end, List<CsvSparseColumn> columns) {
        super(start, end, null, columns.isEmpty());
        this.columns = columns;
    }
    /** data is copied as a new ArrayList. start and end are 0. never emptyLine */
    public ArffSparseLine(Collection<? extends CsvSparseColumn> columns) {
        super(0, 0, null, false);
        this.columns = new ArrayList<>(columns);
    }

    public ArffSparseLine(CsvSparseColumn... columns) {
        this(ArraysAsList.get(columns));
    }

    @Override
    public String get(int i, String defVal) {
        for (CsvSparseColumn column : columns) {
            if (column.index == i) {
                return column.value;
            }
        }
        return defVal;
    }

    /** @return max column index + 1 */
    @Override
    public int size() {
        int max = -1;
        for (CsvSparseColumn column : columns) {
            if (column.index > max) {
                max = column.index;
            }
        }
        return max + 1;
    }

    @Override
    public String toString() {
        return String.format("%d-%d:%s%s{%s}", start, end,
                emptyLine ? "(Empty)" : "",
                isEnd() ? "(End)" : "",
                columns.stream()
                        .map(CsvSparseColumn::toString)
                        .collect(Collectors.joining(",")));
    }

    /** @return internal store of columns: must be sorted by index */
    public List<CsvSparseColumn> getColumns() {
        return columns;
    }

    /** @return a deep copy */
    public ArffSparseLine copy() {
        try {
            ArffSparseLine line = (ArffSparseLine) super.clone();
            line.columns = new ArrayList<>(line.columns.size());
            for (CsvSparseColumn column : this.columns) {
                line.columns.add(CsvSparseColumn.column(column.index, column.value));
            }
            return line;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** extends columns as size+1, size+2, ...
     * @return this*/
    public ArffSparseLine addColumns(String... columns) {
        int i = size();
        for (String col : columns) {
            this.columns.add(CsvSparseColumn.column(i, col));
            ++i;
        }
        return this;
    }

    /** updates an existing column value or adds a new column
     * @return this */
    public ArffSparseLine setColumn(int index, String columnValue) {
        int ins = 0;
        for (CsvSparseColumn column : this.columns) {
            if (column.index == index) {
                column.value = columnValue;
                return this;
            } else if (column.index < index) {
                break;
            }
            ++ins;
        }
        this.columns.add(ins, CsvSparseColumn.column(index, columnValue));
        return this;
    }

    /** overwrites existing columns or appends new columns
     * @return this*/
    @Override
    public ArffSparseLine setColumn(String... columns) {
        int i = 0;
        for (String col : columns) {
            setColumn(i, col);
            ++i;
        }
        return this;
    }

    @Override
    public ArffSparseLine addColumns(int i, String... columns) {
        int ins = 0;
        for (CsvSparseColumn column : this.columns) {
            if (column.index >= i) {
                break;
            }
            ++ins;
        }
        List<CsvSparseColumn> cols = new ArrayList<>(columns.length);
        for (String col : columns) {
            cols.add(CsvSparseColumn.column(i, col));
            ++i;
        }
        int insertedSize = cols.size();
        this.columns.addAll(ins, cols);
        ins += insertedSize;
        int len = this.columns.size();
        for (; ins < len; ++ins) {
            CsvSparseColumn col = this.columns.get(ins);
            col.index += insertedSize;
        }
        return this;
    }

    /**
     * replaces an existing column or adds
     * @return this */
    public ArffSparseLine setColumn(CsvSparseColumn newColumn) {
        int ins = 0;
        for (CsvSparseColumn column : this.columns) {
            if (column.index == newColumn.index) {
                this.columns.set(ins, newColumn);
                return this;
            } else if (column.index < newColumn.index) {
                break;
            }
            ++ins;
        }
        this.columns.add(ins, newColumn);
        return this;
    }
}
