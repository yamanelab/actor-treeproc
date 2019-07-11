package csl.dataproc.csv;

import csl.dataproc.tgls.util.ArraysAsList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * the base class of {@link CsvReader} and {@link CsvWriter}
 */
public class CsvIO {
    protected int maxColumns = 10;
    protected int maxColumnWidth = 0;
    protected boolean trimNonEscape = true;
    protected char delimiter = ',';
    protected List<String> defaultData;

    public CsvIO() {
        this.defaultData = Collections.emptyList();
    }

    public CsvIO withDefaultDataEmptyColumns(int minColumns) {
        return withDefaultDataColumns(minColumns, "");
    }

    public CsvIO withDefaultDataColumns(int minColumns, String defaultValue) {
        String[] def = new String[minColumns];
        Arrays.fill(def, defaultValue);
        return withDefaultData(def);
    }

    public CsvIO withDefaultData(String... defaultData) {
        return withDefaultData(ArraysAsList.get(defaultData));
    }

    public CsvIO withDefaultData(List<String> defaultData) {
        setDefaultData(defaultData);
        return this;
    }

    public void setDefaultData(List<String> defaultData) {
        this.defaultData = defaultData;
        this.maxColumns = Math.min(maxColumns, defaultData.size());
    }

    /**
     * @see #setTrimNonEscape(boolean)
     */
    public CsvIO withTrimNonEscape(boolean trimNonEscape) {
        setTrimNonEscape(trimNonEscape);
        return this;
    }

    /**
     * if true, it will trim each non-quoted field (<code>...,  data  ,...</code> =&gt; <code>...,data,...</code>).
     * default is true.
     */
    public void setTrimNonEscape(boolean trimNonEscape) {
        this.trimNonEscape = trimNonEscape;
    }

    public CsvIO withDelimiterTab() {
        return withDelimiter('\t');
    }

    public CsvIO withDelimiter(char delimiter) {
        setDelimiter(delimiter);
        return this;
    }

    public void setDelimiter(char delimiter) {
        this.delimiter = delimiter;
    }

    public CsvIO withMaxColumns(int maxColumns) {
        setMaxColumns(maxColumns);
        return this;
    }


    public void setMaxColumns(int maxColumns) {
        this.maxColumns = maxColumns;
    }

    public CsvIO withMaxColumnWidth(int maxColumnWidth) {
        this.maxColumnWidth = maxColumnWidth;
        return this;
    }

    public void setMaxColumnWidth(int maxColumnWidth) {
        this.maxColumnWidth = maxColumnWidth;
    }



    public boolean isTrimNonEscape() {
        return trimNonEscape;
    }

    public List<String> getDefaultData() {
        return defaultData;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public int getMaxColumns() {
        return maxColumns;
    }

    public int getMaxColumnWidth() {
        return maxColumnWidth;
    }


    public CsvIO setFields(CsvIO other) {
        setDefaultData(other.getDefaultData());
        setDelimiter(other.getDelimiter());
        setMaxColumns(other.getMaxColumns());
        setMaxColumnWidth(other.getMaxColumnWidth());
        setTrimNonEscape(other.isTrimNonEscape());
        return this;
    }

    public static class CsvSettings extends CsvIO implements Serializable {
        public CsvSettings() {}

        public CsvSettings(CsvIO io) {
            setFields(io);
        }
    }
}
