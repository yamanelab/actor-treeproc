package csl.dataproc.csv;

import csl.dataproc.csv.impl.CsvCharInput;
import csl.dataproc.csv.impl.CsvCharIterator;
import csl.dataproc.tgls.util.ByteBufferArray;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A CSV file reader working with (mainly) {@link ByteBufferArray}.
 * <pre>
 *     //as an Iterable
 *     try (CsvReader r = CsvReader.get(file)) { //reading as UTF-8
 *         for (CsvLine l : r) {
 *             ... l.get(0);
 *             ... l.get(1);
 *         }
 *     }
 * </pre>
 *
 * <pre>
 *     //lazy reading
 *     List&lt;long[]&gt; rs = new ArrayList&lt;long[]&gt;();
 *     try (CsvReader r = CsvReader.get(ByteBufferArray.createForRead(file))) {
 *         for (long[] range: r.ranges()) {
 *             rs.add(range);
 *         }
 *         ...
 *         for (long[] range : rs) {
 *             List&lt;String&gt; columns = m.readColumns(range[0]);
 *             ...
 *             CsvLine l = m.readNext(range[0]);
 *             ...
 *         }
 *     }
 *
 * </pre>
 *
 * <pre>
 *     //providing default column values
 *     for (CsvLine l : r.withDefaultData("", "0", "0")) {
 *         if (l.isEmptyLine()) continue; //Note: a single escaped empty line (...\n""\n...) doesn't mean empty.
 *           //but a single whitespace field without any commas and escapes means empty.
 *         String name = l.get(0);
 *         int x = l.getInt(1);
 *         int y = l.getInt(2);
 *         ...
 *     }
 * </pre>
 * @see CsvLine#isEmptyLine()
 */
public class CsvReader extends CsvIO implements CsvReaderI<CsvLine> {
    protected CsvCharIterator charIterator;

    public static CsvReader get(Path file) {
        return get(ByteBufferArray.createForRead(file));
    }

    public static CsvReader get(Path file, Charset charset) {
        return new CsvReader(ByteBufferArray.createForRead(file), charset);
    }

    public static CsvReader get(ByteBufferArray array) {
        return get(array, StandardCharsets.UTF_8);
    }

    public static CsvReader get(ByteBufferArray array, Charset charset) {
        return new CsvReader(array, charset);
    }

    /**
     * @param reader a reader which needs to be closed after reading
     * @return a new reader
     */
    public static CsvReader get(Reader reader) {
        return new CsvReader(CsvCharIterator.get(reader));
    }

    /**
     * @param stream converted to a reader with UTF-8, which needs to be closed after reading
     * @return a new reader
     */
    public static CsvReader get(InputStream stream) {
        return get(new InputStreamReader(stream, StandardCharsets.UTF_8));
    }

    /**
     * @param data the CSV data
     * @return a new reader
     */
    public static CsvReader get(String data) {
        return get(new StringReader(data));
    }

    public CsvReader(ByteBufferArray array, Charset charset) {
        this(CsvCharIterator.get(array, charset));
    }

    public CsvReader(CsvCharIterator charIterator) {
        super();
        this.charIterator = charIterator;
        setIndex(0);
    }

    public boolean setIndex(long index) {
        charIterator.setIndex(index);
        return charIterator.hasNext();
    }

    public long getIndex() {
        return charIterator.getIndex();
    }

    public CsvCharIterator getCharIterator() {
        return charIterator;
    }

    /** returns the range of a CSV line (it supports escaped newlines) */
    public long[] scanNext() {
        CsvCharInput.MaxColumnWidthCharInput processor = new CsvCharInput.MaxColumnWidthCharInput();
        long from = charIterator.getIndex();
        processLine(processor);
        int mw = processor.getMaxColumnWidth();
        if (mw > maxColumnWidth) {
            this.maxColumnWidth = mw;
        }
        long toEx = charIterator.getIndex();
        return new long[] {from, toEx};
    }

    public List<String> readColumns(long from) {
        return readColumns(from, maxColumnWidth);
    }

    public List<String> readColumns(long from, int columnCapacity) {
        charIterator.setIndex(from);
        CsvCharInput.ColumnsListCharInput processor =
                new CsvCharInput.ColumnsListCharInput(columnCapacity, maxColumns, getDefaultData(), isTrimNonEscape());
        processLine(processor);
        return processor.getResult();
    }

    public CsvLine readNext() {
        long start = charIterator.getIndex();
        CsvCharInput.MaxColumnWidthAndColumnsListCharInput processor =
                new CsvCharInput.MaxColumnWidthAndColumnsListCharInput(maxColumnWidth, maxColumns, getDefaultData(), isTrimNonEscape());
        processLine(processor);
        int mw = processor.getMaxColumnWidth();
        if (mw > maxColumnWidth) {
            maxColumnWidth = mw;
        }
        long end = charIterator.getIndex();
        return new CsvLine(start, end, processor.getResult(), processor.isEmptyLine());
    }

    public CsvLine readNext(long from) {
        charIterator.setIndex(from);
        return readNext();
    }


    private enum LoadState {
        Init, Field, QuotedField, QuoteInQuoted
    }

    public void processLine(CsvCharInput reader) {
        LoadState state = LoadState.Init;
        char delimiter = this.getDelimiter();
        int cols = 1;
        outer: while (charIterator.hasNext()) {
            char c = charIterator.next();
            switch (state) {
                case Init:
                case Field:
                    if (c == delimiter) {
                        reader.nextColumn();
                        ++cols;
                        state = LoadState.Init;
                    } else if (c == '\"' && state.equals(LoadState.Init)) {
                        state = LoadState.QuotedField;
                        reader.startEscapeColumn();
                    } else if (c == '\r') { //CRLF skip
                        continue;
                    } else if (c == '\n') { //empty column
                        break outer;
                    } else {
                        reader.nextChar(c);
                        if (Character.isSurrogate(c)) {
                            reader.nextChar(charIterator.nextSurrogate());
                        }
                        state = LoadState.Field;
                    }
                    break;
                case QuotedField:
                    if (c == '\"') {
                        state = LoadState.QuoteInQuoted;
                    } else {
                        reader.nextChar(c);
                    }
                    break;
                case QuoteInQuoted:
                    if (c == '\"') { //"...""..." -> escaped quote
                        reader.nextChar(c);
                        state = LoadState.QuotedField;
                    } else if (c == delimiter) {
                        reader.nextColumn();
                        ++cols;
                        state = LoadState.Init;
                    } else if (c == '\r') { //CRLF skip
                        continue;
                    } else if (c == '\n') { //empty column
                        break outer;
                    } else {
                        reader.nextChar(c);
                        if (Character.isSurrogate(c)) {
                            reader.nextChar(charIterator.nextSurrogate());
                        }
                        state = LoadState.Field;
                    }
                    break;
            }
        }
        if (cols > maxColumns) {
            maxColumns = cols;
        }

        reader.endLine();
    }

    @Override
    public void close() {
        charIterator.close();
    }

    @Override
    public Iterator<CsvLine> iterator() {
        return new CsvLineIteratorLookAhead(this);
    }

    public Stream<CsvLine> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    public static class CsvLineIterator implements Iterator<CsvLine> {
        protected CsvReader mapper;
        protected long index;
        protected boolean hasNext;

        public CsvLineIterator(CsvReader mapper) {
            this(mapper, 0);
        }

        public CsvLineIterator(CsvReader mapper, long index) {
            this.mapper = mapper;
            this.index = index;
            hasNext = mapper.setIndex(index);
            if (!hasNext) {
                end();
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public CsvLine next() {
            CsvLine line = mapper.readNext(index);
            hasNext = !line.isEnd();
            if (!hasNext) {
                end();
            }
            index = line.end;
            return line;
        }
        public void end() {
        }
    }

    public static class CsvLineIteratorLookAhead extends CsvLineIterator {
        protected CsvLine next;

        public CsvLineIteratorLookAhead(CsvReader mapper) {
            super(mapper);
            readNext();
        }

        public CsvLineIteratorLookAhead(CsvReader mapper, long index) {
            super(mapper, index);
            readNext();
        }

        protected void readNext() {
            if (hasNext) {
                next = super.next();
            } else {
                next = null;
            }
        }

        @Override
        public CsvLine next() {
            CsvLine next = this.next;
            readNext();
            return next;
        }
    }

    public Iterable<long[]> ranges() {
        return () -> new CsvRangeIterator(this);
    }

    public static class CsvRangeIterator implements Iterator<long[]> {
        protected CsvReader mapper;
        protected long index;
        protected boolean hasNext;

        public CsvRangeIterator(CsvReader mapper) {
            this.mapper = mapper;
            index = 0;
            hasNext = mapper.setIndex(0);
            if (!hasNext) {
                end();
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public long[] next() {
            mapper.setIndex(index);
            long[] range = mapper.scanNext();
            index = range[1];
            hasNext = (range[0] < index);
            if (!hasNext) {
                end();
            }
            return range;
        }

        public void end() {
        }
    }

    @Override
    public CsvReader withDefaultDataEmptyColumns(int minColumns) {
        return (CsvReader) super.withDefaultDataEmptyColumns(minColumns);
    }

    @Override
    public CsvReader withDefaultDataColumns(int minColumns, String defaultValue) {
        return (CsvReader) super.withDefaultDataColumns(minColumns, defaultValue);
    }

    @Override
    public CsvReader withDefaultData(String... defaultData) {
        return (CsvReader) super.withDefaultData(defaultData);
    }

    @Override
    public CsvReader withDefaultData(List<String> defaultData) {
        return (CsvReader) super.withDefaultData(defaultData);
    }

    @Override
    public CsvReader withTrimNonEscape(boolean trimNonEscape) {
        return (CsvReader) super.withTrimNonEscape(trimNonEscape);
    }

    @Override
    public CsvReader withDelimiter(char delimiter) {
        return (CsvReader) super.withDelimiter(delimiter);
    }

    @Override
    public CsvReader withDelimiterTab() {
        return (CsvReader) super.withDelimiterTab();
    }

    @Override
    public CsvReader withMaxColumns(int maxColumns) {
        return (CsvReader) super.withMaxColumns(maxColumns);
    }

    @Override
    public CsvReader withMaxColumnWidth(int maxColumnWidth) {
        return (CsvReader) super.withMaxColumnWidth(maxColumnWidth);
    }

    @Override
    public CsvReader setFields(CsvIO other) {
        return (CsvReader) super.setFields(other);
    }

    public List<List<String>> readAllData() {
        List<List<String>> rows = new ArrayList<>();
        for (CsvLine line : this) {
            if (!line.isEmptyLine() && !line.isEnd()) {
                rows.add(line.getData());
            }
        }
        close();
        return rows;
    }

    public List<List<Object>> readAllDataObject() {
        List<List<Object>> rows = new ArrayList<>();
        for (CsvLine line : this) {
            if (!line.isEmptyLine() && !line.isEnd()) {
                rows.add(line.toListObject());
            }
        }
        close();
        return rows;
    }
}
