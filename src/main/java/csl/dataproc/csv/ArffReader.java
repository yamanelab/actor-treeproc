package csl.dataproc.csv;

import csl.dataproc.csv.arff.*;
import csl.dataproc.csv.impl.CsvCharIterator;
import csl.dataproc.tgls.util.ByteBufferArray;

import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * <a href="http://weka.wikispaces.com/ARFF+%28stable+version%29">ARFF (stable version)</a>
 *
 * Note: some arff files used undocumented "range" extension:
 * <pre>
 *     &#64;attribute 'values' int [1..10]
 * </pre>
 *  Moreover, it may allow "exclusive" and "inclusive" description with unbalanced parenthesis i.e. "[1..10)".
 *   Currently, the class does not support such description, but can skip.
 *
 */
public class ArffReader implements CsvReaderI<ArffLine> {
    protected ArffHeader header;
    protected CsvCharIterator charIterator;


    public static ArffReader get(Path file) {
        return get(ByteBufferArray.createForRead(file));
    }

    public static ArffReader get(Path file, Charset charset) {
        return get(ByteBufferArray.createForRead(file), charset);
    }

    public static ArffReader get(ByteBufferArray array) {
        return get(array, StandardCharsets.UTF_8);
    }

    public static ArffReader get(ByteBufferArray array, Charset charset) {
        return new ArffReader(CsvCharIterator.get(array, charset));
    }

    public static ArffReader get(Reader reader) {
        return new ArffReader(CsvCharIterator.get(reader));
    }

    public ArffReader(CsvCharIterator charIterator) {
        this.charIterator = charIterator;
    }

    public long[] scanNext() {
        CsvLine line = readNext();
        return new long[] {line.getStart(), line.getEnd()};
    }

    public boolean setIndex(long index) {
        if (header == null) {
            charIterator.setIndex(index);
        } else if (index == 0 && header != null) {
            charIterator.setIndex(header.getDataIndex());
        }
        return charIterator.hasNext();
    }

    public ArffLine readNext(long index) {
        setIndex(index);
        return readNext();
    }

    public ArffLine readNext() {
        long i = charIterator.getIndex();
        if (i == 0) {
            if (header == null) {
                header = readHeader();
            } else {
                setIndex(Math.max(0, header.getDataIndex()));
            }
        }
        return processLine();
    }

    public ArffHeader getHeader() {
        if (header == null) {
            header = readHeader();
        }
        return header;
    }

    public List<String> readColumns(long from) {
        setIndex(from);
        return readNext().getData();
    }

    public ArffLine processLine() {
        int c = skipCommentOrSpaces(nextChar());

        long start = charIterator.getIndex();
        ArffLine line = null;
        String weight = null;
        if (c == '{') {
            ArrayList<CsvSparseColumn> columns = new ArrayList<>();
            c = skipCommentOrSpaces(nextChar());
            while (c != '}' && c != -1) {
                ParseResult<String> index = nextString(c);
                c = skipCommentOrSpaces(index.getNextChar());
                ParseResult<String> value = nextString(c);
                c = skipCommentOrSpaces(value.getNextChar());
                if (c == ',') {
                    c = skipCommentOrSpaces(nextChar());
                }

                columns.add(new CsvSparseColumn(Integer.valueOf(index.getValue()),
                        value.getValue()));
            }
            long end = charIterator.getIndex();

            c = skipCommentOrSpacesExceptNewline(nextChar());


            if (c == ',') {
                c = skipCommentOrSpacesExceptNewline(nextChar());
                if (c == '{') {
                    c = skipCommentOrSpaces(c);
                    ParseResult<String> w = nextString(c);
                    weight = w.getValue();
                    c = skipCommentOrSpaces(w.getNextChar());
                    if (c == '}') {
                        c = skipCommentOrSpacesExceptNewline(nextChar());
                        end = charIterator.getIndex();
                    }
                }
            }
            columns.trimToSize();
            line = new ArffSparseLine(start, end, columns);
            if (weight != null) {
                line.setWeight(weight);
            }
        } else {
            ArrayList<String> columns = new ArrayList<>();
            long end = -1;
            while (c != -1 && c != '\n') {
                ParseResult<String> col = nextString(c);
                columns.add(col.getValue());
                c = skipCommentOrSpacesExceptNewline(col.getNextChar());
                if (c == ',') {
                    c = skipCommentOrSpacesExceptNewline(nextChar());
                    end = charIterator.getIndex();
                    if (c == ',') {
                        c = skipCommentOrSpacesExceptNewline(nextChar());
                        if (c == '{') {
                            c = skipCommentOrSpaces(c);
                            ParseResult<String> w = nextString(c);
                            weight = w.getValue();
                            c = skipCommentOrSpaces(w.getNextChar());
                            end = charIterator.getIndex();
                            if (c == '}') {
                                //nothing
                            }
                            break;
                        }
                    }
                } else {
                    end = charIterator.getIndex();
                    break;
                }
            }
            columns.trimToSize();
            line = new ArffLine(start, end, columns, columns.isEmpty()); //TODO with defaultData
            if (weight != null) {
                line.setWeight(weight);
            }
        }

        return line;
    }


    public ArffHeader readHeader() {
        int c = skipCommentOrSpaces(-1);
        String name = null;
        long dataIndex = -1;
        ArrayList<ArffAttribute> attributes = new ArrayList<>();
        outer: while (c != -1) {
            switch (c) {
                case '@':
                    ParseResult<String> token = nextToken(-1);
                    c = token.getNextChar();
                    switch (token.getValue().toLowerCase()) {
                        case "relation": {
                            c = skipCommentOrSpaces(c);
                            ParseResult<String> nameToken = nextString(c);
                            name = nameToken.getValue();
                            c = nameToken.getNextChar();
                            break;
                        }
                        case "attribute": {
                            c = skipCommentOrSpaces(c);

                            ParseResult<ArffAttribute> attr = nextAttribute(c);
                            attributes.add(attr.getValue());
                            c = attr.getNextChar();

                            break;
                        }
                        case "data":
                            dataIndex = charIterator.getIndex();
                            break outer;
                    }
                    break;
                default:
                    c = nextChar(); //error?
                    break;
            }
            c = skipCommentOrSpaces(c);
        }
        attributes.trimToSize();
        return new ArffHeader(name, attributes, dataIndex);
    }

    public int skipCommentOrSpaces(int c) {
        if (c == -1) {
            c = nextChar();
        }
        outer: while (c != -1) {
            switch (c) {
                case '%':
                    c = nextComment(c).getNextChar();
                    break;
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                case '\f':
                    c = nextSpaces(c).getNextChar();
                    break;
                default:
                    break outer;
            }
        }
        return c;
    }

    public int skipCommentOrSpacesExceptNewline(int c) {
        if (c == -1) {
            c = nextChar();
        }
        outer: while (c != -1) {
            switch (c) {
                case '%':
                    c = nextComment(c).getNextChar();
                    break outer; //exit by %... newline
                case ' ':
                case '\t':
                case '\r':
                //case '\n':  //exit by a newline
                case '\f':
                    c = nextSpaces(c).getNextChar();
                    break;
                default:
                    break outer;
            }
        }
        return c;
    }

    public ParseResult<String> nextToken(int c) {
        StringBuilder buf = new StringBuilder();
        if (c == -1) {
            c = nextChar();
        }
        outer: while (c != -1) {
            switch (c) {
                case '{':
                case '}':
                case '\'':
                case '"':
                case ',':
                case '\f':
                case '\r':
                case '\t':
                case ' ':
                case '\n':
                //case '%': % comment cannot start internal of a header part
                    break outer;
                case '\\':
                    c = nextChar();
                    if (c != -1) {//escape sequence
                        buf.append((char) c);
                    }
                    break;
                default:
                    buf.append((char) c);
                    break;
            }
            c = nextChar();
        }
        return new ParseResult<>(buf.toString(), c);
    }

    public ParseResult<String> nextString(int c) {
        if (c == '\'' || c == '\"') {
            return nextQuotedString(c);
        } else {
            return nextToken(c);
        }
    }

    public ParseResult<String> nextQuotedString(int start) {
        StringBuilder buf = new StringBuilder();
        int c = nextChar();
        while (c != -1) {
            //no escape sequence
            if (c == start) {
                c = nextChar();
                break;
            } else {
                buf.append((char) c);
                c = nextChar();
            }
        }
        return new ParseResult<>(buf.toString(), c);
    }

    public ParseResult<String> nextSpaces(int c) {
        StringBuilder buf = new StringBuilder();
        if (c == -1) {
            c = nextChar();
        }
        outer: while (c != -1) {
            switch (c) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                case '\f':
                    buf.append((char) c);
                    break;
                default:
                    break outer;
            }
            c = nextChar();
        }
        return new ParseResult<>(buf.toString(), c);
    }

    public ParseResult<String> nextComment(int start) {
        StringBuilder buf = new StringBuilder();
        int c = nextChar();
        buf.append((char) start);
        while (c != -1) {
            if (c == '\n') {
                buf.append((char) c);
                break;
            }
            c = nextChar();
        }
        return new ParseResult<>(buf.toString(), c);
    }

    public ParseResult<ArffAttributeType> nextType(int c) {
        if (c == -1) {
            c = nextChar();
        }
        if (c == '{') { //enum
            return new ParseResult<>(nextEnum(), -1);
        } else {
            ParseResult<String> token = nextToken(c);
            switch (token.getValue().toLowerCase()) {
                case "numeric":
                    return token.newValue(ArffAttributeType.SimpleAttributeType.Numeric);
                case "integer":
                    return token.newValue(ArffAttributeType.SimpleAttributeType.Integer);
                case "real":
                    return token.newValue(ArffAttributeType.SimpleAttributeType.Real);
                case "String":
                    return token.newValue(ArffAttributeType.SimpleAttributeType.String);
                case "date": {
                    c = skipCommentOrSpaces(token.getNextChar());
                    DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
                    if (c != '@') {
                        ParseResult<String> formatValue = nextString(c);
                        c = formatValue.getNextChar();
                        formatter = DateTimeFormatter.ofPattern(formatValue.getValue());
                    }
                    return new ParseResult<>(new ArffAttributeType.DateAttributeType(formatter), c);
                }
                case "relational":
                    c = skipCommentOrSpaces(token.getNextChar());
                    return nextRelationalType(c);
                default:
                    return token.newValue(ArffAttributeType.SimpleAttributeType.String); //error
            }
        }
    }

    public ParseResult<ArffAttributeType> nextRelationalType(int c) {
        ArrayList<ArffAttribute> attributes = new ArrayList<>();
        if (c == -1) {
            c = nextChar();
        }
        String attrName = "";
        outer: while (c != -1) {
            switch (c) {
                case '@':
                    ParseResult<String> token = nextToken(-1);
                    c = token.getNextChar();
                    switch (token.getValue().toLowerCase()) {
                        case "attribute": {
                            c = skipCommentOrSpaces(c);
                            ParseResult<ArffAttribute> attr = nextAttribute(c);
                            attributes.add(attr.getValue());
                            c = attr.getNextChar();
                            break;
                        }
                        case "end": {
                            c = token.getNextChar();
                            c = skipCommentOrSpaces(c);
                            ParseResult<String> name = nextString(c);
                            attrName = name.getValue();
                            c = name.getNextChar();
                            break outer;
                        }
                        default:
                            break outer;
                    }
                    break;
                default:
                    c = nextChar(); //skip
                    break;
            }
            c = skipCommentOrSpaces(c);
        }
        attributes.trimToSize();
        return new ParseResult<>(new ArffAttributeType.RelationalAttributeType(attrName, attributes), c);
    }

    public ParseResult<ArffAttribute> nextAttribute(int c) {
        ParseResult<String> name = nextString(c);
        c = name.getNextChar();

        c = skipCommentOrSpaces(c);
        ParseResult<ArffAttributeType> type = nextType(c);
        return type.newValue(new ArffAttribute(name.getValue(), type.getValue()));
    }

    public ArffAttributeType nextEnum() {
        ArrayList<String> values = new ArrayList<>();
        int c = skipCommentOrSpaces(nextChar());
        while (c != -1) {
            if (c == '}') {
                break;
            } else {
                ParseResult<String> str = nextString(c);
                if (!str.getValue().isEmpty()) {
                    values.add(str.getValue());
                }
                c = skipCommentOrSpaces(str.getNextChar());
                if (c == ',') {
                    c = nextChar();
                    c = skipCommentOrSpaces(c);
                }
            }
        }
        values.trimToSize();
        return new ArffAttributeType.EnumAttributeType(values);

    }

    static Pattern quotePat = Pattern.compile("[^a-zA-Z0-9]");

    public static String quoteString(String strValue) {
        if (quotePat.matcher(strValue).find()) {
            if (strValue.indexOf('\'') != -1) {
                //TODO how to handle ' and " in "..."
                return "\"" + strValue + "\"";
            } else {
                return "'" + strValue + "'";
            }
        } else {
            return strValue;
        }
    }

    protected int nextChar() {
        if (charIterator.hasNext()) {
            return charIterator.next();
        } else {
            return -1;
        }
    }

    public void close() {
        charIterator.close();
    }

    public static class ParseResult<Value> {
        protected Value value;
        protected int nextChar;

        public ParseResult(Value value, int nextChar) {
            this.value = value;
            this.nextChar = nextChar;
        }

        public Value getValue() {
            return value;
        }

        public int getNextChar() {
            return nextChar;
        }

        public <NewValueType> ParseResult<NewValueType> newValue(NewValueType newValue) {
            return new ParseResult<NewValueType>(newValue, nextChar);
        }

        @Override
        public String toString() {
            return "(" + value + "," + nextChar + "(" + (char) nextChar + "))";
        }
    }

    @Override
    public Iterator<ArffLine> iterator() {
        return new ArffLineIteratorLookAhead(this);
    }

    public Stream<ArffLine> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    public static class ArffLineIterator implements Iterator<ArffLine> {
        protected ArffReader mapper;
        protected long index;
        protected boolean hasNext;

        public ArffLineIterator(ArffReader mapper) {
            this(mapper, 0);
        }

        public ArffLineIterator(ArffReader mapper, long index) {
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
        public ArffLine next() {
            ArffLine line = mapper.readNext(index);
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

    public static class ArffLineIteratorLookAhead extends ArffLineIterator {
        protected ArffLine next;
        public ArffLineIteratorLookAhead(ArffReader mapper) {
            super(mapper);
            readNext();
        }

        public ArffLineIteratorLookAhead(ArffReader mapper, long index) {
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
        public ArffLine next() {
            ArffLine next = this.next;
            readNext();
            return next;
        }
    }

    public Iterable<long[]> ranges() {
        return () -> new ArffRangeIterator(this);
    }

    public static class ArffRangeIterator implements Iterator<long[]> {
        protected ArffReader mapper;
        protected long index;
        protected boolean hasNext;

        public ArffRangeIterator(ArffReader mapper) {
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
}
