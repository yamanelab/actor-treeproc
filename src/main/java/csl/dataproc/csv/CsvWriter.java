package csl.dataproc.csv;

import csl.dataproc.csv.impl.CsvCharOutput;
import csl.dataproc.tgls.util.ArraysAsList;
import csl.dataproc.tgls.util.ByteBufferArray;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

/**
 * A CSV writer
 * <pre>
 *     new CsvWriter().withOutputFile(file)
 *            .withDefaultDataColumns(3, "0")
 *            .writeLine("x", "y", "z")
 *            .save(lines)
 *            .close();
 * </pre>
 */
public class CsvWriter extends CsvIO implements Closeable {
    protected CsvCharOutput output;

    public static int bufferCapacity = 1024 * 1024;

    public CsvWriter withOutputFile(Path file) {
        return withOutputFile(file, StandardCharsets.UTF_8);
    }

    public CsvWriter withOutputFile(Path file, Charset charset) {
        return withOutputFile(file, charset, ByteBuffer.allocateDirect(bufferCapacity));
    }

    public CsvWriter withOutputFile(Path file, Charset charset, ByteBuffer buffer) {
        try {
            return withOutputChannel(FileChannel.open(file,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING),
                    charset, buffer);
        } catch (Exception ex) {
            error(ex);
            return null;
        }
    }

    public void error(Exception ex) {
        throw new RuntimeException(ex);
    }

    public CsvWriter withOutputStream(OutputStream stream) {
        return withOutputChannel(Channels.newChannel(stream));
    }

    public CsvWriter withOutputStream(OutputStream stream, Charset charset) {
        return withOutputChannel(Channels.newChannel(stream), charset);
    }

    public CsvWriter withOutputStream(OutputStream stream, Charset charset, ByteBuffer buffer) {
        return withOutputChannel(Channels.newChannel(stream), charset, buffer);
    }

    public CsvWriter withOutputChannel(WritableByteChannel channel) {
        return withOutputChannel(channel, StandardCharsets.UTF_8);
    }

    public CsvWriter withOutputChannel(WritableByteChannel channel, Charset charset) {
        return withOutputChannel(channel, charset, ByteBuffer.allocateDirect(bufferCapacity));
    }

    public CsvWriter withOutputChannel(WritableByteChannel channel, Charset charset, ByteBuffer buffer) {
        setOutput(CsvCharOutput.get(channel, charset, buffer));
        return this;
    }

    public CsvWriter withOutputWriter(Writer writer) {
        setOutput(CsvCharOutput.get(writer));
        return this;
    }

    public CsvWriter withOutputBufferArray(ByteBufferArray array) {
        return withOutputBufferArray(array, StandardCharsets.UTF_8);
    }

    public CsvWriter withOutputBufferArray(ByteBufferArray array, Charset charset) {
        setOutput(CsvCharOutput.get(array, charset));
        return this;
    }

    public void setOutput(CsvCharOutput output) {
        this.output = output;
    }

    public CsvWriter save(Iterable<CsvLine> lines) {
        for (CsvLine line : lines) {
            writeLine(line);
        }
        return this;
    }

    public CsvWriter save(Stream<CsvLine> lines) {
        lines.forEachOrdered(this::writeLine);
        return this;
    }

    public CsvWriter save(CsvLine... lines) {
        for (CsvLine line : lines) {
            writeLine(line);
        }
        return this;
    }

    public CsvWriter saveStrings(Iterable<? extends Iterable<String>> lines) {
        for (Iterable<String> line : lines) {
            writeLine(line);
        }
        return this;
    }

    public CsvWriter writeLine(CsvLine line) {
        return writeLine(line.getData());
    }

    public CsvWriter writeLine(String... columns) {
        return writeLine(ArraysAsList.get(columns));
    }

    public CsvWriter writeLine(Iterable<String> columns) {
        char delimiter = this.delimiter;
        int size = defaultData.size();
        try {
            int i = 0;
            for (String col : columns) {
                if (i > 0) {
                    output.write(delimiter);
                }
                if (i < size && col.isEmpty()) {
                    col = defaultData.get(i);
                }
                write(col);
                ++i;
            }
            for (; i < size; ++i) {
                if (i > 0) {
                    output.write(delimiter);
                }
                write(defaultData.get(i));
            }
            output.write('\n');
        } catch (Exception ex) {
            error(ex);
        }
        return this;
    }

    protected void write(String s) throws IOException {
        int q = quoted(s, delimiter);
        if (q == 0) {
            output.write('\"');
            output.write(s);
            output.write('\"');
        } else if (q > 0) {
            CharBuffer buf = CharBuffer.allocate(s.length() + q + 2);
            buf.put('\"');
            for (int i = 0, l = s.length(); i < l; ++i) {
                char c = s.charAt(i);
                if (c == '\"') {
                    buf.put('\"');
                }
                buf.put(c);
            }
            buf.put('\"');
            buf.flip();
            output.write(buf);
        } else {
            output.write(s);
        }
    }

    /** @return -1: not quoted, 0: quoted but no escape, 1 &gt; : quoted with escapes */
    public static int quoted(String s, char delimiter) {
        int r = -1;
        for (int i = 0, l = s.length(); i < l; ++i) {
            char c = s.charAt(i);
            if (((i == 0 || i == l - 1 ) && Character.isWhitespace(c)) ||
                c == '\n' || c == '\r' || c == delimiter) {
                if (r < 0) {
                    r = 0;
                }
            } else if (c == '\"') {
                if (r < 0) {
                    r = 0;
                }
                r++;
                break;
            }
        }
        return r;
    }

    public void close() {
        try {
            output.close();
        } catch (Exception ex) {
            error(ex);
        }
    }


    @Override
    public CsvWriter withDefaultDataEmptyColumns(int minColumns) {
        return (CsvWriter) super.withDefaultDataEmptyColumns(minColumns);
    }

    @Override
    public CsvWriter withDefaultDataColumns(int minColumns, String defaultValue) {
        return (CsvWriter) super.withDefaultDataColumns(minColumns, defaultValue);
    }

    @Override
    public CsvWriter withDefaultData(String... defaultData) {
        return (CsvWriter) super.withDefaultData(defaultData);
    }

    @Override
    public CsvWriter withDefaultData(List<String> defaultData) {
        return (CsvWriter) super.withDefaultData(defaultData);
    }

    @Override
    public CsvWriter withTrimNonEscape(boolean trimNonEscape) {
        return (CsvWriter) super.withTrimNonEscape(trimNonEscape);
    }

    @Override
    public CsvWriter withDelimiter(char delimiter) {
        return (CsvWriter) super.withDelimiter(delimiter);
    }

    @Override
    public CsvWriter withDelimiterTab() {
        return (CsvWriter) super.withDelimiterTab();
    }

    @Override
    public CsvWriter withMaxColumns(int maxColumns) {
        return (CsvWriter) super.withMaxColumns(maxColumns);
    }

    @Override
    public CsvWriter withMaxColumnWidth(int maxColumnWidth) {
        return (CsvWriter) super.withMaxColumnWidth(maxColumnWidth);
    }

    @Override
    public CsvWriter setFields(CsvIO other) {
        return (CsvWriter) super.setFields(other);
    }
}
